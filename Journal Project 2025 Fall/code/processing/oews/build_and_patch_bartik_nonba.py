

from __future__ import annotations
import pathlib, io, zipfile, re
import polars as pl

BASE  = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
RAW   = BASE / "data_raw" / "oews_raw"
FINAL = BASE / "data_final"
PANEL = FINAL / "panel_cbsa_q.parquet"
OUT   = FINAL / "oews_bartik_nonba_cbsa_y.parquet"


NONBA_MAJOR = {"31","33","35","37","39","41","43","45","47","49","51","53"}

def find_source():
    all_data = RAW / "oe.data.1.AllData"
    if all_data.exists(): return ("all_data", all_data)
    zips = sorted(RAW.glob("oesm*all.zip"))
    if zips: return ("excel", zips)
    raise FileNotFoundError(f"No OEWS sources found in {RAW}")

def choose_base_year(cb: pl.DataFrame) -> int:
    years = (cb.filter(pl.col("non_ba")==1)
               .select("year").drop_nulls().unique()
               .get_column("year").to_list())
    if not years:
        raise RuntimeError("No CBSA non-BA rows found; check areatype inference & occ_code parsing.")
    return 2019 if 2019 in years else max(years)


def load_series_all_data() -> pl.DataFrame:
    ser = pl.read_csv(RAW/"oe.series", separator="\t", infer_schema_length=200_000)
    m = {c.lower(): c for c in ser.columns}
    return ser.select([
        pl.col(m.get("series_id","series_id")).cast(pl.Utf8).alias("series_id"),
        pl.col(m.get("series_title","series_title")).cast(pl.Utf8).alias("series_title"),
        pl.col(m.get("areatype_code","areatype_code")).cast(pl.Utf8).alias("areatype_code"),
        pl.col(m.get("area_code","area_code")).cast(pl.Utf8).alias("area_code"),
        pl.col(m.get("occ_code","occ_code")).cast(pl.Utf8).alias("occ_code"),
        pl.col(m.get("datatype_code","datatype_code")).cast(pl.Utf8).alias("datatype_code"),
    ])

def build_from_all_data(path: pathlib.Path) -> pl.DataFrame:
    ser = load_series_all_data()
    dat = pl.read_csv(path, separator="\t", infer_schema_length=200_000)
    m = {c.lower(): c for c in dat.columns}
    dat = dat.select([
        pl.col(m.get("series_id","series_id")).cast(pl.Utf8).alias("series_id"),
        pl.col(m.get("year","year")).cast(pl.Int32).alias("year"),
        pl.col(m.get("value","value")).cast(pl.Float64).alias("value"),
        *( [pl.col(m["period"]).cast(pl.Utf8).alias("period")] if "period" in m else [] ),
    ])
    if "period" in dat.columns:
        dat = dat.filter(pl.col("period").str.contains("M13|A01", literal=False) | pl.col("period").is_null())

    emp = (ser.filter(
              (pl.col("datatype_code").is_in(["01","02","16","03","04"])) |
              (pl.col("series_title").str.contains("employment", case=False)))
           .select(["series_id","areatype_code","area_code","occ_code"]))

    df = (dat.join(emp, on="series_id", how="inner")
            .filter(pl.col("occ_code")!="00-0000")
            .with_columns(pl.col("occ_code").str.slice(0,2)
                          .is_in(list(NONBA_MAJOR)).cast(pl.Int8).alias("non_ba")))


    us = (df.filter(pl.col("areatype_code")=="N").filter(pl.col("non_ba")==1)
            .select(["occ_code","year","value"]).sort(["occ_code","year"])
            .with_columns(pl.col("value").shift(1).over("occ_code").alias("lag"))
            .with_columns(((pl.col("value")/pl.col("lag"))-1.0).alias("shock"))
            .filter(~pl.col("lag").is_null())
            .select(["occ_code","year","shock"]))

    cb = (df.filter(pl.col("areatype_code")=="M")
            .with_columns(pl.col("area_code").cast(pl.Utf8).str.zfill(5).alias("cbsa"))
            .select(["cbsa","occ_code","year","value"])
            .with_columns(pl.col("occ_code").str.slice(0,2)
                          .is_in(list(NONBA_MAJOR)).cast(pl.Int8).alias("non_ba")))

    base_year = choose_base_year(cb)
    base = cb.filter((pl.col("non_ba")==1) & (pl.col("year")==base_year))

    shares = (base.group_by(["cbsa","occ_code"]).agg(pl.col("value").sum().alias("emp_cbsa_occ"))
                   .join(base.group_by("cbsa").agg(pl.col("value").sum().alias("emp_cbsa_nonba")), on="cbsa")
                   .with_columns((pl.col("emp_cbsa_occ")/pl.col("emp_cbsa_nonba")).alias("share"))
                   .select(["cbsa","occ_code","share"]))

    return (shares.join(us, on="occ_code", how="inner")
                  .group_by(["cbsa","year"])
                  .agg((pl.col("share")*pl.col("shock")).sum().alias("bartik_nonba"))
                  .sort(["cbsa","year"]))


def read_xlsx_from_zip(zip_path: pathlib.Path):
    import pandas as pd
    with zipfile.ZipFile(zip_path, "r") as zf:
        xlsx = next((n for n in zf.namelist() if n.lower().endswith(".xlsx")), None)
        if not xlsx: raise RuntimeError(f"No .xlsx inside {zip_path.name}")
        with zf.open(xlsx) as fh:
            return pd.read_excel(io.BytesIO(fh.read()), sheet_name=0)

def parse_year_from_filename(zip_path: pathlib.Path) -> int:
    m = re.search(r'oesm(\d{2})', zip_path.name.lower())
    if not m: raise RuntimeError(f"Cannot parse year from {zip_path.name}")
    yy = int(m.group(1))
    return 2000+yy if yy <= 25 else 1900+yy

def build_from_excel(zips: list[pathlib.Path]) -> pl.DataFrame:
    import pandas as pd, numpy as np
    frames = []
    for zp in zips:
        df = read_xlsx_from_zip(zp)
        cols = {c.lower(): c for c in df.columns}
        area_type = cols.get("areatype_code") or cols.get("areatype") or cols.get("area_type_code") or cols.get("area_type")
        area_code = cols.get("area_code")     or cols.get("areacode")       or cols.get("area")
        occ_code  = cols.get("occ_code")      or cols.get("occupation_code") or cols.get("soc_code")
        emp_col   = cols.get("tot_emp")       or cols.get("employment")     or cols.get("emp")
        if not (area_code and occ_code and emp_col):
            raise RuntimeError(f"Missing required columns in {zp.name}. First 12 cols: {list(df.columns)[:12]}")

        ac_str = df[area_code].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        at_str = (df[area_type].astype(str).str.strip() if area_type in df.columns else pd.Series([np.nan]*len(df)))

        mini = pd.DataFrame({
            "areatype_code": at_str.replace({"nan": np.nan}),
            "area_code":     ac_str,
            "occ_code":      df[occ_code].astype(str).str.strip(),
            "value":         pd.to_numeric(df[emp_col], errors="coerce"),
            "year":          parse_year_from_filename(zp),
        })
        frames.append(mini)

    big = pl.from_pandas(pd.concat(frames, ignore_index=True))


    big = (big.filter(pl.col("occ_code").is_not_null() & pl.col("value").is_not_null())
              .filter(pl.col("occ_code") != "00-0000")
              .with_columns(
                  pl.col("area_code").cast(pl.Utf8).str.replace_all(r"[^0-9]", "").alias("area_digits"),
                  pl.col("occ_code").cast(pl.Utf8).alias("occ_code"),
                  pl.col("year").cast(pl.Int32).alias("year"),
              )
              .with_columns(
                  pl.when(pl.col("areatype_code").is_not_null())
                    .then(pl.col("areatype_code").cast(pl.Utf8))
                    .otherwise(
                        pl.when(pl.col("area_digits").str.len_bytes() >= 5).then(pl.lit("M"))
                         .when(pl.col("area_digits").str.len_bytes() == 2).then(pl.lit("S"))
                         .otherwise(pl.lit(None))
                    ).alias("areatype_code")
              )
              .with_columns(
                  pl.col("area_digits").str.zfill(5).alias("cbsa"),
                  pl.col("occ_code").str.slice(0,2).is_in(list(NONBA_MAJOR)).cast(pl.Int8).alias("non_ba")
              ))


    if big.filter(pl.col("areatype_code")=="S").height > 0:
        us_base = big.filter(pl.col("areatype_code")=="S")
    elif big.filter(pl.col("areatype_code")=="M").height > 0:
        us_base = big.filter(pl.col("areatype_code")=="M")
    else:
        us_base = big 

    us = (us_base.filter(pl.col("non_ba")==1)
                  .group_by(["occ_code","year"]).agg(pl.col("value").sum().alias("value"))
                  .sort(["occ_code","year"])
                  .with_columns(pl.col("value").shift(1).over("occ_code").alias("lag"))
                  .with_columns(((pl.col("value")/pl.col("lag"))-1.0).alias("shock"))
                  .filter(~pl.col("lag").is_null())
                  .select(["occ_code","year","shock"]))


    cb = (big.filter((pl.col("areatype_code")=="M") | (pl.col("area_digits").str.len_bytes() >= 5))
             .select(["cbsa","occ_code","year","value","non_ba"]))

    base_year = choose_base_year(cb)
    base = cb.filter((pl.col("non_ba")==1) & (pl.col("year")==base_year))

    shares = (base.group_by(["cbsa","occ_code"]).agg(pl.col("value").sum().alias("emp_cbsa_occ"))
                   .join(base.group_by("cbsa").agg(pl.col("value").sum().alias("emp_cbsa_nonba")), on="cbsa")
                   .with_columns((pl.col("emp_cbsa_occ")/pl.col("emp_cbsa_nonba")).alias("share"))
                   .select(["cbsa","occ_code","share"]))

    return (shares.join(us, on="occ_code", how="inner")
                  .group_by(["cbsa","year"])
                  .agg((pl.col("share")*pl.col("shock")).sum().alias("bartik_nonba"))
                  .sort(["cbsa","year"]))

def main():
    pl.Config.set_tbl_rows(8)
    kind, src = find_source()
    print(f"[INFO] Source: {kind} | {src if isinstance(src, pathlib.Path) else [p.name for p in src]}")
    bartik_nb = build_from_all_data(src) if kind=="all_data" else build_from_excel(src)
    if bartik_nb.height == 0:
        raise RuntimeError("Placebo Bartik result is empty.")

    bartik_nb.write_parquet(OUT)
    yrs = bartik_nb.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    cbsas = bartik_nb.select(pl.col("cbsa").n_unique()).item()
    print(f"[OK] Wrote {OUT} | rows={bartik_nb.height} | CBSAs={cbsas} | years={yrs}")


    panel = pl.read_parquet(PANEL).with_columns(
        pl.col("cbsa").cast(pl.Utf8).str.zfill(5).alias("cbsa"),
        pl.col("year").cast(pl.Int32).alias("year"),
    )
    out = panel.join(bartik_nb, on=["cbsa","year"], how="left")
    out.write_parquet(PANEL); out.write_csv(FINAL/"panel_cbsa_q.csv")
    miss = out.select(pl.col("bartik_nonba").is_null().sum()).item()
    print(f"[OK] Patched panel: shape={out.shape} | bartik_nonba null rows={miss}")

if __name__ == "__main__":
    main()
