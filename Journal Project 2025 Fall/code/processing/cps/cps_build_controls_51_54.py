

from __future__ import annotations
import io, re, zipfile, pathlib
import polars as pl


BASE    = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
RAW_CPS = BASE / "data_raw/cps_basic_monthly_raw"
XWALK   = BASE / "data_raw/xwalk/cbsa2fipsxw.csv"
FINAL   = BASE / "data_final"
PANEL   = FINAL / "panel_cbsa_q.parquet"

OUT_ALL   = FINAL / "cps_all_u_cbsa_q.parquet"
OUT_NONBA = FINAL / "cps_u_22_27_nonba_cbsa_q.parquet"
OUT_DEMO  = FINAL / "cps_ba_22_27_demo_cbsa_q.parquet"

def detect_col(df: pl.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols: return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def read_cps_any(path: pathlib.Path) -> tuple[pl.DataFrame, str]:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            name = next((n for n in zf.namelist() if n.lower().endswith((".csv", ".txt"))), None)
            if not name: raise ValueError(f"No CSV/TXT inside {path}")
            with zf.open(name) as fp:
                buf = io.BytesIO(fp.read())
            return pl.read_csv(buf, infer_schema_length=50000), name
    return pl.read_csv(path, infer_schema_length=50000), path.name

MONTH_MAP = {m:i for i,m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"], start=1)}

def infer_month_from_name(name: str) -> int | None:
    s = name.lower()
    for k,v in MONTH_MAP.items():
        if k in s: return v
    m = re.search(r'(?<!\d)(1[0-2]|0?[1-9])(?!\d)', s)
    return int(m.group(1)) if m else None

def load_crosswalk() -> pl.DataFrame:
                                                                                    
    x = pl.read_csv(XWALK, infer_schema_length=20000)
    low = {c.lower(): c for c in x.columns}


    cbsa_col = None
    for k in ["cbsa", "cbsacode", "cbsa_code", "cbsacd", "metropolitanstatisticalareacode"]:
        if k in low: cbsa_col = low[k]; break
    if cbsa_col is None:
        raise ValueError("CBSA code column not found in crosswalk.")


    if "fips" in low:
        return x.select(
            pl.col(low["fips"]).cast(pl.Utf8).str.zfill(5).alias("fips"),
            pl.col(cbsa_col).cast(pl.Utf8).str.zfill(5).alias("cbsa")
        ).unique()

    st = low.get("fipsstatecode") or low.get("statefp") or low.get("state_fips") or low.get("state")
    co = low.get("fipscountycode") or low.get("countyfp") or low.get("county_fips") or low.get("county")
    if not (st and co):
        raise ValueError("Crosswalk missing FIPS parts (state+county).")

    return (
        x.with_columns([
            pl.col(st).cast(pl.Utf8).str.zfill(2).alias("_st"),
            pl.col(co).cast(pl.Utf8).str.zfill(3).alias("_co"),
        ])
        .with_columns((pl.col("_st")+pl.col("_co")).alias("fips"))
        .select(pl.col("fips").cast(pl.Utf8).str.zfill(5),
                pl.col(cbsa_col).cast(pl.Utf8).str.zfill(5).alias("cbsa"))
        .unique()
    )

def make_flags(df: pl.DataFrame, fallback_year: int, src_name: str) -> pl.DataFrame:

    w_col = detect_col(df, ["PWCMPWGT","PWSSWGT","WTFINL","pwcmpwgt","pwsswgt","wtfinl"])
    if not w_col: raise ValueError("No CPS weight found (e.g., PWCMPWGT/WTFINL).")


    year_col = detect_col(df, ["year","YEAR","PERYEAR","HRYEAR4","hryear4"])
    mon_col  = detect_col(df, ["month","MONTH","PEMONTH","pemonth","HRMONTH","hrmonth"])
    out = df.with_columns([
        (pl.col(year_col).cast(pl.Int32) if year_col else pl.lit(int(fallback_year))).alias("year"),
        (pl.col(mon_col).cast(pl.Int32)  if mon_col  else pl.lit(infer_month_from_name(src_name))).alias("month"),
    ])
    if out.select(pl.col("month").is_null().sum()).item() > 0:
        raise ValueError(f"Could not infer month from file name: {src_name}")


    lfs_col = detect_col(df, ["PEMLR","pemlr","EMPSTAT","empstat"])
    if lfs_col and lfs_col.upper().startswith("PE"):
        lf = pl.col(lfs_col).cast(pl.Int32).is_in([1,2,3,4])
        ue = pl.col(lfs_col).cast(pl.Int32).is_in([3,4])
    else:
        lf = pl.col(lfs_col).cast(pl.Int32).is_in([10,12,20]) if lfs_col else pl.lit(None)
        ue = pl.col(lfs_col).cast(pl.Int32).is_in([20])       if lfs_col else pl.lit(None)


    age_col = detect_col(df, ["PRTAGE","peage","PEAGE","age","AGE"])

 
    educ_col = detect_col(df, ["PEEDUCA","peeduca","EDUC","educ"])
    if educ_col and educ_col.upper() == "PEEDUCA":
        is_ba = pl.col(educ_col).cast(pl.Int32).is_in([43,44,45,46])
    else:
        is_ba = (pl.col(educ_col).cast(pl.Int32) >= 111) if educ_col else pl.lit(None)


    sex_col  = detect_col(df, ["PESEX","pesex","SEX","sex"])
    hisp_col = detect_col(df, ["PRDTHSP","prdthsp","HISPAN","hispan"])
    race_col = detect_col(df, ["PTDTRACE","ptdtrace","RACE","race"])
    is_fem   = (pl.col(sex_col).cast(pl.Int32) == 2) if sex_col else None
    is_hisp  = (pl.col(hisp_col).cast(pl.Int32) == 1) if hisp_col else None
    is_white_nh = ((pl.col(race_col).cast(pl.Int32) == 1) & (~is_hisp)) if race_col and is_hisp is not None else None
    is_black_nh = ((pl.col(race_col).cast(pl.Int32) == 2) & (~is_hisp)) if race_col and is_hisp is not None else None

    return out.with_columns([
        pl.col(w_col).cast(pl.Float64).alias("w"),
        (pl.col(age_col).cast(pl.Int32) if age_col else pl.lit(None)).alias("age"),
        is_ba.alias("is_ba"),
        (is_fem).alias("is_female") if is_fem is not None else pl.lit(None).alias("is_female"),
        (is_white_nh).alias("is_white_nh") if is_white_nh is not None else pl.lit(None).alias("is_white_nh"),
        (is_black_nh).alias("is_black_nh") if is_black_nh is not None else pl.lit(None).alias("is_black_nh"),
        (is_hisp).alias("is_hisp") if is_hisp is not None else pl.lit(None).alias("is_hisp"),
        lf.alias("in_lf").cast(pl.Int8),
        ue.alias("is_ue").cast(pl.Int8),
    ])

def ensure_cbsa(df: pl.DataFrame, xwalk: pl.DataFrame) -> pl.DataFrame:

    cbsa_col = detect_col(df, ["gtcbsa","GTCBSA","GTCBSACD","cbsa","CBSA"])
    if cbsa_col:
        return df.with_columns(pl.col(cbsa_col).cast(pl.Utf8).str.zfill(5).alias("cbsa"))

    state_col  = detect_col(df, ["gestfips","GESTFIPS","statefips","STATEFIPS","hrstate"])
    county_col = detect_col(df, ["gtco","GTCO","countyfips","COUNTY","hrcounty"])
    if state_col and county_col:
        tmp = df.with_columns((pl.col(state_col).cast(pl.Utf8).str.zfill(2) +
                               pl.col(county_col).cast(pl.Utf8).str.zfill(3)).alias("fips"))
        return tmp.join(xwalk, on="fips", how="left").with_columns(pl.col("cbsa").cast(pl.Utf8).str.zfill(5))

    return df.with_columns(pl.lit(None).alias("cbsa"))

def q_from_month_expr(m: pl.Expr) -> pl.Expr:
    return ((m - 1)//3 + 1)


def main():
    xwalk = load_crosswalk()

    frames = []
    year_dirs = sorted([p for p in RAW_CPS.iterdir() if p.is_dir() and p.name.isdigit()])
    if not year_dirs:
        raise RuntimeError(f"No CPS year folders under {RAW_CPS}")

    for ydir in year_dirs:
        fallback_year = int(ydir.name)
        files = sorted(list(ydir.glob("*.zip")) + list(ydir.glob("*.csv")) + list(ydir.glob("*.txt")))
        for f in files:
            try:
                m, src_name = read_cps_any(f)
            except Exception:
                continue
            m = make_flags(m, fallback_year=fallback_year, src_name=src_name)
            m = ensure_cbsa(m, xwalk)
            frames.append(m.select(["year","month","cbsa","w","age","is_ba","is_female","is_white_nh","is_black_nh","is_hisp","in_lf","is_ue"]))

    if not frames:
        raise RuntimeError("No CPS micro files readable. Check formats/paths.")
    cps = pl.concat(frames, how="vertical").filter(~pl.col("cbsa").is_null())
    cps = cps.with_columns(q_from_month_expr(pl.col("month")).alias("quarter"))


    all_u = (
        cps.group_by(["cbsa","year","quarter"])
           .agg([
               ((pl.col("is_ue")==1).cast(pl.Float64) * pl.col("w")).sum().alias("_num"),
               ((pl.col("in_lf")==1).cast(pl.Float64) * pl.col("w")).sum().alias("_den"),
           ])
           .with_columns(
               pl.when(pl.col("_den") > 0)
                 .then((pl.col("_num")/pl.col("_den")).cast(pl.Float64))
                 .otherwise(None)
                 .alias("u_all")
           )
           .select(["cbsa","year","quarter","u_all"])
           .sort(["cbsa","year","quarter"])
    )
    all_u.write_parquet(OUT_ALL)


    nonba = (
        cps.filter((pl.col("age")>=22) & (pl.col("age")<=27) & (~pl.col("is_ba")))
           .group_by(["cbsa","year","quarter"])
           .agg([
               ((pl.col("is_ue")==1).cast(pl.Float64) * pl.col("w")).sum().alias("_num"),
               ((pl.col("in_lf")==1).cast(pl.Float64) * pl.col("w")).sum().alias("_den"),
           ])
           .with_columns(
               pl.when(pl.col("_den") > 0)
                 .then((pl.col("_num")/pl.col("_den")).cast(pl.Float64))
                 .otherwise(None)
                 .alias("u_22_27_nonba")
           )
           .select(["cbsa","year","quarter","u_22_27_nonba"])
           .sort(["cbsa","year","quarter"])
    )
    nonba.write_parquet(OUT_NONBA)


    ba_grp = cps.filter((pl.col("age")>=22) & (pl.col("age")<=27) & (pl.col("is_ba")))
    demo = (
        ba_grp.group_by(["cbsa","year","quarter"])
              .agg([
                  ((pl.col("is_female")==1)  .cast(pl.Float64)*pl.col("w")).sum().alias("_sf"),
                  ((pl.col("is_white_nh")==1).cast(pl.Float64)*pl.col("w")).sum().alias("_swh"),
                  ((pl.col("is_black_nh")==1).cast(pl.Float64)*pl.col("w")).sum().alias("_sbl"),
                  ((pl.col("is_hisp")==1)    .cast(pl.Float64)*pl.col("w")).sum().alias("_sh"),
                  pl.col("w").sum().alias("_w")
              ])
              .with_columns([
                  pl.when(pl.col("_w")>0).then(pl.col("_sf") / pl.col("_w")).otherwise(None).alias("share_female_22_27_ba"),
                  pl.when(pl.col("_w")>0).then(pl.col("_swh")/ pl.col("_w")).otherwise(None).alias("share_white_nh_22_27_ba"),
                  pl.when(pl.col("_w")>0).then(pl.col("_sbl")/ pl.col("_w")).otherwise(None).alias("share_black_nh_22_27_ba"),
                  pl.when(pl.col("_w")>0).then(pl.col("_sh") / pl.col("_w")).otherwise(None).alias("share_hisp_22_27_ba"),
              ])
              .select(["cbsa","year","quarter",
                       "share_female_22_27_ba","share_white_nh_22_27_ba",
                       "share_black_nh_22_27_ba","share_hisp_22_27_ba"])
              .sort(["cbsa","year","quarter"])
    )
    demo.write_parquet(OUT_DEMO)


    panel = pl.read_parquet(PANEL)
    panel2 = (panel.join(all_u, on=["cbsa","year","quarter"], how="left")
                   .join(nonba, on=["cbsa","year","quarter"], how="left")
                   .join(demo,  on=["cbsa","year","quarter"], how="left"))
    panel2.write_parquet(PANEL)
    panel2.write_csv(FINAL/"panel_cbsa_q.csv")

    print("[OK] Added columns:",
          [c for c in ["u_all","u_22_27_nonba",
                       "share_female_22_27_ba","share_white_nh_22_27_ba",
                       "share_black_nh_22_27_ba","share_hisp_22_27_ba"]
           if c in panel2.columns])
    print("Rows unchanged:", panel2.height == panel.height)

if __name__ == "__main__":
    pl.Config.set_fmt_str_lengths(200)
    main()
