                                                                        

import sys, re, zipfile, pathlib
from typing import Optional, Tuple, List
import pandas as pd
import polars as pl

CODE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(CODE_DIR) not in sys.path:
    sys.path.append(str(CODE_DIR))

from _utils.engine import ensure_dir, read_parquet_smart, to_polars, write_parquet

BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW  = BASE / "data_raw"
OUT  = BASE / "data_final"
ensure_dir(OUT)

CROSSWALK = OUT / "crosswalk_cbsa.parquet"
SRC_BASE  = RAW / "ipeds_completions_raw"

COUNT_CANDIDATES = [
    "ctotalt","ctotal","ctotal1","ctotal2","ctotalm","ctotalf",
    "awards","awards_total","completions","completions_count","total","number"
]
AW_TEXT = ["credential_level","creddesc","awardlevel_desc","awlevel_desc","award_level_text"]
AW_CODE = ["awlevel","award_level","credlev","cred_level","cahrdegr","awardlevelcode"]

STATE_ABBR_TO_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10","DC":"11",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20","KY":"21",
    "LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30",
    "NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","OH":"39",
    "OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46","TN":"47","TX":"48","UT":"49",
    "VT":"50","VA":"51","WA":"53","WV":"54","WI":"55","WY":"56","PR":"72"
}


def year_from_path(p: pathlib.Path) -> Optional[int]:
    m = re.search(r"(20\d{2})", str(p))
    return int(m.group(1)) if m else None

def read_member_with_fallback(zf: zipfile.ZipFile, name: str) -> Optional[pd.DataFrame]:
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            with zf.open(name) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False, encoding=enc)
            return df
        except UnicodeDecodeError:
            continue
        except Exception:
            continue
    return None

def read_zip_csv(zpath: pathlib.Path, filter_fn=None) -> List[Tuple[pd.DataFrame,str]]:
    out = []
    with zipfile.ZipFile(zpath, "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            if filter_fn and not filter_fn(name):
                continue
            df = read_member_with_fallback(zf, name)
            if df is None: 
                continue
            df.columns = [c.lower() for c in df.columns]
            out.append((df, name))
    return out

def pick_first(cols, cand):
    for c in cand:
        if c in cols: 
            return c
    return None

def bachelors_mask_pl(df: pl.DataFrame, cols_text: List[str], cols_code: List[str]) -> pl.Series:
    tests = []
    for t in cols_text:
        tests.append(pl.col(t).cast(pl.Utf8).str.to_lowercase().str.contains("bachelor"))
    for c in cols_code:
        tests.append(pl.col(c).cast(pl.Int64, strict=False).is_in([5,3,13,23]))
    if not tests: 
        return pl.lit(True)
    cond = tests[0]
    for t in tests[1:]:
        cond = cond | t
    return cond.fill_null(False)


def load_completions_from_C_zip(czip: pathlib.Path, year_hint: int) -> Optional[pl.DataFrame]:
                                                                                         
    members = read_zip_csv(czip)  
    for pdf, _ in members:
        cols = set(pdf.columns)
        if "unitid" not in cols: 
            continue
        count_col = pick_first(cols, COUNT_CANDIDATES)
        if not count_col: 
            continue
        aw_text = [c for c in AW_TEXT if c in cols]
        aw_code = [c for c in AW_CODE if c in cols]

        df = pl.from_pandas(pdf)
        
        df = df.with_columns(pl.lit(int(year_hint), dtype=pl.Int64).alias("_year"))

        df = df.filter(bachelors_mask_pl(df, aw_text, aw_code))
        df = (df.select([
                pl.col("unitid").cast(pl.Utf8),
                pl.col("_year"),
                pl.col(count_col).alias("_n")
             ])
             .with_columns(pl.col("_n").cast(pl.Int64, strict=False))
             .filter(pl.col("_n").is_not_null() & (pl.col("_n") >= 0)))
        if df.height == 0: 
            continue
        return df.group_by(["unitid","_year"]).agg(pl.col("_n").sum().alias("_n"))
    return None


def build_unitid_to_geo_from_HD_zip(hdzip: pathlib.Path) -> Optional[pl.DataFrame]:
    members = read_zip_csv(hdzip)
    for pdf, _ in members:
        cols = set(pdf.columns)
        if "unitid" not in cols: 
            continue

        
        cbsa_col = pick_first(cols, ["cbsa","cbsacode","cbsacode10","cbsacode15","cbsa_code"])
        if cbsa_col:
            df = pl.from_pandas(pdf[["unitid", cbsa_col]])
            df = (df.with_columns(
                    pl.col("unitid").cast(pl.Utf8),
                    pl.col(cbsa_col).cast(pl.Utf8)
                        .str.replace_all(r"\D","")
                        .str.strip_chars()
                        .str.zfill(5)
                        .alias("cbsa")
                 )
                 .select(["unitid","cbsa"]))
            df = df.with_columns(pl.when(pl.col("cbsa")=="00000").then(None).otherwise(pl.col("cbsa")).alias("cbsa"))
            if df.height: 
                return df

        
        county5 = pick_first(cols, ["county_fips","fips county","fips_county","stco","fipsstco","stcountyfp"])
        stfips  = pick_first(cols, ["stfips","fipsstate","state_fips","fips_state","fipsstatecode","statefp","fips"])
        county3 = pick_first(cols, ["countycd","county_code","county","fipscounty","fipscountycode","countyfp"])
        stabbr  = "stabbr" if "stabbr" in cols else None

        if county5:
            df = pl.from_pandas(pdf[["unitid", county5]])
            df = (df.with_columns(
                    pl.col("unitid").cast(pl.Utf8),
                    pl.col(county5).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(5).alias("county_fips")
                 ).select(["unitid","county_fips"]))
            return df

        if county3 and stfips:
            df = pl.from_pandas(pdf[["unitid", stfips, county3]])
            df = (df.with_columns(
                    pl.col("unitid").cast(pl.Utf8),
                    (pl.col(stfips).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(2) +
                     pl.col(county3).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(3)).alias("county_fips")
                 ).select(["unitid","county_fips"]))
            return df

        if county3 and stabbr:
            map_df = pl.DataFrame({"stabbr": list(STATE_ABBR_TO_FIPS.keys()),
                                   "stfips2": list(STATE_ABBR_TO_FIPS.values())})
            df = pl.from_pandas(pdf[["unitid","stabbr", county3]])
            df = (df.with_columns(
                    pl.col("unitid").cast(pl.Utf8),
                    pl.col("stabbr").cast(pl.Utf8).str.to_uppercase(),
                    pl.col(county3).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(3).alias("_c3")
                 )
                 .join(map_df, on="stabbr", how="left")
                 .with_columns((pl.col("stfips2")+pl.col("_c3")).alias("county_fips"))
                 .select(["unitid","county_fips"]))
            return df
    return None

def find_year_zips(ydir: pathlib.Path, year: int) -> Tuple[Optional[pathlib.Path], Optional[pathlib.Path]]:
    czip = (ydir / f"C{year}_A.zip")
    if not czip.exists():
        czip = next((p for p in sorted(ydir.glob("*.zip"))
                     if p.name.lower().startswith(f"c{year}_a") and "dict" not in p.name.lower()), None)
    hdzip = (ydir / f"HD{year}.zip")
    if not hdzip.exists():
        hdzip = next((p for p in sorted(ydir.glob("*.zip"))
                      if p.name.lower().startswith(f"hd{year}")), None)
    return czip, hdzip

def main():
    if not SRC_BASE.exists(): 
        raise SystemExit(f"Missing folder: {SRC_BASE}")
    if not CROSSWALK.exists(): 
        raise SystemExit("Missing crosswalk. Run xwalk_prep.py first.")

    xwalk = to_polars(read_parquet_smart(CROSSWALK)).select(["county_fips","cbsa"])

    year_dirs = [p for p in SRC_BASE.iterdir() if p.is_dir() and re.fullmatch(r"20\d{2}", p.name)]
    parts = []
    for ydir in sorted(year_dirs, key=lambda p: p.name):
        year = int(ydir.name)
        if year < 2017 or year > 2024: 
            continue

        czip, hdzip = find_year_zips(ydir, year)
        if not czip or not hdzip:
            print(f"[SKIP] {ydir} — missing C{year}_A.zip or HD{year}.zip")
            continue

        print(f"[YEAR {year}] {czip.name} + {hdzip.name}")
        unit_year = load_completions_from_C_zip(czip, year_hint=year)
        if unit_year is None or unit_year.height == 0:
            print(f"   [SKIP] completions not found with counts.")
            continue

        geo = build_unitid_to_geo_from_HD_zip(hdzip)
        if geo is None or geo.height == 0:
            print(f"   [SKIP] could not build UNITID->geo.")
            continue

        joined = unit_year.join(geo, on="unitid", how="left")
        if "cbsa" not in joined.columns:
            joined = joined.join(xwalk, on="county_fips", how="left")
        if "cbsa" in joined.columns:
            joined = joined.with_columns(pl.col("cbsa").cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(5).alias("cbsa"))

        part = (joined.filter(pl.col("cbsa").is_not_null())
                      .group_by(["cbsa","_year"])
                      .agg(pl.col("_n").sum().alias("flow_ba"))
                      .rename({"_year":"year"}))
        if part.height:
            parts.append(part)
            print(f"   [OK] rows={part.height}")

    if not parts:
        raise SystemExit("No usable IPEDS flows after scanning all years.")

    flows = pl.concat(parts, how="vertical_relaxed").group_by(["cbsa","year"]).agg(pl.col("flow_ba").sum())
    flows = flows.sort(["cbsa","year"]).with_columns([
        pl.col("flow_ba").shift(1).over("cbsa").alias("flow_ba_l1"),
        pl.col("flow_ba").shift(2).over("cbsa").alias("flow_ba_l2"),
    ])

    write_parquet(flows, OUT / "ipeds_ba_flow_cbsa_y.parquet")

    yrs = flows.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    print("--- SUMMARY ---")
    print(f"years={yrs} | unique_cbsa={flows.select(pl.col('cbsa').n_unique()).to_series().item()} | rows={flows.height}")
    print(f"Wrote: {OUT / 'ipeds_ba_flow_cbsa_y.parquet'}")

if __name__ == "__main__":
    main()
