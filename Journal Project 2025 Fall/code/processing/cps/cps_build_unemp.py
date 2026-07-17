                                                                      

import os, sys, glob, pathlib
import polars as pl

CODE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(CODE_DIR) not in sys.path:
    sys.path.append(str(CODE_DIR))

from _utils.engine import (
    read_csv_smart, to_polars, write_parquet, ensure_dir, read_parquet_smart
)

BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW = BASE / "data_raw"
OUT = BASE / "data_final"
ensure_dir(OUT)

CROSSWALK = OUT / "crosswalk_cbsa.parquet"

def pick(cols, names):
    for n in names:
        if n in cols:
            return n
    return None

def detect_columns(df: pl.DataFrame):
                                                         
    cols = set(df.columns)

    
    age = pick(cols, ["age", "prtage", "peage"])

    
    educ_num = pick(cols, ["educ","educn","educ_num","educode","educ_numeric","peeduca"])
    educ_text = pick(cols, ["education","educ_text","educdesc","educ_desc"])

    
    empstat_num = pick(cols, ["empstat"])     
    lfsr94      = pick(cols, ["lfsr94"])      
    pemlr       = pick(cols, ["pemlr"])       
    emp_text    = pick(cols, ["employmentstatus","emp_status","employment status","lfs"])


    wt = pick(cols, ["wtfinl","wtsupp","pwsswgt","pwwgt","pwcwgt","pspwgt"])


    year  = pick(cols, ["year","hryear4","peyear","year4"])
    month = pick(cols, ["month","hrmonth","pemonth"])


    cbsa_direct = pick(cols, ["gtcbsa","cbsa","cbsacode","cbsa_code"])
    state2 = pick(cols, ["statefip","gestfips","state_fips","fipsstate","statecode","fipsstatecode","stfips"])
    county3 = pick(cols, ["countyfips","gtco","county_code","cntyfips","fipscounty","fipscountycode","county"])

    return {
        "age": age, "educ_num": educ_num, "educ_text": educ_text,
        "empstat_num": empstat_num, "lfsr94": lfsr94, "pemlr": pemlr, "emp_text": emp_text,
        "wt": wt, "year": year, "month": month,
        "cbsa_direct": cbsa_direct, "state2": state2, "county3": county3
    }

def add_is_ba(df: pl.DataFrame, cols: dict) -> pl.DataFrame:
\
\
\
\
\
       
    conds = []
    if cols["educ_num"]:
        col = cols["educ_num"]
        if col == "peeduca":
            conds.append(pl.col(col).cast(pl.Int64, strict=False).ge(43))
        else:
            conds.append(pl.col(col).cast(pl.Int64, strict=False).ge(111))
    if cols["educ_text"]:
        conds.append(
            pl.col(cols["educ_text"]).cast(pl.Utf8).str.to_lowercase()
              .str.contains(r"bachelor|master|professional|doctor").fill_null(False)
        )
    if not conds:
        return df.with_columns(pl.lit(False).alias("_is_ba"))
    cond = conds[0]
    for c in conds[1:]:
        cond = cond | c
    return df.with_columns(cond.alias("_is_ba"))

def add_lf_un_flags(df: pl.DataFrame, cols: dict) -> pl.DataFrame:
    lf = pl.lit(None); un = pl.lit(None)
    if cols["empstat_num"]:
        code = pl.col(cols["empstat_num"]).cast(pl.Int64, strict=False)
        lf = code.lt(30); un = code.ge(20) & code.lt(30)
    elif cols["lfsr94"]:
        code = pl.col(cols["lfsr94"]).cast(pl.Int64, strict=False)
        lf = code.is_in([1,2]); un = code.eq(2)
    elif cols["pemlr"]:
        code = pl.col(cols["pemlr"]).cast(pl.Int64, strict=False)
        lf = code.is_in([1,2]); un = code.eq(2)
    elif cols["emp_text"]:
        txt = pl.col(cols["emp_text"]).cast(pl.Utf8).str.to_lowercase()
        un = txt.str.contains("unemp"); lf = txt.str.contains("unemp|employ")
    return df.with_columns(lf.alias("_lf").cast(pl.Boolean),
                           un.alias("_un").cast(pl.Boolean))

def choose_weight(df: pl.DataFrame, cols: dict) -> pl.DataFrame:
    w = pl.col(cols["wt"]).cast(pl.Float64, strict=False) if cols["wt"] else pl.lit(None)
    return df.with_columns(w.alias("_w"))

def attach_cbsa(df: pl.DataFrame, cols: dict, xwalk: pl.DataFrame) -> pl.DataFrame:
    if cols["cbsa_direct"]:
        cb = pl.col(cols["cbsa_direct"]).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(5)
        return df.with_columns(cb.alias("cbsa"))
    if cols["state2"] and cols["county3"]:
        st = pl.col(cols["state2"]).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(2)
        ct = pl.col(cols["county3"]).cast(pl.Utf8).str.replace_all(r"\D","").str.zfill(3)
        df = df.with_columns((st + ct).alias("county_fips"))
        return df.join(xwalk, on="county_fips", how="left")
    return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("cbsa"))

def qm_from_month(colname: str):
    return (pl.col(colname).cast(pl.Int64, strict=False).clip_min(1).clip_max(12) + 2) // 3

def find_cps_paths():
                                                              
    bases = []
    env = os.getenv("CPS_BASE", "")
    for token in env.split(os.pathsep):
        token = token.strip()
        if token:
            bases.append(pathlib.Path(token).expanduser())
    defaults = [RAW / "cps", RAW / "cps_basic_monthly_raw"]
    for d in defaults:
        if d not in bases:
            bases.append(d)
    paths = []
    for b in bases:
        if b.exists():
            paths += glob.glob(str(b / "**" / "*.zip"), recursive=True)
            paths += glob.glob(str(b / "**" / "*.csv"), recursive=True)
    return sorted(paths)

def main():
    if not CROSSWALK.exists():
        raise SystemExit("Missing crosswalk: data_final/crosswalk_cbsa.parquet. Run xwalk_prep.py first.")
    xwalk = read_parquet_smart(CROSSWALK)
    xwalk = to_polars(xwalk).select(["county_fips","cbsa"])

    paths = find_cps_paths()
    if not paths:
        raise SystemExit("No CPS files found. Set CPS_BASE to your folder with zips, e.g., $HOME/Journal_Project_2025/data_raw/cps_basic_monthly_raw")

    print(f"Found {len(paths)} CPS files to scan.")
    frames = []
    total_rows = total_geo_ok = total_w_ok = total_ba = 0

    for p in paths:
        df = read_csv_smart(p)   
        df = to_polars(df)
        total_rows += df.height

        cols = detect_columns(df)
        if not cols["year"] or not cols["month"] or not cols["age"]:
            print(f"[SKIP] {os.path.basename(p)} missing year/month/age")
            continue

        df = df.with_columns(
            pl.col(cols["year"]).cast(pl.Int64, strict=False).alias("_year"),
            pl.col(cols["month"]).cast(pl.Int64, strict=False).alias("_month"),
            pl.col(cols["age"]).cast(pl.Int64, strict=False).alias("_age"),
        ).filter(pl.col("_year").is_between(2019, 2024, closed="both"))

        df = add_is_ba(df, cols)
        df = add_lf_un_flags(df, cols)
        df = choose_weight(df, cols)

        
        df = df.filter(pl.col("_is_ba") == True)
        df = df.filter(pl.col("_age").is_between(22, 27, closed="both"))

        
        df = attach_cbsa(df, cols, xwalk)

        
        df = df.with_columns(
            pl.when(pl.col("_w").is_not_null() & (pl.col("_w") > 0)).then(pl.col("_w")).otherwise(None).alias("_w_pos"),
            pl.when(pl.col("_lf") == True).then(1).otherwise(0).alias("_lf_i"),
            pl.when(pl.col("_un") == True).then(1).otherwise(0).alias("_un_i"),
        )
        geo_ok = df.filter(pl.col("cbsa").is_not_null()).height
        w_ok   = df.filter(pl.col("_w_pos").is_not_null()).height
        ba_ct  = df.height
        total_geo_ok += geo_ok; total_w_ok += w_ok; total_ba += ba_ct

        df = df.filter(pl.col("cbsa").is_not_null() & pl.col("_w_pos").is_not_null() &
                       ((pl.col("_lf") == True) | (pl.col("_un") == True)))

        
        dfm = (df.group_by(["cbsa","_year","_month"])
                 .agg([
                    (pl.col("_w_pos") * pl.col("_lf_i")).sum().alias("lf_w"),
                    (pl.col("_w_pos") * pl.col("_un_i")).sum().alias("un_w"),
                 ])
                 .with_columns(pl.when(pl.col("lf_w") > 0).then(pl.col("un_w")/pl.col("lf_w"))
                               .otherwise(None).alias("u_22_27_ba_m"))
              )
        frames.append(dfm)
        print(f"[OK] {os.path.basename(p)} | rows_in={ba_ct} | geo_ok={geo_ok} | wt_ok={w_ok}")

    if not frames:
        raise SystemExit("No usable CPS files processed (after filtering).")

    monthly = pl.concat(frames, how="vertical_relaxed")

    
    out_q = (monthly
        .with_columns(q = (pl.col("_month") + 2) // 3)
        .group_by(["cbsa","_year","q"])
        .agg([
            pl.col("lf_w").sum().alias("lf_w"),
            pl.col("un_w").sum().alias("un_w"),
        ])
        .with_columns(pl.when(pl.col("lf_w") > 0).then(pl.col("un_w")/pl.col("lf_w"))
                      .otherwise(None).alias("u_22_27_ba"))
        .rename({"_year":"year","q":"quarter"})
        .select(["cbsa","year","quarter","u_22_27_ba","lf_w"])
        .sort(["cbsa","year","quarter"])
        .filter((pl.col("quarter").is_in([1,2,3,4])) & (pl.col("year").is_between(2019, 2024)))
    )

    
    write_parquet(out_q, OUT / "cps_u_22_27_ba_cbsa_q.parquet")

    print("--- SUMMARY ---")
    print(f"files_processed={len(paths)}")
    print(f"person_rows_2019_2024={total_rows:,}")
    print(f"after_age_BA_filter={total_ba:,}")
    print(f"geo_assignable_rows={total_geo_ok:,}")
    print(f"positive_weight_rows={total_w_ok:,}")
    print(f"quarters_written={out_q.height:,} (rows)")
    bad = out_q.filter((pl.col("u_22_27_ba") < 0) | (pl.col("u_22_27_ba") > 1)).height
    if bad > 0:
        print(f"WARNING: {bad} quarterly rates out of [0,1].")

if __name__ == "__main__":
    main()
