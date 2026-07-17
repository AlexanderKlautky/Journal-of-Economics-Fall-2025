
\
\
\
\
\
\
\
\
\
\
\
   

from __future__ import annotations
import pathlib, zipfile, io
import polars as pl

BASE   = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
RAW_CPS= BASE/"data_raw/cps_basic_monthly_raw"
XWALK  = BASE/"data_raw/xwalk/cbsa2fipsxw.csv"      
FINAL  = BASE/"data_final"
PANEL  = FINAL/"panel_cbsa_q.parquet"

OUT_ALL   = FINAL/"cps_all_u_cbsa_q.parquet"
OUT_NONBA = FINAL/"cps_u_22_27_nonba_cbsa_q.parquet"
OUT_DEMO  = FINAL/"cps_ba_22_27_demo_cbsa_q.parquet"

YEARS = sorted([p.name for p in RAW_CPS.iterdir() if p.is_dir() and p.name.isdigit()])



def detect_col(df: pl.DataFrame, candidates: list[str]):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def read_cps_any(path: pathlib.Path) -> pl.DataFrame:
\
\
       
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            
            name = next((n for n in zf.namelist() if n.lower().endswith((".csv",".txt"))), None)
            if name is None:
                raise ValueError(f"No CSV/TXT inside {path}")
            with zf.open(name) as f:
                data = io.BytesIO(f.read())
            return pl.read_csv(data, infer_schema_length=50000)
    else:
        return pl.read_csv(path, infer_schema_length=50000)

def month_to_q(month: pl.Expr) -> pl.Expr:
    return ((month - 1)//3 + 1)

def ensure_cbsa(df: pl.DataFrame, xwalk: pl.DataFrame) -> pl.DataFrame:
\
\
\
       
    
    cbsa_col = detect_col(df, ["gtcbsa","GTCBSA","GTCBSACD","cbsa","CBSA"])
    if cbsa_col:
        return df.with_columns(pl.col(cbsa_col).cast(pl.Utf8).str.zfill(5).alias("cbsa"))
    
    state_col = detect_col(df, ["gestfips","GESTFIPS","statefips","STATEFIPS"])
    county_col= detect_col(df, ["gtco","GTCO","countyfips","COUNTY"])
    if state_col and county_col:
        fips = (pl.col(state_col).cast(pl.Utf8).str.zfill(2) + pl.col(county_col).cast(pl.Utf8).str.zfill(3))
        tmp = df.with_columns(fips.alias("fips"))
        return tmp.join(xwalk, on="fips", how="left").with_columns(pl.col("cbsa").cast(pl.Utf8).str.zfill(5))
                                                    
    return df.with_columns(pl.lit(None).alias("cbsa"))

def make_flags(df: pl.DataFrame) -> pl.DataFrame:
\
\
\
       
    
    w_col = detect_col(df, ["PWCMPWGT","pwcmpwgt","PWSSWGT","WTFINL","wtfinl","pwsswgt"])
    if not w_col:
        raise ValueError("Could not find CPS weight (e.g., PWCMPWGT/WTFINL).")

    
    age_col = detect_col(df, ["PRTAGE","peage","PEAGE","age","AGE"])
    
    educ_col = detect_col(df, ["PEEDUCA","peeduca","EDUC","educ"])
    
    sex_col  = detect_col(df, ["PESEX","pesex","SEX","sex"])
    
    hisp_col = detect_col(df, ["PRDTHSP","prdthsp","HISPAN","hispan"])
    race_col = detect_col(df, ["PTDTRACE","ptdtrace","RACE","race"])
    
    lfs_col  = detect_col(df, ["PEMLR","pemlr","EMPSTAT","empstat"])

    out = df

    
    year_col = detect_col(df, ["year","YEAR","PERYEAR","HRYEAR4"])
    mon_col  = detect_col(df, ["month","MONTH","PEMONTH","HRMONTH"])
    out = out.with_columns([
        pl.col(year_col).cast(pl.Int32).alias("year"),
        pl.col(mon_col).cast(pl.Int32).alias("month")
    ])

    
    lf = None; ue = None
    if lfs_col and lfs_col.upper().startswith("PE"):
        
        lf = pl.col(lfs_col).cast(pl.Int32).is_in([1,2,3,4])
        ue = pl.col(lfs_col).cast(pl.Int32).is_in([3,4])
    else:
        
        lf = pl.col(lfs_col).cast(pl.Int32).is_in([10,12,20]) if lfs_col else pl.lit(None)
        ue = pl.col(lfs_col).cast(pl.Int32).is_in([20]) if lfs_col else pl.lit(None)

    
    ba = None
    if educ_col and educ_col.upper()=="PEEDUCA":
        
        ba = pl.col(educ_col).cast(pl.Int32).is_in([43,44,45,46])
    else:
        
        ba = pl.col(educ_col).cast(pl.Int32).ge(111) if educ_col else pl.lit(None)

    
    fem = None
    if sex_col:
        fem = (pl.col(sex_col).cast(pl.Int32)==2) if sex_col.upper().startswith("PE") else (pl.col(sex_col).cast(pl.Int32)==2)

    
    white_nh = None; black_nh = None; hisp_any = None
    if race_col and hisp_col:
        hisp_any = (pl.col(hisp_col).cast(pl.Int32).is_in([1]) | (pl.col(hisp_col).cast(pl.Int32)==1))  
        
        white_nh = (pl.col(race_col).cast(pl.Int32)==1) & (~hisp_any)
        black_nh = (pl.col(race_col).cast(pl.Int32)==2) & (~hisp_any)

    out = out.with_columns([
        pl.col(w_col).cast(pl.Float64).alias("w"),
        pl.col(age_col).cast(pl.Int32).alias("age") if age_col else pl.lit(None).alias("age"),
        ba.alias("is_ba"),
        fem.alias("is_female") if fem is not None else pl.lit(None).alias("is_female"),
        white_nh.alias("is_white_nh") if white_nh is not None else pl.lit(None).alias("is_white_nh"),
        black_nh.alias("is_black_nh") if black_nh is not None else pl.lit(None).alias("is_black_nh"),
        hisp_any.alias("is_hisp") if hisp_any is not None else pl.lit(None).alias("is_hisp"),
        lf.alias("in_lf").cast(pl.Int8),
        ue.alias("is_ue").cast(pl.Int8),
    ])
    return out



def main():
    
    xwalk = pl.read_csv(XWALK, infer_schema_length=1000).select(
        pl.col("fips").cast(pl.Utf8).str.zfill(5),
        pl.col("cbsa").cast(pl.Utf8).str.zfill(5),
    )

    frames = []
    for y in YEARS:
        for f in sorted((RAW_CPS/y).glob("*.zip")) + sorted((RAW_CPS/y).glob("*.csv")) + sorted((RAW_CPS/y).glob("*.txt")):
            try:
                m = read_cps_any(f)
            except Exception:
                continue
            
            m = make_flags(m)
            m = ensure_cbsa(m, xwalk)
            frames.append(m.select(["year","month","cbsa","w","age","is_ba","is_female","is_white_nh","is_black_nh","is_hisp","in_lf","is_ue"]))
    if not frames:
        raise RuntimeError("No CPS micro files read. Check RAW_CPS path and file formats.")
    cps = pl.concat(frames, how="vertical").filter(~pl.col("cbsa").is_null())

    
    cps = cps.with_columns((month_to_q(pl.col("month"))).alias("quarter"))

    
    all_u = (
        cps.group_by(["cbsa","year","quarter"])
           .agg([
               (pl.sum((pl.col("is_ue")==1) * pl.col("w")) / pl.sum((pl.col("in_lf")==1) * pl.col("w"))).alias("u_all")
           ])
           .with_columns(pl.col("u_all").cast(pl.Float64))
           .sort(["cbsa","year","quarter"])
    )
    all_u.write_parquet(OUT_ALL)

    
    nonba = (
        cps.filter((pl.col("age")>=22) & (pl.col("age")<=27) & (~pl.col("is_ba")))
           .group_by(["cbsa","year","quarter"])
           .agg([
               (pl.sum((pl.col("is_ue")==1) * pl.col("w")) / pl.sum((pl.col("in_lf")==1) * pl.col("w"))).alias("u_22_27_nonba")
           ])
           .with_columns(pl.col("u_22_27_nonba").cast(pl.Float64))
           .sort(["cbsa","year","quarter"])
    )
    nonba.write_parquet(OUT_NONBA)

    
    ba_grp = cps.filter((pl.col("age")>=22) & (pl.col("age")<=27) & (pl.col("is_ba")))
    demo = (
        ba_grp.group_by(["cbsa","year","quarter"])
              .agg([
                  (pl.sum((pl.col("is_female")==1) * pl.col("w"))     / pl.sum(pl.col("w"))).alias("share_female_22_27_ba"),
                  (pl.sum((pl.col("is_white_nh")==1) * pl.col("w"))  / pl.sum(pl.col("w"))).alias("share_white_nh_22_27_ba"),
                  (pl.sum((pl.col("is_black_nh")==1) * pl.col("w"))  / pl.sum(pl.col("w"))).alias("share_black_nh_22_27_ba"),
                  (pl.sum((pl.col("is_hisp")==1) * pl.col("w"))      / pl.sum(pl.col("w"))).alias("share_hisp_22_27_ba"),
              ])
              .sort(["cbsa","year","quarter"])
    )
    demo.write_parquet(OUT_DEMO)

    
    panel = pl.read_parquet(PANEL)
    panel2 = (panel.join(all_u,   on=["cbsa","year","quarter"], how="left")
                   .join(nonba,   on=["cbsa","year","quarter"], how="left")
                   .join(demo,    on=["cbsa","year","quarter"], how="left"))

    panel2.write_parquet(PANEL)
    panel2.write_csv(FINAL/"panel_cbsa_q.csv")

    
    print("[OK] Added columns:",
          [c for c in ["u_all","u_22_27_nonba","share_female_22_27_ba","share_white_nh_22_27_ba","share_black_nh_22_27_ba","share_hisp_22_27_ba"]
           if c in panel2.columns])
    print("Rows unchanged:", panel2.height == panel.height)

if __name__ == "__main__":
    main()
