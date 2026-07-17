

from __future__ import annotations
import pathlib, sys
import polars as pl

BASE = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
RAW_DIR = BASE / "data_raw" / "US:metro_job_postings "       
RAW_CSV = RAW_DIR / "metro_job_postings_us.csv"
OUT_POST = BASE / "data_final" / "postings_cbsa_q.parquet"
PANEL = BASE / "data_final" / "panel_cbsa_q.parquet"

URL = "https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/US/metro_job_postings_us.csv"

def ensure_raw_csv():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if RAW_CSV.exists():
        print(f"[info] found raw CSV: {RAW_CSV}")
        return
    print(f"[info] raw CSV missing; downloading → {RAW_CSV}")
    try:
        df = pl.read_csv(URL, try_parse_dates=True)
        df.write_csv(RAW_CSV)
        print(f"[ok] saved: {RAW_CSV}  rows={df.height}")
    except Exception as e:
        print(f"[error] download failed: {e}")
        print("       Tip: curl -L '{}' -o '{}'".format(URL, str(RAW_CSV)))
        sys.exit(1)

def coerce_date(daily: pl.DataFrame) -> pl.DataFrame:
                                                                      
    dtype = daily.schema.get("date")
    if dtype == pl.Utf8:
        return daily.with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
    elif dtype == pl.Datetime:
        return daily.with_columns(pl.col("date").cast(pl.Date))
    else:
        
        return daily

def build_postings_quarterly():
    daily = pl.read_csv(RAW_CSV, try_parse_dates=True)
    daily = coerce_date(daily)

    
    daily = daily.filter(
        (pl.col("date") >= pl.date(2020,1,1)) & (pl.col("date") < pl.date(2025,1,1))
    )
    print(f"[read] {RAW_CSV} rows(2020–2024)={daily.height}")

    
    if "indeed_job_postings_index" in daily.columns:
        daily = daily.with_columns(pl.col("indeed_job_postings_index").cast(pl.Float64, strict=False))
    else:
        raise RuntimeError("Column `indeed_job_postings_index` not found in raw file.")

    if "cbsa_code" not in daily.columns:
        raise RuntimeError("Column `cbsa_code` not found in raw file.")

    post_q = (
        daily
        .with_columns(
            pl.col("cbsa_code").cast(pl.Utf8).str.zfill(5).alias("cbsa"),
            (1 + pl.col("indeed_job_postings_index")/100).alias("postings_level_day"),
            pl.col("date").dt.year().alias("year"),
            (((pl.col("date").dt.month() - 1)//3) + 1).alias("quarter"),
        )
        .group_by(["cbsa","year","quarter"])
        .agg([
            pl.col("indeed_job_postings_index").mean().alias("postings_index"),
            pl.col("postings_level_day").mean().alias("postings_level"),
            pl.len().alias("n_days")
        ])
        .with_columns(pl.col("postings_level").log().alias("log_postings"))
        
        .with_columns(
            pl.when(pl.col("n_days") < 30)
              .then(None)
              .otherwise(pl.col("log_postings"))
              .alias("log_postings")
        )
        .sort(["cbsa","year","quarter"])
    )

    OUT_POST.parent.mkdir(parents=True, exist_ok=True)
    post_q.write_parquet(OUT_POST)
    print(f"[ok] wrote postings parquet: {OUT_POST} | rows={post_q.height} | CBSAs={post_q.select(pl.col('cbsa').n_unique()).item()}")
    yrs = post_q.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    print(f"[span] years: {yrs}")
    return post_q

def patch_panel(post_q: pl.DataFrame):
    if not PANEL.exists():
        print(f"[warn] panel not found: {PANEL}")
        print("       Skipping patch. (Once your panel exists, re-run this script.)")
        return
    panel = pl.read_parquet(PANEL)
    merged = panel.join(post_q, on=["cbsa","year","quarter"], how="left")

    
    iv_rows = merged.filter(
        pl.all_horizontal([
            ~pl.col("log_postings").is_null(),
            ~pl.col("bartik_demand").is_null(),
            ~pl.col("u_22_27_ba").is_null()
        ])
    ).height
    try:
        corr = merged.select(pl.corr("log_postings", "bartik_demand").alias("corr")).item()
    except Exception:
        corr = float("nan")

    merged.write_parquet(PANEL)
    merged.write_csv(BASE / "data_final" / "panel_cbsa_q.csv")

    print(f"[ok] patched panel: {PANEL}")
    print(f"[iv] usable rows (log_postings & bartik & outcome present): {iv_rows}")
    print(f"[iv] corr(bartik, log_postings) ~ {corr:.3f}")

def main():
    ensure_raw_csv()
    post_q = build_postings_quarterly()
    patch_panel(post_q)

if __name__ == "__main__":
    main()
