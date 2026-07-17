

import pathlib
import polars as pl

BASE = pathlib.Path.home() / "Journal_Project_2025"
FIN  = BASE / "data_final"
FIN.mkdir(parents=True, exist_ok=True)

def rp(name: str) -> pathlib.Path:
    return FIN / name

def read_parquet_safe(p: pathlib.Path) -> pl.DataFrame:
    if not p.exists():
        print(f"[WARN] missing {p.name} -> using empty df")
        return pl.DataFrame()
    return pl.read_parquet(p)


cps   = read_parquet_safe(rp("cps_u_22_27_ba_cbsa_q.parquet"))         
ipeds = read_parquet_safe(rp("ipeds_ba_flow_cbsa_y.parquet"))           
oews  = read_parquet_safe(rp("oews_bartik_cbsa_y.parquet"))             
jolts = read_parquet_safe(rp("jolts_us_openings_rate_q.parquet"))       
xw    = read_parquet_safe(rp("crosswalk_cbsa.parquet"))                 


def fix_cbsa(df: pl.DataFrame) -> pl.DataFrame:
    if "cbsa" in df.columns:
        df = df.with_columns(
            pl.col("cbsa").cast(pl.Utf8).str.strip_chars().alias("cbsa")
        )
    return df

cps, ipeds, oews, xw = map(fix_cbsa, (cps, ipeds, oews, xw))


title_col = None
for cand in ("cbsa_title", "cbsatitle", "cbsa_name"):
    if cand in oews.columns:
        title_col = cand
        titles = oews.select(["cbsa", cand]).unique()
        break
if title_col is None and "cbsa_title" in xw.columns:
    title_col = "cbsa_title"
    titles = xw.select(["cbsa", "cbsa_title"]).unique()
if title_col is None:
    titles = pl.DataFrame({"cbsa": pl.Series([], dtype=pl.Utf8)})


def clip_yrs(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("year").is_between(2019, 2024)) if "year" in df.columns else df
cps, ipeds, oews, jolts = map(clip_yrs, (cps, ipeds, oews, jolts))


backbone = (cps
    .select(["cbsa","year","quarter","u_22_27_ba","lf_w"])
    .sort(["cbsa","year","quarter"])
)

panel = (backbone
    .join(ipeds.select([c for c in ("cbsa","year","flow_ba","flow_ba_l1","flow_ba_l2") if c in ipeds.columns]),
          on=["cbsa","year"], how="left")
    .join(oews.select([c for c in ("cbsa","year","bartik_demand") if c in oews.columns]),
          on=["cbsa","year"], how="left")
    .join(jolts.select([c for c in ("year","quarter","jolts_openings_rate_us") if c in jolts.columns]),
          on=["year","quarter"], how="left")
)

if not titles.is_empty():
    panel = panel.join(titles, on="cbsa", how="left")


print("[CHECK] rows:", panel.height)
if "u_22_27_ba" in panel.columns:
    bad = panel.filter(~pl.col("u_22_27_ba").is_null() & ((pl.col("u_22_27_ba") < 0) | (pl.col("u_22_27_ba") > 1))).height
    print("[CHECK] cps u-rate outside [0,1] rows:", bad)
if "jolts_openings_rate_us" in panel.columns:
    print("[CHECK] JOLTS null rows:", panel.filter(pl.col("jolts_openings_rate_us").is_null()).height)


out_parq = rp("panel_cbsa_q.parquet")
out_csv  = rp("panel_cbsa_q.csv")
panel.write_parquet(out_parq)
panel.write_csv(out_csv)
print("Wrote:", out_parq)
print("Wrote:", out_csv)
print(panel.head(10))
if "year" in panel.columns:
    yrs = panel.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    print("Years:", yrs)
