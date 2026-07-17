
import sys, pathlib
import pandas as pd
import polars as pl


CODE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(CODE_DIR) not in sys.path:
    sys.path.append(str(CODE_DIR))
from _utils.engine import ensure_dir, write_parquet 

BASE = pathlib.Path.home() / "Journal_Project_2025"
OUT  = BASE / "data_final"
ensure_dir(OUT)


RAW = None
for cand in [BASE / "data_raw" / "jolts_raw", BASE / "data_raw" / "jolts"]:
    if cand.exists():
        RAW = cand
        break
if RAW is None:
    raise SystemExit("Could not find JOLTS folder under data_raw/ (looked for 'jolts_raw' and 'jolts').")


def sniff_sep(sample: bytes) -> str:
    s = sample.decode("utf-8", errors="ignore")
    if "\t" in s: return "\t"
    if "|"  in s: return "|"
    if ","  in s: return ","
    return r"\s+"

def read_any(path: pathlib.Path) -> pl.DataFrame:
    with open(path, "rb") as f:
        sample = f.read(4096)
    sep = sniff_sep(sample)
    df = pd.read_csv(path, sep=sep, engine="python", dtype=str,
                     on_bad_lines="skip", skip_blank_lines=True)
    df.columns = [c.strip().lower() for c in df.columns]
    return pl.from_pandas(df)

def pick(df: pl.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols: return c
    return None

def as_num(expr: pl.Expr) -> pl.Expr:
    return expr.cast(pl.Utf8).str.replace_all(r"[^\d\.\-]", "").cast(pl.Float64, strict=False)


data   = read_any(RAW / "jt.data.1.AllItems")
series = read_any(RAW / "jt.series")
area   = read_any(RAW / "jt.area")
rate   = read_any(RAW / "jt.ratelevel")
elem   = read_any(RAW / "jt.dataelement")
per    = read_any(RAW / "jt.period")
seas   = read_any(RAW / "jt.seasonal")

ind    = None
try:
    ind = read_any(RAW / "jt.industry")
except Exception:
    pass

print("[DIAG] shapes | data:", data.height, "series:", series.height,
      "area:", area.height, "ratelevel:", rate.height, "dataelement:", elem.height,
      "period:", per.height, "seasonal:", seas.height)



s_series = pick(series, ["series_id","seriesid"])
s_seas   = pick(series, ["seasonal_code","seasonal","seasonalid","seasonal_cd"])
s_area   = pick(series, ["area_code","area","areacode"])
s_ind    = pick(series, ["industry_code","industry","industrycode"])
s_elem   = pick(series, ["dataelement_code","dataelement","dataelementcode"])
s_rate   = pick(series, ["ratelevel_code","ratelevel","ratelevelcode"])


d_series = pick(data, ["series_id","seriesid"])
d_year   = pick(data, ["year"])
d_period = pick(data, ["period"])
d_value  = pick(data, ["value"])


a_code = pick(area, ["area_code","area","areacode"])
a_text = pick(area, ["area_text","area_name","areaname","name","text"])

r_code = pick(rate, ["ratelevel_code","ratelevel","ratelevelcode"])
r_text = pick(rate, ["ratelevel_text","text","ratelevelname"])

e_code = pick(elem, ["dataelement_code","dataelement","dataelementcode"])
e_text = pick(elem, ["dataelement_text","text","dataelementname"])

p_code = pick(per,  ["period"])
p_num  = pick(per,  ["period_abbr","period_number","periodnum","p_num"])  
p_text = pick(per,  ["period_text","text"])

s_code = pick(seas, ["seasonal_code","seasonal","seasonalid"])
s_text = pick(seas, ["seasonal_text","text","seasonalname"])

if not all([s_series, s_seas, s_area, s_ind, s_elem, s_rate, d_series, d_year, d_period, d_value,
            a_code, a_text, r_code, r_text, e_code, e_text, p_code, s_code, s_text]):
    raise SystemExit("[FATAL] Could not detect necessary JOLTS column names.")


series_meta = (series
    .select([
        pl.col(s_series).cast(pl.Utf8).alias("series_id"),
        pl.col(s_seas).cast(pl.Utf8).alias("seasonal"),
        pl.col(s_area).cast(pl.Utf8).alias("area_code"),
        pl.col(s_ind).cast(pl.Utf8).alias("industry_code"),
        pl.col(s_elem).cast(pl.Utf8).alias("elem_code"),
        pl.col(s_rate).cast(pl.Utf8).alias("rate_code"),
    ])
)

area_lu = area.select([
    pl.col(a_code).cast(pl.Utf8).alias("area_code"),
    pl.col(a_text).cast(pl.Utf8).alias("area_text")
])

rate_lu = rate.select([
    pl.col(r_code).cast(pl.Utf8).alias("rate_code"),
    pl.col(r_text).cast(pl.Utf8).alias("rate_text")
])

elem_lu = elem.select([
    pl.col(e_code).cast(pl.Utf8).alias("elem_code"),
    pl.col(e_text).cast(pl.Utf8).alias("elem_text")
])

seas_lu = seas.select([
    pl.col(s_code).cast(pl.Utf8).alias("seasonal"),
    pl.col(s_text).cast(pl.Utf8).alias("seasonal_text")
])

series_meta = (series_meta
    .join(area_lu, on="area_code", how="left")
    .join(rate_lu, on="rate_code", how="left")
    .join(elem_lu, on="elem_code", how="left")
    .join(seas_lu, on="seasonal", how="left")
)



us_mask = pl.col("area_code").eq("000000") | pl.col("area_text").str.to_lowercase().str.contains("united states")

tnf_mask = (pl.col("industry_code").fill_null("000000").is_in(["000000","0","",None]))
seas_mask = pl.col("seasonal").eq("S") | pl.col("seasonal_text").str.to_lowercase().str.contains("seasonally")
elem_mask = pl.col("elem_text").str.to_lowercase().str.contains("job openings")
rate_mask = (pl.col("rate_code").eq("R") | pl.col("rate_text").str.to_lowercase().str.contains("rate"))

series_keep = (series_meta
    .filter(us_mask & tnf_mask & seas_mask & elem_mask & rate_mask)
    .select(["series_id","area_code","area_text","seasonal","elem_code","rate_code"])
)

print("[DIAG] series candidates after filters:", series_keep.height)


vals = (data
    .select([
        pl.col(d_series).cast(pl.Utf8).alias("series_id"),
        pl.col(d_year).cast(pl.Int64).alias("year"),
        pl.col(d_period).cast(pl.Utf8).alias("period"),
        pl.col(d_value).cast(pl.Utf8).alias("value_raw")
    ])
    .with_columns(as_num(pl.col("value_raw")).alias("value"))
    .filter(pl.col("value").is_not_null())
)

df = vals.join(series_keep, on="series_id", how="inner")
print("[DIAG] rows after join with data:", df.height)


def period_to_month(expr: pl.Expr) -> pl.Expr:
    return (expr.str.extract(r"(\d+)$", 1)
                .cast(pl.Int64, strict=False))

df = (df
    .with_columns([
        period_to_month(pl.col("period")).alias("month")
    ])
    .filter(pl.col("month").is_between(1, 12))
)

df = (df
    .with_columns( ((pl.col("month") - 1) // 3 + 1).cast(pl.Int8).alias("quarter") )
    .filter(pl.col("year").is_between(2019, 2024))
)

q = (df
    .group_by(["year","quarter"])
    .agg(pl.col("value").mean().alias("jolts_openings_rate_us"))
    .sort(["year","quarter"])
)

print("[DIAG] quarterly rows:", q.height)


outp = OUT / "jolts_us_openings_rate_q.parquet"
write_parquet(q, outp)
print("Wrote:", outp, "| rows=", q.height)
