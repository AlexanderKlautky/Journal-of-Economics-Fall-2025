import sys, pathlib
import pandas as pd
import polars as pl

CODE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(CODE_DIR) not in sys.path:
    sys.path.append(str(CODE_DIR))
from _utils.engine import ensure_dir, write_parquet

BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW = None
for cand in [BASE/"data_raw"/"jolts_raw", BASE/"data_raw"/"jolts"]:
    if cand.exists():
        RAW = cand; break
if RAW is None:
    raise SystemExit("No JOLTS folder found under data_raw/ (jolts_raw or jolts).")

OUT = BASE / "data_final"; ensure_dir(OUT)

def resolve_file(stem: str) -> pathlib.Path:
    want = stem.lower()
    byname = {p.name.lower(): p for p in RAW.iterdir() if p.is_file()}
    for ext in ("", ".txt", ".csv", ".tsv"):
        nm = (stem + ext).lower()
        if nm in byname: return byname[nm]
    for nm, p in byname.items():
        if nm.startswith(want): return p
    raise FileNotFoundError(f"Missing {stem}[.txt/.csv/.tsv] in {RAW}")

def sniff_sep(p: pathlib.Path) -> str:
    s = p.read_bytes()[:4096].decode("utf-8", errors="ignore")
    if "\t" in s: return "\t"
    if "|"  in s: return "|"
    if ","  in s: return ","
    return r"\s+"

def read_any(stem: str) -> pl.DataFrame:
    p = resolve_file(stem)
    df = pd.read_csv(p, sep=sniff_sep(p), engine="python", dtype=str,
                     on_bad_lines="skip", skip_blank_lines=True)
    df.columns = [c.strip().lower() for c in df.columns]
    return pl.from_pandas(df)

def pick(df: pl.DataFrame, cands: list[str]) -> str|None:
    s = set(df.columns)
    for c in cands:
        if c in s: return c
    return None

def as_num(x: pl.Expr) -> pl.Expr:
    return x.cast(pl.Utf8).str.replace_all(r"[^\d\.\-]", "").cast(pl.Float64, strict=False)

def write_empty(outp: pathlib.Path, reason: str):
    empty = pl.DataFrame({
        "year": pl.Series(name="year", values=[], dtype=pl.Int64),
        "quarter": pl.Series(name="quarter", values=[], dtype=pl.Int8),
        "jolts_openings_rate_us": pl.Series(name="jolts_openings_rate_us", values=[], dtype=pl.Float64)
    })
    write_parquet(empty, outp)
    print(f"[EMPTY] {reason}")
    print("Wrote:", outp, "| rows= 0")


data  = read_any("jt.data.1.AllItems")   
ser   = read_any("jt.series")            
area  = read_any("jt.area")              
elem  = read_any("jt.dataelement")       
rl    = read_any("jt.ratelevel")         
per   = read_any("jt.period")            
sea   = read_any("jt.seasonal")          
ind   = None
try:
    ind = read_any("jt.industry")        
except FileNotFoundError:
    pass

print("[HEADERS] data:", data.columns)
print("[HEADERS] series:", ser.columns)
print("[HEADERS] area:", area.columns)
print("[HEADERS] dataelement:", elem.columns)
print("[HEADERS] ratelevel:", rl.columns)
print("[HEADERS] period:", per.columns)
if sea is not None: print("[HEADERS] seasonal:", sea.columns)
if ind is not None: print("[HEADERS] industry:", ind.columns)

                  
d_sid = pick(data, ["series_id","seriesid"]); d_year = pick(data, ["year"]); d_per = pick(data, ["period"]); d_val = pick(data, ["value"])
s_sid = pick(ser,  ["series_id","seriesid"])
s_area = pick(ser, ["area_code","area","areacode"])
s_elem = pick(ser, ["dataelement_code","dataelement","dataelementcode"])
s_rl   = pick(ser, ["ratelevel_code","ratelevel","ratelevelcode"])
s_seas = pick(ser, ["seasonal","seasonal_code","seasonalid"])
s_ind  = pick(ser, ["industry_code","industry","industrycode"])

a_code = pick(area, ["area_code","area","areacode"]); a_text = pick(area, ["area_text","area_name","areaname","areadescription","areatitle"])
e_code = pick(elem, ["dataelement_code","dataelement","dataelementcode"]); e_text = pick(elem, ["dataelement_text","dataelementname","text","element_text"])
rl_code= pick(rl,   ["ratelevel_code","ratelevel","ratelevelcode"]);       rl_text= pick(rl,   ["ratelevel_text","ratelevelname","text"])
p_code = pick(per,  ["period_code","period","periodcode"]);                p_text = pick(per,  ["period_abbr","periodtext","period_abbreviation","text"])
se_code= pick(sea,  ["seasonal","seasonal_code","seasonalid"]) if sea is not None else None
se_text= pick(sea,  ["seasonal_text","seasonalname","text"])   if sea is not None else None
i_code = pick(ind,  ["industry_code","industry","industrycode"]) if ind is not None else None
i_text = pick(ind,  ["industry_text","industryname","text","industry_title"]) if ind is not None else None

needed = [d_sid,d_year,d_per,d_val,s_sid,s_area,s_elem,s_rl]
if not all(needed):
    out = OUT/"jolts_us_openings_rate_q.parquet"
    return_msg = "Missing required keys (series_id/year/period/value or series joins)."
    write_empty(out, return_msg); raise SystemExit(0)


dataN = (data.select([
            pl.col(d_sid).cast(pl.Utf8).alias("series_id"),
            pl.col(d_year).cast(pl.Int64).alias("year"),
            pl.col(d_per).cast(pl.Utf8).alias("period"),
            as_num(pl.col(d_val)).alias("value")
        ])
        .filter(pl.col("value").is_not_null())
)

serN = ser.select([
    pl.col(s_sid).cast(pl.Utf8).alias("series_id"),
    pl.lit(None).alias("area_code") if s_area is None else pl.col(s_area).cast(pl.Utf8).alias("area_code"),
    pl.lit(None).alias("elem_code") if s_elem is None else pl.col(s_elem).cast(pl.Utf8).alias("elem_code"),
    pl.lit(None).alias("ratelevel_code") if s_rl is None else pl.col(s_rl).cast(pl.Utf8).alias("ratelevel_code"),
    pl.lit(None).alias("seasonal") if s_seas is None else pl.col(s_seas).cast(pl.Utf8).alias("seasonal"),
    pl.lit(None).alias("industry_code") if s_ind is None else pl.col(s_ind).cast(pl.Utf8).alias("industry_code"),
])


df = dataN.join(serN, on="series_id", how="inner")

if a_code and a_text:
    df = df.join(area.select([pl.col(a_code).cast(pl.Utf8).alias("area_code"),
                              pl.col(a_text).cast(pl.Utf8).alias("area_text")]),
                 on="area_code", how="left")

if e_code and e_text:
    df = df.join(elem.select([pl.col(e_code).cast(pl.Utf8).alias("elem_code"),
                              pl.col(e_text).cast(pl.Utf8).alias("elem_text")]),
                 on="elem_code", how="left")

if rl_code and rl_text:
    df = df.join(rl.select([pl.col(rl_code).cast(pl.Utf8).alias("ratelevel_code"),
                            pl.col(rl_text).cast(pl.Utf8).alias("ratelevel_text")]),
                 on="ratelevel_code", how="left")

if se_code and se_text:
    df = df.join(sea.select([pl.col(se_code).cast(pl.Utf8).alias("seasonal"),
                             pl.col(se_text).cast(pl.Utf8).alias("seasonal_text")]),
                 on="seasonal", how="left")

if i_code and i_text:
    df = df.join(ind.select([pl.col(i_code).cast(pl.Utf8).alias("industry_code"),
                             pl.col(i_text).cast(pl.Utf8).alias("industry_text")]),
                 on="industry_code", how="left")


def period_num(e: pl.Expr) -> pl.Expr:
    return e.cast(pl.Utf8).str.extract(r"(\d+)$", 1).cast(pl.Int64, strict=False)
df = (df.with_columns(period_num(pl.col("period")).alias("month"))
        .filter(pl.col("month").is_between(1,12))
        .with_columns(((pl.col("month")-1)//3 + 1).cast(pl.Int8).alias("quarter"))
        .filter(pl.col("year").is_between(2019, 2024))
)


if "ratelevel_text" in df.columns:
    df = df.filter(pl.col("ratelevel_text").str.to_lowercase().str.contains("rate"))
else:
   
    if "ratelevel_code" in df.columns:
        df = df.filter(pl.col("ratelevel_code").str.to_uppercase().str.contains("R|RATE"))


if "elem_text" in df.columns:
    df = df.filter(
        pl.col("elem_text").str.to_lowercase().str.contains("job") &
        pl.col("elem_text").str.to_lowercase().str.contains("open")
    )
else:
    
    txt_cols = [c for c in df.columns if "text" in c and df.schema[c] == pl.Utf8]
    cond = None
    for c in txt_cols:
        part = (pl.col(c).str.to_lowercase().str.contains("job") &
                pl.col(c).str.to_lowercase().str.contains("open"))
        cond = part if cond is None else (cond | part)
    if cond is not None:
        df = df.filter(cond)

rows_pre_area = df.height


if "area_text" in df.columns:
    us = df.filter(pl.col("area_text").str.to_lowercase().str.contains("united") &
                   pl.col("area_text").str.to_lowercase().str.contains("state"))
    if us.height > 0:
        df = us
    else:
        
        pass


if "seasonal_text" in df.columns:
    s = df.filter(pl.col("seasonal_text").str.to_lowercase().str.contains("^s|adjust", literal=False))
    if s.height > 0:
        df = s
elif "seasonal" in df.columns:
    s = df.filter(pl.col("seasonal").str.to_uppercase().eq("S"))
    if s.height > 0:
        df = s

print(f"[DIAG] rows after job/opening+rate filter: {rows_pre_area} -> after US/seasonal prefs: {df.height}")


q = (df.group_by(["year","quarter"])
        .agg(pl.col("value").mean().alias("jolts_openings_rate_us"))
        .sort(["year","quarter"])
)

outp = OUT / "jolts_us_openings_rate_q.parquet"
if q.height == 0:
    
    probe_cols = [c for c in df.columns if c.endswith("_text")] or [c for c in df.columns if df.schema[c]==pl.Utf8]
    print("[PROBE] No rows — here are distinct values from text columns (first 40 each):")
    for c in probe_cols[:6]:
        vals = (df.select(pl.col(c).drop_nulls().unique()).to_series().to_list())[:40]
        print(f"  - {c}: {vals}")
    write_empty(outp, "No JOLTS matches after text-based filters.")
else:
    write_parquet(q, outp)
    print("Wrote:", outp, "| rows=", q.height)

