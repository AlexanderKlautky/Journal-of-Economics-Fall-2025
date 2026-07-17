

from __future__ import annotations
import os, json, time, pathlib, math
import urllib.parse, urllib.request
import polars as pl

BASE   = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
FINAL  = BASE / "data_final"
PANEL  = FINAL / "panel_cbsa_q.parquet"
DENOM  = FINAL / "acs_cbsa_y_22_27_pop.parquet"

YEARS  = list(range(2017, 2025)) 

CANDIDATES = [
    ("ACSDT1Y.S0101", "S0101_C01_030E", "S0101_C01_031E"),
    ("ACSST1Y.S0101", "S0101_C01_030",  "S0101_C01_031"),
    ("ACSDT5Y.S0101", "S0101_C01_030E", "S0101_C01_031E"),
    ("ACSST5Y.S0101", "S0101_C01_030",  "S0101_C01_031"),
]

API_KEY = os.getenv("CENSUS_API_KEY", "").strip()

def fetch_acs(year:int, dataset:str, v20_24:str, v25_29:str) -> pl.DataFrame | None:
    base = f"https://api.census.gov/data/{year}/{dataset}"
    params = {
        "get": ",".join(["NAME", v20_24, v25_29]),
        "for": "metropolitan statistical area/micropolitan statistical area:*",
    }
    if API_KEY:
        params["key"] = API_KEY
    url = base + "?" + urllib.parse.urlencode(params, safe=",/ *")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.load(resp)
    except Exception:
        return None
    if not isinstance(data, list) or len(data) < 2:
        return None
    header, *rows = data
    df = pl.DataFrame(rows, schema=header)
    geo_col = "metropolitan statistical area/micropolitan statistical area"
    if geo_col not in df.columns:
        return None

    df = df.rename({geo_col: "cbsa_raw"}).with_columns([
        pl.col("cbsa_raw").cast(pl.Utf8).str.zfill(5).alias("cbsa"),
        pl.col(v20_24).cast(pl.Float64, strict=False).alias("pop_20_24"),
        pl.col(v25_29).cast(pl.Float64, strict=False).alias("pop_25_29"),
    ])
    df = df.select(["cbsa","pop_20_24","pop_25_29"]).with_columns(pl.lit(year).alias("year"))

    df = df.filter(~(pl.col("pop_20_24").is_null() & pl.col("pop_25_29").is_null()))
    if df.height == 0:
        return None
    return df

def build_denom() -> pl.DataFrame:
    frames = []
    for y in YEARS:
        got = None
        for ds, a, b in CANDIDATES:
            tmp = fetch_acs(y, ds, a, b)
            if tmp is not None:
                got = tmp
                print(f"[ok] {y}: {ds} rows={tmp.height}")
                break
            time.sleep(0.2)
        if got is None:
            print(f"[warn] No ACS data for {y} (all candidates).")
            continue
        got = got.with_columns((0.6*pl.col("pop_20_24") + 0.6*pl.col("pop_25_29")).alias("pop_22_27"))
        frames.append(got.select(["cbsa","year","pop_22_27"]))
        time.sleep(0.2)
    if not frames:
        raise RuntimeError("No ACS denominators fetched; check internet/API key.")
    denom = pl.concat(frames, how="vertical").unique().sort(["cbsa","year"])
    FINAL.mkdir(parents=True, exist_ok=True)
    denom.write_parquet(DENOM)
    print(f"[WRITE] {DENOM} | rows={denom.height} | CBSAs={denom.select(pl.col('cbsa').n_unique()).item()}")
    return denom

def patch_panel(denom: pl.DataFrame):
    if not PANEL.exists():
        raise FileNotFoundError(f"Panel not found: {PANEL}")
    df = pl.read_parquet(PANEL)
    merged = df.join(denom, on=["cbsa","year"], how="left")

    to_scale = [c for c in ["flow_ba","flow_ba_11","flow_ba_12","flow_ba_lag1y","flow_ba_lag2y"] if c in merged.columns]
    for c in to_scale:
        merged = merged.with_columns((pl.col(c) / pl.col("pop_22_27")).alias(f"{c}_percap"))
    merged.write_parquet(PANEL)
    merged.write_csv(FINAL / "panel_cbsa_q.csv")
    print("[PATCH] per-capita columns added:", [c for c in merged.columns if c.endswith("_percap")])
    if "flow_ba_percap" in merged.columns:
        ok = (merged.group_by(["cbsa","year"])
                    .agg(pl.col("flow_ba_percap").n_unique().alias("k"))
                    .filter(pl.col("k")>1).height == 0)
        print(f"[QC] flow_ba_percap constant within cbsa-year: {'PASS' if ok else 'FAIL'}")

def main():
    if not API_KEY:
        print("[note] No CENSUS_API_KEY set; the API may throttle. It still tries all datasets.")
    denom = build_denom()
    patch_panel(denom)

if __name__ == "__main__":
    main()
