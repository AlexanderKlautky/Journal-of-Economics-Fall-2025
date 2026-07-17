                      
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
\
\
\
\
\
\
\
\
   

from __future__ import annotations
import argparse
from pathlib import Path
import sys

try:
    import polars as pl
except Exception as e:
    print("This script requires polars. Install with: pip install polars pyarrow", file=sys.stderr)
    raise

                                                         
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="QC for master CBSA×year×quarter panel")
    p.add_argument("--panel", default=None,
                   help="Path to panel file (.parquet or .csv). Defaults to data_final/panel_cbsa_q.parquet (or .csv).")
    p.add_argument("--codebook", default=None,
                   help="Optional path to CODEBOOK_panel.csv to verify column coverage.")
    p.add_argument("--outdir", default=".",
                   help="Directory for QC artifacts.")
    p.add_argument("--na_thresh", type=float, default=0.20,
                   help="Max acceptable missing share for core vars (default 0.20).")
    return p.parse_args()

def default_panel_path() -> Path | None:
    candidates = [
        Path("panel_cbsa_q.parquet"),
        Path("data_final/panel_cbsa_q.parquet"),
        Path("panel_cbsa_q.csv"),
        Path("data_final/panel_cbsa_q.csv"),
    ]
    for c in candidates:
        if c.exists():
            return c
    return None

def load_any(path: Path) -> pl.DataFrame:
    ext = path.suffix.lower()
    if ext == ".parquet":
        return pl.read_parquet(path)
    elif ext == ".csv":
        return pl.read_csv(path, infer_schema_length=200000, ignore_errors=True)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

                                                                
CORE = [
    "cbsa", "year", "quarter",
    "u_22_27_ba", "jolts_openings_rate_us",
    "flow_ba", "bartik_demand", "log_postings"
]
OPTIONAL = ["u_22_27_nonba", "u_all"]

def exists_all(df: pl.DataFrame, cols: list[str]) -> tuple[bool, list[str]]:
    missing = [c for c in cols if c not in df.columns]
    return (len(missing) == 0, missing)

def qc_key_unique(df: pl.DataFrame) -> tuple[bool, int]:
    dups = (
        df.group_by(["cbsa", "year", "quarter"])
          .len()
          .filter(pl.col("len") > 1)
          .height
    )
    return (dups == 0, dups)

def qc_ranges(df: pl.DataFrame, col: str) -> tuple[dict, str]:
    if col not in df.columns:
        return ({"min": None, "max": None}, "MISSING")
    mm = df.select(pl.min(col).alias("min"), pl.max(col).alias("max")).to_dicts()[0]
    status = "fraction_0to1" if (mm["min"] is not None and mm["max"] is not None and 0.0 - 1e-9 <= mm["min"] <= 1.0 + 1e-9 and 0.0 - 1e-9 <= mm["max"] <= 1.0 + 1e-9) \
             else "percent_0to100" if (mm["min"] is not None and mm["max"] is not None and 0.0 - 1e-9 <= mm["min"] and mm["max"] <= 100.0 + 1e-9) \
             else "raw_large"
    return (mm, status)

def qc_constant_within_cbsa_year(df: pl.DataFrame, col: str) -> tuple[bool, int]:
    if col not in df.columns:
        return (False, -1)
    nvar = (
        df.group_by(["cbsa", "year"])
          .agg(pl.col(col).n_unique().alias("k"))
          .filter(pl.col("k") > 1)
          .height
    )
    return (nvar == 0, nvar)

def qc_uniform_within_year_quarter(df: pl.DataFrame, col: str) -> tuple[bool, int]:
    if col not in df.columns:
        return (False, -1)
    nvar = (
        df.group_by(["year", "quarter"])
          .agg(pl.col(col).n_unique().alias("k"))
          .filter(pl.col("k") > 1)
          .height
    )
    return (nvar == 0, nvar)

def missing_share(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    out = (
        df.select([pl.col(c).is_null().mean().alias(c) for c in cols if c in df.columns])
          .melt(variable_name="var", value_name="missing_share")
          .sort(pl.col("missing_share").desc(nulls_last=True))
    )
    return out

def within_region_variance(df: pl.DataFrame, col: str) -> float | None:
    if col not in df.columns:
        return None
    tmp = (
        df.drop_nulls(col)
          .with_columns(pl.col(col).cast(pl.Float64))
          .group_by("cbsa")
          .agg((pl.col(col) - pl.col(col).mean()).alias("_demean"))
          .explode("_demean")
          .select(pl.col("_demean")**2)
          .to_series()
    )
    return float(tmp.mean()) if tmp.len() else 0.0

def corr_matrix(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame | None:
    kept = [c for c in cols if c in df.columns]
    if len(kept) < 3:
        return None
    x = df.select([pl.col(c).cast(pl.Float64) for c in kept])
                                     
    corr = pl.DataFrame({
        a: [x.select(pl.corr(a, b)).item() for b in kept]
        for a in kept
    })
    return corr.with_columns(pl.Series("__var__", kept)).select(["__var__", *kept])

def nat_quarter_series(df: pl.DataFrame, col: str) -> pl.DataFrame | None:
    if col not in df.columns:
        return None
    return (
        df.with_columns(pl.col(col).cast(pl.Float64))
          .group_by(["year", "quarter"])
          .agg(pl.mean(col).alias(col))
          .sort(["year", "quarter"])
    )

                                                          
def main():
    args = parse_args()
    panel_path = Path(args.panel) if args.panel else default_panel_path()
    if panel_path is None:
        print("Could not find panel_cbsa_q.{parquet,csv}. Pass --panel <path>.", file=sys.stderr)
        sys.exit(2)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    print("\nQC: Master Panel Certification (Python/Polars)")
    print(f"Panel file: {panel_path}")

                
    df = load_any(panel_path)
    print(f"Rows: {df.height:,}  Cols: {len(df.columns)}")

                                   
    ok_core, missing_core = exists_all(df, CORE)
    print("\n--- CHECKS: Core presence & key ---")
    print("required columns present:", "PASS" if ok_core else f"FAIL (missing: {missing_core})")

                     
    if all(k in df.columns for k in ["cbsa","year","quarter"]):
        pass_key, n_dups = qc_key_unique(df)
        print("unique key (cbsa,year,quarter):", "PASS" if pass_key else f"FAIL (duplicates={n_dups})")
    else:
        print("unique key (cbsa,year,quarter): SKIPPED (key columns missing)")

                   
    if all(k in df.columns for k in ["cbsa","year","quarter"]):
        n_cbsa = df.select(pl.col("cbsa").n_unique()).item()
        n_q    = df.select(pl.struct(["year","quarter"]).alias("t")).select(pl.col("t").n_unique()).item()
        print(f"Coverage: regions={n_cbsa}  quarters={n_q}")

                         
    print("\n--- CHECKS: Ranges & structure ---")
    for col in ["u_22_27_ba", "jolts_openings_rate_us"]:
        mm, status = qc_ranges(df, col)
        print(f"{col:>24} min/max={mm}  unit_guess={status}")

                                             
    for col in ["flow_ba", "bartik_demand", "flow_ba_11", "flow_ba_12"]:
        if col in df.columns:
            ok, n_bad = qc_constant_within_cbsa_year(df, col)
            print(f"{col:>24} constant within cbsa-year:", "PASS" if ok else f"FAIL (vary groups={n_bad})")

                                       
    if "jolts_openings_rate_us" in df.columns:
        ok, n_bad = qc_uniform_within_year_quarter(df, "jolts_openings_rate_us")
        print("jolts_openings_rate_us uniform within year-quarter:", "PASS" if ok else f"FAIL (vary year-q={n_bad})")

                      
    print("\n--- CHECKS: Missingness (share) ---")
    miss_cols = [c for c in CORE + OPTIONAL if c in df.columns]
    miss_tbl = missing_share(df, miss_cols)
    if miss_tbl.height:
        print(miss_tbl.head(10))
        pass_missing = bool((miss_tbl.select(pl.col("missing_share").max()).item() <= args.na_thresh))
        print(f"Missingness acceptable (<= {args.na_thresh:0.2f}):", "PASS" if pass_missing else "FAIL")
    else:
        pass_missing = False
        print("No variables to evaluate missingness on (FAIL).")

                                                        
    print("\n--- CHECKS: Within-CBSA variation ---")
    for v in [c for c in ["u_22_27_ba", "log_postings", "bartik_demand", "flow_ba"] if c in df.columns]:
        wv = within_region_variance(df, v)
        lab = "OK" if (wv is not None and wv > 0) else "LOW/NA"
        print(f"{v:>16} within-CBSA var ≈ {wv}  [{lab}]")

                                         
    print("\n--- CHECKS: Correlations (pairwise) ---")
    corr_vars = [c for c in ["u_22_27_ba","log_postings","bartik_demand","flow_ba","u_22_27_nonba","u_all"] if c in df.columns]
    C = corr_matrix(df, corr_vars)
    if C is not None:
        print(C)
        C.write_csv(outdir / "qc_corr_matrix.csv")
    else:
        print("Not enough variables for a correlation matrix.")
        C = None

                                           
    print("\n--- CHECKS: National quarterly U(BA 22–27) ---")
    nat = nat_quarter_series(df, "u_22_27_ba")
    if nat is not None:
        print(nat.head(12))

                                          
    pass_codebook = True
    if args.codebook:
        codebook_path = Path(args.codebook)
        if codebook_path.exists():
            cb = pl.read_csv(codebook_path)
                                     
            name_col = None
            for cand in ["var","variable","name","column","Variable","Name","Column"]:
                if cand in cb.columns:
                    name_col = cand; break
            if name_col:
                cols = set(cb.select(pl.col(name_col).cast(pl.Utf8)).to_series().str.to_lowercase().to_list())
                panel_cols = set([c.lower() for c in df.columns])
                pass_codebook = panel_cols.issubset(cols)
                print("\nCodebook covers all panel columns:", "PASS" if pass_codebook else "FAIL")
            else:
                pass_codebook = False
                print("\nCodebook present but no recognizable name column; skipping (FAIL).")
        else:
            print("\nCodebook path not found; skipping.")
    else:
        print("\nCodebook not supplied; skipping.")

                        
    print("\n========== VERDICT ==========")
    pass_core = ok_core
    pass_key  = qc_key_unique(df)[0] if all(k in df.columns for k in ["cbsa","year","quarter"]) else False
    pass_unit = True                                  
    verdict = all([pass_core, pass_key, pass_missing, pass_codebook, True])

    if verdict:
        print("MASTER PANEL READY for analysis.")
    else:
        print("NOT READY. See FAILED items above.")

                    
    log_lines = [
        f"panel: {panel_path}",
        f"rows: {df.height}  cols: {len(df.columns)}",
        f"pass_core: {pass_core}",
        f"pass_key: {pass_key}",
        f"pass_missing: {pass_missing}",
        f"pass_codebook: {pass_codebook}",
    ]
    (outdir / "qc_master_panel.log").write_text("\n".join(log_lines))
    if miss_tbl is not None and miss_tbl.height:
        miss_tbl.write_csv(outdir / "qc_missingness.csv")
    print("\nArtifacts written:",
          (outdir / "qc_master_panel.log").resolve(),
          (outdir / "qc_missingness.csv").resolve() if miss_tbl.height else "(no missingness CSV)",
          (outdir / "qc_corr_matrix.csv").resolve() if C is not None else "(no corr CSV)")

if __name__ == "__main__":
    main()
