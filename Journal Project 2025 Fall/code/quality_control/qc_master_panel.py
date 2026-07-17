
\
\
\
\
\
\
   

from __future__ import annotations
import argparse, csv, sys
from pathlib import Path
import duckdb

CORE = [
    "cbsa", "year", "quarter",
    "u_22_27_ba", "jolts_openings_rate_us",
    "flow_ba", "bartik_demand", "log_postings",
]
OPTIONAL = ["u_22_27_nonba", "u_all"]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", required=False,
                    help="Path to panel (.parquet or .csv). Default: data_final/panel_cbsa_q.parquet (or .csv)")
    ap.add_argument("--codebook", required=False, help="Optional CODEBOOK_panel.csv")
    ap.add_argument("--outdir", default=".", help="Where to write QC artifacts")
    ap.add_argument("--na_thresh", type=float, default=0.20, help="Allowed missing share (default 0.20)")
    return ap.parse_args()

def default_panel_path():
    for p in ["panel_cbsa_q.parquet", "data_final/panel_cbsa_q.parquet",
              "panel_cbsa_q.csv", "data_final/panel_cbsa_q.csv"]:
        if Path(p).exists(): return Path(p)
    return None

def sql_source_for(path: Path) -> str:
    p = str(path).replace("'", "''")
    if path.suffix.lower() == ".parquet":
        return f"read_parquet('{p}')"
    elif path.suffix.lower() == ".csv":
        return f"read_csv_auto('{p}', HEADER=TRUE)"
    else:
        raise SystemExit(f"Unsupported file type: {path.suffix}")

def get_columns(con: duckdb.DuckDBPyConnection, view_name: str) -> list[str]:
    rel = con.sql(f"SELECT * FROM {view_name} LIMIT 0")
    return rel.columns

def write_csv(path: Path, rows, header=None):
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        if header: w.writerow(header)
        for r in rows: w.writerow(r)

def main():
    args = parse_args()
    panel_path = Path(args.panel) if args.panel else default_panel_path()
    if not panel_path or not panel_path.exists():
        sys.exit("Could not find panel. Pass --panel <path>.")

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    print("\nQC: Master Panel Certification (DuckDB)")
    print(f"Panel: {panel_path}")

    con = duckdb.connect()
    source = sql_source_for(panel_path)
    con.execute(f"CREATE OR REPLACE VIEW v AS SELECT * FROM {source}")

    cols = get_columns(con, "v")
    n_rows = con.sql("SELECT COUNT(*) FROM v").fetchone()[0]
    print(f"Rows: {n_rows:,}  Cols: {len(cols)}")


    cols_lower = {c.lower() for c in cols}
    missing_core = [c for c in CORE if c.lower() not in cols_lower]
    pass_core = len(missing_core) == 0
    print("\n--- CHECKS: Core presence & key ---")
    print("required columns present:", "PASS" if pass_core else f"FAIL (missing: {missing_core})")


    have_key = all(k in cols_lower for k in ["cbsa","year","quarter"])
    if have_key:
        dups = con.sql("""
            SELECT COUNT(*) FROM (
              SELECT cbsa, year, quarter, COUNT(*) n
              FROM v GROUP BY 1,2,3 HAVING n > 1
            )
        """).fetchone()[0]
        pass_key = (dups == 0)
        print("unique key (cbsa,year,quarter):", "PASS" if pass_key else f"FAIL (duplicates={dups})")
        cov = con.sql("""
            SELECT COUNT(DISTINCT cbsa) AS n_cbsa,
                   COUNT(DISTINCT (year||'-Q'||quarter)) AS n_quarters
            FROM v
        """).fetchone()
        print(f"Coverage: regions={cov[0]}  quarters={cov[1]}")
    else:
        pass_key = False
        print("unique key: FAIL (missing cbsa/year/quarter)")


    print("\n--- CHECKS: Ranges & structure ---")
    def minmax(col):
        if col.lower() not in cols_lower: return None
        return con.sql(f"SELECT MIN({col}), MAX({col}) FROM v").fetchone()
    def unit_guess(mm):
        if not mm: return "MISSING"
        mn, mx = mm
        if mn is None or mx is None: return "MISSING"
        if 0.0 <= mn <= 1.0 and 0.0 <= mx <= 1.0: return "fraction_0to1"
        if 0.0 <= mn and mx <= 100.0: return "percent_0to100"
        return "raw_large"
    for col in ["u_22_27_ba", "jolts_openings_rate_us"]:
        mm = minmax(col)
        print(f"{col:>24} min/max={mm}  unit_guess={unit_guess(mm)}")


    for col in ["flow_ba", "bartik_demand"]:
        if col.lower() in cols_lower:
            nbad = con.sql(f"""
              SELECT COUNT(*) FROM (
                SELECT cbsa, year, COUNT(DISTINCT {col}) k
                FROM v GROUP BY 1,2 HAVING k > 1
              )
            """).fetchone()[0]
            print(f"{col:>24} constant within cbsa-year:", "PASS" if nbad == 0 else f"FAIL (vary groups={nbad})")


    if "jolts_openings_rate_us" in cols_lower:
        nbad = con.sql("""
          SELECT COUNT(*) FROM (
            SELECT year, quarter, COUNT(DISTINCT jolts_openings_rate_us) k
            FROM v GROUP BY 1,2 HAVING k > 1
          )
        """).fetchone()[0]
        print("jolts_openings_rate_us uniform within year-quarter:", "PASS" if nbad == 0 else f"FAIL (vary year-q={nbad})")


    print("\n--- CHECKS: Missingness (share) ---")
    miss_cols = [c for c in CORE + OPTIONAL if c.lower() in cols_lower]
    miss_rows = []
    for c in miss_cols:
        share = con.sql(f"SELECT AVG(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::DOUBLE FROM v").fetchone()[0]
        miss_rows.append((c, share))
    miss_rows.sort(key=lambda x: x[1] if x[1] is not None else -1, reverse=True)
    for r in miss_rows[:10]:
        print(f"{r[0]:<24} {r[1]}")
    pass_missing = (len(miss_rows) > 0 and max(x[1] for x in miss_rows if x[1] is not None) <= args.na_thresh)
    print(f"Missingness acceptable (<= {args.na_thresh:.2f}):", "PASS" if pass_missing else "FAIL")
    write_csv(Path(outdir, "qc_missingness.csv"), miss_rows, header=["var","missing_share"])


    print("\n--- CHECKS: Within-CBSA variation ---")
    def within_cbsa_var(colname: str):
        if colname.lower() not in cols_lower:
            return None

        wv = con.sql(f"""
            WITH per AS (
                SELECT cbsa, VAR_POP({colname}) AS wvar, COUNT(*) AS n
                FROM v WHERE {colname} IS NOT NULL
                GROUP BY cbsa
            )
            SELECT AVG(wvar) FROM per
            -- Weighted version (uncomment if preferred):
            -- SELECT SUM(wvar * n) / NULLIF(SUM(n),0) FROM per
        """).fetchone()[0]
        return wv
    for v in [x for x in ["u_22_27_ba","log_postings","bartik_demand","flow_ba"] if x.lower() in cols_lower]:
        wv = within_cbsa_var(v)
        label = "OK" if (wv is not None and wv > 0) else "LOW/NA"
        print(f"{v:>16} within-CBSA var ≈ {wv}  [{label}]")


    print("\n--- CHECKS: Correlations (pairwise) ---")
    corr_vars = [x for x in ["u_22_27_ba","log_postings","bartik_demand","flow_ba","u_22_27_nonba","u_all"] if x.lower() in cols_lower]
    corr_mat = []
    if len(corr_vars) >= 3:
        header = [""] + corr_vars
        for a in corr_vars:
            row = [a]
            for b in corr_vars:
                val = con.sql(f"SELECT corr({a}, {b}) FROM v").fetchone()[0]
                row.append(val)
            corr_mat.append(row)
        for r in corr_mat: print(r)
        write_csv(Path(outdir, "qc_corr_matrix.csv"), corr_mat, header=header)
    else:
        print("Not enough variables for a correlation matrix.")


    print("\n--- CHECKS: National quarterly U(BA 22–27) ---")
    if "u_22_27_ba" in cols_lower:
        rows = con.sql("""
            SELECT year, quarter, AVG(u_22_27_ba) AS u_ba
            FROM v GROUP BY 1,2 ORDER BY 1,2 LIMIT 12
        """).fetchall()
        for r in rows: print(r)


    pass_codebook = True
    if args.codebook:
        cb = Path(args.codebook)
        if cb.exists():
            with cb.open() as f:
                rdr = csv.DictReader(f)
                name_field = None
                for cand in ["var","variable","name","column","Variable","Name","Column"]:
                    if cand in rdr.fieldnames:
                        name_field = cand; break
                if not name_field:
                    pass_codebook = False
                else:
                    listed = { (row[name_field] or "").strip().lower() for row in rdr }
                    pass_codebook = set(c.lower() for c in cols).issubset(listed)
        else:
            pass_codebook = False
    print("\nCodebook covers all columns:" , "PASS" if pass_codebook else "FAIL (or not supplied)")


    verdict = all([pass_core, pass_key, pass_missing, pass_codebook])
    print("\n========== VERDICT ==========")
    print("MASTER PANEL READY for analysis." if verdict else "NOT READY. See FAILED items above.")

    log_lines = [
        f"panel: {panel_path}",
        f"rows: {n_rows}  cols: {len(cols)}",
        f"pass_core: {pass_core}",
        f"pass_key: {pass_key}",
        f"pass_missing: {pass_missing}",
        f"pass_codebook: {pass_codebook}",
    ]
    Path(outdir, "qc_master_panel.log").write_text("\n".join(log_lines))
    print("\nArtifacts:", Path(outdir, "qc_master_panel.log").resolve(),
          Path(outdir, "qc_missingness.csv").resolve(),
          Path(outdir, "qc_corr_matrix.csv").resolve() if len(corr_vars) >= 3 else "(no corr CSV)")

if __name__ == "__main__":
    main()

