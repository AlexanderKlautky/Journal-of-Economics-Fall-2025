
\
\
\
   

from __future__ import annotations
import argparse, csv, sys
from pathlib import Path
import duckdb

def sql_source_for(path: Path) -> str:
    p = str(path).replace("'", "''")
    return "read_parquet('{p}')" if path.suffix.lower()==".parquet" else f"read_csv_auto('{p}', HEADER=TRUE)"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", required=True, help="panel_cbsa_q.parquet or .csv")
    ap.add_argument("--out", required=False, default=None, help="output CSV path (default: alongside panel as CODEBOOK_panel.csv)")
    args = ap.parse_args()

    panel = Path(args.panel)
    out = Path(args.out) if args.out else panel.parent / "CODEBOOK_panel.csv"

    con = duckdb.connect()
    src = sql_source_for(panel)
    con.execute(f"CREATE OR REPLACE VIEW v AS SELECT * FROM {src}")


    cols = con.sql("SELECT * FROM v LIMIT 0").columns

    dtypes = {r[0]: r[1] for r in con.sql("DESCRIBE SELECT * FROM v").fetchall()}

    rows = []
    for c in cols:

        mn, mx = con.sql(f"SELECT MIN({c}), MAX({c}) FROM v").fetchone()

        miss = con.sql(f"SELECT AVG(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::DOUBLE FROM v").fetchone()[0]

        def unit_guess(mn, mx):
            if mn is None or mx is None: return "unknown"
            try:
                if 0.0 <= float(mn) <= 1.0 and 0.0 <= float(mx) <= 1.0: return "fraction_0to1"
                if 0.0 <= float(mn) and float(mx) <= 100.0: return "percent_0to100"
            except Exception:
                pass
            return "raw/nominal"
        unit = unit_guess(mn, mx)

        ex = con.sql(f"SELECT {c} FROM v WHERE {c} IS NOT NULL LIMIT 1").fetchone()
        ex = None if ex is None else ex[0]
        rows.append([c, dtypes.get(c, ""), unit, mn, mx, miss, ex, ""])  


    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["var","dtype","unit_guess","min","max","missing_share","example","description"])
        for r in rows: w.writerow(r)

    print(f"Wrote codebook → {out.resolve()}")

if __name__ == "__main__":
    main()
