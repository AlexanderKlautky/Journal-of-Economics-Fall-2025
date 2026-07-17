
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
import duckdb

def src(path: Path) -> str:
    p = str(path).replace("'", "''")
    return f"read_parquet('{p}')" if path.suffix.lower()=='.parquet' else f"read_csv_auto('{p}', HEADER=TRUE)"

def find_one(dirpath: Path, patterns: list[str]) -> Path | None:
    for pat in patterns:
        hits = list(dirpath.glob(pat))
        if hits: return sorted(hits)[0]
    return None

def view_has_cols(con: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    cols = [r[0].lower() for r in con.sql(f"DESCRIBE SELECT * FROM {view}").fetchall()]
    return set(cols)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--finaldir", default="/Users/alexanderklautky/Journal_Project_2025/data_final",
                    help="Folder with final parquet/csv files")
    ap.add_argument("--panel", default=None, help="Optional direct path to panel_cbsa_q.parquet")
    args = ap.parse_args()

    d = Path(args.finaldir)
    panel = Path(args.panel) if args.panel else find_one(d, ["panel_cbsa_q.parquet","panel_cbsa_q.csv"])

    paths = {
        "u_ba":     find_one(d, ["cps_u_22_27_ba_cbsa_q.parquet","cps_u_22_27_ba_cbsa_q.csv"]),
        "postings": find_one(d, ["postings_cbsa_q.parquet","postings_cbsa_q.csv"]),
        "bartik":   find_one(d, ["oews_bartik_cbsa_y.parquet","oews_bartik_cbsa_y.csv"]),
        "flow":     find_one(d, ["ipeds_ba_flow_cbsa_y.parquet","ipeds_ba_flow_cbsa_y.csv"]),
        "u_nonba":  find_one(d, ["cps_u_22_27_nonba_cbsa*.parquet","cps_u_22_27_nonba_cbsa*.csv"]),
        "u_all":    find_one(d, ["cps_all_u_cbsa_q.parquet","cps_all_u_cbsa_q.csv"]),
        "jolts":    find_one(d, ["jolts_us_openings_rate_q.parquet","jolts_us_openings_rate_q.csv"]),
        "panel":    panel,
    }

    con = duckdb.connect()


    for name, p in paths.items():
        if p is None: continue
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {src(p)}")


    if paths["panel"]:
        con.execute("CREATE OR REPLACE VIEW foot_panel AS SELECT DISTINCT cbsa, year, quarter FROM panel")
    if paths["u_ba"]:
        con.execute("CREATE OR REPLACE VIEW foot_u_ba AS SELECT DISTINCT cbsa, year, quarter FROM u_ba")
    if paths["postings"]:
        con.execute("CREATE OR REPLACE VIEW foot_postings AS SELECT DISTINCT cbsa, year, quarter FROM postings")
    if paths["u_nonba"]:
        con.execute("CREATE OR REPLACE VIEW foot_u_nonba AS SELECT DISTINCT cbsa, year, quarter FROM u_nonba")
    if paths["u_all"]:
        con.execute("CREATE OR REPLACE VIEW foot_u_all AS SELECT DISTINCT cbsa, year, quarter FROM u_all")
    if paths["jolts"]:
        con.execute("CREATE OR REPLACE VIEW foot_jolts AS SELECT DISTINCT year, quarter FROM jolts")
    if paths["bartik"]:
        con.execute("""
            CREATE OR REPLACE VIEW foot_bartik AS
            SELECT DISTINCT cbsa, year FROM bartik
        """)
    if paths["flow"]:
        con.execute("""
            CREATE OR REPLACE VIEW foot_flow AS
            SELECT DISTINCT cbsa, year FROM flow
        """)


    print("\n=== Coverage by source ===")
    for name in ["foot_u_ba","foot_postings","foot_bartik","foot_flow","foot_u_nonba","foot_u_all","foot_jolts","foot_panel"]:
        try:
            cols = view_has_cols(con, name)
        except duckdb.CatalogException:
            continue
 
        n_cbsa = con.sql(f"SELECT COUNT(*) FROM (SELECT DISTINCT cbsa FROM {name})").fetchone()[0] if "cbsa" in cols else 0

        if "year" in cols and "quarter" in cols:
            n_yq = con.sql(f"SELECT COUNT(*) FROM (SELECT DISTINCT year, quarter FROM {name})").fetchone()[0]
        elif "year" in cols:
            n_yq = 4 * con.sql(f"SELECT COUNT(*) FROM (SELECT DISTINCT year FROM {name})").fetchone()[0]
        else:
            n_yq = 0
        n_rows = con.sql(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"{name:>14}: cbsa={n_cbsa:4d}  yq={n_yq:4d}  rows={n_rows:6d}")


    print("\n=== Theoretical vs actual ===")

    if paths["u_ba"] and paths["postings"]:
        con.execute("""
            CREATE OR REPLACE VIEW time_intersect AS
            SELECT y.year, y.quarter
            FROM (SELECT DISTINCT year, quarter FROM foot_u_ba) y
            JOIN (SELECT DISTINCT year, quarter FROM foot_postings) p USING (year, quarter)
        """)

        con.execute("""
            CREATE OR REPLACE VIEW cbsa_intersect AS
            SELECT a.cbsa
            FROM (SELECT DISTINCT cbsa FROM foot_u_ba) a
            JOIN (SELECT DISTINCT cbsa FROM foot_postings) b USING (cbsa)
            JOIN (SELECT DISTINCT cbsa FROM foot_bartik)   c USING (cbsa)
            JOIN (SELECT DISTINCT cbsa FROM foot_flow)     d USING (cbsa)
        """)
        exp = con.sql("SELECT (SELECT COUNT(*) FROM cbsa_intersect) * (SELECT COUNT(*) FROM time_intersect)").fetchone()[0]
        print(f"Expected rows (u_ba×postings×bartik×flow grid): {exp}")
    else:
        exp = None
        print("Expected rows: N/A (missing u_ba or postings)")

    actual = con.sql("SELECT COUNT(*) FROM foot_panel").fetchone()[0] if paths["panel"] else None
    print(f"Actual rows in panel: {actual if actual is not None else 'N/A'}")


    print("\n=== Sequential inner-join counts (who trims most?) ===")
    if paths["u_ba"] and paths["postings"] and paths["bartik"] and paths["flow"]:
        con.execute("""
            CREATE OR REPLACE VIEW step1 AS
            SELECT f.* FROM foot_u_ba f
            JOIN (SELECT * FROM cbsa_intersect) c USING (cbsa)
            JOIN (SELECT * FROM time_intersect) t USING (year, quarter)
        """)
        n1 = con.sql("SELECT COUNT(*) FROM step1").fetchone()[0]

        con.execute("CREATE OR REPLACE VIEW step2 AS SELECT s.* FROM step1 s JOIN foot_postings p USING (cbsa,year,quarter)")
        n2 = con.sql("SELECT COUNT(*) FROM step2").fetchone()[0]


        con.execute("CREATE OR REPLACE VIEW step3 AS SELECT s.* FROM step2 s JOIN foot_bartik   b USING (cbsa,year)")
        n3 = con.sql("SELECT COUNT(*) FROM step3").fetchone()[0]

        con.execute("CREATE OR REPLACE VIEW step4 AS SELECT s.* FROM step3 s JOIN foot_flow     f USING (cbsa,year)")
        n4 = con.sql("SELECT COUNT(*) FROM step4").fetchone()[0]

        print(f"u_ba only (on intersect grid) : {n1}")
        print(f"+ postings                    : {n2}  (Δ {n2-n1})")
        print(f"+ bartik                      : {n3}  (Δ {n3-n2})")
        print(f"+ flow                        : {n4}  (Δ {n4-n3})")
        if actual is not None:
            print(f"final panel                   : {actual}  (compare to step4)")
    else:
        print("Join path incomplete (need u_ba, postings, bartik, flow).")

    print("\nTip: the step with the largest negative Δ is your bottleneck.")
    print("JOLTS is national (time-only), so it won't affect CBSA coverage.")

if __name__ == "__main__":
    main()
