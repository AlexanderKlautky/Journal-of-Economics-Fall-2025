                                           
import sys, pathlib
import polars as pl

CODE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(CODE_DIR) not in sys.path:
    sys.path.append(str(CODE_DIR))

from _utils.engine import read_csv_smart, to_polars, write_parquet, ensure_dir

BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW = BASE / "data_raw"
OUT = BASE / "data_final"
ensure_dir(OUT)

def main():
    path = RAW / "xwalk" / "cbsa2fipsxw.csv"
    df = read_csv_smart(path)   
    df = to_polars(df)

    cols = set(df.columns)
    required = {"fipsstatecode", "fipscountycode", "cbsacode"}
    missing = sorted(list(required - cols))
    if missing:
        print("Available columns:", df.columns)
        raise SystemExit(f"Missing expected columns: {missing}")

    has_title = "cbsatitle" in cols

    exprs = [
        pl.col("fipsstatecode").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(2).alias("state2"),
        pl.col("fipscountycode").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(3).alias("county3"),
        pl.col("cbsacode").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(5).alias("cbsa"),
    ]
    if has_title:
        exprs.append(pl.col("cbsatitle").cast(pl.Utf8).alias("cbsa_title"))
    else:
        exprs.append(pl.lit(None, dtype=pl.Utf8).alias("cbsa_title"))

    base = df.select(exprs)
    out = (
        base.with_columns((pl.col("state2") + pl.col("county3")).alias("county_fips"))
            .select(["county_fips", "cbsa", "cbsa_title"])
            .unique()
    )

    write_parquet(out, OUT / "crosswalk_cbsa.parquet")
    print(f"crosswalk_cbsa.parquet | rows={out.height} | unique_cbsa={out.select('cbsa').n_unique()}")

if __name__ == "__main__":
    main()
