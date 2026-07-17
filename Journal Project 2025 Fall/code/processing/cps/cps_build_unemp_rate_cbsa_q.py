import os, glob, zipfile, pathlib
import polars as pl


BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW_ROOT = BASE / "data_raw" / "cps_basic_monthly_raw"
OUT_DIR = BASE / "data_final"
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = list(range(2019, 2025))


AGE_COL    = "prtage"
EDU_COL    = "peeduca"
LF_COL     = "pemlr"      
WEIGHT_COL = "pwcmpwgt"   
CBSA_COL   = "gtcbsa"
YEAR_COL   = "hryear4"
MONTH_COL  = "hrmonth"

NEEDED_COLS = [AGE_COL, EDU_COL, LF_COL, WEIGHT_COL, CBSA_COL, YEAR_COL, MONTH_COL]


def month_to_quarter_expr(col: str = MONTH_COL) -> pl.Expr:
                                                   
    return (((pl.col(col).cast(pl.Int64) - 1) // 3) + 1).cast(pl.Int8)


def add_lf_un_flags(df: pl.DataFrame) -> pl.DataFrame:
\
\
\
\
       
    code = pl.col(LF_COL).cast(pl.Int64, strict=False)
    return df.with_columns(
        lf = code.is_between(1, 4),
        un = code.is_between(3, 4)
    )


def read_cps_zip(path: pathlib.Path) -> pl.DataFrame:
                                                                          
    with zipfile.ZipFile(path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV in {path}")
        name = csv_names[0]
        with zf.open(name) as f:
            df = pl.read_csv(f, infer_schema_length=0)


    df = df.rename({c: c.lower() for c in df.columns})
    missing = [c for c in NEEDED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns {missing} in {path.name}")
    return df.select(NEEDED_COLS)


def build_unemp_cbsa_q() -> pl.DataFrame:
\
\
\
\
\
       
    dfs = []

    for year in YEARS:
        year_dir = RAW_ROOT / str(year)
        paths = sorted(year_dir.glob("cpsb*_csv.zip"))
        if not paths:
            print(f"WARNING: no cpsb*_csv.zip in {year_dir}")
            continue

        for path in paths:
            print(f"Reading {path} ...")
            df = read_cps_zip(path)


            df = df.filter(
                (pl.col(AGE_COL).cast(pl.Int64).is_between(22, 27)) &
                (pl.col(EDU_COL).cast(pl.Int64) >= 39) &   
                (pl.col(WEIGHT_COL).cast(pl.Float64) > 0)
            )


            df = df.filter(pl.col(CBSA_COL).cast(pl.Int64) > 0)

            if df.is_empty():
                continue


            df = df.with_columns(
                quarter = month_to_quarter_expr(),
            )
            df = add_lf_un_flags(df)

            dfs.append(df)

    if not dfs:
        raise RuntimeError("No CPS rows after filtering – check paths/filters.")

    all_df = pl.concat(dfs, how="vertical_relaxed")


    all_df = all_df.with_columns(
        w = pl.col(WEIGHT_COL).cast(pl.Float64)
    )

    out = (
        all_df
        .group_by([CBSA_COL, YEAR_COL, "quarter"])
        .agg(
            lf_w = (pl.col("w") * pl.col("lf").cast(pl.Float64)).sum(),
            un_w = (pl.col("w") * pl.col("un").cast(pl.Float64)).sum()
        )
        .with_columns(
            u_rate_22_27_ba = pl.when(pl.col("lf_w") > 0)
                                .then(pl.col("un_w") / pl.col("lf_w"))
                                .otherwise(None)
        )
        .sort([CBSA_COL, YEAR_COL, "quarter"])
    )
    return out


def main():
    out = build_unemp_cbsa_q()
    out_path = OUT_DIR / "cps_unemp_rate_cbsa_q_22_27_ba.parquet"
    out.write_parquet(out_path)
    print(f"Wrote {out.height} CBSA x year x quarter rows to {out_path}")
    print(out.head(10))


if __name__ == "__main__":
    main()
