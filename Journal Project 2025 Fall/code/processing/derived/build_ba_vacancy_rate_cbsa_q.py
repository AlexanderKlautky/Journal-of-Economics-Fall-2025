

import polars as pl
from pathlib import Path


BASE = Path.home() / "Journal_Project_2025"
PANEL_PATH = BASE / "data_final" / "panel_cbsa_q.parquet"
CPS_PATH   = BASE / "data_final" / "cps_unemp_rate_cbsa_q_22_27_ba.parquet"
OUT_PATH   = BASE / "data_final" / "ba_vacancy_rate_cbsa_q.parquet"


panel = pl.read_parquet(PANEL_PATH)

print("Columns in panel containing 'posting':")
print([c for c in panel.columns if "posting" in c.lower()])
print()


POSTINGS_COL = "postings_level"   

if POSTINGS_COL not in panel.columns:
    raise ValueError(
        f"Column {POSTINGS_COL!r} not found in panel.\n"
        "If you later have a BA-specific postings count (e.g. 'ba_postings_level'), "
        "change POSTINGS_COL to that name."
    )

panel_post = (
    panel
    .select("cbsa", "year", "quarter", POSTINGS_COL)
    .with_columns(
        cbsa    = pl.col("cbsa").cast(pl.Int64),
        year    = pl.col("year").cast(pl.Int64),
        quarter = pl.col("quarter").cast(pl.Int64),
    )
)


cps = pl.read_parquet(CPS_PATH)

cps_lf = (
    cps
    .select("gtcbsa", "hryear4", "quarter", "lf_w", "un_w")
    .with_columns(
        cbsa    = pl.col("gtcbsa").cast(pl.Int64),
        year    = pl.col("hryear4").cast(pl.Int64),
        quarter = pl.col("quarter").cast(pl.Int64),
    )
    .select("cbsa", "year", "quarter", "lf_w", "un_w")
)


joined = (
    panel_post
    .join(cps_lf, on=["cbsa", "year", "quarter"], how="inner")
)


joined = (
    joined

    .with_columns(
        emp_ba = pl.col("lf_w") - pl.col("un_w")
    )


    .with_columns(
        v_rate_postings_ba22_27 = pl.when(
            (pl.col(POSTINGS_COL) + pl.col("emp_ba")) > 0
        )
        .then(
            pl.col(POSTINGS_COL) /
            (pl.col(POSTINGS_COL) + pl.col("emp_ba"))
        )
        .otherwise(None)
    )
    .select("cbsa", "year", "quarter", "v_rate_postings_ba22_27")
    .unique(subset=["cbsa", "year", "quarter"], keep="first")
    .sort(["cbsa", "year", "quarter"])
)

print("Preview of BA vacancy-rate proxy data:")
print(joined.head(10))

joined.write_parquet(OUT_PATH)
print(f"\nWrote BA vacancy-rate file to: {OUT_PATH}")

