
from __future__ import annotations
import pathlib, polars as pl

BASE = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
FIN  = BASE / "data_final"

PANEL = FIN / "panel_cbsa_q.parquet"
NONBA = FIN / "oews_bartik_nonba_cbsa_y.parquet"

def main():
    panel = pl.read_parquet(PANEL)
    b = pl.read_parquet(NONBA)


    out = panel.join(b, on=["cbsa","year"], how="left")
    out.write_parquet(PANEL)
    out.write_csv(FIN / "panel_cbsa_q.csv")


    miss = out.select(pl.col("bartik_nonba").is_null().sum().alias("nulls")).item()
    by_year = out.filter(pl.col("bartik_nonba").is_null()).group_by("year").len().sort("year")
    print("[CHECK] bartik_nonba null rows:", miss)
    print(by_year)

if __name__ == "__main__":
    main()
