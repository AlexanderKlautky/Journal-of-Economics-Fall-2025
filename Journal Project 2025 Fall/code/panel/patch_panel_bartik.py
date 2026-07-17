import pathlib, polars as pl

BASE = pathlib.Path.home() / "Journal_Project_2025"
PANEL_P = BASE / "data_final" / "panel_cbsa_q.parquet"
BARTIK_P = BASE / "data_final" / "oews_bartik_cbsa_y.parquet"
CSV_OUT  = BASE / "data_final" / "panel_cbsa_q.csv"

panel = pl.read_parquet(PANEL_P)
bartik = pl.read_parquet(BARTIK_P)


panel = panel.with_columns(
    pl.col("cbsa").cast(pl.Utf8, strict=False),
)
if "year" not in panel.columns:
    
    if "date" in panel.columns:
        panel = panel.with_columns(pl.col("date").cast(pl.Utf8).str.slice(0,4).cast(pl.Int64).alias("year"))
    elif "yyyymm" in panel.columns:
        panel = panel.with_columns(pl.col("yyyymm").cast(pl.Utf8).str.slice(0,4).cast(pl.Int64).alias("year"))
    else:
        raise RuntimeError("Could not find or derive year in panel.")
panel = panel.with_columns(pl.col("year").cast(pl.Int64, strict=False))

bartik = bartik.with_columns(
    pl.col("cbsa").cast(pl.Utf8, strict=False),
    pl.col("year").cast(pl.Int64, strict=False),
)


drop_cols = [c for c in panel.columns if c.startswith("bartik_demand")]
if drop_cols:
    panel = panel.drop(drop_cols)

out = (panel.join(bartik, on=["cbsa","year"], how="left")
            .with_columns(pl.col("bartik_demand").cast(pl.Float64, strict=False)))

print("[CHECK] rows:", out.height)
print("[CHECK] bartik null rows:", out.filter(pl.col("bartik_demand").is_null()).height)

out.write_parquet(PANEL_P)
out.write_csv(CSV_OUT)
print("Wrote:", PANEL_P)
print("Wrote:", CSV_OUT)
