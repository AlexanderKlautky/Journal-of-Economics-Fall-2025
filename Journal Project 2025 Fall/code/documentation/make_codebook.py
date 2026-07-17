

from __future__ import annotations
import pathlib, math
import polars as pl

BASE   = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
FINAL  = BASE / "data_final"
PANEL  = FINAL / "panel_cbsa_q.parquet"
CB_CSV = FINAL / "CODEBOOK_panel.csv"
README = FINAL / "README_panel.md"


DESC = {
    "cbsa": ("string", "CBSA code (5-digit) used as region id"),
    "year": ("int", "Calendar year"),
    "quarter": ("int", "Quarter of year (1–4)"),
    "u_22_27_ba": ("float (0–1)", "CPS unemployment rate, ages 22–27 with BA (quarterly, CBSA)"),
    "lf_w": ("float", "CPS weighted labor-force size for the 22–27 BA group (optional)"),
    "jolts_openings_rate_us": ("float (%, SA)", "US job openings rate (seasonally adjusted), repeated across CBSAs"),
    "flow_ba": ("int/float", "IPEDS BA completions (annual, CBSA); repeated across quarters"),
    "flow_ba_11": ("int/float", "IPEDS BA completions with 11-month alignment (optional)"),
    "flow_ba_12": ("int/float", "IPEDS BA completions with 12-month alignment (optional)"),
    "bartik_demand": ("float", "OEWS Bartik demand shifter (annual, CBSA); repeated across quarters"),
    "postings_index": ("float (Δ% vs 2020-02-01, SA)", "Indeed metro postings, daily SA index averaged to quarter"),
    "postings_level": ("float", "1 + postings_index/100, averaged to quarter"),
    "log_postings": ("float", "ln(postings_level), quarterly; endogenous regressor for IV"),
    "n_days": ("int", "Count of daily observations contributing to the quarter’s postings average"),
}

ANNUAL_CBSA = ["flow_ba", "flow_ba_11", "flow_ba_12", "bartik_demand"]
UNIFORM_YQ  = ["jolts_openings_rate_us"]

def summarize_numeric(col: str, df: pl.DataFrame) -> dict:
    s = df.select([
        pl.col(col).is_null().sum().alias("na"),
        pl.col(col).count().alias("n"),
        pl.col(col).mean().alias("mean"),
        pl.col(col).std().alias("sd"),
        pl.col(col).min().alias("min"),
        pl.col(col).max().alias("max"),
    ]).to_dicts()[0]

    for k, v in s.items():
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            s[k] = ""
    s["na_pct"] = "" if s["n"] in ("", 0) else round(100 * s["na"] / (s["na"] + s["n"]), 2)
    return s

def main():
    df = pl.read_parquet(PANEL)
    cols = df.columns


    yrs = df.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    n_cbsa = df.select(pl.col("cbsa").n_unique()).item()
    n_rows = df.height


    dups = df.group_by(["cbsa","year","quarter"]).len().filter(pl.col("len")>1).height
    annual_notes = {}
    for c in ANNUAL_CBSA:
        if c in cols:
            nvar = df.group_by(["cbsa","year"]).agg(pl.col(c).n_unique().alias("k")).filter(pl.col("k")>1).height
            annual_notes[c] = (nvar == 0)

    yq_notes = {}
    for c in UNIFORM_YQ:
        if c in cols:
            nv = df.group_by(["year","quarter"]).agg(pl.col(c).n_unique().alias("k")).filter(pl.col("k")>1).height
            yq_notes[c] = (nv == 0)


    rows = []
    for c in cols:
        dtype = str(df.schema[c])
        unit, desc = DESC.get(c, (dtype, ""))
        rec = {"name": c, "dtype": dtype, "unit_or_scale": unit, "description": desc}
        if df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt32, pl.UInt64):
            stats = summarize_numeric(c, df)
            rec.update(stats)
        else:
            na = df.select(pl.col(c).is_null().sum().alias("na")).item()
            n  = df.select(pl.col(c).count().alias("n")).item()
            rec.update({"na": na, "n": n, "na_pct": round(100*na/(na+n),2) if (na+n)>0 else ""})
        if c in annual_notes:
            rec["constant_within_cbsa_year"] = "YES" if annual_notes[c] else "NO"
        if c in yq_notes:
            rec["uniform_within_year_quarter"] = "YES" if yq_notes[c] else "NO"
        rows.append(rec)

    pl.DataFrame(rows).write_csv(CB_CSV)

  
    md = []
    md.append("# panel_cbsa_q — Codebook\n\n")
    md.append(f"- **Grain:** CBSA × year × quarter\n")
    md.append(f"- **Rows:** {n_rows} | **CBSAs:** {n_cbsa} | **Years:** {yrs['min']}–{yrs['max']}\n")
    md.append(f"- **Unique key:** (cbsa, year, quarter) — {'PASS' if dups==0 else 'FAIL: duplicates present'}\n")
    md.append("\n## Variables\n")
    for c in cols:
        unit, desc = DESC.get(c, (str(df.schema[c]), ""))
        md.append(f"- **{c}** (*{df.schema[c]}*) — {desc}\n")
    md.append("\n## Consistency notes\n")
    for c in ANNUAL_CBSA:
        if c in cols:
            md.append(f"- `{c}` constant within CBSA-year: {'YES' if annual_notes[c] else 'NO'}\n")
    for c in UNIFORM_YQ:
        if c in cols:
            md.append(f"- `{c}` uniform within year-quarter (national series): {'YES' if yq_notes[c] else 'NO'}\n")
    md.append("\n## Units & interpretation\n")
    md.append("- `u_22_27_ba` and `jolts_openings_rate_us` are rates (your JOLTS currently in percent; scale/interpret accordingly).\n")
    md.append("- `postings_index` is the Indeed SA index (Δ% vs 2020-02-01) averaged to quarter; `log_postings = ln(1+index/100)`.\n")
    md.append("- `flow_ba` and `bartik_demand` are annual and are repeated across quarters.\n")
    md.append("\n## Merge logic (recap)\n")
    md.append("- CPS provides the skeleton CBSA×quarter grid.\n")
    md.append("- JOLTS joined on (year, quarter); IPEDS and Bartik on (cbsa, year); postings on (cbsa, year, quarter).\n")
    md.append("\n## IV window\n")
    md.append("- For IV with postings, restrict to **2020–2024** where postings exist.\n")
    md.append("\n")
    README.write_text("".join(md))

    print(f"[OK] Wrote: {CB_CSV}")
    print(f"[OK] Wrote: {README}")

if __name__ == "__main__":
    main()
