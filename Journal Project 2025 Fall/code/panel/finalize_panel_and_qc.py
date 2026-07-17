

from __future__ import annotations
import pathlib, sys
import polars as pl

BASE = pathlib.Path("/Users/alexanderklautky/Journal_Project_2025")
FIN  = BASE / "data_final"


CPS   = FIN / "cps_u_22_27_ba_cbsa_q.parquet"       
JOLTS = FIN / "jolts_us_openings_rate_q.parquet"    
IPEDS = FIN / "ipeds_ba_flow_cbsa_y.parquet"        
BART  = FIN / "oews_bartik_cbsa_y.parquet"          
POST  = FIN / "postings_cbsa_q.parquet"             

PANEL = FIN / "panel_cbsa_q.parquet"
PANEL_CSV = FIN / "panel_cbsa_q.csv"
README = FIN / "README_panel.md"

def must_exist(p: pathlib.Path, name: str) -> pl.DataFrame:
    if not p.exists():
        print(f"[ERR] Missing required source: {name} -> {p}")
        sys.exit(1)
    try:
        return pl.read_parquet(p)
    except Exception as e:
        print(f"[ERR] Failed to read {name}: {e}")
        sys.exit(1)

def build_panel() -> pl.DataFrame:
    print("\n=== BUILD: assembling panel from sources ===")
    cps = must_exist(CPS, "CPS")
    
    skel = cps.select(["cbsa","year","quarter"]).unique().sort(["cbsa","year","quarter"])

    df = skel.join(
        cps.select(["cbsa","year","quarter","u_22_27_ba"] + [c for c in cps.columns if c=="lf_w"]),
        on=["cbsa","year","quarter"], how="left"
    )

    if JOLTS.exists():
        jol = must_exist(JOLTS, "JOLTS").select(["year","quarter","jolts_openings_rate_us"])
        df = df.join(jol, on=["year","quarter"], how="left")
    else:
        print("[WARN] JOLTS file missing; proceeding without it.")

    if IPEDS.exists():
        ip = must_exist(IPEDS, "IPEDS").select([c for c in ["cbsa","year","flow_ba","flow_ba_11","flow_ba_12"] if c in pl.read_parquet(IPEDS).columns])
        df = df.join(ip, on=["cbsa","year"], how="left")
    else:
        print("[WARN] IPEDS file missing; proceeding without it.")

    if BART.exists():
        bt = must_exist(BART, "BARTIK").select(["cbsa","year","bartik_demand"])
        df = df.join(bt, on=["cbsa","year"], how="left")
    else:
        print("[WARN] Bartik file missing; proceeding without it.")

    if POST.exists():
        po = must_exist(POST, "POSTINGS").select(["cbsa","year","quarter","postings_index","postings_level","log_postings","n_days"] if "n_days" in pl.read_parquet(POST).columns else ["cbsa","year","quarter","postings_index","postings_level","log_postings"])
        df = df.join(po, on=["cbsa","year","quarter"], how="left")
    else:
        print("[WARN] Postings parquet missing; proceeding without it.")

    df = df.sort(["cbsa","year","quarter"])
    df.write_parquet(PANEL)
    df.write_csv(PANEL_CSV)
    print(f"[OK] Wrote panel: {PANEL}  | shape={df.shape}")
    return df

def qc_panel(df: pl.DataFrame) -> None:
    print("\n=== QC: core keys & presence ===")
    req = ["cbsa","year","quarter","u_22_27_ba","jolts_openings_rate_us","flow_ba","bartik_demand","log_postings"]
    missing_cols = [c for c in req if c not in df.columns]
    print("required columns present:", len(missing_cols)==0, "| missing:", missing_cols)

    dups = df.group_by(["cbsa","year","quarter"]).len().filter(pl.col("len")>1).height
    print("unique key (cbsa,year,quarter):", "PASS" if dups==0 else f"FAIL (dups={dups})")

    print("\n=== QC: ranges & uniformity ===")
    for col in ["u_22_27_ba","jolts_openings_rate_us"]:
        if col in df.columns:
            mm = df.select(pl.min(col).alias("min"), pl.max(col).alias("max")).to_dicts()[0]
            in01 = (mm["min"] >= -1e-9) and (mm["max"] <= 1+1e-9)
            print(f"{col} min/max = {mm}  | in [0,1]: {in01}")

    
    for col in ["flow_ba","flow_ba_11","flow_ba_12","bartik_demand"]:
        if col in df.columns:
            nvar = df.group_by(["cbsa","year"]).agg(pl.col(col).n_unique().alias("k")).filter(pl.col("k")>1).height
            print(f"{col} constant within cbsa-year:", "PASS" if nvar==0 else f"FAIL ({nvar} groups vary)")

    
    if "jolts_openings_rate_us" in df.columns:
        nj = df.group_by(["year","quarter"]).agg(pl.col("jolts_openings_rate_us").n_unique().alias("k")).filter(pl.col("k")>1).height
        print("jolts uniform within year-quarter:", "PASS" if nj==0 else f"FAIL ({nj} year-q vary)")

    print("\n=== QC: missingness (counts) ===")
    for col in ["u_22_27_ba","jolts_openings_rate_us","flow_ba","bartik_demand","log_postings"]:
        if col in df.columns:
            n = df.select(pl.col(col).is_null().sum().alias("na")).item()
            print(f"{col}: NA={n}")

    print("\n=== QC: coverage & IV preflight ===")
    yrs = df.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    n_cbsa = df.select(pl.col("cbsa").n_unique()).item()
    print(f"year span: {yrs} | CBSAs: {n_cbsa} | rows: {df.height}")

    iv_df = df.filter(pl.all_horizontal([
        ~pl.col("u_22_27_ba").is_null(),
        ~pl.col("bartik_demand").is_null(),
        ~pl.col("log_postings").is_null()
    ]))
    print("IV-usable rows:", iv_df.height, "| IV CBSAs:", iv_df.select(pl.col("cbsa").n_unique()).item())
    if iv_df.height > 0:
        try:
            c = iv_df.select(pl.corr("log_postings","bartik_demand").alias("corr")).item()
            print(f"corr(bartik, log_postings) = {c:.3f} (unconditional diagnostic)")
        except Exception:
            pass

def write_readme(df: pl.DataFrame) -> None:
    print("\n=== WRITE: README_panel.md ===")
    yrs = df.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
    lines = [
        "# panel_cbsa_q codebook\n",
        "Grain: **CBSA × year × quarter**.\n",
        "## Variables\n",
        "- `cbsa` (str, 5-digit): Core-Based Statistical Area code.\n",
        "- `year` (int), `quarter` (int 1–4).\n",
        "- `u_22_27_ba` (float, 0–1): CPS unemployment rate for ages 22–27 with BA.\n",
        "- `jolts_openings_rate_us` (float, 0–1): US job openings rate (seasonally adjusted), replicated across CBSAs.\n",
        "- `flow_ba` (int/float): IPEDS BA completions aggregated to CBSA-year; **repeated across quarters**.\n",
        "- `bartik_demand` (float): OEWS Bartik demand shifter at CBSA-year; **repeated across quarters**.\n",
        "- `postings_index` (float, %Δ vs 2020-02-01): Indeed metro postings index (daily SA) averaged to quarter.\n",
        "- `postings_level` (float): `1 + postings_index/100` averaged to quarter.\n",
        "- `log_postings` (float): `ln(postings_level)` (quarterly mean level in logs) — used as endogenous regressor in IV.\n",
        "- `n_days` (int, optional): count of daily observations contributing to the quarter’s postings.\n",
        "\n## Time span\n",
        f"- CPS/JOLTS posted here; IPEDS flows 2017–2024; Bartik {yrs['min']}–{yrs['max']} as available; postings 2020–2024.\n",
        "\n## Notes\n",
        "- Annual series (`flow_ba`, `bartik_demand`) are constant within CBSA-year and repeated across quarters.\n",
        "- `log_postings` is computed from the quarterly mean of daily posting *levels* to stabilize the first stage.\n",
        "- Merge keys: CPS/JOLTS on `(year, quarter)`; IPEDS/Bartik on `(cbsa, year)`; postings on `(cbsa, year, quarter)`.\n",
    ]
    README.write_text("".join(lines))
    print(f"[OK] Wrote {README}")

def main():
    
    rebuild = (not PANEL.exists())
    if not rebuild:
        df0 = pl.read_parquet(PANEL)
        need = {"u_22_27_ba","jolts_openings_rate_us","flow_ba","bartik_demand","log_postings"}
        if not need.issubset(set(df0.columns)):
            print("[INFO] Existing panel missing required columns; rebuilding.")
            rebuild = True

    if rebuild:
        df = build_panel()
    else:
        print(f"[INFO] Using existing panel: {PANEL}")
        df = pl.read_parquet(PANEL)

    qc_panel(df)
    write_readme(df)

    
    sample = df.sample(fraction=0.1, with_replacement=False, shuffle=True, seed=42)
    sample_path = FIN / "panel_cbsa_q_sample10.csv"
    sample.write_csv(sample_path)
    print(f"[OK] Wrote sample CSV: {sample_path} | rows={sample.height}")

if __name__ == "__main__":
    main()
