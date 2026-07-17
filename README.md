[README.md](https://github.com/user-attachments/files/30111409/README.md)
# A Quantitative Analysis of the Post-COVID-19 Labor Market for Recent Graduates

Replication materials for this papers analysis of the U.S. labor market for recent college graduates after COVID-19.

The project builds a CBSA-by-quarter panel for 2020-2024 from CPS microdata, Indeed Hiring Lab postings, OEWS employment data, IPEDS completion flows, and JOLTS controls. The paper uses the panel to describe unemployment, underemployment, postings, vacancy rates, BA supply, and matching efficiency, then estimates Bartik-instrumented demand models.

The analysis is designed around 261 CBSAs observed over 20 quarters.

## Repository contents

- `code/` - Python data-construction pipeline and R analysis.
- `requirements.txt` - Python dependencies.
- `CODEBOOK_panel.csv` and `README_panel.md` - panel documentation.
- `data_raw/`, `data_int/`, and `data_final/` - expected local data directories. These data directories are not included in this repository by default.

## 1. Python data pipeline

Install the Python dependencies before running the pipeline:

    python -m pip install -r requirements.txt

The Python scripts create and validate the analysis panel. Run source-specific scripts only after placing the corresponding raw files in `data_raw/`.

### Source acquisition

- `code/downloads/download_cps_basic_monthly.py` downloads CPS Basic Monthly files.
- `code/downloads/download_ipeds_completions.py` downloads IPEDS completions files.

### Shared geography

- `code/processing/reference/xwalk_prep.py` prepares the county-to-CBSA crosswalk used by the CPS and IPEDS steps.

### CPS measures

- `code/processing/cps/cps_build_unemp.py` constructs the principal BA 22-27 unemployment measure.
- `code/processing/cps/cps_build_unemp_rate_cbsa_q.py` creates the CBSA-quarter unemployment-rate file.
- `code/processing/cps/cps_build_controls_51_54.py` creates additional unemployment controls and demographic measures.
- `code/processing/cps/build_u_22_27_nonba.py` and `build_u_28_35_ba.py` create placebo/control groups.

### Postings, supply, and demand shifters

- `code/processing/postings/postings_build.py` builds the CBSA-quarter postings series.
- `code/processing/postings/filter_sector_postings.py` and `build_ba_sector_postings.py` prepare the BA-oriented postings series.
- `code/processing/ipeds/ipeds_build_flows.py` builds annual CBSA BA-completion flows.
- `code/processing/oews/oews_build_bartik.py` builds the OEWS Bartik demand shifter.
- `code/processing/oews/build_bartik_entry.py` creates the entry-oriented Bartik measure.
- `code/processing/jolts/jolts_build_controls.py` builds the national JOLTS openings-rate control.

### Derived measures and panel assembly

- `code/processing/derived/build_ba_vacancy_rate_cbsa_q.py` creates the young-graduate vacancy-rate proxy.
- `code/processing/derived/build_d_entry_idx` builds the entry-level postings index.
- `code/panel/build_panel.py` creates the master panel.
- `code/panel/patch_panel_bartik.py`, `patch_panel_bartik_nonba.py`, and `build_acs_denominator_and_patch.py` add panel variables.
- `code/panel/finalize_panel_and_qc.py` finalizes the panel and performs checks.

### Quality control and documentation

- `code/quality_control/qc_master_panel.py` and `qc_master_panel_polars.py` test panel keys, coverage, missingness, and variable structure.
- `code/quality_control/audit_panel_rowcount_and_bottleneck.py` diagnoses merge-related row loss.
- `code/documentation/make_codebook.py` and `emit_codebook_from_panel.py` generate panel documentation.

### Suggested execution order

1. Download and place raw source data in `data_raw/`.
2. Prepare the crosswalk.
3. Build CPS, postings, IPEDS, JOLTS, and OEWS source files.
4. Build derived vacancy, entry, and Bartik measures.
5. Build and finalize `data_final/panel_cbsa_q.parquet`.
6. Run the QC and codebook scripts.

Some scripts use project-specific absolute paths. Before running them in a new environment, set those paths to your local project root or preserve the expected `data_raw/`, `data_int/`, and `data_final/` structure.

## 2. R analysis and final joins

The R analysis is in `code/Journal_Project_2025_R_Code.Rmd`.

It should be run after the Python pipeline has produced the final panel. The R Markdown file:

1. Loads the master panel.
2. Joins CPS unemployment-rate, vacancy-rate, and BA-postings files when available.
3. Produces descriptive time-series and quartile figures.
4. Builds a panel codebook.
5. Estimates baseline OLS models, first-stage diagnostics, lagged and cumulative demand specifications, robustness checks, and placebo analyses.

The paper's main empirical design is a CBSA and quarter-by-year fixed-effects 2SLS model. It instruments cumulative lagged postings demand with four lags of the OEWS Bartik demand shifter, clusters standard errors by CBSA, and uses young non-BA unemployment as a control in the principal unemployment specification.

The R workflow also contains the matching-function analysis based on CPS unemployment-to-employment flows and unemployment stocks. This supports the paper's estimate of the matching elasticity and quarter-level relative matching efficiency.

## Data availability and replication

Raw CPS, Indeed, OEWS, IPEDS, and JOLTS inputs may be subject to source-specific access terms or redistribution restrictions. Do not upload restricted raw data without confirming the applicable terms.

For a public replication package, include:

- this code;
- `requirements.txt`;
- the R Markdown analysis;
- `CODEBOOK_panel.csv` and `README_panel.md`;
- a permitted final analysis dataset or a small synthetic/sample version; and
- clear instructions for obtaining any raw inputs that cannot be redistributed.

## Reference

Klautky, Alexander. *A Quantitative Analysis of the Post-COVID-19 Labor Market for Recent Graduates*. CWRU Journal of Economics, Volume IV.
