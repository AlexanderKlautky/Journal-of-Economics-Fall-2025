# panel_cbsa_q — Codebook

- **Grain:** CBSA × year × quarter
- **Rows:** 5393 | **CBSAs:** 261 | **Years:** 2019–2024
- **Unique key:** (cbsa, year, quarter) — PASS

## Variables
- **cbsa** (*String*) — CBSA code (5-digit) used as region id
- **year** (*Int64*) — Calendar year
- **quarter** (*Int64*) — Quarter of year (1–4)
- **u_22_27_ba** (*Float64*) — CPS unemployment rate, ages 22–27 with BA (quarterly, CBSA)
- **lf_w** (*Float64*) — CPS weighted labor-force size for the 22–27 BA group (optional)
- **flow_ba** (*Int64*) — IPEDS BA completions (annual, CBSA); repeated across quarters
- **flow_ba_l1** (*Int64*) — 
- **flow_ba_l2** (*Int64*) — 
- **jolts_openings_rate_us** (*Float64*) — US job openings rate (seasonally adjusted), repeated across CBSAs
- **bartik_demand** (*Float64*) — OEWS Bartik demand shifter (annual, CBSA); repeated across quarters
- **postings_index** (*Float64*) — Indeed metro postings, daily SA index averaged to quarter
- **postings_level** (*Float64*) — 1 + postings_index/100, averaged to quarter
- **n_days** (*UInt32*) — Count of daily observations contributing to the quarter’s postings average
- **log_postings** (*Float64*) — ln(postings_level), quarterly; endogenous regressor for IV

## Consistency notes
- `flow_ba` constant within CBSA-year: YES
- `bartik_demand` constant within CBSA-year: YES
- `jolts_openings_rate_us` uniform within year-quarter (national series): YES

## Units & interpretation
- `u_22_27_ba` and `jolts_openings_rate_us` are rates (your JOLTS currently in percent; scale/interpret accordingly).
- `postings_index` is the Indeed SA index (Δ% vs 2020-02-01) averaged to quarter; `log_postings = ln(1+index/100)`.
- `flow_ba` and `bartik_demand` are annual and are repeated across quarters.

## Merge logic (recap)
- CPS provides the skeleton CBSA×quarter grid.
- JOLTS joined on (year, quarter); IPEDS and Bartik on (cbsa, year); postings on (cbsa, year, quarter).

## IV window
- For IV with postings, restrict to **2020–2024** where postings exist.

