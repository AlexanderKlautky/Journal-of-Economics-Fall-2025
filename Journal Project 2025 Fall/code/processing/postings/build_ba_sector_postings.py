import pandas as pd


path_in = "/Users/alexanderklautky/Journal_Project_2025/data_raw/job_postings_by_sector_US.csv"
path_out = "/Users/alexanderklautky/Journal_Project_2025/data_raw/job_postings_ba_daily_q.csv"


df = pd.read_csv(path_in)
print("Rows in original file:", len(df))


sectors_to_drop = [
    "Loading & Stocking",
    "Sports",
    "Community and Social Services",
    "Cleaning & Sanitation",
    "Retail",
    "Security & Public Safety",
    "Installation & maintenance",
    "Hospitality & Tourism",
    "Food Preparation & Services",
    "Driving",
    "Customer Services",
    "Construction",
    "Childcare",
]

drop_lower = {s.lower() for s in sectors_to_drop}


mask_drop = df["display_name"].str.lower().isin(drop_lower)
print("Rows to drop:", mask_drop.sum())

df_keep = df[~mask_drop].copy()
print("Rows after dropping sectors:", len(df_keep))


mask_us_new = (df_keep["jobcountry"] == "US") & (
    df_keep["variable"].str.lower() == "new postings"
)
df_keep = df_keep[mask_us_new].copy()
print("Rows after keeping US + 'new postings':", len(df_keep))


daily = (
    df_keep
    .groupby("date", as_index=False)["indeed_job_postings_index"]
    .mean()
    .rename(columns={"indeed_job_postings_index": "ba_postings_index"})
)

print("Unique dates after grouping:", len(daily))


daily["date"] = pd.to_datetime(daily["date"])
daily["year"] = daily["date"].dt.year
daily["quarter"] = daily["date"].dt.quarter

print("\nHead of final daily dataframe:")
print(daily.head())


daily.to_csv(path_out, index=False)
print("\nFinal daily file written to:", path_out)
