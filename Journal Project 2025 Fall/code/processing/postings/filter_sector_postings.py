import pandas as pd


path_in = "/Users/alexanderklautky/Journal_Project_2025/data_raw/job_postings_by_sector_US.csv"
path_out = "/Users/alexanderklautky/Journal_Project_2025/data_raw/job_postings_by_sector_US_filtered.csv"


df = pd.read_csv(path_in)


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


mask = df["display_name"].isin(sectors_to_drop)
print("Rows before:", len(df))
print("Rows to drop:", mask.sum())

df_filtered = df[~mask].copy()
print("Rows after:", len(df_filtered))


df_filtered.to_csv(path_out, index=False)
print("Filtered file written to:", path_out)
