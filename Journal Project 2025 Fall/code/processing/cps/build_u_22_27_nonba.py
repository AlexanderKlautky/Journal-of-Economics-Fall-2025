from __future__ import annotations
from pathlib import Path
import io, zipfile, re, sys
import polars as pl


def find_root() -> Path:
    cands = [Path(__file__).resolve().parent, Path.cwd(), Path.home()/"Journal_Project_2025"]
    for c in cands:
        cur = c
        for _ in range(6):
            if all((cur/p).exists() for p in ("data_raw","data_int","data_final")):
                return cur
            if cur.parent == cur: break
            cur = cur.parent
    raise FileNotFoundError("Need a project root with data_raw, data_int, data_final.")

ROOT   = find_root()
RAW    = ROOT/"data_raw"/"cps_basic_monthly_raw"
INT    = ROOT/"data_int"
FINAL  = ROOT/"data_final"
OUT_INT   = INT/"u_22_27_nonba_cbsa_q.parquet"
OUT_FINAL = FINAL/"panel_additions_cbsa_q.parquet"

print(f"[info] project_root: {ROOT}")
print(f"[info] CPS raw dir:   {RAW}")


def pick(cols: list[str], options: list[str]) -> str | None:
    low = {c.lower(): c for c in cols}
    for o in options:
        if o.lower() in low: return low[o.lower()]
    return None

def infer_year_month_from_name(name: str) -> tuple[int|None,int|None]:
    m = re.search(r"(\d{4})(\d{2})", name)
    if m: return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d{2})(\d{2})", name)
    if m2:
        yy, mm = int(m2.group(1)), int(m2.group(2))
        return 2000+yy, mm
    return None, None

def zfill5(expr: pl.Expr) -> pl.Expr:
    return expr.cast(pl.Utf8, strict=False).str.replace_all(r"\D","").str.zfill(5)

def read_csv_from_zip(zpath: Path) -> tuple[pl.DataFrame, str]:
    with zipfile.ZipFile(zpath) as Z:
        csv_members = [n for n in Z.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError("zip has no .csv inside")
        csv_members.sort(key=lambda n: Z.getinfo(n).file_size, reverse=True)
        with Z.open(csv_members[0]) as f:
            data = f.read()
    return pl.read_csv(io.BytesIO(data), infer_schema_length=50000, ignore_errors=True), csv_members[0]


def file_month_aggregate(zpath: Path) -> pl.DataFrame | None:
    try:
        df, inner = read_csv_from_zip(zpath)
    except Exception as e:
        print(f"[skip] {zpath.name}: {e}")
        return None

    cols = df.columns


    age   = pick(cols, ["PRTAGE","prtage","AGE","age"])
    educ  = pick(cols, ["PEEDUCA","peeduca","EDUC","educ"])
    stat  = pick(cols, ["PEMLR","pemlr"])
    wt    = pick(cols, ["WTFINL","wtfinl","PWSSWGT","pwsswgt","PWCNTWGT","pwcntwgt"])
    yearc = pick(cols, ["YEAR","year","HRYEAR4","hryear4"])
    monthc= pick(cols, ["MONTH","month","HRMONTH","hrmonth","PEMONTH","pemonth"])
    cbsa  = pick(cols, ["CBSA","cbsa","GTCBSACODE","gtcbsacode","GTCBSA","gtcbsa","METFIPS","metfips","MSA","msa"])


    yy, mm = infer_year_month_from_name(zpath.name) if (yearc is None or monthc is None) else (None, None)


    missing = [n for n,v in dict(age=age,educ=educ,stat=stat,wt=wt,cbsa=cbsa).items() if v is None]
    if missing:
        print(f"[skip] {zpath.name}: missing {missing}")
        return None

    df = df.select([c for c in [age,educ,stat,wt,yearc,monthc,cbsa] if c])

    if yearc is None and yy is not None:
        df = df.with_columns(pl.lit(int(yy)).alias("__year"));  yearc="__year"
    if monthc is None and mm is not None:
        df = df.with_columns(pl.lit(int(mm)).alias("__month")); monthc="__month"
    if yearc is None or monthc is None:
        print(f"[skip] {zpath.name}: no year/month info")
        return None

    df = df.with_columns(
        year    = pl.col(yearc).cast(pl.Int16, strict=False),
        quarter = ((pl.col(monthc).cast(pl.Int16, strict=False) - 1)//3 + 1).cast(pl.Int8),
        w       = pl.col(wt).cast(pl.Float64, strict=False),
        age_i   = pl.col(age).cast(pl.Int16, strict=False),
    )


    if educ.lower() == "peeduca":
        non_ba = (pl.col(educ).cast(pl.Int16, strict=False) < 43)
    else:
        non_ba = pl.when(pl.col(educ).cast(pl.Utf8).str.contains("Bachelor|Master|Prof|Doctor", literal=True, strict=False))\
                   .then(pl.lit(False)).otherwise(pl.lit(True))

    st = pl.col(stat).cast(pl.Int16, strict=False)
    in_lf = st.is_in([1,2,3,4]) & non_ba & pl.col("age_i").is_between(22,27)
    unemp = st.is_in([3,4])     & non_ba & pl.col("age_i").is_between(22,27)

    g = (df.with_columns(cbsa5 = zfill5(pl.col(cbsa)))
           .group_by(["cbsa5","year","quarter"])
           .agg([
               (pl.when(in_lf).then(pl.col("w")).otherwise(0.0)).sum().alias("lf_w"),
               (pl.when(unemp).then(pl.col("w")).otherwise(0.0)).sum().alias("u_w"),
           ])
           .with_columns(file = pl.lit(zpath.name))
           .select(["cbsa5","year","quarter","u_w","lf_w","file"])
        )
    print(f"[ok] {zpath.name}: aggregated {g.height} rows")
    return g


def build_all() -> pl.DataFrame:
    if not RAW.exists():
        raise FileNotFoundError(f"{RAW} not found.")
    zips = sorted(RAW.glob("**/*.zip"))
    if not zips:
        raise FileNotFoundError(f"No ZIPs under {RAW}")

    parts = []
    for z in zips:
        g = file_month_aggregate(z)
        if g is not None and g.height:
            parts.append(g)

    if not parts:
        raise RuntimeError("No CPS files produced aggregates (missing required columns).")

    m = pl.concat(parts, how="vertical_relaxed")
    q = (m.group_by(["cbsa5","year","quarter"])
           .agg([pl.col("u_w").sum().alias("u_w"), pl.col("lf_w").sum().alias("lf_w")])
           .with_columns(u_22_27_nonba = (pl.col("u_w")/pl.col("lf_w")).clip(0.0,1.0))
           .select([pl.col("cbsa5").alias("cbsa"), "year","quarter","u_22_27_nonba"])
           .sort(["cbsa","year","quarter"]))
    return q


def save_outputs(uq: pl.DataFrame):
    INT.mkdir(parents=True, exist_ok=True)
    FINAL.mkdir(parents=True, exist_ok=True)

    uq.write_parquet(OUT_INT)

    if OUT_FINAL.exists():
        add = pl.read_parquet(OUT_FINAL)


        drop_cols = [c for c in add.columns if c.endswith("_right") or c.endswith("_left")]
        if drop_cols:
            add = add.drop(drop_cols)

        if "cbsa" in add.columns and "cbsa" in uq.columns:
            tgt = add.schema["cbsa"]
            if uq.schema["cbsa"] != tgt:
                try:
                    uq = uq.with_columns(pl.col("cbsa").cast(tgt, strict=False))
                except Exception:
                    add = add.with_columns(pl.col("cbsa").cast(pl.Utf8, strict=False))
                    uq  = uq.with_columns(pl.col("cbsa").cast(pl.Utf8, strict=False))


        uq = uq.select(["cbsa","year","quarter","u_22_27_nonba"])

        out = (add.join(uq, on=["cbsa","year","quarter"], how="full", suffix="_r")
                 .unique(subset=["cbsa","year","quarter"], keep="last"))
        out.write_parquet(OUT_FINAL)
    else:

        uq.write_parquet(OUT_FINAL)

def main():
    uq = build_all()
    save_outputs(uq)
    print(f" u_22_27_nonba built: {uq.height:,} rows")
    print(f"   → {OUT_INT}")
    print(f"   → {OUT_FINAL} (created/updated)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[ERROR]", e)
        sys.exit(1)
