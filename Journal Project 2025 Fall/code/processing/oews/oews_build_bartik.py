
from __future__ import annotations
import pathlib, re, zipfile, shutil, atexit, io, csv
import polars as pl
from xlsx2csv import Xlsx2csv

BASE = pathlib.Path.home() / "Journal_Project_2025"
RAW  = BASE / "data_raw" / "oews_raw"
OUT  = BASE / "data_final" / "oews_bartik_cbsa_y.parquet"

CBSA_TAG, STATE_TAG, US_TAG = "M", "S", "N"
NULLS = {"", " ", ".", "NA", "N/A", "*", "**", "#", "—", "–", "-"}

TMP = RAW / "_tmp_oews_fast"
TMP.mkdir(exist_ok=True)
atexit.register(lambda: shutil.rmtree(TMP, ignore_errors=True))

def log(m: str) -> None:
    print(m, flush=True)

def empty_bartik() -> pl.DataFrame:
        return pl.DataFrame(
        [],
        schema={"cbsa": pl.Utf8, "year": pl.Int64, "bartik_demand": pl.Float64}
    )


def infer_year(name: str) -> int | None:
    m = re.search(r"(20\d{2})", name)
    if m: return int(m.group(1))
    m2 = re.search(r"oesm(\d{2})all", name.lower())
    return 2000 + int(m2.group(1)) if m2 else None

def xlsx_bytes_from_zip(z: pathlib.Path) -> bytes | None:
    with zipfile.ZipFile(z) as Z:
        xs = [n for n in Z.namelist() if n.lower().endswith(".xlsx")]
        if not xs: return None
        xs.sort(key=lambda n: Z.getinfo(n).file_size, reverse=True)
        return Z.read(xs[0])

def sheet_csv(xbytes: bytes, sid: int) -> str:
    tmp = TMP / f"s{sid}.xlsx"
    tmp.write_bytes(xbytes)
    try:
        buf = io.StringIO()
        Xlsx2csv(str(tmp), outputencoding="utf-8", sheetid=sid).convert(buf)
        return buf.getvalue()
    finally:
        tmp.unlink(missing_ok=True)


HDR = {
    "occupation_code": {"occ_code", "o_code", "occupation_code", "occ code", "occ"},
    "area_code": {"area_code", "area"},
    "area_type": {"area_type", "areatype", "areatype_code", "area type"},
    "tot_emp": {"tot_emp", "total_employment", "emp_total", "employment", "total employment"},
}
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def find_header(csv_text: str):
    rows = list(csv.reader(io.StringIO(csv_text)))
    for i, row in enumerate(rows[:400]):
        toks = [norm(x) for x in row]
        if not any(toks): continue
        idx = {}
        for canon, syns in HDR.items():
            hit = None
            for j, t in enumerate(toks):
                if t in syns:
                    hit = j; break
            if hit is None:
                idx = None; break
            idx[canon] = hit
        if idx: return i, idx
    return None, None

def derive_areatype(area_type: str, area_code: str) -> str | None:
\
\
\
\
\
\
       
    t = norm(area_type or "")
    ac = (area_code or "").strip()

    
    if t in {"n","nat","nation","national","u.s.","us"} or t.startswith("nation"):
        return US_TAG
    if ac.upper().startswith("US") or re.fullmatch(r"0+", ac):
        return US_TAG

    
    if re.fullmatch(r"\d{5}", ac):
        return CBSA_TAG

    
    if re.fullmatch(r"[A-Z]{2}", ac) or re.fullmatch(r"\d{2}", ac) or re.fullmatch(r"\d{2}000", ac):
        return STATE_TAG

    
    if "state" in t:
        return STATE_TAG
    if "metro" in t or "cbsa" in t:
        return CBSA_TAG

    return None

def parse_after_header(csv_text: str, hdr_line: int, idx: dict, year: int) -> pl.DataFrame:
    out = {"areatype_code": [], "area_code": [], "occupation_code": [], "year": [], "emp": []}
    rdr = csv.reader(io.StringIO(csv_text))
    for _ in range(hdr_line + 1):
        try: next(rdr)
        except StopIteration: return pl.DataFrame(out)
    for row in rdr:
        if not row: continue
        if max(idx.values()) >= len(row): continue
        occ = row[idx["occupation_code"]].strip()
        area = row[idx["area_code"]].strip()
        typ  = row[idx["area_type"]].strip()
        empv = row[idx["tot_emp"]].strip()
        
        if norm(occ) in HDR["occupation_code"]: continue
        if norm(area) in HDR["area_code"]: continue
        at = derive_areatype(typ, area)
        if at is None: continue
        if empv in NULLS: emp = None
        else:
            val = re.sub(r"[,\s]", "", empv)
            try: emp = float(val)
            except: emp = None
        out["areatype_code"].append(at)
        out["area_code"].append(area)
        out["occupation_code"].append(occ)
        out["year"].append(int(year))
        out["emp"].append(emp)
    return pl.DataFrame(out)


def load_one(z: pathlib.Path) -> pl.DataFrame:
    year = infer_year(z.name)
    if not year:
        log(f"[WARN] {z.name}: cannot infer year; skip")
        return pl.DataFrame({"areatype_code":[],"area_code":[],"occupation_code":[],"year":[],"emp":[]})

    xb = xlsx_bytes_from_zip(z)
    if xb is None:
        log(f"[WARN] {z.name}: no xlsx in zip")
        return pl.DataFrame({"areatype_code":[],"area_code":[],"occupation_code":[],"year":[],"emp":[]})

    
    try:
        txt1 = sheet_csv(xb, 1)
        h1, idx1 = find_header(txt1)
        if h1 is not None:
            df1 = parse_after_header(txt1, h1, idx1, year)
            if df1.height:
                log(f"[INFO] {z.name}: sheet 1 -> {df1.height} rows (fast)")
                return df1
    except Exception:
        pass

        best = None; best_sid = None
    for sid in (2,3,4,5):
        try:
            txt = sheet_csv(xb, sid)
        except Exception:
            continue
        h, idx = find_header(txt)
        if h is None: continue
        df = parse_after_header(txt, h, idx, year)
        if best is None or df.height > best.height:
            best, best_sid = df, sid
    if best is None or best.is_empty():
        log(f"[WARN] {z.name}: no usable sheet; skip")
        return pl.DataFrame({"areatype_code":[],"area_code":[],"occupation_code":[],"year":[],"emp":[]})

    log(f"[INFO] {z.name}: sheet {best_sid} -> {best.height} rows")
    return best


def build_from_xlsx() -> pl.DataFrame:
    zips = sorted(RAW.glob("oesm*all.zip"))
    frames = [load_one(z) for z in zips]
    frames = [f for f in frames if f.height]
    if not frames:
        log("[ERROR] XLSX produced 0 rows.")
        return empty_bartik()

    df = pl.concat(frames, how="vertical_relaxed")


    cbsa_occ = (
        df.filter(pl.col("areatype_code") == CBSA_TAG)
          .rename({"area_code": "cbsa"})
          .filter(pl.col("occupation_code") != "00-0000")
          .group_by(["cbsa","occupation_code","year"])
          .agg(pl.col("emp").fill_null(0.0).sum().alias("emp"))
    )
    years = sorted(cbsa_occ.select("year").unique().to_series().to_list())
    if len(years) < 2:
        log(f"[ERROR] need ≥2 CBSA years; have {years}")
        return empty_bartik()

    base = 2019 if 2019 in years else max(years)
    log(f"[DIAG] baseline year for shares: {base}")

    cbsa_base = cbsa_occ.filter(pl.col("year") == base)
    shares = (
        cbsa_base.group_by("cbsa").agg(pl.col("emp").sum().alias("emp_total"))
                 .join(cbsa_base, on="cbsa")
                 .with_columns((pl.col("emp") / pl.col("emp_total")).alias("share"))
                 .select(["cbsa","occupation_code","share"])
    )


    us_occ = (
        df.filter(pl.col("areatype_code") == US_TAG)
          .filter(pl.col("occupation_code") != "00-0000")
          .group_by(["occupation_code","year"])
          .agg(pl.col("emp").fill_null(0.0).sum().alias("emp_us"))
          .sort(["occupation_code","year"])
          .with_columns(((pl.col("emp_us")/pl.col("emp_us").shift(1))-1.0)
                        .over("occupation_code").alias("shock"))
          .drop_nulls(subset=["shock"])
          .select(["occupation_code","year","shock"])
    )


    if us_occ.is_empty():
        log("[WARN] no explicit national rows; aggregating states for national shocks")
        us_occ = (
            df.filter(pl.col("areatype_code") == STATE_TAG)
              .filter(pl.col("occupation_code") != "00-0000")
              .group_by(["occupation_code","year"])
              .agg(pl.col("emp").fill_null(0.0).sum().alias("emp_us"))
              .sort(["occupation_code","year"])
              .with_columns(((pl.col("emp_us")/pl.col("emp_us").shift(1))-1.0)
                            .over("occupation_code").alias("shock"))
              .drop_nulls(subset=["shock"])
              .select(["occupation_code","year","shock"])
        )
        if us_occ.is_empty():
            log("[ERROR] still no national shocks after state aggregation")
            return empty_bartik()


    bartik = (
        us_occ.join(shares, on="occupation_code", how="inner")
              .with_columns((pl.col("share")*pl.col("shock")).alias("contrib"))
              .group_by(["cbsa","year"])
              .agg(pl.col("contrib").sum().alias("bartik_demand"))
              .with_columns(
                  pl.col("cbsa").cast(pl.Utf8),
                  pl.col("year").cast(pl.Int64),
                  pl.col("bartik_demand").cast(pl.Float64),
              )
              .select(["cbsa","year","bartik_demand"])
              .sort(["cbsa","year"])
    )
    return bartik

def main():
    bartik = build_from_xlsx()

    try:
        bartik.write_parquet(OUT)
        log(f"Wrote: {OUT} | rows={bartik.height}")
        if bartik.height:
            span = bartik.select(pl.min("year").alias("min"), pl.max("year").alias("max")).to_dicts()[0]
            n = bartik.select(pl.col("cbsa").n_unique()).item()
            log(f"Years: {span} | CBSAs: {n}")
    except Exception as e:
        log(f"[FATAL] write_parquet failed ({e}); writing typed-empty instead")
        empty_bartik().write_parquet(OUT)
        log(f"Wrote (empty): {OUT}")

if __name__ == "__main__":
    main()
