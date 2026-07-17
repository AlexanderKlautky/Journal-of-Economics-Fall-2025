

from __future__ import annotations
from pathlib import Path
import io, zipfile, re, csv, sys
import polars as pl
from xlsx2csv import Xlsx2csv

SHARE_BASE_YEAR = 2019
CBSA_TAG, STATE_TAG, US_TAG = "M", "S", "N"
NULLS = {"", " ", ".", "NA", "N/A", "*", "**", "#", "—", "–", "-"}


def find_root() -> Path:
    cands = [Path(__file__).resolve().parent, Path.cwd(), Path.home()/"Journal_Project_2025"]
    for c in cands:
        cur = c
        for _ in range(8):
            if all((cur/p).exists() for p in ("data_raw","data_int","data_final")):
                return cur
            if cur.parent == cur: break
            cur = cur.parent
    raise FileNotFoundError("Need data_raw/data_int/data_final.")
ROOT  = find_root()
RAW   = ROOT/"data_raw"/"oews_raw"
INT   = ROOT/"data_int"
FINAL = ROOT/"data_final"
OUT_INT   = INT/"bartik_entry_cbsa_q.parquet"
OUT_FINAL = FINAL/"panel_additions_cbsa_q.parquet"

print(f"[info] project_root: {ROOT}")


HDR = {
  "occupation_code": {"occ_code","o_code","occupation_code","occ code","occ"},
  "area_code": {"area_code","area"},
  "area_type": {"area_type","areatype","areatype_code","area type"},
  "tot_emp": {"tot_emp","total_employment","emp_total","employment","total employment"},
}
def norm(s: str) -> str: return re.sub(r"\s+"," ",(s or "").strip().lower())

def infer_year_from_name(name: str) -> int|None:
    m4 = re.search(r"\b(20\d{2})\b", name)
    if m4: return int(m4.group(1))
    m2 = re.search(r"(?i)oesm(\d{2})", name)
    if m2:
        yy = int(m2.group(1))
        return 2000 + yy
    return None

def infer_year_from_text(txt: str) -> int|None:
    for line in txt.splitlines()[:60]:
        m = re.search(r"\b(20\d{2})\b", line)
        if m: return int(m.group(1))
    return None

def clean_soc(x: str) -> str|None:
    d = re.sub(r"\D","", (x or ""));  return d[:6].rjust(6,"0") if d else None

def as_float(x: str) -> float|None:
    if x is None: return None
    x = x.strip()
    if x in NULLS: return None
    try: return float(x.replace(",",""))
    except: return None

def xlsx_bytes_from_zip(z: Path) -> bytes|None:
    with zipfile.ZipFile(z) as Z:
        xs = [n for n in Z.namelist() if n.lower().endswith(".xlsx")]
        if not xs: return None
        xs.sort(key=lambda n: Z.getinfo(n).file_size, reverse=True)
        return Z.read(xs[0])

def xlsx_sheet_to_csv_text(xbytes: bytes, sheetid: int) -> str:
    tmp = RAW/"_tmp_oews.xlsx"
    tmp.write_bytes(xbytes)
    try:
        buf = io.StringIO()
        Xlsx2csv(str(tmp), outputencoding="utf-8", sheetid=sheetid, dateformat="%Y-%m-%d").convert(buf)
        return buf.getvalue()
    finally:
        try: tmp.unlink()
        except: pass

def detect_header(csv_text: str):
    rows = list(csv.reader(io.StringIO(csv_text)))
    for i, row in enumerate(rows[:400]):
        toks = [norm(x) for x in row]
        if not any(toks): continue
        idx = {}
        for k, syns in HDR.items():
            hit = None
            for j,t in enumerate(toks):
                if t in syns: hit = j; break
            if hit is None: idx=None; break
            idx[k]=hit
        if idx: return i, idx
    return None, None

def derive_areatype(area_type: str, area_code: str) -> str|None:
    t = norm(area_type or ""); ac = (area_code or "").strip()
    if t in {"n","nat","nation","national","u.s.","us"} or t.startswith("nation"): return US_TAG
    if ac.upper().startswith("US") or re.fullmatch(r"0+", ac): return US_TAG
    if re.fullmatch(r"\d{5}", ac): return CBSA_TAG
    if re.fullmatch(r"[A-Z]{2}", ac) or re.fullmatch(r"\d{2}", ac) or re.fullmatch(r"\d{2}000", ac): return STATE_TAG
    if "state" in t: return STATE_TAG
    if "metro" in t or "cbsa" in t: return CBSA_TAG
    return None

def parse_rows(csv_text: str, hdr_line: int, idx: dict, year: int) -> pl.DataFrame:
    out = {"areatype_code":[], "area_code":[], "occupation_code":[], "year":[], "emp":[]}
    rdr = csv.reader(io.StringIO(csv_text))
    for _ in range(hdr_line+1):
        try: next(rdr)
        except StopIteration: return pl.DataFrame(out)
    for row in rdr:
        if not row: continue
        if max(idx.values()) >= len(row): continue
        occ = (row[idx["occupation_code"]] or "").strip()
        area= (row[idx["area_code"]] or "").strip()
        typ = (row[idx["area_type"]] or "").strip()
        empv= (row[idx["tot_emp"]] or "").strip()
        if norm(occ) in HDR["occupation_code"] or norm(area) in HDR["area_code"]:
            continue
        at = derive_areatype(typ, area)
        if at is None: continue
        emp = as_float(empv)
        out["areatype_code"].append(at)
        out["area_code"].append(area)
        out["occupation_code"].append(occ)
        out["year"].append(int(year))
        out["emp"].append(emp)
    return pl.DataFrame(out)

def load_one_zip(z: Path) -> pl.DataFrame:
    year = infer_year_from_name(z.name)
    xb = xlsx_bytes_from_zip(z)
    if xb is None:
        print(f"[warn] {z.name}: no .xlsx inside; skip")
        return pl.DataFrame({"areatype_code":[],"area_code":[],"occupation_code":[],"year":[],"emp":[]})
    for sid in (1,2,3,4,5,6,7,8):
        try:
            csv_text = xlsx_sheet_to_csv_text(xb, sid)
            if year is None:
                y2 = infer_year_from_text(csv_text)
                if y2: year = y2
            hdr_line, idx = detect_header(csv_text)
            if idx and year:
                df = parse_rows(csv_text, hdr_line, idx, year)
                if df.height:
                    print(f"[info] {z.name}: sheet {sid} ✓ year={year} rows={df.height}")
                    return df
        except Exception:
            continue
    print(f"[warn] {z.name}: no usable sheet/year; skip")
    return pl.DataFrame({"areatype_code":[],"area_code":[],"occupation_code":[],"year":[],"emp":[]})

def load_oews_all() -> pl.DataFrame:
    zips = sorted([p for p in RAW.glob("**/*.zip") if "oesm" in p.name.lower()])
    if not zips: raise FileNotFoundError(f"No OEWS ZIPs under {RAW}")
    print(f"[info] OEWS ZIP files: {len(zips)}")
    frames = [load_one_zip(z) for z in zips]
    frames = [f for f in frames if f.height]
    if not frames:
        raise RuntimeError("No OEWS rows parsed from any ZIP.")
    return pl.concat(frames, how="vertical_relaxed")

def build_bartik_entry() -> pl.DataFrame:
    df = load_oews_all()


    cbsa_occ = (df.filter(pl.col("areatype_code")==CBSA_TAG)
                  .rename({"area_code":"cbsa"})
                  .filter(pl.col("occupation_code")!="00-0000")
                  .group_by(["cbsa","occupation_code","year"])
                  .agg(pl.col("emp").fill_null(0.0).sum().alias("emp")))
    years = sorted(cbsa_occ.select("year").unique().to_series().to_list())
    base = SHARE_BASE_YEAR if SHARE_BASE_YEAR in years else (max(years) if years else SHARE_BASE_YEAR)
    print(f"[info] share base year: {base}")
    cbsa_base = cbsa_occ.filter(pl.col("year")==base)
    shares = (cbsa_base.group_by("cbsa").agg(pl.col("emp").sum().alias("emp_total"))
                .join(cbsa_base, on="cbsa")
                .with_columns(share = pl.when(pl.col("emp_total")>0).then(pl.col("emp")/pl.col("emp_total")).otherwise(0.0))
                .select([pl.col("cbsa").cast(pl.Utf8), pl.col("occupation_code"), pl.col("share").cast(pl.Float64)]))


    us_occ = (df.filter(pl.col("areatype_code")==US_TAG)
                .filter(pl.col("occupation_code")!="00-0000")
                .group_by(["occupation_code","year"])
                .agg(pl.col("emp").fill_null(0.0).sum().alias("emp_us"))
                .sort(["occupation_code","year"]))
    if us_occ.is_empty():
        print("[warn] no explicit national rows; aggregating states")
        us_occ = (df.filter(pl.col("areatype_code")==STATE_TAG)
                    .filter(pl.col("occupation_code")!="00-0000")
                    .group_by(["occupation_code","year"])
                    .agg(pl.col("emp").fill_null(0.0).sum().alias("emp_us"))
                    .sort(["occupation_code","year"]))
    us_occ = (us_occ.with_columns(pl.when(pl.col("emp_us")>0).then(pl.col("emp_us").log()).otherwise(None).alias("log_e"))
                    .with_columns((pl.col("log_e")-pl.col("log_e").shift(1)).over("occupation_code").alias("dlog"))
                    .with_columns(pl.col("dlog").fill_null(0.0))
                    .select(["occupation_code","year","dlog"]))


    bartik_y = (us_occ.join(shares, on="occupation_code", how="inner")
                    .with_columns((pl.col("share")*pl.col("dlog")).alias("__w"))
                    .group_by(["cbsa","year"])
                    .agg(pl.col("__w").sum().alias("bartik_y"))
                    .sort(["cbsa","year"]))
    q = pl.DataFrame({"quarter":[1,2,3,4]}, schema={"quarter":pl.Int8})
    bartik_q = (bartik_y.join(q, how="cross")
                  .select(pl.col("cbsa").cast(pl.Utf8).str.zfill(5).alias("cbsa"),
                          pl.col("year").cast(pl.Int16),
                          pl.col("quarter"),
                          pl.col("bartik_y").cast(pl.Float64).alias("bartik_entry"))
                  .sort(["cbsa","year","quarter"]))
    dup = bartik_q.select(pl.struct(["cbsa","year","quarter"]).is_duplicated().any().alias("dup")).item()
    if dup: raise AssertionError("Duplicate (cbsa,year,quarter) in bartik_entry.")
    return bartik_q

def save_outputs(btq: pl.DataFrame):
    INT.mkdir(parents=True, exist_ok=True); FINAL.mkdir(parents=True, exist_ok=True)
    btq.write_parquet(OUT_INT)

    if OUT_FINAL.exists():
        add = pl.read_parquet(OUT_FINAL)

        if "cbsa" in add.columns and "cbsa" in btq.columns:
            adt = add.schema["cbsa"]; bdt = btq.schema["cbsa"]
            if adt != bdt:

                try:
                    btq = btq.with_columns(pl.col("cbsa").cast(adt, strict=False))
                except Exception:
                    pass

                if btq.schema["cbsa"] != adt:
                    add = add.with_columns(pl.col("cbsa").cast(pl.Utf8, strict=False))
                    btq = btq.with_columns(pl.col("cbsa").cast(pl.Utf8, strict=False))
        out = (add.join(btq, on=["cbsa","year","quarter"], how="full")
                 .unique(subset=["cbsa","year","quarter"], keep="last"))
        out.write_parquet(OUT_FINAL)
    else:
        btq.write_parquet(OUT_FINAL)

def main():
    btq = build_bartik_entry()
    save_outputs(btq)
    print(f"✅ bartik_entry built: {btq.height:,} rows")
    print(f"   → {OUT_INT}")
    print(f"   → {OUT_FINAL} (created/updated)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[ERROR]", e)
        sys.exit(1)
