import os
import sys
import zipfile
import subprocess

BASE = "https://data.nber.org/cps-basic3/csv"
DEST_ROOT = os.path.expanduser("~/Journal_Project_2025/Data_Raw/CPS_Basic_Monthly")
YEARS = list(range(2019, 2024))
MONTHS = [f"{m:02d}" for m in range(1, 13)]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"

def run_curl(url, outpath):
    tmp = outpath + ".part"
    if os.path.exists(tmp):
        os.remove(tmp)
    r = subprocess.run(
        ["/usr/bin/curl", "-fL", "-k", "--retry", "3", "--retry-delay", "2",
         "--compressed", "-A", UA, "-H", "Referer: https://data.nber.org/cps-basic3/csv/",
         "-o", tmp, url],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    if r.returncode != 0:
        if os.path.exists(tmp):
            os.remove(tmp)
        return None
    os.replace(tmp, outpath)
    return outpath

def is_valid_zip(path):
    try:
        with open(path, "rb") as f:
            if f.read(4) != b"PK\x03\x04":
                return False
        with zipfile.ZipFile(path) as zf:
            if zf.testzip() is not None:
                return False
        return os.path.getsize(path) > 1024
    except Exception:
        return False

def is_valid_csv(path):
    try:
        if os.path.getsize(path) < 1024:
            return False
        with open(path, "rb") as f:
            head = f.read(4096).strip()
        if head.startswith(b"<"):
            return False
        return b"," in head or b"\t" in head
    except Exception:
        return False

def zip_single(csv_path, zip_dest):
    with zipfile.ZipFile(zip_dest + ".part", "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname=os.path.basename(csv_path))
    os.replace(zip_dest + ".part", zip_dest)

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def main():
    ensure_dir(DEST_ROOT)
    created, skipped, failed = 0, 0, []

    for y in YEARS:
        ydir = os.path.join(DEST_ROOT, str(y))
        ensure_dir(ydir)
        for mm in MONTHS:
            final_zip = os.path.join(ydir, f"cpsb{y}{mm}_csv.zip")
            if os.path.exists(final_zip) and is_valid_zip(final_zip):
                print(f"SKIP {os.path.basename(final_zip)}")
                skipped += 1
                continue

            tried = []
            for url in (f"{BASE}/cpsb{y}{mm}_csv.zip", f"{BASE}/cpsb{y}{mm}.csv.zip"):
                tried.append(url)
                tmpzip = final_zip + ".dl"
                dl = run_curl(url, tmpzip)
                if dl and is_valid_zip(dl):
                    os.replace(dl, final_zip)
                    print(f"OK   {os.path.basename(final_zip)}")
                    created += 1
                    break
                if dl and os.path.exists(dl):
                    os.remove(dl)
            else:
                csv_url = f"{BASE}/cpsb{y}{mm}.csv"
                csv_path = os.path.join(ydir, f"cpsb{y}{mm}.csv")
                dl = run_curl(csv_url, csv_path)
                if dl and is_valid_csv(dl):
                    zip_single(csv_path, final_zip)
                    os.remove(csv_path)
                    print(f"OK   {os.path.basename(final_zip)} (from CSV)")
                    created += 1
                else:
                    if dl and os.path.exists(dl):
                        os.remove(dl)
                    if os.path.exists(csv_path):
                        os.remove(csv_path)
                    print(f"MISS cpsb{y}{mm} (blocked/404)")
                    failed.append(f"{y}-{mm}")

    print("\nSummary")
    print(f"Created: {created}")
    print(f"Skipped: {skipped}")
    print(f"Missing: {len(failed)}")
    if failed:
        for tag in failed:
            print(f"  {tag}")
        sys.exit(1)

if __name__ == "__main__":
    main()


