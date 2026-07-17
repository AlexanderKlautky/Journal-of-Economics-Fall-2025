from urllib.request import Request, urlopen
from urllib.parse import urlencode
import json

BASE = "https://educationdata.urban.org/api/v1/college-university/ipeds/"
ENDPOINTS = [
    ("completions-cip-6", {"credential_level": 5}),
    ("completions-cip-4", {"credential_level": 5}),
    ("completions",       {"credential_level": 5}),
]
                                                                                            
TEST_UNITID = 100751
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def hit(ep, params):
    q = params.copy()
    q["year"] = 2020
    q["unitid"] = TEST_UNITID
    q["per_page"] = 100
    url = BASE + ep + "/?" + urlencode(q, doseq=True)
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=60) as r:
        data = json.load(r)
    return url, data.get("results", [])

def main():
    for ep, params in ENDPOINTS:
        try:
            url, rows = hit(ep, params)
            print("EP:", ep)
            print("URL:", url)
            print("N rows:", len(rows))
            if rows:
                keys = list(rows[0].keys())
                print("Columns:", keys)
                print("Sample row:", {k: rows[0].get(k) for k in keys[:8]})
                break
        except Exception as e:
            print("EP failed:", ep, "-", e)

if __name__ == "__main__":
    main()
