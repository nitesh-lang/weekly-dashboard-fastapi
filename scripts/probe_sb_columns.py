"""Direct probe — single bogus-column request per SB report type, print full
body so we get the COMPLETE allowed-columns list.  Sleeps between calls to
avoid throttling."""
import sys, time, json
from pathlib import Path
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=False)
from weekly_app.etl.ads_api_pull import get_access_token, base_headers, ADS_API_BASE

REPORTS_CT = "application/vnd.createasyncreportrequest.v3+json"
token, _ = get_access_token()
PROFILE_ID = "1122755254933937"  # Nexlev vendor
h = base_headers(PROFILE_ID, token)
h["Content-Type"] = REPORTS_CT
h["Accept"]       = REPORTS_CT

def probe(name, body):
    for attempt in range(6):
        r = requests.post(f"{ADS_API_BASE}/reporting/reports", headers=h, json=body, timeout=60)
        if r.status_code == 429:
            wait = 15 * (attempt + 1)
            print(f"  [{name}] 429, sleeping {wait}s")
            time.sleep(wait)
            continue
        print(f"━━━ {name} ━━━")
        print(f"status: {r.status_code}")
        print(f"body:\n{r.text}\n")
        return
    print(f"━━━ {name} ━━━  FAILED — kept getting 429s")

probe("sbAds", {
    "name": "probe sbAds", "startDate":"2026-05-17","endDate":"2026-05-23",
    "configuration":{"adProduct":"SPONSORED_BRANDS","groupBy":["ads"],
                     "columns":["__bogus__"],
                     "reportTypeId":"sbAds",
                     "timeUnit":"DAILY","format":"GZIP_JSON"}
})
time.sleep(10)
probe("sbPurchasedProduct", {
    "name": "probe sbPurch", "startDate":"2026-05-17","endDate":"2026-05-23",
    "configuration":{"adProduct":"SPONSORED_BRANDS","groupBy":["purchasedAsin"],
                     "columns":["__bogus__"],
                     "reportTypeId":"sbPurchasedProduct",
                     "timeUnit":"DAILY","format":"GZIP_JSON"}
})
