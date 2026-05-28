"""
Probe — Amazon Ads Reports API v3.

Goal: create one SP Advertised-Product report for ONE profile / ONE week,
poll until done, download + decompress, dump first 3 rows.  Lets us
confirm reportTypeId / column names / response shape before building
the full pull module.
"""
import gzip
import io
import json
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=False)

from weekly_app.etl.ads_api_pull import (
    get_access_token, base_headers, ADS_API_BASE,
)

# Audio Array seller is the smallest profile (~55 SP campaigns) — fast probe.
PROFILE_ID = "36023856736901"

# Week 21 = May 17-23, 2026 (user's Sun-Sat convention)
START = "2026-05-17"
END   = "2026-05-23"

REPORTS_CT = "application/vnd.createasyncreportrequest.v3+json"

def main():
    token, _ = get_access_token()
    h = base_headers(PROFILE_ID, token)
    h["Content-Type"] = REPORTS_CT
    h["Accept"]       = REPORTS_CT

    body = {
        "name": f"probe spAdvertisedProduct {START}_{END}",
        "startDate": START,
        "endDate":   END,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["advertiser"],
            "columns": [
                "campaignName", "campaignId",
                "adGroupName", "adGroupId",
                "advertisedAsin", "advertisedSku",
                "impressions", "clicks", "cost",
                "purchases7d", "sales7d", "unitsSoldClicks7d",
                "purchasesSameSku7d", "salesOtherSku7d",
            ],
            "reportTypeId": "spAdvertisedProduct",
            "timeUnit": "SUMMARY",
            "format":   "GZIP_JSON",
        }
    }

    print(f"━━━ Create report ━━━")
    print(f"profile={PROFILE_ID}  range={START}..{END}")
    r = requests.post(f"{ADS_API_BASE}/reporting/reports",
                      headers=h, json=body, timeout=60)
    print(f"create status: {r.status_code}")
    if r.status_code >= 400:
        print(f"body: {r.text[:600]}")
        sys.exit(1)
    rep = r.json()
    report_id = rep.get("reportId")
    print(f"reportId: {report_id}")
    print(f"initial status: {rep.get('status')}")

    print(f"\n━━━ Polling ━━━")
    poll_headers = {k: v for k, v in h.items() if k != "Content-Type"}
    url = None
    for i in range(60):  # up to ~10 min
        time.sleep(10)
        s = requests.get(f"{ADS_API_BASE}/reporting/reports/{report_id}",
                         headers=poll_headers, timeout=30)
        if s.status_code >= 400:
            print(f"  poll status: {s.status_code} {s.text[:200]}")
            sys.exit(1)
        sj = s.json()
        st = sj.get("status")
        print(f"  [{i+1:>2}] status={st}", end="")
        if st == "COMPLETED":
            url = sj.get("url")
            print(f"  url={url[:80] if url else None}...")
            break
        elif st == "FAILED":
            print(f"  failureReason={sj.get('failureReason')}")
            sys.exit(1)
        print()
    if not url:
        print("⚠ never reached COMPLETED")
        sys.exit(1)

    print(f"\n━━━ Download ━━━")
    d = requests.get(url, timeout=60)
    print(f"download status: {d.status_code}  bytes={len(d.content)}")
    raw = gzip.decompress(d.content)
    data = json.loads(raw)
    print(f"rows: {len(data) if isinstance(data, list) else 'not-a-list'}")
    print(f"\n━━━ First 3 rows ━━━")
    rows = data if isinstance(data, list) else []
    for row in rows[:3]:
        print(json.dumps(row, indent=2, default=str))
        print()
    if rows:
        print(f"\n━━━ Column keys present ━━━")
        all_keys = set()
        for r in rows[:50]:
            all_keys.update(r.keys())
        for k in sorted(all_keys):
            print(f"  {k}")

if __name__ == "__main__":
    main()
