"""Check status of an existing report by ID (no creation, no polling)."""
import sys, gzip, json
from pathlib import Path
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=False)

from weekly_app.etl.ads_api_pull import get_access_token, base_headers, ADS_API_BASE

REPORT_ID = sys.argv[1] if len(sys.argv) > 1 else "8f1114b5-e9c4-4422-812d-a174443f476b"
PROFILE_ID = "36023856736901"

token, _ = get_access_token()
h = base_headers(PROFILE_ID, token)
r = requests.get(f"{ADS_API_BASE}/reporting/reports/{REPORT_ID}",
                 headers=h, timeout=30)
print(f"status: {r.status_code}")
print(json.dumps(r.json(), indent=2, default=str))

# If complete, fetch + inspect
j = r.json()
if j.get("status") == "COMPLETED" and j.get("url"):
    d = requests.get(j["url"], timeout=60)
    raw = gzip.decompress(d.content)
    data = json.loads(raw)
    print(f"\nrows: {len(data) if isinstance(data, list) else 'not-list'}")
    rows = data if isinstance(data, list) else []
    print(f"\nfirst row:\n{json.dumps(rows[0], indent=2, default=str) if rows else '(empty)'}")
    print(f"\nall keys present:")
    all_keys = set()
    for rr in rows[:50]:
        all_keys.update(rr.keys())
    for k in sorted(all_keys):
        print(f"  {k}")
