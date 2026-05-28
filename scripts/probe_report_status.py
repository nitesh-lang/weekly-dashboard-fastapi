"""Read-only status check of submitted reports — does NOT touch state file."""
import json, sys
from collections import Counter
from pathlib import Path
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=False)
from weekly_app.etl.ads_api_pull import get_access_token, base_headers, ADS_API_BASE

state = json.load(open(ROOT / "data" / "processed" / ".ads_reports_state.json"))
token, _ = get_access_token()

# Sample one PENDING report per (profile × rtype) to estimate completion
sampled = {}
for j in state["jobs"]:
    if j["status"] != "PENDING" or not j.get("report_id"):
        continue
    key = (j["profile_id"], j["rtype"])
    if key not in sampled:
        sampled[key] = j

print(f"Checking {len(sampled)} sample reports (1 per profile×rtype)...")
counts = Counter()
for j in sampled.values():
    try:
        r = requests.get(f"{ADS_API_BASE}/reporting/reports/{j['report_id']}",
                         headers=base_headers(j['profile_id'], token), timeout=30)
        st = r.json().get('status', 'ERROR')
    except Exception as e:
        st = f"EXC:{e}"
    counts[(j['rtype'], st)] += 1
    print(f"  {j['rtype']:<16} {j['profile_id']:<18} rid={j['report_id'][:8]}…  → {st}")

print(f"\nSummary: {dict(counts)}")
