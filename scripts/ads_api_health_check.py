"""Per-profile Ads API health check — confirms that the refresh token's
LWA consent actually grants campaign-read access to each IN profile the
weekly cron depends on.

Why this matters: /v2/profiles is just metadata — it lists every profile
the token CAN see, but a profile in that list might still 401/403 on
actual data calls if the user who consented didn't grant access to that
specific ad account.

Probes the most common SP/SD/SB campaign-list endpoints per profile and
reports OK / FAIL with the HTTP status, so a cron pre-flight can spot
broken access before the weekly pull silently returns partial data.

Read-only.  No writes.
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from weekly_app.etl.ads_api_pull import (
    get_access_token,
    base_headers,
    ADS_API_BASE,
    SP_CAMPAIGNS_CT,
)

# Profiles the weekly cron actually reads.  Hard-coded against the
# authoritative /v2/profiles list we pulled today (2026-06-25).  Add
# new ones here as you onboard ad accounts.
TARGET_PROFILES = [
    # (profile_id, label, account_type)
    ("36023856736901",  "Audio Array",                  "seller"),
    ("1250883527123538", "Cambium",                      "seller"),
    ("866679611348308", "Cambium Retail Private Limited","seller"),
    ("4164167382477819","Nexlev",                       "seller"),
    ("1980119202240829","Viomi",                        "seller"),
    ("3633693611482083","AudioArray",                   "vendor"),
    ("1122755254933937","Nexlev",                       "vendor"),
    ("3563790765020958","White Mulberry",               "vendor"),
]


def probe_sp_campaigns(profile_id: str, access_token: str) -> tuple[int, str, int]:
    """Returns (status_code, snippet, campaign_count_if_2xx)."""
    headers = {
        **base_headers(profile_id, access_token),
        "Accept":       SP_CAMPAIGNS_CT,
        "Content-Type": SP_CAMPAIGNS_CT,
    }
    body = {"maxResults": 1, "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]}}
    r = requests.post(f"{ADS_API_BASE}/sp/campaigns/list", json=body, headers=headers, timeout=30)
    count = 0
    if 200 <= r.status_code < 300:
        try:
            payload = r.json()
            count = (payload.get("totalResults")
                     or len(payload.get("campaigns") or [])
                     or 0)
        except ValueError:
            pass
    snippet = (r.text or "")[:160].replace("\n", " ")
    return r.status_code, snippet, count


def main() -> None:
    print("Loading credentials from .env / GitHub secrets…")
    access_token, _profiles = get_access_token()
    print(f"  LWA exchange OK — access_token length={len(access_token)}\n")

    print(f"{'profile_id':<18}  {'name':<32} {'type':<6} {'status':>6}  campaigns")
    print("-" * 90)
    n_ok = n_fail = 0
    for pid, label, ptype in TARGET_PROFILES:
        try:
            status, snippet, count = probe_sp_campaigns(pid, access_token)
        except requests.RequestException as e:
            print(f"{pid:<18}  {label:<32} {ptype:<6} {'ERR':>6}  network: {e}")
            n_fail += 1
            continue
        tag = "OK" if 200 <= status < 300 else "FAIL"
        if tag == "OK":
            n_ok += 1
        else:
            n_fail += 1
        print(f"{pid:<18}  {label:<32} {ptype:<6} {status:>6}  "
              f"{count if tag=='OK' else snippet[:60]}")
    print("-" * 90)
    print(f"OK: {n_ok}  FAIL: {n_fail}")
    if n_fail:
        print("\n[!] At least one profile is unreachable.  Weekly cron will return "
              "PARTIAL data unless the LWA consent is re-walked WITH that ad "
              "account's permission included.")
        sys.exit(1)


if __name__ == "__main__":
    main()
