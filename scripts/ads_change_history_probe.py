"""Read-only investigation: who made the ~261 unauthorized bid changes on
Audio Array India SP-KT-Broad campaign at 02:03 IST on 2026-06-22?

This script reuses the project's PROVEN Ads-API auth path from
weekly_app/etl/ads_api_pull.py (LWA refresh-token exchange) instead
of rolling its own.  No writes, no revokes — purely lists/inspects.

What it does, in order:
  1. Exchange AMS_ADS_REFRESH_TOKEN -> Ads access_token via get_access_token()
  2. List profiles from /v2/profiles, isolate Audio Array India + every other
     IN profile (so we can audit ALL accounts, not just AA)
  3. Probe known + candidate change-history endpoints to find which (if any)
     respond on the current Ads API.  Records each endpoint's response code
     and any error body for the caller.
  4. If at least one endpoint works: pull the time window
     Jun 21 18:00Z - Jun 22 02:00Z (covers the 02:03 IST batch) for each
     profile + SPONSORED_PRODUCTS, dump raw JSON, and group by actor.

Output:
  scripts/_audit/ads_change_probe/<timestamp>/
      profiles.json
      endpoint_probe.json
      changes_<profile_id>.json   (one per profile, raw)
      actor_summary_<profile_id>.txt
"""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# Make weekly_app importable so we get the same auth path the production cron uses.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from weekly_app.etl.ads_api_pull import (
    get_access_token,
    base_headers,
    ADS_API_BASE,
)

OUT_DIR = ROOT / "scripts" / "_audit" / "ads_change_probe" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Suspect window — 8 hours covering 02:03 IST (= 20:33 UTC prior day) ± buffer.
# Overridable via env (the GitHub Actions workflow passes user inputs through
# PROBE_WINDOW_START / PROBE_WINDOW_END so the run can target a different
# incident without editing the script).
WIN_START_UTC = os.environ.get("PROBE_WINDOW_START") or "2026-06-21T18:00:00Z"
WIN_END_UTC   = os.environ.get("PROBE_WINDOW_END")   or "2026-06-22T02:00:00Z"

# Endpoint candidates to probe.  Most are educated guesses based on Amazon
# Ads API conventions — the script records which ACTUALLY exist on the
# current API surface and which 404.
ENDPOINT_CANDIDATES = [
    # 1) Modern "Audit" surface (some docs reference this)
    ("POST", "/audit/changes/list",                "v1 list"),
    ("POST", "/audit/changes",                     "v1 plain"),
    ("POST", "/audit/v1/changes",                  "v1 prefixed"),
    # 2) Insights surface that the standalone script tried
    ("POST", "/insights/v1/changeHistory",         "insights v1"),
    ("POST", "/changeHistory",                     "root changeHistory"),
    # 3) Per-product-type audit endpoints
    ("POST", "/sp/audit",                          "sp audit"),
    ("POST", "/sp/campaigns/audit",                "sp campaigns audit"),
    # 4) GET variants
    ("GET",  "/audit/changes",                     "audit GET"),
    ("GET",  "/changeHistory",                     "changeHistory GET"),
]


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def step_1_auth() -> tuple[str, list[dict]]:
    print("[1] LWA token exchange via weekly_app.etl.ads_api_pull.get_access_token()…")
    try:
        access_token, profiles = get_access_token()
    except RuntimeError as e:
        print(f"    FAIL: {e}")
        sys.exit(1)
    print(f"    OK — access_token length={len(access_token)}, profiles from env JSON={len(profiles)}")
    return access_token, profiles


def step_2_profiles(access_token: str) -> list[dict]:
    print("\n[2] Listing /v2/profiles (authoritative profile list, not env-derived)…")
    # /v2/profiles is profile-list endpoint; doesn't need a scope header.
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": __import__("os").environ["AMS_ADS_CLIENT_ID"],
    }
    r = requests.get(f"{ADS_API_BASE}/v2/profiles", headers=headers, timeout=30)
    if r.status_code != 200:
        print(f"    FAIL: HTTP {r.status_code} {r.text[:300]}")
        sys.exit(1)
    profiles = r.json()
    write_json(OUT_DIR / "profiles.json", profiles)
    print(f"    OK — {len(profiles)} profiles returned")
    for p in profiles:
        acc = p.get("accountInfo", {})
        print(f"      profile_id={p.get('profileId')}  "
              f"name={acc.get('name','?')}  type={acc.get('type','?')}  "
              f"marketplace={p.get('countryCode','?')}")
    return profiles


def step_3_probe_endpoints(access_token: str, profile_id: str) -> dict:
    print(f"\n[3] Probing candidate change-history endpoints for profile {profile_id}…")
    headers = base_headers(profile_id, access_token)
    probe_body = {
        "startDateTime": WIN_START_UTC,
        "endDateTime":   WIN_END_UTC,
        "adProducts":    ["SPONSORED_PRODUCTS"],
        "maxResults":    10,
    }
    results: dict[str, dict] = {}
    for method, path, label in ENDPOINT_CANDIDATES:
        url = f"{ADS_API_BASE}{path}"
        try:
            if method == "POST":
                r = requests.post(url, json=probe_body, headers={**headers, "Content-Type": "application/json"}, timeout=20)
            else:
                r = requests.get(url, headers=headers, timeout=20)
            snippet = r.text[:250].replace("\n", " ")
            results[f"{method} {path}"] = {
                "label": label,
                "status": r.status_code,
                "body_snippet": snippet,
            }
            tag = "OK" if 200 <= r.status_code < 300 else "MISS"
            print(f"    [{tag:4}] {method:4} {path:35s} -> {r.status_code}  {snippet[:80]}")
        except requests.RequestException as e:
            results[f"{method} {path}"] = {"label": label, "status": -1, "error": str(e)}
            print(f"    [ERR ] {method:4} {path:35s} -> {e}")
    write_json(OUT_DIR / "endpoint_probe.json", results)
    return results


def step_4_pull_window(access_token: str, profile_id: str, profile_name: str,
                       working_endpoint: tuple[str, str]) -> None:
    method, path = working_endpoint
    url = f"{ADS_API_BASE}{path}"
    print(f"\n[4] Pulling change history via {method} {path} "
          f"for {profile_name} (profile_id={profile_id})…")
    headers = base_headers(profile_id, access_token)
    body = {
        "startDateTime": WIN_START_UTC,
        "endDateTime":   WIN_END_UTC,
        "adProducts":    ["SPONSORED_PRODUCTS"],
        "maxResults":    1000,
    }
    if method == "POST":
        r = requests.post(url, json=body, headers={**headers, "Content-Type": "application/json"}, timeout=60)
    else:
        r = requests.get(url, headers=headers, timeout=60)
    if r.status_code != 200:
        print(f"    FAIL: HTTP {r.status_code} {r.text[:400]}")
        return
    payload = r.json()
    write_json(OUT_DIR / f"changes_{profile_id}.json", payload)
    print(f"    OK — payload keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__}")

    # Heuristic actor grouping — actual field name varies by API version
    records = []
    if isinstance(payload, dict):
        for key in ("changes", "results", "data", "records", "history"):
            if key in payload and isinstance(payload[key], list):
                records = payload[key]
                break
    elif isinstance(payload, list):
        records = payload
    print(f"    raw record count: {len(records)}")

    actor_buckets: dict[str, list[dict]] = {}
    for rec in records:
        actor = (rec.get("actor")
                 or rec.get("userId")
                 or rec.get("changedBy")
                 or rec.get("modifiedBy")
                 or rec.get("applicationId")
                 or rec.get("clientId")
                 or "UNKNOWN")
        actor_buckets.setdefault(str(actor), []).append(rec)

    summary_lines = [
        f"Profile: {profile_name} (id={profile_id})",
        f"Window:  {WIN_START_UTC} -> {WIN_END_UTC}",
        f"Records: {len(records)}",
        "",
        "Actor breakdown:",
    ]
    for actor, recs in sorted(actor_buckets.items(), key=lambda x: -len(x[1])):
        summary_lines.append(f"  {actor:40s}  {len(recs):4d} changes")
        # surface first record's timestamps + entity to help cross-check the 02:03 burst
        sample = recs[0]
        ts_field = next((k for k in ("timestamp", "modifiedTime", "changeDate", "createdAt") if k in sample), None)
        entity   = sample.get("campaignName") or sample.get("entityName") or sample.get("entityId") or ""
        if ts_field:
            summary_lines.append(f"      sample: {sample.get(ts_field)}  {entity}")
    (OUT_DIR / f"actor_summary_{profile_id}.txt").write_text("\n".join(summary_lines), encoding="utf-8")
    print("\n" + "\n".join(summary_lines))


def main() -> None:
    print(f"Output dir: {OUT_DIR}")
    access_token, env_profiles = step_1_auth()
    profiles = step_2_profiles(access_token)

    # Investigate ALL India profiles, not just Audio Array (user said "all account")
    in_profiles = [p for p in profiles if p.get("countryCode") == "IN"]
    print(f"\n  IN profiles to investigate: {len(in_profiles)}")

    # Probe endpoints ONCE per a known profile (uses first IN profile if available)
    if not in_profiles:
        print("  No IN profiles found — exiting.")
        return
    probe_results = step_3_probe_endpoints(access_token, str(in_profiles[0]["profileId"]))

    working = [
        tuple(key.split(" ", 1))
        for key, info in probe_results.items()
        if 200 <= info.get("status", 0) < 300
    ]
    if not working:
        print("\n[!] NO candidate change-history endpoint returned 2xx.")
        print("    The Amazon Ads API does NOT publicly expose a change-history endpoint")
        print("    at any of the paths we tried.  Recommended alternative path:")
        print("    1. Amazon Ads Console (UI) -> Campaign Manager -> open the campaign")
        print("       -> top-right 'History' tab.  Filter by date.  Export CSV.")
        print("    2. Seller/Vendor Central -> Settings -> User Permissions for the")
        print("       LIST of users who could possibly have made changes.")
        print("    3. Seller Central -> Settings -> Permissions -> Manage Your Apps")
        print("       (Connected Apps) -> revoke any third-party app you don't recognise.")
        print("    4. Amazon Ads Console -> Account info -> Account Access -> revoke")
        print("       any agency/manager-account access.")
        return

    print(f"\n[ok] {len(working)} working endpoint(s): {working}")
    chosen = working[0]
    for p in in_profiles:
        pid  = str(p["profileId"])
        name = p.get("accountInfo", {}).get("name", "?")
        step_4_pull_window(access_token, pid, name, chosen)


if __name__ == "__main__":
    main()
