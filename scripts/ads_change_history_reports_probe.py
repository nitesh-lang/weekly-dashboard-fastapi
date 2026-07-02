"""Read-only probe: try to pull Ads change-history via the *async Reports*
API v3 — the same code path `weekly_app.etl.ads_reports_pull` already uses
successfully.

The prior probe (`scripts/ads_change_history_probe.py`) hit direct
`/audit/*` / `/changeHistory` endpoints with `Bearer <LWA>` auth and every
one returned 403 with "Invalid key=value pair in Authorization header" —
those endpoints expect a different auth scheme (AWS SigV4-style) than
we have. So this probe skips them entirely.

What it does:
  1. Reuse `weekly_app.etl.ads_api_pull.get_access_token()` + `base_headers()`
     — the proven auth for the profile/reports endpoints.
  2. For every IN profile: submit an async report for each candidate
     `reportTypeId` in CANDIDATE_REPORTS with the target window.
  3. Records for each (profile, reportType):
       - submit HTTP status + response body
       - reportId (if submit accepted)
       - final processingStatus (if we poll it)
       - `failureReason` when Amazon rejects the config
       - `url` + downloaded row count if we got a completed document
  4. Any downloaded documents are dumped to disk so we can inspect the
     actual columns (`user`, `userId`, `changedBy`, etc.).

Auth SigV4 endpoints (audit / changeHistory) are OUT OF SCOPE here.

Output:
  scripts/_audit/ads_change_reports_probe/<timestamp>/
      profiles.json
      submit_matrix.json          # per (profile, report_type) submit result
      poll_matrix.json            # per accepted reportId poll result
      report_<profile>_<type>.json.gz   # raw downloaded content when DONE
      SUMMARY.md                  # human-readable "what worked, what didn't"

Env / CLI:
  PROBE_WINDOW_START=YYYY-MM-DD   # default = 7 days ago (UTC)
  PROBE_WINDOW_END=YYYY-MM-DD     # default = yesterday (UTC)
  --profiles A,B,C                # restrict to specific profile IDs
  --types    a,b                  # restrict to specific reportTypeIds

Nothing is written to any Amazon-side resource — this is submit/poll/read only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

from weekly_app.etl.ads_api_pull import (
    get_access_token,
    base_headers,
    ADS_API_BASE,
)

REPORTS_CT = "application/vnd.createasyncreportrequest.v3+json"

OUT_DIR = ROOT / "scripts" / "_audit" / "ads_change_reports_probe" / \
    dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# ─────────────────────────────────────────────────────────────────────
# Candidate report-type IDs to try.  Amazon Ads API v3's canonical
# performance reports are well-known; the change-history ones (if
# they exist) are less documented.  We try every plausible id — the
# probe records exactly which the API accepts vs 400s with "unknown
# reportTypeId".  Even a rejection here is useful signal — it means
# the identifier truly doesn't exist on this account.
# ─────────────────────────────────────────────────────────────────────
CANDIDATE_REPORTS: list[dict] = [
    # ── Explicit "change history" variants (documented in various
    # Ads API changelogs; not always enabled per profile) ──
    {
        "key":          "sp_campaigns_change_history",
        "reportTypeId": "spCampaignsChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["campaign"],
        "timeUnit":     "DAILY",
        # Guessed columns — Amazon rejects unknown columns with a
        # helpful error listing the valid set, so this doubles as
        # a schema-discovery probe.
        "columns": [
            "date", "campaignId", "campaignName",
            "changeType", "changeReason",
            "previousValue", "newValue",
            "changedBy", "userId", "userEmail",
        ],
    },
    {
        "key":          "sp_keywords_change_history",
        "reportTypeId": "spKeywordsChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["keyword"],
        "timeUnit":     "DAILY",
        "columns": [
            "date", "campaignId", "adGroupId", "keywordId", "keywordText",
            "changeType", "previousValue", "newValue", "changedBy", "userEmail",
        ],
    },
    {
        "key":          "sp_targeting_change_history",
        "reportTypeId": "spTargetingChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["target"],
        "timeUnit":     "DAILY",
        "columns": [
            "date", "campaignId", "adGroupId", "targetId",
            "changeType", "previousValue", "newValue", "changedBy", "userEmail",
        ],
    },
    {
        "key":          "sp_ad_group_change_history",
        "reportTypeId": "spAdGroupsChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["adGroup"],
        "timeUnit":     "DAILY",
        "columns": [
            "date", "campaignId", "adGroupId",
            "changeType", "previousValue", "newValue", "changedBy", "userEmail",
        ],
    },
    # ── Non-hyphenated variants (Amazon sometimes uses both forms) ──
    {
        "key":          "sp_change_history",
        "reportTypeId": "spChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["campaign"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "changeType", "changedBy"],
    },
    {
        "key":          "sponsored_products_change_history",
        "reportTypeId": "sponsoredProductsChangeHistory",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["campaign"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "changeType", "changedBy"],
    },
    # ── SD + SB variants ──
    {
        "key":          "sd_campaigns_change_history",
        "reportTypeId": "sdCampaignsChangeHistory",
        "adProduct":    "SPONSORED_DISPLAY",
        "groupBy":      ["campaign"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "changeType", "changedBy"],
    },
    {
        "key":          "sb_campaigns_change_history",
        "reportTypeId": "sbCampaignsChangeHistory",
        "adProduct":    "SPONSORED_BRANDS",
        "groupBy":      ["campaign"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "changeType", "changedBy"],
    },
    # ── Bid-level candidates (the actual event we're chasing was
    # bid changes) ──
    {
        "key":          "sp_bid_changes",
        "reportTypeId": "spBidChanges",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["keyword"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "keywordId", "previousBid", "newBid", "changedBy"],
    },
    {
        "key":          "sp_keyword_bid_changes",
        "reportTypeId": "spKeywordBidChanges",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["keyword"],
        "timeUnit":     "DAILY",
        "columns": ["date", "campaignId", "keywordId", "previousBid", "newBid", "changedBy"],
    },
]


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────
def _reports_headers(profile_id: str, token: str) -> dict:
    h = base_headers(profile_id, token)
    h["Content-Type"] = REPORTS_CT
    h["Accept"]       = REPORTS_CT
    return h


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def _in_profiles(profiles: list[dict], restrict: list[str] | None) -> list[dict]:
    """Isolate IN marketplace profiles.  When the PROFILES_FALLBACK
    schema is in use (AdPilot-format env) there's no countryCode field
    — every entry is already an IN profile, so accept all.  When the
    v2/profiles schema is present, filter to IN explicitly."""
    if profiles and "countryCode" in (profiles[0] or {}):
        ins = [p for p in profiles if (p.get("countryCode") or "").upper() == "IN"]
    else:
        ins = list(profiles)
    if restrict:
        wanted = {str(x).strip() for x in restrict}
        ins = [p for p in ins if str(p.get("id") or p.get("profileId")) in wanted]
    return ins


# ─────────────────────────────────────────────────────────────────────
# Steps
# ─────────────────────────────────────────────────────────────────────
def step_auth() -> tuple[str, list[dict]]:
    # get_access_token() returns (token, profiles) already — no need to
    # hit /v2/profiles separately.  Doing so with profile_id=None sends
    # `Amazon-Advertising-API-Scope: None` which 401s.
    token, profiles = get_access_token()
    _write_json(OUT_DIR / "profiles.json", profiles)
    return token, profiles


def step_submit(token: str, profiles: list[dict], types: list[dict],
                start: dt.date, end: dt.date) -> dict:
    """For every (profile, candidate report type), attempt submit.
    Record the exact HTTP status + response body — Amazon's error
    messages are highly informative (they list valid column names when
    a column is rejected, and list valid reportTypeIds when the id
    itself is wrong)."""
    matrix: dict[str, dict] = {}
    for prof in profiles:
        pid = str(prof.get("id") or prof.get("profileId"))
        name = (prof.get("label") or (prof.get("accountInfo") or {}).get("name")
                or prof.get("name", "?"))
        print(f"\n[profile {pid}] {name}")
        matrix[pid] = {"profile_name": name, "attempts": {}}
        for cfg in types:
            body = {
                "name": f"probe {cfg['key']} {start.isoformat()}_{end.isoformat()} p{pid}",
                "startDate": start.isoformat(),
                "endDate":   end.isoformat(),
                "configuration": {
                    "adProduct":    cfg["adProduct"],
                    "groupBy":      cfg["groupBy"],
                    "columns":      cfg["columns"],
                    "reportTypeId": cfg["reportTypeId"],
                    "timeUnit":     cfg["timeUnit"],
                    "format":       "GZIP_JSON",
                },
            }
            try:
                r = requests.post(
                    f"{ADS_API_BASE}/reporting/reports",
                    headers=_reports_headers(pid, token),
                    json=body, timeout=60,
                )
            except requests.RequestException as e:
                matrix[pid]["attempts"][cfg["key"]] = {
                    "reportTypeId": cfg["reportTypeId"],
                    "status": "NETWORK_ERROR",
                    "error":  str(e),
                }
                print(f"  {cfg['key']:<40} NETWORK ERROR")
                continue
            body_snippet = r.text[:1200]
            report_id = None
            if r.status_code < 400:
                try:
                    report_id = r.json().get("reportId")
                except Exception:
                    pass
            matrix[pid]["attempts"][cfg["key"]] = {
                "reportTypeId": cfg["reportTypeId"],
                "status":       r.status_code,
                "reportId":     report_id,
                "body_snippet": body_snippet,
            }
            marker = "OK" if r.status_code < 400 else f"{r.status_code}"
            print(f"  {cfg['key']:<40} {marker}")
            # Amazon-Ads Ads API throttles at ~10 r/s per profile.
            # Being polite: 200ms between submits.
            time.sleep(0.2)
    _write_json(OUT_DIR / "submit_matrix.json", matrix)
    return matrix


def step_poll(token: str, matrix: dict, max_wait_s: int = 600) -> dict:
    """Poll every reportId that submit accepted.  Cap total wait per
    report at max_wait_s (Amazon's async pipeline usually completes
    within 30-120s for small windows)."""
    out: dict[str, dict] = {}
    for pid, entry in matrix.items():
        out[pid] = {}
        for key, att in (entry.get("attempts") or {}).items():
            rid = att.get("reportId")
            if not rid:
                continue
            out[pid][key] = {"reportId": rid, "polls": []}
            print(f"\n[poll pid={pid} type={key} rid={rid}]")
            deadline = time.time() + max_wait_s
            while time.time() < deadline:
                r = requests.get(
                    f"{ADS_API_BASE}/reporting/reports/{rid}",
                    headers=base_headers(pid, token), timeout=30,
                )
                if r.status_code >= 400:
                    out[pid][key]["polls"].append({
                        "http": r.status_code,
                        "body_snippet": r.text[:400],
                    })
                    break
                j = r.json()
                status = j.get("status")
                out[pid][key]["polls"].append({
                    "status":        status,
                    "failureReason": j.get("failureReason"),
                    "url":           bool(j.get("url")),
                })
                print(f"  poll → {status}")
                if status in ("COMPLETED", "FAILED", "CANCELLED"):
                    out[pid][key]["final"] = j
                    break
                time.sleep(6)
    _write_json(OUT_DIR / "poll_matrix.json", out)
    return out


def step_download(poll_out: dict, matrix: dict) -> None:
    """Download every COMPLETED report, gzip-dump raw, plus a small
    tail of decoded rows so the SUMMARY.md can preview the schema."""
    for pid, per_type in poll_out.items():
        for key, s in per_type.items():
            final = s.get("final") or {}
            if final.get("status") != "COMPLETED":
                continue
            url = final.get("url")
            if not url:
                continue
            try:
                d = requests.get(url, timeout=180)
            except requests.RequestException as e:
                print(f"  [download pid={pid} type={key}] network error: {e}")
                continue
            if d.status_code >= 400:
                print(f"  [download pid={pid} type={key}] HTTP {d.status_code}")
                continue
            raw_path = OUT_DIR / f"report_{pid}_{key}.json.gz"
            raw_path.write_bytes(d.content)
            print(f"  wrote {raw_path.name}  ({len(d.content):,} bytes)")
            try:
                unz = gzip.decompress(d.content)
                data = json.loads(unz)
                rows = data if isinstance(data, list) else data.get("data", [])
                _write_json(
                    OUT_DIR / f"report_{pid}_{key}_preview.json",
                    {
                        "columns_seen": sorted(list({k for row in rows[:20] for k in (row or {}).keys()})),
                        "row_count": len(rows),
                        "first_5_rows": rows[:5],
                    },
                )
            except Exception as e:
                print(f"    (preview skipped: {e})")


def step_summary(matrix: dict, poll_out: dict) -> None:
    """Human-readable roll-up."""
    lines: list[str] = []
    lines.append(f"# Ads change-history reports probe")
    lines.append(f"Run window: {WIN_START} → {WIN_END}")
    lines.append("")
    for pid, entry in matrix.items():
        lines.append(f"## Profile {pid} — {entry.get('profile_name')}")
        for key, att in (entry.get("attempts") or {}).items():
            rid  = att.get("reportId")
            code = att.get("status")
            body = (att.get("body_snippet") or "").replace("\n", " ")[:200]
            if rid:
                pf = ((poll_out.get(pid, {}).get(key) or {}).get("final") or {})
                final_status = pf.get("status") or "(no poll)"
                fail = pf.get("failureReason")
                lines.append(
                    f"- **{key}** (`{att.get('reportTypeId')}`) → submit {code}, "
                    f"poll {final_status}"
                    + (f" — failureReason: {fail}" if fail else "")
                )
            else:
                lines.append(f"- **{key}** (`{att.get('reportTypeId')}`) → submit {code} — {body}")
        lines.append("")
    (OUT_DIR / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────
# Window helpers
# ─────────────────────────────────────────────────────────────────────
WIN_START = os.environ.get("PROBE_WINDOW_START") or (
    (dt.date.today() - dt.timedelta(days=7)).isoformat()
)
WIN_END   = os.environ.get("PROBE_WINDOW_END") or (
    (dt.date.today() - dt.timedelta(days=1)).isoformat()
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", default=None,
                    help="Comma-separated profileIds to restrict the probe to.")
    ap.add_argument("--types", default=None,
                    help="Comma-separated candidate keys (see CANDIDATE_REPORTS).")
    ap.add_argument("--max-wait", type=int, default=600,
                    help="Max seconds to wait for a single report to complete.")
    args = ap.parse_args()

    start = dt.date.fromisoformat(WIN_START)
    end   = dt.date.fromisoformat(WIN_END)
    print(f"Window: {start} → {end}")

    types = CANDIDATE_REPORTS
    if args.types:
        wanted = {t.strip() for t in args.types.split(",") if t.strip()}
        types = [t for t in CANDIDATE_REPORTS if t["key"] in wanted]
        if not types:
            print(f"ERROR: no candidate matched --types={args.types}")
            return 2

    restrict = [p.strip() for p in args.profiles.split(",")] if args.profiles else None

    print("Auth + list profiles…")
    token, profiles = step_auth()
    ins = _in_profiles(profiles, restrict)
    if not ins:
        print("No IN profiles found — abort.")
        return 1
    print(f"IN profiles: {len(ins)}")
    for p in ins:
        print(f"  {p.get('id') or p.get('profileId')}  "
              f"{p.get('label') or (p.get('accountInfo') or {}).get('name', '?')}")

    print("\nSubmit matrix…")
    matrix = step_submit(token, ins, types, start, end)

    print("\nPoll matrix…")
    poll_out = step_poll(token, matrix, max_wait_s=args.max_wait)

    print("\nDownload COMPLETED reports…")
    step_download(poll_out, matrix)

    step_summary(matrix, poll_out)
    print(f"\nOutput → {OUT_DIR.relative_to(ROOT)}")
    print(f"See {OUT_DIR.relative_to(ROOT) / 'SUMMARY.md'} for a roll-up.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
