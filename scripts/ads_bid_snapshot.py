"""Snapshot every Sponsored Products keyword + campaign + ad-group + target
state across all IN profiles.  Runs every ~30 min via cron so the paired
diff script can detect bid-change bursts within one interval.

Uses the same auth path as weekly_app.etl.ads_reports_pull (proven).
Reads-only — pulls list endpoints, writes nothing back to Amazon.

Output shape:
  data/processed/ads_bid_snapshots/
      <profile_id>/
          <YYYYMMDDTHHMMSSZ>.json.gz     # timestamped snapshot
          latest.json.gz                 # symlink/copy pointer for diff

Each snapshot is a dict:
  {
    "profile_id":     "1234...",
    "profile_label":  "Nexlev (vendor)",
    "captured_at":    "2026-07-03T00:00:00Z",
    "campaigns":  [{campaignId, name, state, budget, biddingStrategy, ...}],
    "adGroups":   [{adGroupId, campaignId, name, defaultBid, state, ...}],
    "keywords":   [{keywordId, adGroupId, campaignId, keywordText,
                    matchType, bid, state}],
    "targets":    [{targetId, adGroupId, campaignId, bid, state,
                    expression: [...]}],
  }

Env:
  AMS_ADS_CLIENT_ID / AMS_ADS_CLIENT_SECRET / AMS_ADS_REFRESH_TOKEN
  (same trio the Weekly Auto-Sync cron already uses)

CLI:
  python scripts/ads_bid_snapshot.py
  python scripts/ads_bid_snapshot.py --profiles 1122755254933937
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import sys
from pathlib import Path

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
    _post_paginated_v4,
)

OUT_BASE = ROOT / "data" / "processed" / "ads_bid_snapshots"

# Sponsored Products v3 content-types.
SP_CAMPAIGNS_CT = "application/vnd.spCampaign.v3+json"
SP_ADGROUPS_CT  = "application/vnd.spAdGroup.v3+json"
SP_KEYWORDS_CT  = "application/vnd.spKeyword.v3+json"
SP_TARGETS_CT   = "application/vnd.spTargetingClause.v3+json"


def _snapshot_profile(profile: dict, token: str) -> dict:
    pid = str(profile.get("id") or profile.get("profileId"))
    label = (profile.get("label")
             or (profile.get("accountInfo") or {}).get("name") or "?")
    h = base_headers(pid, token)

    def _list(url: str, ct: str, list_key: str) -> list:
        hh = {**h, "Accept": ct, "Content-Type": ct}
        try:
            return _post_paginated_v4(url, hh, {}, list_key)
        except Exception as e:
            print(f"    ! {url} failed: {e}")
            return []

    print(f"  [{pid}] {label}")
    campaigns = _list(f"{ADS_API_BASE}/sp/campaigns/list", SP_CAMPAIGNS_CT, "campaigns")
    print(f"    campaigns : {len(campaigns)}")
    adGroups  = _list(f"{ADS_API_BASE}/sp/adGroups/list", SP_ADGROUPS_CT, "adGroups")
    print(f"    adGroups  : {len(adGroups)}")
    keywords  = _list(f"{ADS_API_BASE}/sp/keywords/list", SP_KEYWORDS_CT, "keywords")
    print(f"    keywords  : {len(keywords)}")
    targets   = _list(f"{ADS_API_BASE}/sp/targets/list", SP_TARGETS_CT, "targetingClauses")
    print(f"    targets   : {len(targets)}")

    return {
        "profile_id":    pid,
        "profile_label": label,
        "captured_at":   dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "campaigns":     campaigns,
        "adGroups":      adGroups,
        "keywords":      keywords,
        "targets":       targets,
    }


def _write_snapshot(snap: dict) -> Path:
    pid = snap["profile_id"]
    out_dir = OUT_BASE / pid
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{ts}.json.gz"
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        json.dump(snap, f, default=str)

    # Rotate: keep only the 2 most recent .json.gz files per profile so
    # the repo doesn't grow by 192 files/day (48 runs × 4 profiles).
    # The diff script compares "the two most recent" anyway.
    all_snaps = sorted(out_dir.glob("*.json.gz"), reverse=True)
    for old in all_snaps[2:]:
        try:
            old.unlink()
        except Exception:
            pass
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", default=None,
                    help="Comma-separated profileIds to restrict the snapshot to.")
    args = ap.parse_args()

    print("Auth + list profiles…")
    token, profiles = get_access_token()
    if args.profiles:
        wanted = {p.strip() for p in args.profiles.split(",") if p.strip()}
        profiles = [p for p in profiles
                    if str(p.get("id") or p.get("profileId")) in wanted]
    if not profiles:
        print("No profiles selected — abort.")
        return 1

    print(f"Snapshotting {len(profiles)} profile(s)…")
    for prof in profiles:
        snap = _snapshot_profile(prof, token)
        path = _write_snapshot(snap)
        print(f"  → {path.relative_to(ROOT)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
