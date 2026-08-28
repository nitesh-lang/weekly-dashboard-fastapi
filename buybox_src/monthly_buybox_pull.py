"""
Monthly Amazon Ads sync — SP + SD + SB pull with L0-L4 SB attribution.

COPIED FROM: nitesh-lang/weekly-dashboard-fastapi @ ads_reports_pull.py
SCAFFOLDED:  2026-06-04 · L0-L4 attribution ported 2026-06-04
PROJECT:     C:\\Users\\Admin\\Buybox_report

What this does:
  • Submits 4 Reports API v3 jobs per profile:
      sp_adprod         spAdvertisedProduct      (SP, 95-day retention)
      sd_adprod         sdAdvertisedProduct      (SD, 60-day retention)
      sb_ad             sbAds   groupBy=ads      (SB ad-level, 60-day)
      sb_purchproduct   sbPurchasedProduct       (SB per-ASIN NTB sales)
  • Polls until all jobs complete (state file = race-safe).
  • Attributes SB spend to ASINs using the same L0-L4 cascade the
    weekly project uses (see sb_attribution.py) — campaign-level even
    split across ASINs returned by:
        L0  Ads-API campaign→ASIN map (sb_campaign_asins.json)
        L1  ASIN literal in campaign name
        L2  Model token in campaign name
        L3  Category token in campaign name
        L4  Synonym map (optional)
    Brand is assigned per-ASIN from sku_master.xlsx so Tonor ASINs
    running inside the AudioArray ad account get re-tagged to brand
    = Tonor (mirrors weekly step4 behaviour).  Each output row carries
    its attribution layer in the `method` column.
  • Writes per-brand, per-month CSVs:
        data/<Brand>/<YYYY-MM>/ads_sp.csv
        data/<Brand>/<YYYY-MM>/ads_sd.csv
        data/<Brand>/<YYYY-MM>/ads_sb.csv
        data/<Brand>/<YYYY-MM>/ads_sb_attributed.csv

Files this script reads (besides API):
  data/master/sku_master.xlsx          — copied from weekly project
  data/processed/sb_campaign_asins.json — L0 lookup (latest weekly cron output)
  data/<Brand>/<MonthName>/business_report.{xlsx,csv}
                                       — operator-dropped, drives active-ASIN set

Env (read from .env — same names as the weekly project for single source of truth):
  AMS_ADS_CLIENT_ID
  AMS_ADS_CLIENT_SECRET
  AMS_ADS_REFRESH_TOKEN     — AdPilot-style JSON dict; profiles auto-discover
  ADS_REGION                — NA | EU | FE  (default EU = India)

CLI:
  python monthly_buybox_pull.py submit            # last full calendar month
  python monthly_buybox_pull.py poll
  python monthly_buybox_pull.py run               # submit + poll blocking
  python monthly_buybox_pull.py run --start 2026-04-01 --end 2026-04-30
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent

# Load .env BEFORE reading env vars (real env wins).
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

# Region picker — ADS_REGION env var.  India = EU.
ADS_REGION_BASE = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",   # India lives here
    "FE": "https://advertising-api-fe.amazon.com",
}
ADS_API_BASE  = ADS_REGION_BASE.get(
    os.environ.get("ADS_REGION", "EU").upper(),
    ADS_REGION_BASE["EU"],
)
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
REPORTS_CT    = "application/vnd.createasyncreportrequest.v3+json"

DATA_DIR   = ROOT / "data"
STATE_FILE = ROOT / "data" / ".ads_reports_state.json"


# ─────────────────────────────────────────────────────────────────────────
# REPORT_TYPES — same 4 entries proven in the weekly cron
# ─────────────────────────────────────────────────────────────────────────
REPORT_TYPES: dict[str, dict] = {
    "sp_adprod": {
        "reportTypeId": "spAdvertisedProduct",
        "adProduct":    "SPONSORED_PRODUCTS",
        "groupBy":      ["advertiser"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "advertisedAsin", "advertisedSku",
            "impressions", "clicks", "cost",
            "purchases7d", "sales7d", "unitsSoldClicks7d",
        ],
        "asin_col":  "advertisedAsin",
        "sku_col":   "advertisedSku",
        "spend_col": "cost",
        "sales_col": "sales7d",
        "units_col": "unitsSoldClicks7d",
        "orders_col": "purchases7d",
        "out_name":  "ads_sp.csv",
    },
    "sd_adprod": {
        "reportTypeId": "sdAdvertisedProduct",
        "adProduct":    "SPONSORED_DISPLAY",
        "groupBy":      ["advertiser"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "promotedAsin", "promotedSku",
            "impressions", "clicks", "cost",
            "purchases", "sales", "unitsSold",
        ],
        "asin_col":  "promotedAsin",
        "sku_col":   "promotedSku",
        "spend_col": "cost",
        "sales_col": "sales",
        "units_col": "unitsSold",
        "orders_col": "purchases",
        "out_name":  "ads_sd.csv",
    },
    "sb_ad": {
        # SB has no per-ASIN cost — per-ad metrics here, attribution later.
        # Amazon v3: sbAds requires groupBy=["ads"] (plural).
        "reportTypeId": "sbAds",
        "adProduct":    "SPONSORED_BRANDS",
        "groupBy":      ["ads"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignName", "campaignId",
            "adGroupName",  "adGroupId",
            "adId",
            "impressions", "clicks", "cost",
            "purchases", "sales", "unitsSold",
        ],
        "spend_col":  "cost",
        "sales_col":  "sales",
        "units_col":  "unitsSold",
        "orders_col": "purchases",
        "out_name":   "ads_sb.csv",
    },
    "sb_purchproduct": {
        # Per-(adGroup × purchasedAsin) NTB sales — weights the SB split.
        # Amazon v3 sbPurchasedProduct only exposes NTB metrics at ASIN
        # grain, but NTB sales correlate strongly with total sales for
        # weighting purposes.
        "reportTypeId": "sbPurchasedProduct",
        "adProduct":    "SPONSORED_BRANDS",
        "groupBy":      ["purchasedAsin"],
        "timeUnit":     "DAILY",
        "columns": [
            "date",
            "campaignId", "adGroupId",
            "purchasedAsin",
            "newToBrandUnitsSold14d",
            "newToBrandSales14d",
            "newToBrandPurchases14d",
        ],
        "asin_col":  "purchasedAsin",
        "sales_col": "newToBrandSales14d",
        "out_name":  None,   # consumed by attribute_sb(), not written standalone
    },
}


# ─────────────────────────────────────────────────────────────────────────
# AUTH — same AMS_ADS_* env names as the weekly project (single source of truth)
# ─────────────────────────────────────────────────────────────────────────
def parse_refresh_token_env(raw: str) -> tuple[str, list[dict]]:
    """Returns (refresh_token, profiles_list).

    AMS_ADS_REFRESH_TOKEN format options:
      1. Bare 'Atzr|...' string  — profiles list will be empty;
                                    monthly project must use a JSON-dict
                                    token, OR profiles must be added below.
      2. AdPilot-style JSON dict — auto-discovers profiles + extracts token:
           {"<profile_id>": {"refresh_token": "Atzr|...",
                              "profile_name": "...",
                              "account_type": "seller"|"vendor",
                              "marketplace":  "IN", ...}, ...}
    """
    raw = (raw or "").strip()
    if len(raw) >= 2 and (
        (raw.startswith("'") and raw.endswith("'")) or
        (raw.startswith('"') and raw.endswith('"'))
    ):
        raw = raw[1:-1].strip()
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"AMS_ADS_REFRESH_TOKEN looks like JSON but failed to parse: {e}"
            )
        token: str | None = None
        profiles: list[dict] = []
        for pid, info in data.items():
            if not isinstance(info, dict):
                continue
            tok = info.get("refresh_token")
            if not tok:
                continue
            if token is None:
                token = tok
            profiles.append({
                "id":    str(info.get("profile_id") or pid),
                "label": info.get("profile_name") or pid,
                "brand": _brand_for_profile_name(info.get("profile_name") or ""),
            })
        if not token:
            raise RuntimeError("AMS_ADS_REFRESH_TOKEN JSON had no refresh_token values")
        return token, profiles
    # Bare token format — no profiles auto-discovered (operator should
    # switch to JSON-dict format to use this scaffold).
    return raw, []


# Profile name → brand folder.  Substring matched, case-insensitive.
# Used for per-row brand tagging when one ad account spans multiple brands
# (e.g. AudioArray account hosts both AA and Tonor ASINs).
PROFILE_NAME_TO_BRAND = [
    ("audio array",     "Audio Array"),
    ("audioarray",      "Audio Array"),
    ("nexlev",          "Nexlev"),
    ("white mulberry",  "White Mulberry"),
    ("cambium retail",  "White Mulberry"),
    ("tonor",           "Tonor"),
    ("fossil",          "Fossil"),
]


def _brand_for_profile_name(name: str) -> str:
    n = (name or "").lower()
    for kw, brand in PROFILE_NAME_TO_BRAND:
        if kw in n:
            return brand
    return "(unmapped)"


def get_access_token() -> tuple[str, list[dict]]:
    """Returns (access_token, profiles)."""
    cid = os.environ.get("AMS_ADS_CLIENT_ID", "")
    sec = os.environ.get("AMS_ADS_CLIENT_SECRET", "")
    tok_env = os.environ.get("AMS_ADS_REFRESH_TOKEN", "")
    missing = [k for k, v in (("AMS_ADS_CLIENT_ID", cid),
                              ("AMS_ADS_CLIENT_SECRET", sec),
                              ("AMS_ADS_REFRESH_TOKEN", tok_env)) if not v]
    if missing:
        raise RuntimeError(
            f"Missing env var(s): {', '.join(missing)}.  "
            f"Set in .env (or CI / Render → Environment)."
        )
    tok, profiles = parse_refresh_token_env(tok_env)
    r = requests.post(LWA_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": tok,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code >= 400:
        try:
            body = r.json()
            err = body.get("error", "unknown")
            desc = body.get("error_description", "")
            hint = ""
            if err == "invalid_client":
                hint = "  → CLIENT_ID or CLIENT_SECRET is wrong (or has whitespace)"
            elif err == "invalid_grant":
                hint = "  → REFRESH_TOKEN expired/revoked, or wrong LWA app"
            elif err == "invalid_request":
                hint = "  → Refresh token format is malformed"
            raise RuntimeError(
                f"LWA token endpoint returned {r.status_code} {err}: {desc}{hint}"
            )
        except (ValueError, KeyError):
            raise RuntimeError(f"LWA token endpoint returned {r.status_code}: {r.text[:200]}")
    return r.json()["access_token"], profiles


def base_headers(profile_id: str, access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": os.environ["AMS_ADS_CLIENT_ID"],
        "Amazon-Advertising-API-Scope":    profile_id,
    }


def _reports_headers(profile_id: str, token: str) -> dict:
    h = base_headers(profile_id, token)
    h["Content-Type"] = REPORTS_CT
    h["Accept"]       = REPORTS_CT
    return h


# ─────────────────────────────────────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────────────────────────────────────
def previous_calendar_month(today: dt.date | None = None) -> tuple[dt.date, dt.date]:
    """Returns (first_day, last_day) of the calendar month BEFORE today."""
    t = today or dt.date.today()
    first_this  = dt.date(t.year, t.month, 1)
    last_prev   = first_this - dt.timedelta(days=1)
    first_prev  = dt.date(last_prev.year, last_prev.month, 1)
    return first_prev, last_prev


def chunk_range(start: dt.date, end: dt.date, max_days: int = 31) -> list[tuple[dt.date, dt.date]]:
    """Amazon's reports cap at 31 days per request; split longer ranges."""
    out, cur = [], start
    while cur <= end:
        nxt = min(cur + dt.timedelta(days=max_days - 1), end)
        out.append((cur, nxt))
        cur = nxt + dt.timedelta(days=1)
    return out


def clip_start_for_retention(rtype_key: str, start: dt.date) -> dt.date:
    """SD and SB reports only retain ~60 days; SP retains ~95.  Floor the
    start date silently so backfill submits don't 400 on out-of-window."""
    if rtype_key in ("sd_adprod", "sb_ad", "sb_purchproduct"):
        floor = dt.date.today() - dt.timedelta(days=60)
        if start < floor:
            return floor
    elif rtype_key == "sp_adprod":
        floor = dt.date.today() - dt.timedelta(days=95)
        if start < floor:
            return floor
    return start


def month_label(d: dt.date) -> str:
    # Folder name for data/<Brand>/<month>/ -- "Jul26", not "2026-07".
    # The two-digit year is what keeps Jul 2025 and Jul 2026 in separate
    # folders; Scripts/generate_raw_data.js parses this shape back out.
    return f"{d.strftime('%b')}{d.strftime('%y')}"


# ─────────────────────────────────────────────────────────────────────────
# REPORTS API — submit / poll / download
# ─────────────────────────────────────────────────────────────────────────
def submit_report(profile_id: str, token: str, rtype_key: str,
                  start: dt.date, end: dt.date, max_retries: int = 6) -> str:
    cfg = REPORT_TYPES[rtype_key]
    body = {
        "name": f"{rtype_key} {start.isoformat()}_{end.isoformat()} p{profile_id}",
        "startDate": start.isoformat(),
        "endDate":   end.isoformat(),
        "configuration": {
            "adProduct":    cfg["adProduct"],
            "groupBy":      cfg["groupBy"],
            "columns":      cfg["columns"],
            "reportTypeId": cfg["reportTypeId"],
            "timeUnit":     cfg["timeUnit"],
            "format":       "GZIP_JSON",
        }
    }
    for attempt in range(max_retries):
        r = requests.post(f"{ADS_API_BASE}/reporting/reports",
                          headers=_reports_headers(profile_id, token),
                          json=body, timeout=60)
        if r.status_code == 429:
            time.sleep(10 * (attempt + 1))   # SB throttles hard; linear back-off
            continue
        if r.status_code >= 400:
            raise RuntimeError(
                f"submit {rtype_key}/{profile_id}/{start}-{end}: "
                f"{r.status_code} {r.text[:400]}"
            )
        return r.json()["reportId"]
    raise RuntimeError(
        f"submit {rtype_key}/{profile_id}/{start}-{end}: 429 (max retries)"
    )


def check_report(profile_id: str, token: str, report_id: str) -> dict:
    r = requests.get(f"{ADS_API_BASE}/reporting/reports/{report_id}",
                     headers=base_headers(profile_id, token), timeout=30)
    if r.status_code >= 400:
        return {"status": "ERROR_HTTP", "failureReason": f"{r.status_code} {r.text[:200]}"}
    return r.json()


def download_report(url: str) -> list[dict]:
    d = requests.get(url, timeout=180)
    raw = gzip.decompress(d.content)
    data = json.loads(raw)
    return data if isinstance(data, list) else []


# ─────────────────────────────────────────────────────────────────────────
# STATE FILE — survives crashes; lets submit + poll run independently
# ─────────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"jobs": []}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Don't persist downloaded row payloads — they can be huge.  Keep job
    # metadata only; rows are re-rolled into the writer's in-memory accum.
    slim = {**state, "jobs": [
        {k: v for k, v in j.items() if k != "rows"}
        for j in state.get("jobs", [])
    ]}
    STATE_FILE.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────
# SUBMIT ALL + POLL ALL
# ─────────────────────────────────────────────────────────────────────────
def submit_all(start: dt.date, end: dt.date) -> dict:
    token, profiles = get_access_token()
    if not profiles:
        raise RuntimeError(
            "No profiles discovered.  AMS_ADS_REFRESH_TOKEN should be the "
            "AdPilot-style JSON dict so profiles auto-discover.  See "
            "parse_refresh_token_env() docstring."
        )
    print(f"🔑 token OK · {len(profiles)} profile(s)")
    chunks = chunk_range(start, end, max_days=31)
    print(f"📅 range {start}..{end} → {len(chunks)} chunk(s)")

    jobs = []
    for prof in profiles:
        for rtype in REPORT_TYPES.keys():
            for (cs, ce) in chunks:
                cs_clipped = clip_start_for_retention(rtype, cs)
                if cs_clipped > ce:
                    print(f"  · skip {prof['label']:<30} {rtype:<14} {cs}..{ce} (outside retention window)")
                    continue
                if cs_clipped != cs:
                    print(f"  · clip {prof['label']:<30} {rtype:<14} {cs} → {cs_clipped} (retention floor)")
                try:
                    rid = submit_report(prof["id"], token, rtype, cs_clipped, ce)
                    print(f"  ✓ {prof['label']:<30} {rtype:<14} {cs_clipped}..{ce} → {rid}")
                    jobs.append({
                        "report_id":  rid,
                        "profile_id": prof["id"],
                        "label":      prof["label"],
                        "brand":      prof.get("brand", "(unmapped)"),
                        "rtype":      rtype,
                        "start":      cs_clipped.isoformat(),
                        "end":        ce.isoformat(),
                        "status":     "SUBMITTED",
                    })
                except Exception as e:
                    print(f"  ✗ {prof['label']:<30} {rtype:<14} {cs_clipped}..{ce}: {e}")
                    jobs.append({
                        "report_id":  None,
                        "profile_id": prof["id"],
                        "label":      prof["label"],
                        "brand":      prof.get("brand", "(unmapped)"),
                        "rtype":      rtype,
                        "start":      cs_clipped.isoformat(),
                        "end":        ce.isoformat(),
                        "status":     "SUBMIT_FAILED",
                        "error":      str(e)[:300],
                    })
                time.sleep(0.5)   # tiny stagger
    state = {"jobs": jobs, "submitted_at": dt.datetime.utcnow().isoformat()}
    save_state(state)
    print(f"💾 wrote {len(jobs)} job(s) to {STATE_FILE.relative_to(ROOT)}")
    return state


def poll_until_done(poll_every: int = 20, max_minutes: int = 60) -> dict:
    state = load_state()
    if not state.get("jobs"):
        print("ℹ no jobs in state file — nothing to poll.")
        return state
    token, _ = get_access_token()

    # Re-download rows for jobs already COMPLETED in a prior poll cycle.
    # State persists job metadata + URL but strips row payloads (too large),
    # so without this the writer would see those jobs as empty.
    rehydrate = [j for j in state["jobs"]
                 if j.get("status") == "COMPLETED" and j.get("url") and not j.get("rows")]
    if rehydrate:
        print(f"♻ re-downloading {len(rehydrate)} previously-completed job(s)")
        for j in rehydrate:
            try:
                rows = download_report(j["url"])
                j["rows"]      = rows
                j["row_count"] = len(rows)
                print(f"  ✓ {j['label']:<30} {j['rtype']:<14} {j['start']}..{j['end']} → {len(rows):,} rows (rehydrated)")
            except Exception as e:
                print(f"  ✗ {j['label']:<30} {j['rtype']:<14} {j['start']}..{j['end']} → rehydrate failed: {e}")

    deadline = time.time() + max_minutes * 60
    while time.time() < deadline:
        pending = [j for j in state["jobs"]
                   if j["status"] in ("SUBMITTED", "PENDING", "IN_QUEUE", "IN_PROGRESS", "PROCESSING")]
        if not pending:
            break
        print(f"⏳ {len(pending)} pending — polling...")
        for j in pending:
            if not j.get("report_id"):
                continue
            info = check_report(j["profile_id"], token, j["report_id"])
            j["status"] = info.get("status", "UNKNOWN")
            j["url"]    = info.get("url")
            if j["status"] == "COMPLETED" and j["url"]:
                rows = download_report(j["url"])
                j["row_count"] = len(rows)
                j["rows"]      = rows   # in-memory only; not persisted
                print(f"  ✓ {j['label']:<30} {j['rtype']:<14} {j['start']}..{j['end']} → {len(rows):,} rows")
            elif j["status"] == "FAILED":
                j["failure"] = info.get("failureReason", "")
                print(f"  ✗ {j['label']:<30} {j['rtype']:<14} {j['start']}..{j['end']} → FAILED: {j['failure']}")
        save_state(state)
        if any(j["status"] in ("SUBMITTED", "PENDING", "IN_QUEUE", "IN_PROGRESS", "PROCESSING") for j in state["jobs"]):
            time.sleep(poll_every)
    return state


# ─────────────────────────────────────────────────────────────────────────
# SB ATTRIBUTION — campaign-level L0-L4 cascade with even-split
# ─────────────────────────────────────────────────────────────────────────
# Ports weekly_app/etl/sb_ingest.py:attribute_campaign() from the weekly
# project so monthly numbers reconcile to weekly when rolled up.  See
# sb_attribution.py for the full cascade + the L0 / L1 / L2 / L3 layers.
#
# The earlier NTB-sales-weighted variant was removed because it produced
# different ASIN allocations than the weekly's even-split, breaking
# cross-project reconciliation.  sb_purchproduct rows still get pulled
# for future audit / debug use but are no longer fed into the splitter.
from sb_attribution import attribute_sb_l0_l4


def attribute_sb(sb_ad_rows: list[dict], _sb_purch_rows: list[dict],
                 target_year: int, target_month: int) -> list[dict]:
    """Campaign-level L0–L4 attribution with even-split distribution.
    See sb_attribution.attribute_sb_l0_l4 for the cascade detail.
    sb_purch_rows is accepted but unused — kept in the signature so
    the writer's call-site can pass it without changing shape later
    if we want to layer NTB-weighting back in as an L0.5 step.
    """
    return attribute_sb_l0_l4(sb_ad_rows, target_year, target_month)


# ─────────────────────────────────────────────────────────────────────────
# OUTPUT WRITER — per-brand, per-month CSVs
# ─────────────────────────────────────────────────────────────────────────
# Friendly column names — same shape as the weekly project's operator-
# exported xlsx so downstream code is portable.
def _normalise_rows(rtype_key: str, rows: list[dict]) -> pd.DataFrame:
    """Rename API columns to friendly names AND enrich every row's
    SKU / Model / Brand / Category via master-by-ASIN lookup.
    Mirrors the weekly's step4 'ASIN is truth' rule: raw API values
    for SKU/Model are NOT trusted — master is the source of truth.
    """
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    cfg = REPORT_TYPES[rtype_key]

    # 1) Pre-rename ASIN column to lowercase `asin` so the master
    #    enrichment helper has a stable column name to read from.
    if "asin_col" in cfg and cfg["asin_col"] in df.columns:
        df = df.rename(columns={cfg["asin_col"]: "asin"})
    # Stash the raw advertised SKU (API value) as `raw_sku` so the
    # canonical `sku` from master can overwrite without losing the
    # original — useful when auditing FBA vs FBP vs FBM SKU drift.
    if "sku_col" in cfg and cfg["sku_col"] in df.columns:
        df = df.rename(columns={cfg["sku_col"]: "raw_sku"})

    # 2) ASIN → master enrichment.  Writes canonical sku / model /
    #    brand / category_l0/l1/l2.  Rows whose ASIN isn't in master
    #    are left untouched (raw_sku still available as fallback).
    from sb_attribution import enrich_with_master
    if "asin" in df.columns:
        for col in ("sku", "model", "brand",
                    "category_l0", "category_l1", "category_l2"):
            if col not in df.columns:
                df[col] = ""
        df = enrich_with_master(df, asin_col="asin")

    # 3) Friendly metric names — match operator's xlsx convention so
    #    downstream React dashboard / Excel pivots feel familiar.
    rename: dict[str, str] = {}
    if cfg.get("spend_col") in df.columns:
        rename[cfg["spend_col"]]  = "Spend"
    if cfg.get("sales_col") in df.columns:
        rename[cfg["sales_col"]]  = "14 Day Total Sales (₹)"
    if cfg.get("units_col") in df.columns:
        rename[cfg["units_col"]]  = "14 Day Total Units (#)"
    if cfg.get("orders_col") in df.columns:
        rename[cfg["orders_col"]] = "ams_orders"
    if rename:
        df = df.rename(columns=rename)
    if "impressions" in df.columns:
        df = df.rename(columns={"impressions": "Impressions"})
    if "clicks" in df.columns:
        df = df.rename(columns={"clicks": "Clicks"})
    return df


def write_output(state: dict, start: dt.date) -> None:
    """Per-brand, per-month CSVs:
        data/<Brand>/<YYYY-MM>/ads_sp.csv
        data/<Brand>/<YYYY-MM>/ads_sd.csv
        data/<Brand>/<YYYY-MM>/ads_sb.csv
        data/<Brand>/<YYYY-MM>/ads_sb_attributed.csv
    """
    by_brand_rtype: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for j in state.get("jobs", []):
        if j.get("status") != "COMPLETED":
            continue
        brand = j.get("brand", "(unmapped)")
        rtype = j["rtype"]
        for r in (j.get("rows") or []):
            by_brand_rtype[(brand, rtype)].append(r)

    if not by_brand_rtype:
        print("⚠ no completed rows — check state file for failures.")
        return

    mlabel = month_label(start)
    print(f"📁 writing brand × rtype CSVs under data/<brand>/{mlabel}/")

    # ── Per-rtype: pool rows across ALL profiles, enrich with master
    #    (brand is set canonically per-row via ASIN), THEN split by the
    #    post-enrichment brand.  This is the SP/SD equivalent of the SB
    #    global-attribute pattern below.  Catches Tonor ASINs running
    #    inside the AudioArray profile and routes them to data/Tonor/.
    for rtype in ("sp_adprod", "sd_adprod", "sb_ad"):
        pooled: list[dict] = []
        for (_b, r_t), rows in by_brand_rtype.items():
            if r_t == rtype:
                pooled.extend(rows)
        if not pooled:
            continue
        df = _normalise_rows(rtype, pooled)
        if df.empty or "brand" not in df.columns:
            continue
        # Master-derived brand may be blank for off-master ASINs; bucket
        # those into "(unmapped)" so they stay visible without polluting
        # a real brand folder.
        df["brand"] = df["brand"].astype(str).str.strip().replace(
            {"": "(unmapped)", "nan": "(unmapped)", "None": "(unmapped)"}
        )
        for brand_out in sorted(df["brand"].dropna().unique()):
            sub = df[df["brand"] == brand_out]
            if sub.empty:
                continue
            bdir = DATA_DIR / brand_out / mlabel
            bdir.mkdir(parents=True, exist_ok=True)
            out = bdir / REPORT_TYPES[rtype]["out_name"]
            sub.to_csv(out, index=False)
            print(f"  · {brand_out:<20} {out.relative_to(ROOT)}  ({len(sub):,} rows)")

    # ── Global SB attribution (cross-profile) ──
    all_sb_ad: list[dict] = []
    all_sb_purch: list[dict] = []
    for (_b, rtype), rows in by_brand_rtype.items():
        if rtype == "sb_ad":
            all_sb_ad.extend(rows)
        elif rtype == "sb_purchproduct":
            all_sb_purch.extend(rows)

    if all_sb_ad:
        attributed = attribute_sb(all_sb_ad, all_sb_purch, start.year, start.month)
        if attributed:
            df_attr = pd.DataFrame(attributed)
            # Split by the brand the cascade returned; unmapped goes to
            # data/(unmapped)/<month>/ so it stays visible without
            # polluting any brand folder.
            for brand_out in sorted(df_attr["brand"].dropna().unique()):
                sub = df_attr[df_attr["brand"] == brand_out]
                if sub.empty:
                    continue
                folder = "(unmapped)" if (brand_out or "").lower() in ("", "(unmapped)") else brand_out
                bdir = DATA_DIR / folder / mlabel
                bdir.mkdir(parents=True, exist_ok=True)
                out = bdir / "ads_sb_attributed.csv"
                sub.to_csv(out, index=False)
                layer_counts = sub["method"].value_counts().to_dict()
                layer_str = " · ".join(f"{k}={v}" for k, v in sorted(layer_counts.items()))
                print(f"  · {folder:<20} {out.relative_to(ROOT)}  ({len(sub):,} rows; {layer_str})")


# ─────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────
def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Monthly Amazon Ads sync (SP+SD+SB)")
    ap.add_argument("mode", choices=["submit", "poll", "run"])
    ap.add_argument("--start", type=_parse_date, default=None,
                    help="YYYY-MM-DD (default: first day of previous calendar month)")
    ap.add_argument("--end",   type=_parse_date, default=None,
                    help="YYYY-MM-DD (default: last day of previous calendar month)")
    args = ap.parse_args(argv)

    if args.start is None or args.end is None:
        first, last = previous_calendar_month()
        if args.start is None: args.start = first
        if args.end   is None: args.end   = last
    print(f"📅 range: {args.start}  →  {args.end}")

    if args.mode == "submit":
        submit_all(args.start, args.end)
    elif args.mode == "poll":
        state = poll_until_done()
        write_output(state, args.start)
    elif args.mode == "run":
        submit_all(args.start, args.end)
        state = poll_until_done()
        write_output(state, args.start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
