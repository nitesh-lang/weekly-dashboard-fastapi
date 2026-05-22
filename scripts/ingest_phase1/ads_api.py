"""Ads API v3 client — list profiles, request SP/SD/SB reports, poll, download.

Reference: https://advertising.amazon.com/API/docs/en-us/reference/api-overview
"""
from __future__ import annotations

import gzip
import io
import json
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd

from .auth import get_access_token
from .config import Config

REPORT_POLL_INTERVAL_S = 15
REPORT_POLL_TIMEOUT_S = 60 * 30  # 30 min — Ads API typically finishes in 5-10


# ── Column rename: API column → existing xlsx column ──
# Existing manual upload (read by etl/step3_ads_aggregation.py) uses these names.
SP_COLUMN_RENAME = {
    "advertisedAsin": "Advertised ASIN",
    "campaignName": "Campaign Name",
    "impressions": "Impressions",
    "clicks": "Clicks",
    "cost": "Spend",
    "sales14d": "14 Day Total Sales (₹)",
    "unitsSoldClicks14d": "14 Day Total Units (#)",
}
SD_COLUMN_RENAME = {
    "promotedAsin": "Advertised ASIN",
    "campaignName": "Campaign Name",
    "impressions": "Impressions",
    "clicks": "Clicks",
    "cost": "Spend",
    "sales": "14 Day Total Sales (₹)",
    "unitsSold": "14 Day Total Units (#)",
}
SB_COLUMN_RENAME = {
    # SB doesn't expose advertised ASIN at the campaign level; we leave blank
    # so master_split routes it to Unassigned for human review.
    "campaignName": "Campaign Name",
    "impressions": "Impressions",
    "clicks": "Clicks",
    "cost": "Spend",
    "sales": "14 Day Total Sales (₹)",
    "unitsSold": "14 Day Total Units (#)",
}

OUTPUT_COLUMNS = [
    "Advertised ASIN",
    "Campaign Name",
    "Impressions",
    "Clicks",
    "Spend",
    "14 Day Total Sales (₹)",
    "14 Day Total Units (#)",
]


@dataclass
class ReportSpec:
    sheet: str                # "SP" | "SD" | "SB"
    name: str                 # human label
    payload: dict[str, Any]   # POST body for /reporting/reports
    rename: dict[str, str]


def _headers(cfg: Config, token: str, profile_id: str | None = None) -> dict[str, str]:
    h = {
        "Amazon-Advertising-API-ClientId": cfg.client_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
    }
    if profile_id:
        h["Amazon-Advertising-API-Scope"] = str(profile_id)
    return h


def list_profiles(cfg: Config) -> list[dict[str, Any]]:
    token = get_access_token(cfg)
    headers = {
        "Amazon-Advertising-API-ClientId": cfg.client_id,
        "Authorization": f"Bearer {token}",
    }
    resp = httpx.get(f"{cfg.ads_endpoint}/v2/profiles", headers=headers, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def _build_report_specs(start: date, end: date) -> list[ReportSpec]:
    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")
    # IMPORTANT: v3 reports default to ENABLED-only when no state filter is
    # set. Historical spend lives on campaigns that are now paused, so include
    # all states. Filter field name differs by groupBy:
    #   - groupBy=["advertiser"] (SP, SD): adCreativeStatus
    #   - groupBy=["campaign"]   (SB):     campaignStatus
    ad_states_filter = [
        {"field": "adCreativeStatus", "values": ["ENABLED", "PAUSED", "ARCHIVED"]}
    ]
    campaign_states_filter = [
        {"field": "campaignStatus",   "values": ["ENABLED", "PAUSED", "ARCHIVED"]}
    ]
    return [
        ReportSpec(
            sheet="SP",
            name="Sponsored Products — advertised product",
            rename=SP_COLUMN_RENAME,
            payload={
                "name": f"SP advertisedProduct {start_s}_{end_s}",
                "startDate": start_s,
                "endDate": end_s,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy": ["advertiser"],
                    "columns": [
                        "advertisedAsin",
                        "campaignName",
                        "impressions",
                        "clicks",
                        "cost",
                        "sales14d",
                        "unitsSoldClicks14d",
                    ],
                    "filters": ad_states_filter,
                    "reportTypeId": "spAdvertisedProduct",
                    "timeUnit": "SUMMARY",
                    "format": "GZIP_JSON",
                },
            },
        ),
        ReportSpec(
            sheet="SD",
            name="Sponsored Display — advertised product",
            rename=SD_COLUMN_RENAME,
            payload={
                "name": f"SD advertisedProduct {start_s}_{end_s}",
                "startDate": start_s,
                "endDate": end_s,
                "configuration": {
                    "adProduct": "SPONSORED_DISPLAY",
                    "groupBy": ["advertiser"],
                    "columns": [
                        "promotedAsin",
                        "campaignName",
                        "impressions",
                        "clicks",
                        "cost",
                        "sales",
                        "unitsSold",
                    ],
                    "filters": ad_states_filter,
                    "reportTypeId": "sdAdvertisedProduct",
                    "timeUnit": "SUMMARY",
                    "format": "GZIP_JSON",
                },
            },
        ),
        ReportSpec(
            sheet="SB",
            name="Sponsored Brands — campaign",
            rename=SB_COLUMN_RENAME,
            payload={
                "name": f"SB campaign {start_s}_{end_s}",
                "startDate": start_s,
                "endDate": end_s,
                "configuration": {
                    "adProduct": "SPONSORED_BRANDS",
                    "groupBy": ["campaign"],
                    "columns": [
                        "campaignName",
                        "impressions",
                        "clicks",
                        "cost",
                        "sales",
                        "unitsSold",
                    ],
                    "filters": campaign_states_filter,
                    "reportTypeId": "sbCampaigns",
                    "timeUnit": "SUMMARY",
                    "format": "GZIP_JSON",
                },
            },
        ),
    ]


def _request_report(cfg: Config, profile_id: str, spec: ReportSpec) -> str:
    url = f"{cfg.ads_endpoint}/reporting/reports"
    for attempt in range(5):
        token = get_access_token(cfg)
        resp = httpx.post(
            url,
            headers=_headers(cfg, token, profile_id),
            content=json.dumps(spec.payload),
            timeout=30.0,
        )
        if resp.status_code == 429:
            wait = 5 * (2 ** attempt)  # 5, 10, 20, 40, 80
            print(f"    [{spec.sheet}] 429 throttled — retry in {wait}s (attempt {attempt+1}/5)", flush=True)
            time.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(
                f"[{spec.sheet}] create report failed ({resp.status_code}): {resp.text[:400]}"
            )
        return resp.json()["reportId"]
    raise RuntimeError(f"[{spec.sheet}] still 429 after 5 retries — give Amazon a break and rerun later")


def _poll_report(cfg: Config, profile_id: str, report_id: str, label: str) -> str:
    deadline = time.time() + REPORT_POLL_TIMEOUT_S
    url = f"{cfg.ads_endpoint}/reporting/reports/{report_id}"
    last_status = "PENDING"
    while time.time() < deadline:
        token = get_access_token(cfg)
        headers = {
            "Amazon-Advertising-API-ClientId": cfg.client_id,
            "Amazon-Advertising-API-Scope": str(profile_id),
            "Authorization": f"Bearer {token}",
        }
        r = httpx.get(url, headers=headers, timeout=30.0)
        r.raise_for_status()
        body = r.json()
        status = body.get("status", "UNKNOWN")
        if status != last_status:
            print(f"    [{label}] status={status}", flush=True)
            last_status = status
        if status == "COMPLETED":
            return body["url"]
        if status == "FAILED":
            raise RuntimeError(f"[{label}] report FAILED: {body.get('failureReason')}")
        time.sleep(REPORT_POLL_INTERVAL_S)
    raise TimeoutError(f"[{label}] report did not complete within {REPORT_POLL_TIMEOUT_S}s")


def _download_report(download_url: str) -> list[dict[str, Any]]:
    # The download URL is presigned S3 — do NOT send Ads API headers, they'll
    # be rejected by S3 as an extra signed header.
    r = httpx.get(download_url, timeout=120.0, follow_redirects=True)
    r.raise_for_status()
    raw = r.content
    try:
        decompressed = gzip.decompress(raw)
    except OSError:
        decompressed = raw  # already plain JSON
    data = json.loads(decompressed.decode("utf-8"))
    if isinstance(data, dict) and "rows" in data:
        return data["rows"]
    if isinstance(data, list):
        return data
    raise RuntimeError(f"Unexpected report payload shape: {type(data).__name__}")


def fetch_reports_for_profile(
    cfg: Config,
    profile_id: str,
    *,
    start: date,
    end: date,
    profile_label: str,
) -> dict[str, pd.DataFrame]:
    """Returns {sheet_name: DataFrame} with normalised columns."""
    specs = _build_report_specs(start, end)
    out: dict[str, pd.DataFrame] = {}
    for spec in specs:
        label = f"{profile_label}/{spec.sheet}"
        print(f"  -> requesting {label} ({spec.name})...", flush=True)
        try:
            rid = _request_report(cfg, profile_id, spec)
            url = _poll_report(cfg, profile_id, rid, label)
            rows = _download_report(url)
        except RuntimeError as e:
            # Common case: account isn't enrolled in SB or SD — skip that sheet,
            # don't blow up the entire pull.
            msg = str(e).lower()
            if "not enrolled" in msg or "no access" in msg or "not authorized" in msg:
                print(f"    [{label}] skipped — account not enrolled in this ad product", flush=True)
                out[spec.sheet] = pd.DataFrame(columns=OUTPUT_COLUMNS)
                continue
            raise

        df = pd.DataFrame(rows)
        if df.empty:
            df = pd.DataFrame(columns=list(spec.rename.keys()))
        df = df.rename(columns=spec.rename)

        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = "" if col in ("Advertised ASIN", "Campaign Name") else 0
        df = df[OUTPUT_COLUMNS]

        if "Advertised ASIN" in df.columns:
            df["Advertised ASIN"] = df["Advertised ASIN"].fillna("").astype(str).str.strip().str.upper()

        for col in ("Impressions", "Clicks", "Spend", "14 Day Total Sales (₹)", "14 Day Total Units (#)"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        out[spec.sheet] = df
        print(f"    [{label}] rows={len(df)}, spend={df['Spend'].sum():.2f}", flush=True)

        # If the report claimed COMPLETED but returned no rows, save the raw
        # download for postmortem — these are rare and worth seeing.
        if len(df) == 0:
            try:
                from pathlib import Path
                from .config import OUTPUT_ROOT
                debug_dir = OUTPUT_ROOT.parent / "_debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                ts = time.strftime("%Y%m%d_%H%M%S")
                debug_path = debug_dir / f"{profile_label}_{spec.sheet}_{ts}.json"
                debug_path.write_text(json.dumps(rows, indent=2)[:200_000], encoding="utf-8")
                print(f"    [{label}] (0-rows) raw response saved -> {debug_path}", flush=True)
            except Exception as e:
                print(f"    [{label}] (0-rows) debug dump failed: {e}", flush=True)
    return out


def week_bounds(week: int, year: int) -> tuple[date, date]:
    """Sunday-start week. Week 1 = first Sunday of the year (or Dec 31 prior)."""
    jan1 = date(year, 1, 1)
    # Move to the Sunday of week 1: the Sunday on/before jan1.
    # weekday(): Mon=0..Sun=6. Days since last Sunday = (weekday+1) % 7.
    delta = (jan1.weekday() + 1) % 7
    week1_sunday = jan1 - timedelta(days=delta)
    start = week1_sunday + timedelta(weeks=week - 1)
    end = start + timedelta(days=6)
    return start, end
