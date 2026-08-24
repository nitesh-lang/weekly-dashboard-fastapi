"""
SP-API Vendor Sales Pull (1P / Vendor Central) — Weekly Pipeline
================================================================

Pulls Sun-Sat weekly shipped units + revenue per ASIN from the Vendor
Sales Report, splits by brand via sku_master.xlsx, writes per-brand
xlsx files into the same week folder the SOH script uses.

Mirrors scripts/sp_vendor_soh_pull.py — same accounts, same auth
pattern, same brand split, same audit folder.  Only the report type +
schema differs.

Accounts (must match SOH script):
    AUDIOARRAY     -> Cambium Retail Pvt Ltd vendor account (AA + Tonor)
    WHITEMULBERRY  -> Clicktech Retail Pvt Ltd (CRPL) vendor account (WM)

Output (per current week NN, Sun-Sat convention):
    data/raw/sales/Week {NN}/Audio_Array/Vendor Sales (SP-API).xlsx
    data/raw/sales/Week {NN}/Tonor/Vendor Sales (SP-API).xlsx
    data/raw/sales/Week {NN}/White_Mulberry/Vendor Sales (SP-API).xlsx
    data/raw/sales/Week {NN}/_audit/vendor_sales_unmatched_<acct>.csv

Schema produced (compatible with the "1p Sales" sheet inside the
operator's manual other_channels.xlsx — same key columns):
    ASIN, SKU, Brand, Model, category_l0, category_l1, category_l2,
    Qty (shippedUnits), Sale (shippedRevenue ₹), OrderedUnits,
    OrderedSale, CustomerReturns, Channel="1p Sales", Week,
    WindowStart, WindowEnd

Required env vars (.env — same as SOH script):
    SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY
    SP_API_VENDOR_REFRESH_TOKEN_WHITEMULBERRY
    SP_LWA_CLIENT_ID_AUDIOARRAY      SP_LWA_CLIENT_SECRET_AUDIOARRAY
    SP_LWA_CLIENT_ID_WHITEMULBERRY   SP_LWA_CLIENT_SECRET_WHITEMULBERRY

Window convention:
    Default target window = the last fully closed Sun-Sat week.
    Run on Mon → window = previous Sun..Sat (e.g. Mon 2026-06-15 →
    Sun 2026-06-07 .. Sat 2026-06-13 = W24).

    Amazon's vendor data has the same ~3-day publication lag as the
    SOH report.  The script auto-walks-back the window's end date
    one day at a time (up to 3 days) if Amazon says "not yet
    available", so a Mon morning run might return Sun..Fri instead
    of Sun..Sat.  WindowEnd column shows what Amazon actually
    returned.

Usage:
    python scripts/sp_vendor_sales_pull.py                       # last full week
    python scripts/sp_vendor_sales_pull.py --account AUDIOARRAY
    python scripts/sp_vendor_sales_pull.py --week-end 2026-06-13  # force Sat
"""
from __future__ import annotations

import argparse
import functools
import gzip
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass
print = functools.partial(print, flush=True)

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
MARKETPLACE_ID = "A21TJRUUN4KGV"
REPO_ROOT  = Path(__file__).resolve().parent.parent
SKU_MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"

ACCOUNTS = {
    # CRPL holds Audio Array + Tonor + White Mulberry — loop all three.
    "AUDIOARRAY": {
        "Audio Array":    "Audio_Array",
        "Tonor":          "Tonor",
        "White Mulberry": "White_Mulberry",
    },
    # Clicktech (legacy / dormant — kept for completeness).
    "WHITEMULBERRY": {
        "White Mulberry": "White_Mulberry",
    },
}

OUTPUT_COLS = [
    "ASIN", "SKU", "Brand", "Model",
    "category_l0", "category_l1", "category_l2",
    # Primary metrics (vendor's invoiceable view).  `Qty` = OrderedUnits
    # and `Sale` = OrderedRevenue because under MANUFACTURING
    # distributorView shippedRevenue is always ₹0 (Amazon recognises
    # revenue at order-acceptance time, not shipment).
    "Qty", "Sale",
    # Auxiliary: physical movement.  ShippedRevenue is ₹0 under
    # MANUFACTURING view; ShippedUnits still tracks units moved.
    "ShippedUnits", "ShippedRevenue",
    "CustomerReturns", "Channel", "Week",
    "WindowStart", "WindowEnd",
]


def get_access_token(acct: str) -> str:
    cid = os.environ.get(f"SP_LWA_CLIENT_ID_{acct}")     or os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get(f"SP_LWA_CLIENT_SECRET_{acct}") or os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    if not rt:
        raise SystemExit(f"Missing SP_API_VENDOR_REFRESH_TOKEN_{acct} in .env")
    if not cid or not sec:
        raise SystemExit(f"Missing LWA creds for {acct}")
    r = requests.post(LWA_URL, data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": cid, "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"LWA failed for {acct}: HTTP {r.status_code} {r.text[:300]}")
    return r.json()["access_token"]


def last_full_sun_sat_week(reference: datetime | None = None) -> tuple[str, str]:
    """Return (sunday_iso, saturday_iso) for the last completed Sun-Sat
    week as of `reference` (UTC now by default).

    Run on Mon 2026-06-15 → returns ('2026-06-07', '2026-06-13') = W24.
    """
    ref = reference or datetime.now(timezone.utc)
    # weekday(): Mon=0 ... Sun=6.  We want the Saturday on or before ref.
    # Saturday's weekday is 5.  Days to step back: (weekday + 2) % 7.
    days_back_to_sat = (ref.weekday() + 2) % 7
    saturday = (ref - timedelta(days=days_back_to_sat)).date()
    sunday   = saturday - timedelta(days=6)
    return sunday.isoformat(), saturday.isoformat()


def sun_sat_week_number(date_str: str) -> int:
    """Same convention as the SOH script — Sun-Sat week number for a date."""
    d = datetime.fromisoformat(date_str).date()
    return (d + timedelta(days=1)).isocalendar().week


class DataNotAvailable(Exception):
    pass


def _read_error_doc(token: str, doc_id: str) -> str:
    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": token}, timeout=30)
    if rd.status_code != 200:
        return ""
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=60)
    if dl.status_code != 200:
        return ""
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def _reuse_done_report(token: str, report_type: str,
                       start_date: str, end_date: str) -> str | None:
    """Most recent DONE report of this type+window, but only if created in
    the last 24h.  Ported from sp_fba_shipments_pull with one addition: the
    age cap.  Vendor report CONTENT matures as Amazon publishes late data —
    reusing a same-window report from days ago would silently freeze stale
    numbers, which is the exact class of bug this repo keeps fighting."""
    try:
        r = requests.get(
            f"{SPAPI_HOST}/reports/2021-06-30/reports",
            params={"reportTypes": report_type, "marketplaceIds": MARKETPLACE_ID,
                    "processingStatuses": "DONE", "pageSize": 10},
            headers={"x-amz-access-token": token}, timeout=30)
        if r.status_code != 200:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        for rpt in (r.json().get("reports") or []):
            if (rpt.get("dataStartTime") or "")[:10] != start_date:
                continue
            if (rpt.get("dataEndTime") or "")[:10] != end_date:
                continue
            created = rpt.get("createdTime") or ""
            try:
                if datetime.fromisoformat(created.replace("Z", "+00:00")) < cutoff:
                    continue
            except ValueError:
                continue
            return rpt.get("reportDocumentId")
    except requests.RequestException:
        pass
    return None


def _adaptive_polls(budget_seconds: int):
    """Yield (waited_so_far, delay_just_slept).  2s doubling to a 30s cap —
    fast reports return in seconds instead of eating flat 5s ticks, slow
    ones still get the full budget."""
    delay, waited = 2.0, 0.0
    while waited < budget_seconds:
        time.sleep(delay)
        waited += delay
        yield waited, delay
        delay = min(delay * 2, 30.0)


def pull_vendor_sales(token: str, start_iso: str, end_iso: str) -> list[dict]:
    """Submit + poll + download the Vendor Sales Report for [start, end]
    inclusive of the Saturday end-date.  Returns the salesAggregate /
    salesByAsin list of per-(asin, day) rows."""
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    start_dt = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end_iso  ).replace(tzinfo=timezone.utc, hour=23, minute=59, second=59)
    body = {
        "reportType": "GET_VENDOR_SALES_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataEndTime":   end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportOptions": {
            "reportPeriod": "DAY",
            "distributorView": "MANUFACTURING",
            "sellingProgram": "RETAIL",
        },
    }
    # Report reuse REMOVED 2026-08-24 — the LIST endpoint hides
    # reportOptions, so a same-window report with different
    # reportPeriod/distributorView is indistinguishable.  It silently
    # corrupted W34 seller sales; vendor carries the same risk.
    doc_id = None
    if True:
        print(f"  create report: {body['dataStartTime']} -> {body['dataEndTime']}")
        r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=headers, timeout=30)
        if r.status_code != 202:
            raise SystemExit(f"create report failed: HTTP {r.status_code} {r.text[:300]}")
        rep_id = r.json()["reportId"]
        print(f"  reportId: {rep_id}")

        # 15-min budget, adaptive 2s→30s.  Vendor reports occasionally need
        # all of it under Amazon-side queue load.
        for waited, _ in _adaptive_polls(900):
            rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rep_id}",
                              headers={"x-amz-access-token": token}, timeout=30)
            if rr.status_code != 200:
                continue
            j = rr.json()
            status = j.get("processingStatus")
            print(f"  poll[{int(waited)}s]: {status}")
            if status == "DONE":
                doc_id = j.get("reportDocumentId")
                break
            if status in ("FATAL", "CANCELLED"):
                doc_id = j.get("reportDocumentId")
                err_text = _read_error_doc(token, doc_id) if doc_id else ""
                if "not yet available" in err_text.lower():
                    raise DataNotAvailable(err_text.strip())
                print(f"  FATAL body: {err_text[:600]}")
                raise SystemExit(f"report {status}")
        if not doc_id:
            raise SystemExit("timed out waiting for report")

    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": token}, timeout=30)
    rd.raise_for_status()
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=120)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8", errors="replace"))
    # Response has BOTH salesAggregate (account-level daily totals, no
    # asin key) and salesByAsin (per-ASIN daily rows).  We want the
    # per-ASIN view — picking salesByAsin first.
    return payload.get("salesByAsin") or payload.get("salesAggregate") or []


def aggregate_window(rows: list[dict], window_start: str, window_end: str) -> pd.DataFrame:
    """Sum per-ASIN shipped / ordered units + revenue across the window's
    daily rows.  Returns a tidy DataFrame keyed by ASIN."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "startDate" in df.columns:
        df["startDate"] = df["startDate"].astype(str)
        df = df[(df["startDate"] >= window_start) & (df["startDate"] <= window_end)]
    if df.empty:
        return pd.DataFrame()

    def _amt(x):
        return x.get("amount") if isinstance(x, dict) else None

    df["shipped_amt"] = df.get("shippedRevenue", pd.Series([None]*len(df))).apply(_amt)
    df["ordered_amt"] = df.get("orderedRevenue", pd.Series([None]*len(df))).apply(_amt)
    df["asin_n"]      = df["asin"].astype(str).str.strip().str.upper()
    df["shippedUnits"]    = pd.to_numeric(df.get("shippedUnits"),    errors="coerce").fillna(0)
    df["orderedUnits"]    = pd.to_numeric(df.get("orderedUnits"),    errors="coerce").fillna(0)
    df["customerReturns"] = pd.to_numeric(df.get("customerReturns"), errors="coerce").fillna(0)
    df["shipped_amt"]     = pd.to_numeric(df["shipped_amt"], errors="coerce").fillna(0)
    df["ordered_amt"]     = pd.to_numeric(df["ordered_amt"], errors="coerce").fillna(0)

    g = df.groupby("asin_n", as_index=False).agg(
        # Primary = ORDERED (revenue-bearing).  Auxiliary = SHIPPED
        # (physical movement; revenue is ₹0 under MANUFACTURING view).
        Qty=("orderedUnits", "sum"),
        Sale=("ordered_amt", "sum"),
        ShippedUnits=("shippedUnits", "sum"),
        ShippedRevenue=("shipped_amt", "sum"),
        CustomerReturns=("customerReturns", "sum"),
    )
    g = g.rename(columns={"asin_n": "ASIN"})
    g["Qty"]             = g["Qty"].astype(int)
    g["ShippedUnits"]    = g["ShippedUnits"].astype(int)
    g["CustomerReturns"] = g["CustomerReturns"].astype(int)
    g["Sale"]            = g["Sale"].round(2)
    g["ShippedRevenue"]  = g["ShippedRevenue"].round(2)
    return g


def annotate_with_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if not SKU_MASTER.exists():
        print(f"  WARN: {SKU_MASTER} not found — Brand/Model left blank")
        df["Brand"] = ""
        df["Model"] = ""
        return df
    sm = pd.read_excel(SKU_MASTER)
    sm.columns = sm.columns.str.strip()
    sm["ASIN"] = sm["ASIN"].astype(str).str.strip().str.upper()
    keep = ["ASIN", "Brand", "Model", "category_l0", "category_l1", "category_l2", "FBA SKU"]
    sm = sm[[c for c in keep if c in sm.columns]].drop_duplicates(subset=["ASIN"], keep="first")
    sm = sm.rename(columns={"FBA SKU": "SKU"})
    return df.merge(sm, on="ASIN", how="left")


def write_brand_xlsx(df: pd.DataFrame, brand: str, week_no: int,
                     folder_slug: str, window_start: str, window_end: str) -> int:
    sub = df[df["Brand"].astype(str).str.strip().str.lower() == brand.lower()].copy()
    if sub.empty:
        print(f"  WARN: no ASINs matched brand={brand}")
    sub["Channel"]     = "1p Sales"
    sub["Week"]        = week_no
    sub["WindowStart"] = window_start
    sub["WindowEnd"]   = window_end
    for col in OUTPUT_COLS:
        if col not in sub.columns:
            sub[col] = ""
    sub = sub[OUTPUT_COLS]
    out_dir = REPO_ROOT / "data" / "raw" / "sales" / f"Week {week_no}" / folder_slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "Vendor Sales (SP-API).xlsx"
    sub.to_excel(out_path, index=False)
    print(f"  -> {out_path.relative_to(REPO_ROOT)}  ({len(sub)} rows, sum Qty={int(sub['Qty'].sum())}, sum Sale=₹{float(sub['Sale'].sum()):,.0f})")
    return len(sub)


def run_account(acct: str, window_start: str, window_end: str, fallback_days: int = 3) -> None:
    brands = ACCOUNTS[acct]
    print(f"\n=== {acct} (window {window_start} -> {window_end}) ===")
    tok = get_access_token(acct)

    rows = None
    effective_end = window_end
    try_end = window_end
    # Anchor start at the operator Sunday — when the end shifts back because
    # of Amazon's publication lag, the window must SHRINK (partial-week)
    # rather than slide back as a fixed 7-day block.  Sliding back leaks
    # the previous week's Saturday into the current week (e.g. AA W24:
    # falling back from Jun 13 -> Jun 12 dragged Jun 6 in, +₹5.4L bleed).
    anchored_start = window_start
    for attempt in range(fallback_days + 1):
        try_end = (datetime.fromisoformat(window_end).date() - timedelta(days=attempt)).isoformat()
        try_start = anchored_start
        try:
            rows = pull_vendor_sales(tok, try_start, try_end)
            effective_end = try_end
            if attempt > 0:
                days = (datetime.fromisoformat(try_end).date() - datetime.fromisoformat(anchored_start).date()).days + 1
                print(f"  fell back window end {window_end} -> {try_end} (publication lag; partial {days}-day pull)")
            break
        except DataNotAvailable:
            print(f"  data not available for {try_end}; trying earlier date")
            continue
    if rows is None:
        print(f"  gave up after {fallback_days+1} attempts; latest tried = {try_end}")
        return

    effective_start = anchored_start
    print(f"  daily rows pulled: {len(rows)}")
    snap = aggregate_window(rows, effective_start, effective_end)
    if snap.empty:
        print(f"  no sales rows for {acct} in {effective_start}..{effective_end}")
        return
    print(f"  effective window: {effective_start}..{effective_end}, ASINs={len(snap)}, units={int(snap['Qty'].sum())}, sale=₹{float(snap['Sale'].sum()):,.0f}")
    annotated = annotate_with_master(snap)
    present = annotated["Brand"].astype(str).str.strip().value_counts().to_dict()
    print(f"  brand split (per sku_master): {present}")

    week_no = sun_sat_week_number(effective_end)
    print(f"  Sun-Sat week: W{week_no}")

    for brand, folder_slug in brands.items():
        write_brand_xlsx(annotated, brand, week_no, folder_slug, effective_start, effective_end)

    unm = annotated[annotated["Brand"].isna() | (annotated["Brand"].astype(str).str.lower() == "nan")].copy()
    if not unm.empty:
        audit_dir = REPO_ROOT / "data" / "raw" / "sales" / f"Week {week_no}" / "_audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        unm_path = audit_dir / f"vendor_sales_unmatched_{acct.lower()}.csv"
        cols = ["ASIN", "Qty", "Sale", "ShippedUnits", "ShippedRevenue", "CustomerReturns"]
        unm = unm.sort_values("Sale", ascending=False)
        unm[cols].to_csv(unm_path, index=False)
        print(f"  {len(unm)} ASINs missing from sku_master "
              f"(ordered units={int(unm['Qty'].sum())}, ordered sale=₹{float(unm['Sale'].sum()):,.0f}) "
              f"-> {unm_path.relative_to(REPO_ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=list(ACCOUNTS.keys()) + ["ALL"], default="ALL")
    ap.add_argument("--week-end", default=None,
                    help="Saturday ISO date (YYYY-MM-DD).  Default = last full Sun-Sat week.")
    args = ap.parse_args()
    load_dotenv(REPO_ROOT / ".env")

    if args.week_end:
        end_iso   = args.week_end
        start_iso = (datetime.fromisoformat(end_iso).date() - timedelta(days=6)).isoformat()
    else:
        start_iso, end_iso = last_full_sun_sat_week()

    print(f"Window: {start_iso} (Sun) -> {end_iso} (Sat)  →  W{sun_sat_week_number(end_iso)}")
    targets = list(ACCOUNTS.keys()) if args.account == "ALL" else [args.account]
    for acct in targets:
        run_account(acct, start_iso, end_iso)


if __name__ == "__main__":
    main()
