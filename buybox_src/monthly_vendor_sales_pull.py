"""
Monthly SP-API Vendor Sales pull (1P) — GET_VENDOR_SALES_REPORT.

Ports FastAPI/scripts/sp_vendor_sales_pull.py from Sun..Sat weekly to a
full-calendar-month window.  reportPeriod=DAY returns per-ASIN per-day
rows; we aggregate to the month.

Accounts:
    AUDIOARRAY  — CRPL vendor account, splits into Audio Array + Tonor
                  (White Mulberry brand rows skipped: WM 1P is dormant
                  per operator instruction — never write WM sales_vendor.csv)

Auth (per-account LWA app because of vendor-context flow):
    SP_LWA_CLIENT_ID_AUDIOARRAY / SP_LWA_CLIENT_SECRET_AUDIOARRAY
    SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY

Output per brand:
    data/<Brand>/<YYYY-MM>/sales_vendor.csv

CLI:
    python monthly_vendor_sales_pull.py                       # last full month
    python monthly_vendor_sales_pull.py --start 2026-07-01 --end 2026-07-31
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import functools
import gzip
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass
print = functools.partial(print, flush=True)

ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

LWA_URL        = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST     = "https://sellingpartnerapi-eu.amazon.com"
MARKETPLACE_ID = "A21TJRUUN4KGV"

SKU_MASTER = ROOT / "data" / "master" / "sku_master.xlsx"

# WM 1P intentionally excluded — operator instruction; dormant vendor token.
ACCOUNTS = {
    "AUDIOARRAY": {
        "Audio Array": "Audio Array",
        "Tonor":       "Tonor",
    },
}

OUTPUT_COLS = [
    "ASIN", "SKU", "Brand", "Model",
    "category_l0", "category_l1", "category_l2",
    "Qty", "Sale",
    "ShippedUnits", "ShippedRevenue",
    "CustomerReturns", "Channel", "Month",
    "WindowStart", "WindowEnd",
]


# ─────────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────────
def previous_calendar_month(reference: dt.date | None = None) -> tuple[str, str]:
    ref = reference or dt.date.today()
    first_of_this = ref.replace(day=1)
    end = first_of_this - dt.timedelta(days=1)
    start = end.replace(day=1)
    return start.isoformat(), end.isoformat()


def month_end_of(date_iso: str) -> str:
    d = dt.date.fromisoformat(date_iso)
    last = calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=last).isoformat()


# ─────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────
def get_access_token(acct: str) -> str:
    cid = (os.environ.get(f"SP_LWA_CLIENT_ID_{acct}")
           or os.environ.get("SP_LWA_CLIENT_ID"))
    sec = (os.environ.get(f"SP_LWA_CLIENT_SECRET_{acct}")
           or os.environ.get("SP_LWA_CLIENT_SECRET"))
    rt  = os.environ.get(f"SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    if not rt:
        raise SystemExit(f"Missing SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    if not cid or not sec:
        raise SystemExit(f"Missing LWA creds for {acct} (vendor)")
    r = requests.post(LWA_URL, data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": cid, "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"LWA failed for {acct}: HTTP {r.status_code} {r.text[:300]}")
    return r.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────
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


def pull_vendor_sales(token: str, start_iso: str, end_iso: str) -> list[dict]:
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    start_dt = dt.datetime.fromisoformat(start_iso).replace(tzinfo=dt.timezone.utc)
    end_dt   = dt.datetime.fromisoformat(end_iso).replace(
        tzinfo=dt.timezone.utc, hour=23, minute=59, second=59)
    body = {
        "reportType":     "GET_VENDOR_SALES_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime":  start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataEndTime":    end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportOptions": {
            "reportPeriod":     "DAY",
            "distributorView":  "MANUFACTURING",
            "sellingProgram":   "RETAIL",
        },
    }
    print(f"  create report: {body['dataStartTime']} -> {body['dataEndTime']}")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                      json=body, headers=headers, timeout=30)
    if r.status_code != 202:
        raise SystemExit(f"create report failed: HTTP {r.status_code} {r.text[:300]}")
    rep_id = r.json()["reportId"]
    print(f"  reportId: {rep_id}")

    doc_id = None
    for i in range(180):
        time.sleep(5)
        rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rep_id}",
                          headers={"x-amz-access-token": token}, timeout=30)
        if rr.status_code != 200:
            continue
        j = rr.json()
        status = j.get("processingStatus")
        print(f"  poll[{i}]: {status}")
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
    return payload.get("salesByAsin") or payload.get("salesAggregate") or []


# ─────────────────────────────────────────────────────────────────────
# Aggregation
# ─────────────────────────────────────────────────────────────────────
def aggregate_window(rows: list[dict], start_iso: str, end_iso: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "startDate" in df.columns:
        df["startDate"] = df["startDate"].astype(str)
        df = df[(df["startDate"] >= start_iso) & (df["startDate"] <= end_iso)]
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


def write_brand_csv(df: pd.DataFrame, brand: str, folder: str,
                    month_key: str, start_iso: str, end_iso: str) -> int:
    sub = df[df["Brand"].astype(str).str.strip().str.lower() == brand.lower()].copy()
    if sub.empty:
        print(f"  WARN: no ASINs matched brand={brand}")
    sub["Channel"]     = "1p Sales"
    sub["Month"]       = month_key
    sub["WindowStart"] = start_iso
    sub["WindowEnd"]   = end_iso
    for col in OUTPUT_COLS:
        if col not in sub.columns:
            sub[col] = ""
    sub = sub[OUTPUT_COLS]
    out_dir = ROOT / "data" / folder / month_key
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sales_vendor.csv"
    sub.to_csv(out_path, index=False)
    qty  = int(sub["Qty"].sum())
    sale = float(sub["Sale"].sum())
    print(f"  -> {out_path.relative_to(ROOT)}  "
          f"({len(sub)} rows, Qty={qty}, Sale=Rs {sale:,.0f})")
    return len(sub)


# ─────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────
# Amazon Vendor Sales report caps reportPeriod=DAY at 15 days per request.
# For a calendar month we split into two chunks and concatenate.
VENDOR_DAY_MAX = 15


def _chunk_range(start_iso: str, end_iso: str,
                 max_days: int = VENDOR_DAY_MAX) -> list[tuple[str, str]]:
    s = dt.date.fromisoformat(start_iso)
    e = dt.date.fromisoformat(end_iso)
    chunks: list[tuple[str, str]] = []
    cur = s
    while cur <= e:
        end = min(cur + dt.timedelta(days=max_days - 1), e)
        chunks.append((cur.isoformat(), end.isoformat()))
        cur = end + dt.timedelta(days=1)
    return chunks


def run_account(acct: str, start_iso: str, end_iso: str,
                month_key: str, fallback_days: int = 3) -> None:
    brands = ACCOUNTS[acct]
    print(f"\n=== {acct} ({start_iso} -> {end_iso}) ===")
    tok = get_access_token(acct)

    chunks = _chunk_range(start_iso, end_iso)
    print(f"  splitting into {len(chunks)} chunk(s) "
          f"(reportPeriod=DAY cap {VENDOR_DAY_MAX}d)")

    rows: list[dict] = []
    effective_end = start_iso  # will advance as chunks succeed
    anchored_start = start_iso
    for c_start, c_end in chunks:
        chunk_rows = None
        try_end = c_end
        for attempt in range(fallback_days + 1):
            try_end = (dt.date.fromisoformat(c_end)
                       - dt.timedelta(days=attempt)).isoformat()
            if try_end < c_start:
                break
            try:
                chunk_rows = pull_vendor_sales(tok, c_start, try_end)
                if attempt > 0:
                    days = (dt.date.fromisoformat(try_end)
                            - dt.date.fromisoformat(c_start)).days + 1
                    print(f"  chunk {c_start}..{c_end}: fell back end -> "
                          f"{try_end} ({days}-day partial)")
                effective_end = try_end
                break
            except DataNotAvailable:
                print(f"  chunk {c_start}..{c_end}: not available for {try_end}; "
                      f"trying earlier")
                continue
        if chunk_rows is None:
            print(f"  chunk {c_start}..{c_end}: gave up after "
                  f"{fallback_days+1} attempts")
            continue
        print(f"  chunk {c_start}..{try_end}: {len(chunk_rows)} daily rows")
        rows.extend(chunk_rows)

    if not rows:
        print(f"  no rows pulled for {acct}")
        return

    print(f"  daily rows pulled: {len(rows)}")
    snap = aggregate_window(rows, anchored_start, effective_end)
    if snap.empty:
        print(f"  no sales rows for {acct} in {anchored_start}..{effective_end}")
        return
    print(f"  ASINs={len(snap)}, units={int(snap['Qty'].sum())}, "
          f"sale=Rs {float(snap['Sale'].sum()):,.0f}")
    annotated = annotate_with_master(snap)
    present = annotated["Brand"].astype(str).str.strip().value_counts().to_dict()
    print(f"  brand split (per sku_master): {present}")

    for brand, folder in brands.items():
        write_brand_csv(annotated, brand, folder, month_key,
                        anchored_start, effective_end)

    unm = annotated[annotated["Brand"].isna()
                    | (annotated["Brand"].astype(str).str.lower() == "nan")].copy()
    if not unm.empty:
        audit_dir = ROOT / "data" / "_audit" / month_key
        audit_dir.mkdir(parents=True, exist_ok=True)
        unm_path = audit_dir / f"vendor_sales_unmatched_{acct.lower()}.csv"
        cols = ["ASIN", "Qty", "Sale", "ShippedUnits",
                "ShippedRevenue", "CustomerReturns"]
        unm.sort_values("Sale", ascending=False)[cols].to_csv(unm_path, index=False)
        print(f"  {len(unm)} ASINs missing from sku_master "
              f"(ordered units={int(unm['Qty'].sum())}, "
              f"ordered sale=Rs {float(unm['Sale'].sum()):,.0f}) "
              f"-> {unm_path.relative_to(ROOT)}")


# ─────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────
def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def main() -> int:
    ap = argparse.ArgumentParser(description="Monthly SP-API vendor (1P) sales pull")
    ap.add_argument("--start", type=_parse_date, default=None)
    ap.add_argument("--end",   type=_parse_date, default=None)
    ap.add_argument("--account", choices=list(ACCOUNTS.keys()) + ["ALL"], default="ALL")
    args = ap.parse_args()

    if args.start is None or args.end is None:
        s, e = previous_calendar_month()
        args.start = _parse_date(s)
        args.end   = _parse_date(e)

    start_iso = args.start.isoformat()
    end_iso   = args.end.isoformat()
    # data/<Brand>/<month>/ folder name -- "Jul26", not "2026-07", so that
    # Jul 2025 and Jul 2026 never share a folder.  Parsed back out by
    # Scripts/generate_raw_data.js; change both together or data goes missing.
    month_key = args.start.strftime("%b%y")

    print(f"Window: {start_iso} -> {end_iso}  ({month_key})")
    targets = list(ACCOUNTS.keys()) if args.account == "ALL" else [args.account]
    for acct in targets:
        run_account(acct, start_iso, end_iso, month_key)
    print("\nDONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
