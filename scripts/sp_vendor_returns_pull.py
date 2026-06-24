"""SP-API Vendor (1P) Returns pull — automated replacement for the
operator's manual `1p Returns.xlsx` drop under `data/raw/returns/`.

The 1P returns picture is already in each weekly Vendor Sales (SP-API)
file as the `CustomerReturns` column.  This script aggregates the
same data over a 90-day window — matching the operator's manual
Vendor Central "Sales Diagnostic" export — and writes a single
consolidated `1p Returns.xlsx` matching the existing column schema:

    ASIN, Product Title, Brand, Store Code,
    Ordered Revenue, Ordered Units, Shipped Revenue, Shipped COGS,
    Shipped Units, Customer Returns

Distributor view = MANUFACTURING (revenue at order).  Same Sun-Sat
convention as the weekly pull; "last 90 days" is computed off the
last completed Saturday, so the window is aligned to the operator's
reporting calendar.

WM 1P (Vendor) is skipped — its token is dormant (Clicktech, not
WMI-DRPL).  Once the WMI-DRPL token is sorted, the script picks it
up automatically.

Required env (.env / GitHub secrets) — same as sp_vendor_sales_pull.py:
    SP_LWA_CLIENT_ID_AUDIOARRAY        SP_LWA_CLIENT_SECRET_AUDIOARRAY
    SP_LWA_CLIENT_ID_WHITEMULBERRY     SP_LWA_CLIENT_SECRET_WHITEMULBERRY
    SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY
    SP_API_VENDOR_REFRESH_TOKEN_WHITEMULBERRY
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


REPO_ROOT       = Path(__file__).resolve().parent.parent
SKU_MASTER      = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
RAW_RET_DIR     = REPO_ROOT / "data" / "raw" / "returns"
OUT_FILE        = RAW_RET_DIR / "1p Returns.xlsx"

SPAPI_HOST      = "https://sellingpartnerapi-eu.amazon.com"
MARKETPLACE_ID  = "A21TJRUUN4KGV"

# Account → set of brands this vendor account is allowed to cover.
# AUDIOARRAY's vendor token sits under Cambium Retail PCMU; it returns
# 1P sales for any ASIN listed under that vendor — typically AA + Tonor
# + WM-1P-via-CRPL (currently dormant).  We accept whatever brand the
# ASIN-master tags after the join.
ACCOUNTS = {
    "AUDIOARRAY":    {"Audio Array", "Tonor", "White Mulberry"},
    "WHITEMULBERRY": {"White Mulberry"},
}


def get_access_token(acct: str) -> str:
    cid = os.environ.get(f"SP_LWA_CLIENT_ID_{acct}")     or os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get(f"SP_LWA_CLIENT_SECRET_{acct}") or os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    if not (cid and sec and rt):
        raise SystemExit(f"Missing one of SP_LWA_CLIENT_ID_{acct} / SP_LWA_CLIENT_SECRET_{acct} / "
                         f"SP_API_VENDOR_REFRESH_TOKEN_{acct}")
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": rt,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"LWA exchange failed for {acct}: HTTP {r.status_code} {r.text[:200]}")
    return r.json()["access_token"]


def last_completed_saturday(reference: datetime | None = None) -> datetime:
    ref = reference or datetime.now(timezone.utc)
    days_back_to_sat = (ref.weekday() + 2) % 7
    return (ref - timedelta(days=days_back_to_sat)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )


def pull_vendor_sales_window(token: str, start_dt: datetime, end_dt: datetime) -> list[dict]:
    H = {"x-amz-access-token": token, "Content-Type": "application/json"}
    body = {
        "reportType":     "GET_VENDOR_SALES_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime":  start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataEndTime":    end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportOptions": {
            "reportPeriod":    "DAY",
            "distributorView": "MANUFACTURING",
            "sellingProgram":  "RETAIL",
        },
    }
    print(f"  create vendor-sales report: {body['dataStartTime']} → {body['dataEndTime']}")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"  create failed: HTTP {r.status_code} {r.text[:200]}")
        return []
    rep_id = r.json()["reportId"]

    doc_id = None
    for i in range(90):
        time.sleep(5)
        rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rep_id}",
                          headers={"x-amz-access-token": token}, timeout=30)
        if rr.status_code != 200:
            continue
        j = rr.json()
        st = j.get("processingStatus")
        print(f"    poll[{i}]: {st}")
        if st == "DONE":
            doc_id = j.get("reportDocumentId")
            break
        if st in ("FATAL", "CANCELLED"):
            err_doc_id = j.get("reportDocumentId")
            err_text = ""
            if err_doc_id:
                try:
                    erd = requests.get(
                        f"{SPAPI_HOST}/reports/2021-06-30/documents/{err_doc_id}",
                        headers={"x-amz-access-token": token}, timeout=30,
                    )
                    if erd.status_code == 200:
                        edl = requests.get(erd.json()["url"], timeout=60)
                        err_blob = edl.content
                        if erd.json().get("compressionAlgorithm") == "GZIP":
                            err_blob = gzip.decompress(err_blob)
                        err_text = err_blob.decode("utf-8", errors="replace")[:600]
                except Exception:
                    pass
            print(f"    report {st}; doc-error: {err_text}")
            return []
    if not doc_id:
        print("  timed out waiting for vendor-sales report")
        return []

    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": token}, timeout=30)
    rd.raise_for_status()
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=180)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8", errors="replace"))
    return payload.get("salesByAsin") or []


def aggregate_per_asin(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)

    def _amt(x):
        return x.get("amount") if isinstance(x, dict) else None

    df["asin_n"]            = df["asin"].astype(str).str.strip().str.upper()
    df["orderedUnits"]      = pd.to_numeric(df.get("orderedUnits"),    errors="coerce").fillna(0)
    df["shippedUnits"]      = pd.to_numeric(df.get("shippedUnits"),    errors="coerce").fillna(0)
    df["customerReturns"]   = pd.to_numeric(df.get("customerReturns"), errors="coerce").fillna(0)
    df["ordered_amt"]       = pd.to_numeric(
        df.get("orderedRevenue", pd.Series([None]*len(df))).apply(_amt), errors="coerce"
    ).fillna(0)
    df["shipped_amt"]       = pd.to_numeric(
        df.get("shippedRevenue", pd.Series([None]*len(df))).apply(_amt), errors="coerce"
    ).fillna(0)
    df["shipped_cogs_amt"]  = pd.to_numeric(
        df.get("shippedCogs",    pd.Series([None]*len(df))).apply(_amt), errors="coerce"
    ).fillna(0)

    g = df.groupby("asin_n", as_index=False).agg(
        OrderedRevenue=("ordered_amt",      "sum"),
        OrderedUnits  =("orderedUnits",     "sum"),
        ShippedRevenue=("shipped_amt",      "sum"),
        ShippedCOGS   =("shipped_cogs_amt", "sum"),
        ShippedUnits  =("shippedUnits",     "sum"),
        CustomerReturns=("customerReturns", "sum"),
    )
    g["OrderedUnits"]     = g["OrderedUnits"].astype(int)
    g["ShippedUnits"]     = g["ShippedUnits"].astype(int)
    g["CustomerReturns"]  = g["CustomerReturns"].astype(int)
    return g.rename(columns={"asin_n": "ASIN"})


def load_master() -> pd.DataFrame:
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    return m[["ASIN", "Brand"]].assign(
        ASIN  = lambda d: d["ASIN"].astype(str).str.strip(),
        Brand = lambda d: d["Brand"].astype(str).str.strip(),
    ).drop_duplicates("ASIN", keep="first")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90,
                    help="Days back from last completed Saturday (default: 90).")
    ap.add_argument("--accounts", default="ALL",
                    help="Comma-separated subset (default: all vendor accounts).")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    sat = last_completed_saturday()
    end_dt   = sat
    start_dt = (sat - timedelta(days=args.days - 1)).replace(hour=0, minute=0, second=0)
    print(f"Window: {start_dt.date()} → {end_dt.date()} ({args.days}d)")

    accts = list(ACCOUNTS.keys()) if args.accounts.upper() == "ALL" else [
        a.strip().upper() for a in args.accounts.split(",")
    ]

    # Vendor Sales API tends to FATAL on larger windows under throttle —
    # 14-day chunks process reliably and add minimal cron overhead.
    # 90d / 14d ≈ 7 chunks per account, ~100s each, well within the
    # weekly cron's budget.
    CHUNK_DAYS = 14
    chunks: list[tuple[datetime, datetime]] = []
    cs = start_dt
    while cs <= end_dt:
        ce = min(cs + timedelta(days=CHUNK_DAYS - 1), end_dt)
        ce = ce.replace(hour=23, minute=59, second=59)
        chunks.append((cs, ce))
        cs = ce + timedelta(seconds=1)
        cs = cs.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Chunks ({len(chunks)} × ≤{CHUNK_DAYS}d):")
    for cs_, ce_ in chunks:
        print(f"   {cs_.date()} → {ce_.date()}")

    frames = []
    for acct in accts:
        print(f"\n[{acct}]")
        try:
            token = get_access_token(acct)
        except SystemExit as e:
            print(f"  skip — {e}")
            continue

        rows_all: list[dict] = []
        for cs_, ce_ in chunks:
            try:
                chunk_rows = pull_vendor_sales_window(token, cs_, ce_)
            except RuntimeError as e:
                print(f"  chunk {cs_.date()}..{ce_.date()} FAILED: {e}")
                chunk_rows = []
            rows_all.extend(chunk_rows)
            # Brief space between chunks so Amazon doesn't throttle the
            # back-to-back report submissions.
            time.sleep(2)

        if not rows_all:
            print(f"  [{acct}] no data across {len(chunks)} chunks")
            continue
        df = aggregate_per_asin(rows_all)
        df["seller_account"] = acct
        frames.append(df)
        print(f"  [{acct}] {len(df)} ASINs aggregated from {len(rows_all)} daily rows")

    if not frames:
        print("⚠ No vendor data pulled — keeping existing 1p Returns.xlsx untouched.")
        return

    combined = (pd.concat(frames, ignore_index=True)
                  .groupby("ASIN", as_index=False).agg(
                      OrderedRevenue =("OrderedRevenue",  "sum"),
                      OrderedUnits   =("OrderedUnits",    "sum"),
                      ShippedRevenue =("ShippedRevenue",  "sum"),
                      ShippedCOGS    =("ShippedCOGS",     "sum"),
                      ShippedUnits   =("ShippedUnits",    "sum"),
                      CustomerReturns=("CustomerReturns", "sum"),
                  ))

    master = load_master()
    out = combined.merge(master, on="ASIN", how="left")
    # Filter to ASINs in master only (matches the operator's manual filter).
    out = out[out["Brand"].astype(str).str.strip().ne("") & out["Brand"].notna()].copy()

    # Re-shape to the manual file's column order (Product Title left blank;
    # the SP-API vendor sales response doesn't carry titles).  Store Code
    # is always "IN" for our marketplace.
    out["Product Title"] = ""
    out["Store Code"]    = "IN"
    final = out[[
        "ASIN", "Product Title", "Brand", "Store Code",
        "OrderedRevenue", "OrderedUnits",
        "ShippedRevenue", "ShippedCOGS", "ShippedUnits",
        "CustomerReturns",
    ]].rename(columns={
        "OrderedRevenue":  "Ordered Revenue",
        "OrderedUnits":    "Ordered Units",
        "ShippedRevenue":  "Shipped Revenue",
        "ShippedCOGS":     "Shipped COGS",
        "ShippedUnits":    "Shipped Units",
        "CustomerReturns": "Customer Returns",
    }).sort_values("Ordered Revenue", ascending=False).reset_index(drop=True)

    # Match the manual file's layout: top metadata row, then the table.
    # operator's manual file has the Viewing Range etc. in row 1; we
    # reproduce that so a diff against the prior file is cosmetic-only.
    meta = (f"Viewing Range=[{start_dt.strftime('%d/%m/%y')} - "
            f"{end_dt.strftime('%d/%m/%y')}]"
            f"  Currency=[INR]  View By=[ASIN]  "
            f"Distributor View=[Manufacturing]  Source=[SP-API]")
    RAW_RET_DIR.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as xw:
        # Write metadata to row 0; column headers go to row 1; data row 2+.
        pd.DataFrame({"meta": [meta]}).to_excel(xw, sheet_name="Sheet0",
                                                 index=False, header=False)
        final.to_excel(xw, sheet_name="Sheet0", index=False,
                       startrow=1, header=True)

    print(f"\n-> {OUT_FILE.relative_to(REPO_ROOT)}  "
          f"({len(final)} ASINs, OrderedUnits={int(final['Ordered Units'].sum())}, "
          f"CustomerReturns={int(final['Customer Returns'].sum())})")
    print("\n✅ DONE")


if __name__ == "__main__":
    main()
