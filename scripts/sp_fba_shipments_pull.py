"""SP-API FBA Customer Shipment Sales pull — FBA-only revenue per ASIN
per Sun-Sat week, across all 5 seller accounts.

Companion to sp_seller_sales_pull.py which pulls the AGGREGATE
GET_SALES_AND_TRAFFIC_REPORT (mixed FBA + FBM, includes traffic).
This script uses GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA
which is FBA-only and exposes per-shipment line items with
fulfillment-center-id, shipment-date, item-price, item-tax — useful
for FBA-side reconciliation that the aggregate report can't do.

Flow:
    1. Each of the 5 seller accounts → LWA → access token.
    2. Submit GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA for
       the Sun-Sat window.  Poll until DONE.
    3. Download the TSV; parse line items.
    4. Apply 18% GST removal (same operator rule as seller sales).
    5. Aggregate per (seller_account, asin) → units + net sales.
    6. ASIN-first master join + brand split.
    7. Write per-brand `FBA Shipments (SP-API).xlsx` (aggregated) and
       per-account raw line dumps under `_audit/fba_shipments_<acct>.csv`.

Sun-Sat week convention: same as the other 3P pulls — last completed
week as of `now()`.

PII note: GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA contains
buyer PII fields (name, email, phone, address).  We DO NOT request a
Restricted Data Token — those fields come back masked or empty.  The
non-PII fields we use (asin, sku, shipment-date, quantity-shipped,
item-price, item-tax, fulfillment-center-id, sales-channel) are
unrestricted.

CLI:
  python scripts/sp_fba_shipments_pull.py
  python scripts/sp_fba_shipments_pull.py --accounts AUDIOARRAY,NEXLEV
  python scripts/sp_fba_shipments_pull.py --week-end 2026-06-27
"""
from __future__ import annotations

import argparse
import csv
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


# ─────────────────────────────────────────────────────────────────────
# Config — mirrors sp_seller_sales_pull
# ─────────────────────────────────────────────────────────────────────
REPO_ROOT     = Path(__file__).resolve().parent.parent
SKU_MASTER    = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
RAW_SALES_DIR = REPO_ROOT / "data" / "raw" / "sales"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT     = "A21TJRUUN4KGV"
GST_RATE   = 0.18
REPORT_TYPE = "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA"

SELLER_ACCOUNTS = [
    "AUDIOARRAY",
    "CAMBIUMRETAIL",
    "NEXLEV",
    "VIOMI",
    "WHITEMULBERRY",
]

# Brand folder names on disk (mirror sales_auto_etl convention).
BRAND_FOLDER = {
    "Audio Array":    "Audio_Array",
    "Fossil":         "Fossil",
    "Nexlev":         "Nexlev",
    "Tonor":          "Tonor",
    "White Mulberry": "White_Mulberry",
}

OUTPUT_COLS = [
    # Mirror the operator's amazon_sales.xlsx schema where it overlaps,
    # so a downstream ETL can read this file without a new code path.
    "SKU", "(Child) ASIN", "(Parent) ASIN", "Brand", "Model",
    "Units Ordered", "Ordered Product Sales",
    "Seller Account", "Week", "WindowStart", "WindowEnd",
]


# ─────────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────────
def last_full_sun_sat_week(reference: datetime | None = None) -> tuple[str, str]:
    ref = reference or datetime.now(timezone.utc)
    days_back_to_sat = (ref.weekday() + 2) % 7
    saturday = (ref - timedelta(days=days_back_to_sat)).date()
    sunday   = saturday - timedelta(days=6)
    return sunday.isoformat(), saturday.isoformat()


def sun_sat_week_number(date_str: str) -> int:
    d = datetime.fromisoformat(date_str).date()
    return (d + timedelta(days=1)).isocalendar().week


# ─────────────────────────────────────────────────────────────────────
# Auth — same fail-loud pattern as the other 3P pulls
# ─────────────────────────────────────────────────────────────────────
def get_access_token(account: str) -> str | None:
    cid = os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get("SP_LWA_CLIENT_SECRET")
    if not (cid and sec):
        print(
            "ERROR: SP_LWA_CLIENT_ID and/or SP_LWA_CLIENT_SECRET unset. "
            "Set in .env and as GitHub Actions secrets. Aborting (exit 2).",
            file=sys.stderr,
        )
        sys.exit(2)
    rt = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    if not rt:
        print(f"  WARN: SP_REFRESH_TOKEN_{account} unset — skipping {account}.",
              file=sys.stderr)
        return None
    r = requests.post(LWA_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": rt,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        print(f"  WARN: LWA failed for {account}: HTTP {r.status_code} {r.text[:200]} — skipping.",
              file=sys.stderr)
        return None
    return r.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────
# Report pull — submit, poll, download, parse TSV
# ─────────────────────────────────────────────────────────────────────
def _find_existing_done_report(tok: str, start_iso: str, end_iso: str) -> str | None:
    """Look up the most recent DONE report of this type+window without
    submitting a new one.  Helps when Amazon's dedup throttle returns
    FATAL on re-submission of an identical window within a short period."""
    H = {"x-amz-access-token": tok}
    params = {
        "reportTypes":         REPORT_TYPE,
        "marketplaceIds":      IN_MKT,
        "processingStatuses":  "DONE",
        "pageSize":            10,
    }
    try:
        r = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                         params=params, headers=H, timeout=30)
        if r.status_code != 200:
            return None
        for rpt in (r.json().get("reports") or []):
            ds = (rpt.get("dataStartTime") or "")[:10]
            de = (rpt.get("dataEndTime") or "")[:10]
            if ds == start_iso and de == end_iso:
                return rpt.get("reportDocumentId")
    except requests.RequestException:
        pass
    return None


def pull_fba_shipments(account: str, start_iso: str, end_iso: str) -> list[dict]:
    tok = get_access_token(account)
    if tok is None:
        return []
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}

    # First, look for an already-DONE report covering the same window —
    # Amazon's dedup throttle returns FATAL on identical re-submits, so
    # if we just pulled this report a few minutes ago we want to reuse
    # the existing document rather than fight the throttle.
    doc_id = _find_existing_done_report(tok, start_iso, end_iso)
    if doc_id:
        print(f"  [{account}] reusing existing DONE report (doc={doc_id[:12]}…)")
    else:
        body = {
            "reportType":     REPORT_TYPE,
            "marketplaceIds": [IN_MKT],
            "dataStartTime":  f"{start_iso}T00:00:00+00:00",
            "dataEndTime":    f"{end_iso}T23:59:59+00:00",
        }
        print(f"  [{account}] submit FBA shipments report: {start_iso} → {end_iso}")
        r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                          json=body, headers=H, timeout=30)
        if r.status_code != 202:
            print(f"  [{account}] submit failed HTTP {r.status_code}: {r.text[:200]}")
            return []
        rid = r.json()["reportId"]

        for _ in range(60):
            time.sleep(5)
            rr = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
                              headers={"x-amz-access-token": tok}, timeout=30)
            if rr.status_code != 200:
                continue
            st = rr.json().get("processingStatus")
            if st == "DONE":
                doc_id = rr.json().get("reportDocumentId")
                break
            if st in ("FATAL", "CANCELLED"):
                print(f"  [{account}] report {st} (id={rid}) — likely dedup throttle; "
                      f"re-run in ~10 min or use --week-end with a slight offset")
                return []
        else:
            print(f"  [{account}] poll timeout (id={rid})")
            return []

    if not doc_id:
        return []

    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": tok}, timeout=30)
    rd.raise_for_status()
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=120)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    # Report is TSV.  Some Amazon reports use cp1252 for INR symbol;
    # try utf-8 first, fall back gracefully.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows: list[dict] = []
    for r in reader:
        rows.append({k: (v or "").strip() for k, v in r.items()})
    print(f"  [{account}] pulled {len(rows)} shipment line items")
    # Tag each row with the source account for downstream split.
    for r in rows:
        r["seller_account"] = account
    return rows


# ─────────────────────────────────────────────────────────────────────
# Aggregation + master join
# ─────────────────────────────────────────────────────────────────────
def load_sku_master() -> pd.DataFrame:
    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master.xlsx missing at {SKU_MASTER}")
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    out = m[["ASIN", "FBA SKU", "Brand", "Model"]].copy()
    out.columns = ["asin", "sku", "brand", "model"]
    for c in out.columns:
        out[c] = out[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    return out[out["asin"].ne("")].drop_duplicates(subset=["asin"])


def normalize_lines(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from the line items with the columns we need.

    Amazon's FBA Customer Shipment Sales TSV column names use
    hyphenated lowercase: amazon-order-id, sku, asin, shipment-date,
    quantity-shipped, item-price, item-tax, fulfillment-center-id,
    sales-channel, etc."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.columns = [c.strip().lower() for c in df.columns]

    # Defensive renames — Amazon IN's FBA Customer Shipment Sales
    # report uses `quantity` + `item-price-per-unit` (per-unit, not
    # per-line).  Other marketplaces / report versions sometimes ship
    # `quantity-shipped` + `item-price` (per-line).  Map both.
    aliases = {
        "asin":                ["asin"],
        "sku":                 ["sku", "merchant-sku"],
        "quantity":            ["quantity", "quantity-shipped", "quantity_shipped"],
        "item_price_per_unit": ["item-price-per-unit", "item-price", "principal-price"],
        "shipping_price":      ["shipping-price", "shipping_price"],
        "gift_wrap_price":     ["gift-wrap-price", "gift_wrap_price"],
        "shipment_date":       ["shipment-date", "shipment_date"],
        "fulfillment_center":  ["fulfillment-center-id", "fulfillment_center_id"],
        "amazon_order_id":     ["amazon-order-id", "amazon_order_id"],
        "currency":            ["currency"],
    }
    out = pd.DataFrame()
    for canon, opts in aliases.items():
        col = next((c for c in opts if c in df.columns), None)
        out[canon] = df[col] if col else ""

    out["seller_account"] = df.get("seller_account", "")
    out["quantity"]            = pd.to_numeric(out["quantity"],            errors="coerce").fillna(0).astype(int)
    out["item_price_per_unit"] = pd.to_numeric(out["item_price_per_unit"], errors="coerce").fillna(0.0)
    out["shipping_price"]      = pd.to_numeric(out["shipping_price"],      errors="coerce").fillna(0.0)
    out["gift_wrap_price"]     = pd.to_numeric(out["gift_wrap_price"],     errors="coerce").fillna(0.0)
    out["asin"]                = out["asin"].astype(str).str.strip()
    out["sku"]                 = out["sku"].astype(str).str.strip()
    # Per-LINE gross = qty × item-price-per-unit (+ shipping + gift-wrap).
    # We exclude shipping + gift-wrap from product-revenue gross_sales
    # so the aggregate aligns with operator's other revenue cuts; both
    # are still in the audit dump if needed.
    out["gross_sales"] = (out["quantity"] * out["item_price_per_unit"]).round(2)
    return out


def aggregate_per_asin(lines: pd.DataFrame) -> pd.DataFrame:
    """Collapse line items → one row per (seller_account, asin)."""
    if lines.empty:
        return pd.DataFrame(columns=["seller_account", "asin", "units", "gross_sales", "net_sales"])
    agg = (lines.groupby(["seller_account", "asin"], as_index=False)
                 .agg(units=("quantity",    "sum"),
                      gross_sales=("gross_sales", "sum")))
    # Apply 18% GST removal (operator rule).  net = gross / 1.18.
    agg["net_sales"] = (agg["gross_sales"] / (1 + GST_RATE)).round(2)
    return agg


def merge_and_split(
    agg: pd.DataFrame, master: pd.DataFrame,
    week_no: int, start_iso: str, end_iso: str,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    if agg.empty:
        return {}, pd.DataFrame()

    annotated = agg.merge(master, on="asin", how="left")
    matched   = annotated[annotated["brand"].astype(str).ne("")].copy()
    unmatched = annotated[annotated["brand"].astype(str).eq("")].copy()

    per_brand: dict[str, pd.DataFrame] = {}
    for brand, brand_df in matched.groupby("brand"):
        folder = BRAND_FOLDER.get(brand)
        if not folder:
            continue
        out = pd.DataFrame({
            "SKU":                            brand_df["sku"].fillna(""),
            "(Child) ASIN":                   brand_df["asin"],
            "(Parent) ASIN":                  brand_df["asin"],
            "Brand":                          brand_df["brand"],
            "Model":                          brand_df["model"].fillna(""),
            "Units Ordered":                  brand_df["units"].astype(int),
            "Ordered Product Sales":          brand_df["net_sales"].round(2),
            "Seller Account":                 brand_df["seller_account"],
            "Week":                           week_no,
            "WindowStart":                    start_iso,
            "WindowEnd":                      end_iso,
        })
        per_brand[folder] = out[OUTPUT_COLS]

    return per_brand, unmatched


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", default="ALL",
                    help="Comma-separated subset of seller accounts (default: ALL 5).")
    ap.add_argument("--week-end", default=None,
                    help="Saturday ISO date (YYYY-MM-DD).  Default = last completed Sun-Sat week.")
    args = ap.parse_args()

    if args.week_end:
        end_iso   = args.week_end
        start_iso = (datetime.fromisoformat(end_iso).date() - timedelta(days=6)).isoformat()
    else:
        start_iso, end_iso = last_full_sun_sat_week()
    week_no = sun_sat_week_number(end_iso)

    accounts = SELLER_ACCOUNTS if args.accounts.upper() == "ALL" else [
        a.strip().upper() for a in args.accounts.split(",") if a.strip()
    ]
    print(f"Window: {start_iso} (Sun) → {end_iso} (Sat)  →  W{week_no}")
    print(f"Report:  {REPORT_TYPE}")
    print(f"Accounts: {accounts}")

    week_dir = RAW_SALES_DIR / f"Week {week_no}"
    week_dir.mkdir(parents=True, exist_ok=True)
    audit_dir = week_dir / "_audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    # 1) Pull from each account; dump raw line items per-account for audit.
    all_lines: list[pd.DataFrame] = []
    for acct in accounts:
        rows = pull_fba_shipments(acct, start_iso, end_iso)
        if not rows:
            continue
        lines = normalize_lines(rows)
        if lines.empty:
            continue
        # Per-account audit dump — raw line items, with FC + shipment_date.
        audit_out = audit_dir / f"fba_shipments_{acct.lower()}.csv"
        lines.to_csv(audit_out, index=False)
        print(f"  -> {audit_out.relative_to(REPO_ROOT)} "
              f"({len(lines)} line items, "
              f"units={int(lines['quantity'].sum())}, "
              f"gross=Rs {float(lines['gross_sales'].sum()):,.0f})")
        all_lines.append(lines)

    if not all_lines:
        print("No FBA shipment lines pulled — abort.")
        return 1

    lines = pd.concat(all_lines, ignore_index=True)

    # 2) Aggregate per (account, asin) and master-join.
    agg = aggregate_per_asin(lines)
    master = load_sku_master()
    per_brand, unmatched = merge_and_split(agg, master, week_no, start_iso, end_iso)

    # 3) Write per-brand FBA Shipments xlsx (mirrors Seller Sales schema).
    print()
    print(f"Writing per-brand FBA Shipments outputs for W{week_no}:")
    for folder, df in sorted(per_brand.items()):
        brand_dir = week_dir / folder
        brand_dir.mkdir(parents=True, exist_ok=True)
        out_path = brand_dir / "FBA Shipments (SP-API).xlsx"
        df.to_excel(out_path, index=False)
        u = int(df["Units Ordered"].sum())
        s = float(df["Ordered Product Sales"].sum())
        print(f"  -> {out_path.relative_to(REPO_ROOT)}  "
              f"({len(df)} rows, units={u}, net sales=Rs {s:,.0f})")

    # 4) Unmatched audit dump
    if not unmatched.empty:
        unmatched_path = audit_dir / "fba_shipments_unmatched.csv"
        cols = ["seller_account", "asin", "units", "gross_sales", "net_sales"]
        unmatched[cols].sort_values("net_sales", ascending=False).to_csv(unmatched_path, index=False)
        print(f"  -> {unmatched_path.relative_to(REPO_ROOT)}  "
              f"({len(unmatched)} ASINs missing from sku_master, "
              f"net sales=Rs {float(unmatched['net_sales'].sum()):,.0f})")

    print()
    print("OK DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
