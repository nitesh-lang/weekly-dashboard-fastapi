"""
Monthly SP-API Seller Sales pull (3P) — GET_SALES_AND_TRAFFIC_REPORT.

Ports FastAPI/scripts/sp_seller_sales_pull.py from Sun..Sat weekly to a
full-calendar-month window.  One `dateGranularity=MONTH` call per seller
account returns per-child-ASIN unit / revenue / traffic / buy-box for the
month.

Accounts (5):  AUDIOARRAY  CAMBIUMRETAIL  NEXLEV  VIOMI  WHITEMULBERRY
Auth        :  shared SP_LWA_CLIENT_ID/SECRET + per-account SP_REFRESH_TOKEN_*

Output per brand (mirrors ads_*.csv layout the buybox pipeline already uses):
    data/<Brand>/<YYYY-MM>/sales_seller.csv

Column shape matches the operator's amazon_sales.xlsx (net-of-18%-GST).

CLI (mirrors monthly_buybox_pull.py):
    python monthly_seller_sales_pull.py                       # last full month
    python monthly_seller_sales_pull.py --start 2026-07-01 --end 2026-07-31
    python monthly_seller_sales_pull.py --accounts NEXLEV,VIOMI
"""
from __future__ import annotations

import argparse
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

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT     = "A21TJRUUN4KGV"

GST_RATE = 0.18

SELLER_ACCOUNTS = [
    "AUDIOARRAY", "CAMBIUMRETAIL", "NEXLEV", "VIOMI", "WHITEMULBERRY",
]

BRAND_FOLDER = {
    "Audio Array":    "Audio Array",
    "Nexlev":         "Nexlev",
    "Tonor":          "Tonor",
    "White Mulberry": "White Mulberry",
    # Fossil intentionally omitted — not a buybox-report brand.
    # SP-API still returns Fossil rows, they land in the unmatched
    # audit file if the ASIN is present in sku_master.
}

SKU_MASTER = ROOT / "data" / "master" / "sku_master.xlsx"

OUTPUT_COLS = [
    "SKU", "(Child) ASIN", "(Parent) ASIN", "Brand", "Model",
    "Units Ordered", "Ordered Product Sales",
    "Sessions", "Page Views", "Buy Box Percentage",
    "Seller Account", "Month", "WindowStart", "WindowEnd",
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


# ─────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────
def get_access_token(account: str) -> str | None:
    cid = os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get("SP_LWA_CLIENT_SECRET")
    if not (cid and sec):
        print(
            "ERROR: SP_LWA_CLIENT_ID and/or SP_LWA_CLIENT_SECRET unset. "
            "Set them in .env (or as GitHub Actions repo secrets).",
            file=sys.stderr,
        )
        sys.exit(2)
    rt = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    if not rt:
        print(f"  WARN: SP_REFRESH_TOKEN_{account} unset — skipping {account}.",
              file=sys.stderr)
        return None
    r = requests.post(LWA_URL, data={
        "grant_type": "refresh_token", "refresh_token": rt,
        "client_id": cid, "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        print(f"  WARN: LWA exchange failed for {account}: "
              f"HTTP {r.status_code} {r.text[:200]}",
              file=sys.stderr)
        return None
    return r.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────
def pull_sales_and_traffic(account: str, start_iso: str, end_iso: str) -> list[dict]:
    tok = get_access_token(account)
    if tok is None:
        return []
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    body = {
        "reportType":     "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [IN_MKT],
        "dataStartTime":  f"{start_iso}T00:00:00+00:00",
        "dataEndTime":    f"{end_iso}T23:59:59+00:00",
        "reportOptions": {
            "asinGranularity": "CHILD",
            "dateGranularity": "MONTH",
        },
    }
    print(f"  [{account}] create report: {start_iso} → {end_iso}")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                      json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"  [{account}] create failed: HTTP {r.status_code} {r.text[:200]}")
        return []
    rid = r.json()["reportId"]

    doc_id = None
    for i in range(180):
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
            print(f"  [{account}] report {st} (id={rid})")
            return []
    if not doc_id:
        print(f"  [{account}] timed out waiting for report")
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
    payload = json.loads(raw.decode("utf-8"))
    return payload.get("salesAndTrafficByAsin", []) or []


# ─────────────────────────────────────────────────────────────────────
# Shape
# ─────────────────────────────────────────────────────────────────────
def flatten_rows(account: str, rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        sa = r.get("salesByAsin", {}) or {}
        tr = r.get("trafficByAsin", {}) or {}
        out.append({
            "seller_account":  account,
            "parent_asin":     str(r.get("parentAsin", "")).strip(),
            "asin":            str(r.get("childAsin",  "")).strip(),
            "units":           sa.get("unitsOrdered", 0),
            "units_b2b":       sa.get("unitsOrderedB2B", 0),
            "gross_sales":     (sa.get("orderedProductSales") or {}).get("amount", 0),
            "gross_sales_b2b": (sa.get("orderedProductSalesB2B") or {}).get("amount", 0),
            "sessions":        tr.get("sessions", 0),
            "page_views":      tr.get("pageViews", 0),
            "buy_box_pct":     tr.get("buyBoxPercentage", 0),
        })
    return out


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


def merge_and_split(rows: list[dict], master: pd.DataFrame,
                    month_key: str, start_iso: str, end_iso: str
                   ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    if not rows:
        return {}, pd.DataFrame()

    df = pd.DataFrame(rows)
    for c in ("units", "units_b2b", "gross_sales", "gross_sales_b2b",
              "sessions", "page_views"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["buy_box_pct"] = pd.to_numeric(df["buy_box_pct"], errors="coerce").fillna(0)

    agg = (df.groupby(["seller_account", "asin", "parent_asin"], as_index=False)
             .agg(
                 units=("units", "sum"),
                 units_b2b=("units_b2b", "sum"),
                 gross_sales=("gross_sales", "sum"),
                 gross_sales_b2b=("gross_sales_b2b", "sum"),
                 sessions=("sessions", "sum"),
                 page_views=("page_views", "sum"),
                 buy_box_pct=("buy_box_pct", "mean"),
             ))

    agg["net_sales"]     = (agg["gross_sales"]     / (1 + GST_RATE)).round(2)
    agg["net_sales_b2b"] = (agg["gross_sales_b2b"] / (1 + GST_RATE)).round(2)

    annotated = agg.merge(master, on="asin", how="left")
    # A left merge leaves NaN in the master columns for ASINs that aren't in
    # sku_master, and .astype(str) turns NaN into the *string* "nan" — which
    # is not "" and so used to pass the matched test, after which groupby()
    # dropped the NaN group silently and the unmatched audit file was never
    # written.  Normalise to "" first so the split is honest.
    for c in ("brand", "sku", "model"):
        if c in annotated.columns:
            annotated[c] = (annotated[c].astype(str).str.strip()
                            .replace({"nan": "", "NaN": "", "None": "", "<NA>": ""}))
    matched   = annotated[annotated["brand"].ne("")].copy()
    unmatched = annotated[annotated["brand"].eq("")].copy()

    per_brand: dict[str, pd.DataFrame] = {}
    skipped: list[str] = []
    for brand, brand_df in matched.groupby("brand"):
        folder = BRAND_FOLDER.get(brand)
        if not folder:
            # Real brand in sku_master, but not a buybox-report brand
            # (Fossil, Saregama, the watch labels...).  Intentional, but
            # say so rather than dropping the rows in silence.
            skipped.append(f"{brand} ({len(brand_df)} ASINs, "
                           f"Rs {float(brand_df['net_sales'].sum()):,.0f})")
            continue
        out = pd.DataFrame({
            "SKU":                   brand_df["sku"],
            "(Child) ASIN":          brand_df["asin"],
            "(Parent) ASIN":         brand_df["parent_asin"],
            "Brand":                 brand_df["brand"],
            "Model":                 brand_df["model"],
            "Units Ordered":         brand_df["units"].astype(int),
            "Ordered Product Sales": brand_df["net_sales"],
            "Sessions":              brand_df["sessions"].astype(int),
            "Page Views":            brand_df["page_views"].astype(int),
            "Buy Box Percentage":    brand_df["buy_box_pct"].round(4),
            "Seller Account":        brand_df["seller_account"],
            "Month":                 month_key,
            "WindowStart":           start_iso,
            "WindowEnd":             end_iso,
        })
        per_brand[folder] = out[OUTPUT_COLS]

    if skipped:
        print(f"  non-buybox brands skipped: {'; '.join(sorted(skipped))}")

    return per_brand, unmatched


def write_outputs(per_brand: dict[str, pd.DataFrame], unmatched: pd.DataFrame,
                  month_key: str, start_iso: str, end_iso: str) -> None:
    for folder, df in sorted(per_brand.items()):
        out_dir = ROOT / "data" / folder / month_key
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "sales_seller.csv"
        df.to_csv(out_path, index=False)
        units = int(df["Units Ordered"].sum())
        sales = float(df["Ordered Product Sales"].sum())
        print(f"  -> {out_path.relative_to(ROOT)}  "
              f"({len(df)} rows, units={units}, net sales=Rs {sales:,.0f})")

    if not unmatched.empty:
        audit_dir = ROOT / "data" / "_audit" / month_key
        audit_dir.mkdir(parents=True, exist_ok=True)
        audit_path = audit_dir / "seller_sales_unmatched.csv"
        cols = ["seller_account", "asin", "parent_asin", "units", "units_b2b",
                "gross_sales", "gross_sales_b2b", "net_sales", "net_sales_b2b",
                "sessions", "page_views"]
        (unmatched[cols].sort_values("net_sales", ascending=False)
                        .to_csv(audit_path, index=False))
        print(f"  -> {audit_path.relative_to(ROOT)}  "
              f"({len(unmatched)} ASINs missing from sku_master, "
              f"net sales=Rs {float(unmatched['net_sales'].sum()):,.0f})")


# ─────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────
def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def main() -> int:
    ap = argparse.ArgumentParser(description="Monthly SP-API seller (3P) sales pull")
    ap.add_argument("--start", type=_parse_date, default=None)
    ap.add_argument("--end",   type=_parse_date, default=None)
    ap.add_argument("--accounts", default="ALL",
                    help="Comma-separated subset of seller accounts (default: ALL 5)")
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

    accounts = (SELLER_ACCOUNTS if args.accounts.upper() == "ALL"
                else [a.strip().upper() for a in args.accounts.split(",") if a.strip()])
    print(f"Window: {start_iso} → {end_iso}  ({month_key})")
    print(f"Accounts: {accounts}")

    all_rows: list[dict] = []
    for acct in accounts:
        try:
            rows = pull_sales_and_traffic(acct, start_iso, end_iso)
            print(f"  [{acct}] pulled {len(rows)} ASIN rows")
            all_rows.extend(flatten_rows(acct, rows))
        except Exception as e:
            print(f"  [{acct}] FAILED: {type(e).__name__}: {e}")

    if not all_rows:
        print("No data pulled from any account — exiting.")
        return 0

    master = load_sku_master()
    per_brand, unmatched = merge_and_split(all_rows, master, month_key, start_iso, end_iso)
    if not per_brand:
        print("No master-matched rows — nothing to write.")
        return 0

    print(f"\nWriting per-brand outputs for {month_key}:")
    write_outputs(per_brand, unmatched, month_key, start_iso, end_iso)
    print("\nDONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
