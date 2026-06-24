"""SP-API Seller Sales pull — replaces the operator's manual
`amazon_sales.xlsx` upload by fetching the same "Detail Page Sales
and Traffic By Child Item" business report from all 5 seller
accounts (AUDIOARRAY / CAMBIUMRETAIL / NEXLEV / VIOMI / WHITEMULBERRY)
in one go.

Flow:
    1. For each seller account, exchange the LWA refresh_token →
       short-lived access_token.
    2. Submit GET_SALES_AND_TRAFFIC_REPORT with
       asinGranularity=CHILD, dateGranularity=WEEK for the
       Sun..Sat window.  Poll until DONE.
    3. Download the JSON report; collect every salesAndTrafficByAsin
       row across all 5 accounts.
    4. Merge per-ASIN (sum units/sales/sessions across the seller
       accounts that surfaced the same ASIN).
    5. Apply 18% GST removal: net = gross / 1.18  (operator's
       documented rule — invoice value displayed is gross, MIS
       reports/reconciliation use net).
    6. Filter to ASINs present in sku_master.xlsx.  Tag each row
       with its master Brand + Model + SKU.
    7. Per-brand split → write `data/raw/sales/Week NN/<Brand>/
       Seller Sales (SP-API).xlsx` (one xlsx per brand, mirrors the
       columns the operator's existing `amazon_sales.xlsx` carries
       so sales_auto_etl.py can adopt it later).

Sun..Sat week convention: same as the vendor scripts — last
completed week as of `now()`.  Run on Mon morning → returns
previous Sun..Sat.  --week-end YYYY-MM-DD lets you target a
specific Saturday.

Master mismatch handling: ASINs not found in sku_master land in
data/raw/sales/Week NN/_audit/seller_sales_unmatched.csv with
their per-account split — operator backfill task.

Required env (loads from .env):
    SP_LWA_CLIENT_ID                  ← shared AdPilot Production app
    SP_LWA_CLIENT_SECRET
    SP_REFRESH_TOKEN_AUDIOARRAY
    SP_REFRESH_TOKEN_CAMBIUMRETAIL
    SP_REFRESH_TOKEN_NEXLEV
    SP_REFRESH_TOKEN_VIOMI
    SP_REFRESH_TOKEN_WHITEMULBERRY
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
import os


# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
REPO_ROOT     = Path(__file__).resolve().parent.parent
SKU_MASTER    = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
RAW_SALES_DIR = REPO_ROOT / "data" / "raw" / "sales"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"   # IN marketplace = EU region
IN_MKT     = "A21TJRUUN4KGV"

GST_RATE = 0.18   # net = gross / (1 + 0.18) — matches operator manual process

# Seller accounts to pull — each maps to a refresh token in .env.
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
    # Mirror the operator's amazon_sales.xlsx schema so downstream
    # ETL can swap source without touching parse_amazon().
    # OPERATOR RULE: only Ordered Product Sales (3P consumer) lands here.
    # B2B revenue is tracked separately in other_channels.xlsx's "B2B"
    # sheet — do NOT mix it into the Amazon channel.
    "SKU", "(Child) ASIN", "(Parent) ASIN", "Brand", "Model",
    "Units Ordered", "Ordered Product Sales",
    "Sessions", "Page Views", "Buy Box Percentage",
    "Seller Account",   # which of the 5 accounts contributed each row
    "Week", "WindowStart", "WindowEnd",
]


# ─────────────────────────────────────────────────────────────────────
# Date helpers — Sun..Sat
# ─────────────────────────────────────────────────────────────────────
def last_full_sun_sat_week(reference: datetime | None = None) -> tuple[str, str]:
    """Return (sunday_iso, saturday_iso) for the last completed Sun-Sat
    week as of `reference` (UTC now by default).  Same logic as the
    vendor scripts — keeps the entire pipeline on one week convention."""
    ref = reference or datetime.now(timezone.utc)
    days_back_to_sat = (ref.weekday() + 2) % 7
    saturday = (ref - timedelta(days=days_back_to_sat)).date()
    sunday   = saturday - timedelta(days=6)
    return sunday.isoformat(), saturday.isoformat()


def sun_sat_week_number(date_str: str) -> int:
    """Sun-Sat week number for a given date (consistent with operator's
    convention — Saturday is the last day of the week)."""
    d = datetime.fromisoformat(date_str).date()
    return (d + timedelta(days=1)).isocalendar().week


# ─────────────────────────────────────────────────────────────────────
# Auth / API
# ─────────────────────────────────────────────────────────────────────
def get_access_token(account: str) -> str:
    cid = os.environ.get("SP_LWA_CLIENT_ID")
    sec = os.environ.get("SP_LWA_CLIENT_SECRET")
    rt  = os.environ.get(f"SP_REFRESH_TOKEN_{account}")
    if not (cid and sec and rt):
        raise SystemExit(
            f"Missing one of SP_LWA_CLIENT_ID / SP_LWA_CLIENT_SECRET / "
            f"SP_REFRESH_TOKEN_{account} in .env"
        )
    r = requests.post(LWA_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": rt,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise SystemExit(
            f"LWA token exchange failed for {account}: HTTP {r.status_code} "
            f"{r.text[:200]}"
        )
    return r.json()["access_token"]


def pull_sales_and_traffic(account: str, start_iso: str, end_iso: str) -> list[dict]:
    """Submit + poll + download GET_SALES_AND_TRAFFIC_REPORT for the
    given seller account & window.  Returns the `salesAndTrafficByAsin`
    list (per-child-ASIN rollup with units / sales / sessions / pv /
    buy-box, plus _B2B variants)."""
    tok = get_access_token(account)
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    body = {
        "reportType":     "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [IN_MKT],
        "dataStartTime":  f"{start_iso}T00:00:00+00:00",
        "dataEndTime":    f"{end_iso}T23:59:59+00:00",
        "reportOptions": {
            "asinGranularity": "CHILD",
            "dateGranularity": "WEEK",
        },
    }
    print(f"  [{account}] create report: {start_iso} → {end_iso}")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"  [{account}] create failed: HTTP {r.status_code} {r.text[:200]}")
        return []
    rid = r.json()["reportId"]

    for i in range(60):
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
    else:
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
# Aggregation
# ─────────────────────────────────────────────────────────────────────
def flatten_rows(account: str, rows: list[dict]) -> list[dict]:
    """Turn the nested salesByAsin/trafficByAsin sub-dicts into a flat
    record per child ASIN, carrying the seller-account tag for
    traceability."""
    out: list[dict] = []
    for r in rows:
        sa = r.get("salesByAsin", {}) or {}
        tr = r.get("trafficByAsin", {}) or {}
        out.append({
            "seller_account":   account,
            "parent_asin":      str(r.get("parentAsin", "")).strip(),
            "asin":             str(r.get("childAsin",  "")).strip(),
            "units":            sa.get("unitsOrdered", 0),
            "units_b2b":        sa.get("unitsOrderedB2B", 0),
            "gross_sales":      (sa.get("orderedProductSales") or {}).get("amount", 0),
            "gross_sales_b2b":  (sa.get("orderedProductSalesB2B") or {}).get("amount", 0),
            "sessions":         tr.get("sessions", 0),
            "page_views":       tr.get("pageViews", 0),
            "buy_box_pct":      tr.get("buyBoxPercentage", 0),
        })
    return out


def load_sku_master() -> pd.DataFrame:
    """{ASIN → SKU/Brand/Model} master lookup."""
    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master.xlsx missing at {SKU_MASTER}")
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    out = m[["ASIN", "FBA SKU", "Brand", "Model"]].copy()
    out.columns = ["asin", "sku", "brand", "model"]
    for c in out.columns:
        out[c] = out[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    return out[out["asin"].ne("")].drop_duplicates(subset=["asin"])


def merge_and_split(
    rows: list[dict],
    master: pd.DataFrame,
    week_no: int,
    start_iso: str,
    end_iso: str,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Returns ({brand_folder → per-brand frame}, unmatched frame).

    Each per-brand frame is ready to be written as an xlsx in the
    shape sales_auto_etl.parse_amazon expects, plus a Seller Account
    column for traceability.
    """
    if not rows:
        return {}, pd.DataFrame()

    df = pd.DataFrame(rows)

    # Numeric cleanup
    for c in ("units", "units_b2b", "gross_sales", "gross_sales_b2b",
              "sessions", "page_views"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["buy_box_pct"] = pd.to_numeric(df["buy_box_pct"], errors="coerce").fillna(0)

    # Aggregate per (asin, seller_account) — same ASIN can appear in
    # multiple seller accounts.  Keep the seller tag so the operator
    # can see which account contributed which slice.
    agg = (
        df.groupby(["seller_account", "asin", "parent_asin"], as_index=False)
          .agg(
              units=("units", "sum"),
              units_b2b=("units_b2b", "sum"),
              gross_sales=("gross_sales", "sum"),
              gross_sales_b2b=("gross_sales_b2b", "sum"),
              sessions=("sessions", "sum"),
              page_views=("page_views", "sum"),
              buy_box_pct=("buy_box_pct", "mean"),
          )
    )

    # Apply 18% GST removal — operator's documented rule.  Per-line
    # division by 1.18 gives the same net total as deducting 18% off
    # the brand-level sum, but preserves per-row arithmetic for audit.
    agg["net_sales"]     = (agg["gross_sales"]     / (1 + GST_RATE)).round(2)
    agg["net_sales_b2b"] = (agg["gross_sales_b2b"] / (1 + GST_RATE)).round(2)

    # Merge with sku_master on ASIN — anything that doesn't match
    # goes to the audit bucket.
    annotated = agg.merge(master, on="asin", how="left")
    matched   = annotated[annotated["brand"].astype(str).ne("")].copy()
    unmatched = annotated[annotated["brand"].astype(str).eq("")].copy()

    # Per-brand split
    per_brand: dict[str, pd.DataFrame] = {}
    for brand, brand_df in matched.groupby("brand"):
        folder = BRAND_FOLDER.get(brand)
        if not folder:
            # Unknown brand value in master — skip safely; will surface in audit.
            continue
        out = pd.DataFrame({
            "SKU":                          brand_df["sku"],
            "(Child) ASIN":                 brand_df["asin"],
            "(Parent) ASIN":                brand_df["parent_asin"],
            "Brand":                        brand_df["brand"],
            "Model":                        brand_df["model"],
            "Units Ordered":                brand_df["units"].astype(int),
            "Ordered Product Sales":        brand_df["net_sales"],
            "Sessions":                     brand_df["sessions"].astype(int),
            "Page Views":                   brand_df["page_views"].astype(int),
            "Buy Box Percentage":           brand_df["buy_box_pct"].round(4),
            "Seller Account":               brand_df["seller_account"],
            "Week":                         week_no,
            "WindowStart":                  start_iso,
            "WindowEnd":                    end_iso,
        })
        per_brand[folder] = out[OUTPUT_COLS]

    return per_brand, unmatched


# ─────────────────────────────────────────────────────────────────────
# Write outputs
# ─────────────────────────────────────────────────────────────────────
def write_outputs(
    per_brand: dict[str, pd.DataFrame],
    unmatched: pd.DataFrame,
    week_no: int,
    start_iso: str,
    end_iso: str,
) -> None:
    week_dir = RAW_SALES_DIR / f"Week {week_no}"
    week_dir.mkdir(parents=True, exist_ok=True)

    for folder, df in sorted(per_brand.items()):
        brand_dir = week_dir / folder
        brand_dir.mkdir(parents=True, exist_ok=True)
        out_path = brand_dir / "Seller Sales (SP-API).xlsx"
        df.to_excel(out_path, index=False)
        units  = int(df["Units Ordered"].sum())
        sales  = float(df["Ordered Product Sales"].sum())
        print(
            f"  -> {out_path.relative_to(REPO_ROOT)}  "
            f"({len(df)} rows, units={units}, net sales=Rs {sales:,.0f})"
        )

    if not unmatched.empty:
        audit_dir = week_dir / "_audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        unmatched_out = audit_dir / "seller_sales_unmatched.csv"
        cols = ["seller_account", "asin", "parent_asin", "units", "units_b2b",
                "gross_sales", "gross_sales_b2b", "net_sales", "net_sales_b2b",
                "sessions", "page_views"]
        unmatched[cols].sort_values("net_sales", ascending=False).to_csv(unmatched_out, index=False)
        print(
            f"  -> {unmatched_out.relative_to(REPO_ROOT)}  "
            f"({len(unmatched)} ASINs missing from sku_master, "
            f"net sales=Rs {float(unmatched['net_sales'].sum()):,.0f})"
        )


# ─────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--accounts", default="ALL",
        help="Comma-separated subset of seller accounts (default: ALL 5).",
    )
    ap.add_argument(
        "--week-end", default=None,
        help="Saturday ISO date (YYYY-MM-DD).  Default = last full Sun-Sat week.",
    )
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")

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
    print(f"Accounts: {accounts}")

    # ── Pull all accounts in serial.  Reports API has tight rate
    # limits per app; serial avoids 429 throttling and is plenty fast
    # for 5 accounts in IN.
    all_rows: list[dict] = []
    for acct in accounts:
        try:
            rows = pull_sales_and_traffic(acct, start_iso, end_iso)
            print(f"  [{acct}] pulled {len(rows)} ASIN rows")
            all_rows.extend(flatten_rows(acct, rows))
        except Exception as e:
            print(f"  [{acct}] FAILED: {type(e).__name__}: {e}")

    if not all_rows:
        print("⚠ No data pulled from any account — exiting.")
        return

    master = load_sku_master()
    per_brand, unmatched = merge_and_split(
        all_rows, master, week_no, start_iso, end_iso
    )

    if not per_brand:
        print("⚠ No master-matched rows — nothing to write.")
        return

    print(f"\nWriting per-brand outputs for W{week_no}:")
    write_outputs(per_brand, unmatched, week_no, start_iso, end_iso)
    print("\n✅ DONE")


if __name__ == "__main__":
    main()
