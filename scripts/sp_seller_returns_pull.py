"""SP-API Seller FBA Customer Returns pull — automated replacement for
the operator's manual `<Account>.xlsx` returns drops under
`data/raw/returns/`.

Flow mirrors sp_seller_sales_pull.py / sp_seller_inventory_pull.py:
    1. For each of the 5 seller accounts, exchange LWA refresh_token
       → access_token.
    2. Submit GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA report for the
       last 90 days (3 months — operator's standard tracking window).
    3. Download + parse TSV; keep the 12-column schema the operator
       has been using (return-date, order-id, sku, asin, fnsku,
       product-name, quantity, fulfillment-center-id,
       detailed-disposition, reason, license-plate-number,
       customer-comments).
    4. Master-filter rows to sku_master ASINs; exclude Fossil brand
       per operator's convention (Fossil returns tracked separately).
    5. Per-account write to `data/raw/returns/<file>.xlsx`,
       overwriting the existing manual file:
          AUDIOARRAY     -> Audio Array.xlsx
          CAMBIUMRETAIL  -> CRPL.xlsx          (Fossil rows dropped)
          NEXLEV         -> Nexlev.xlsx
          VIOMI          -> viomi.xlsx
          WHITEMULBERRY  -> WhiteMulberry.xlsx (skipped if empty)
    6. Unmatched-ASIN audit -> _audit/seller_returns_unmatched.csv

WM 1P (Vendor) returns are NOT pulled here — that side stays manual
until the WMI-DRPL token is sorted (matching the seller sales/inv
script's behaviour).

Required env (.env / GitHub secrets):
    SP_LWA_CLIENT_ID
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
import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
REPO_ROOT     = Path(__file__).resolve().parent.parent
SKU_MASTER    = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
RAW_RET_DIR   = REPO_ROOT / "data" / "raw" / "returns"

SPAPI_HOST    = "https://sellingpartnerapi-eu.amazon.com"
IN_MKT        = "A21TJRUUN4KGV"
REPORT_TYPE   = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"

ACCOUNTS = [
    "AUDIOARRAY",
    "CAMBIUMRETAIL",
    "NEXLEV",
    "VIOMI",
    "WHITEMULBERRY",
]

# Per-account output filename — matches existing manual files so the
# returns_snapshot ETL doesn't need any path changes.
ACCOUNT_FILE = {
    "AUDIOARRAY":    "Audio Array.xlsx",
    "CAMBIUMRETAIL": "CRPL.xlsx",
    "NEXLEV":        "Nexlev.xlsx",
    "VIOMI":         "viomi.xlsx",
    "WHITEMULBERRY": "WhiteMulberry.xlsx",
}

# Excluded brands — operator keeps Fossil returns tracking separately,
# so any Fossil rows that land in CAMBIUMRETAIL's pull get dropped
# before the file is written.
EXCLUDED_BRANDS = {"Fossil"}

# 12 canonical columns the operator's manual files carry — preserve
# order so a diff against an existing file shows real changes only.
OUTPUT_COLS = [
    "return-date",
    "order-id",
    "sku",
    "asin",
    "fnsku",
    "product-name",
    "quantity",
    "fulfillment-center-id",
    "detailed-disposition",
    "reason",
    "license-plate-number",
    "customer-comments",
]


# ─────────────────────────────────────────────────────────────────────
# Auth
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
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": rt,
        "client_id":     cid,
        "client_secret": sec,
    }, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"LWA exchange failed for {account}: HTTP {r.status_code}")
    return r.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────
# Report pull
# ─────────────────────────────────────────────────────────────────────
def pull_returns(account: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Submit GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA over the given
    UTC window + poll + download.  Returns the raw TSV as a DataFrame."""
    tok = get_access_token(account)
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    body = {
        "reportType":     REPORT_TYPE,
        "marketplaceIds": [IN_MKT],
        "dataStartTime":  start_iso,
        "dataEndTime":    end_iso,
    }
    print(f"  [{account}] submitting {REPORT_TYPE} ({start_iso[:10]} → {end_iso[:10]})…")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                      json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"  [{account}] create failed: HTTP {r.status_code} {r.text[:200]}")
        return pd.DataFrame()
    rid = r.json()["reportId"]

    doc_id = None
    for _ in range(80):
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
            return pd.DataFrame()
    if not doc_id:
        print(f"  [{account}] timed out waiting for report")
        return pd.DataFrame()

    rd = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
                      headers={"x-amz-access-token": tok}, timeout=30)
    rd.raise_for_status()
    doc = rd.json()
    dl = requests.get(doc["url"], timeout=120)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", errors="replace")
    if not text.strip():
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(text), sep="\t")
    df["seller_account"] = account
    return df


# ─────────────────────────────────────────────────────────────────────
# Master-join (ASIN-first, per the alignment rule)
# ─────────────────────────────────────────────────────────────────────
def load_master_asin_lookup() -> pd.DataFrame:
    """ASIN → Brand lookup for filtering.  Fossil rows get dropped
    downstream via EXCLUDED_BRANDS."""
    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master.xlsx missing at {SKU_MASTER}")
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()
    out = m[["ASIN", "Brand"]].copy()
    out["ASIN"]  = out["ASIN"].astype(str).str.strip()
    out["Brand"] = out["Brand"].astype(str).str.strip()
    out = out[out["ASIN"].ne("") & out["ASIN"].str.lower().ne("nan")]
    return out.drop_duplicates(subset=["ASIN"], keep="first").reset_index(drop=True)


def annotate_and_filter(
    df: pd.DataFrame,
    master_asin: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach Brand by ASIN, then drop Fossil + unmatched rows.
    Returns (matched_filtered, unmatched_audit)."""
    if df.empty:
        return df, pd.DataFrame()

    df = df.copy()
    df["asin"] = df["asin"].astype(str).str.strip()
    annotated = df.merge(
        master_asin.rename(columns={"ASIN": "asin", "Brand": "_master_brand"}),
        on="asin", how="left",
    )

    unmatched = annotated[annotated["_master_brand"].isna()].copy()
    matched   = annotated[annotated["_master_brand"].notna()].copy()

    # Drop excluded brands (Fossil)
    matched = matched[~matched["_master_brand"].isin(EXCLUDED_BRANDS)].copy()
    return matched, unmatched


# ─────────────────────────────────────────────────────────────────────
# Per-account write
# ─────────────────────────────────────────────────────────────────────
def write_per_account(
    account_frames: dict[str, pd.DataFrame],
    unmatched_combined: pd.DataFrame,
) -> None:
    RAW_RET_DIR.mkdir(parents=True, exist_ok=True)

    for acct, df in account_frames.items():
        filename = ACCOUNT_FILE.get(acct)
        if not filename:
            continue
        out_path = RAW_RET_DIR / filename
        if df.empty:
            # Don't blow away a manually-curated file with an empty pull.
            # Skip writing if the SP-API came back with 0 rows for this acct.
            print(f"  -> {filename}  (SKIPPED — empty SP-API result, keeping prior file)")
            continue

        # Keep exactly the 12 canonical columns + in the canonical order.
        out_cols = [c for c in OUTPUT_COLS if c in df.columns]
        df[out_cols].to_excel(out_path, index=False)
        date_min = df["return-date"].astype(str).str[:10].min()
        date_max = df["return-date"].astype(str).str[:10].max()
        print(
            f"  -> {out_path.relative_to(REPO_ROOT)}  "
            f"({len(df)} rows, {date_min} → {date_max}, "
            f"qty={int(pd.to_numeric(df['quantity'], errors='coerce').fillna(0).sum())})"
        )

    if not unmatched_combined.empty:
        audit_dir = RAW_RET_DIR / "_audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        unm_out = audit_dir / "seller_returns_unmatched.csv"
        cols = ["seller_account", "return-date", "asin", "sku", "quantity", "product-name"]
        cols = [c for c in cols if c in unmatched_combined.columns]
        unmatched_combined[cols].to_csv(unm_out, index=False)
        print(
            f"  -> {unm_out.relative_to(REPO_ROOT)}  "
            f"({len(unmatched_combined)} rows for ASINs missing from sku_master)"
        )


# ─────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", default="ALL",
                    help="Comma-separated subset (default: all 5).")
    ap.add_argument("--days", type=int, default=90,
                    help="Number of days back to pull (default: 90 = 3 months).")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    # SP-API needs ISO 8601 UTC timestamps with TZ.  Use end-of-day so
    # the right-edge row (today) is included.
    now_utc   = datetime.now(timezone.utc).replace(microsecond=0)
    end_iso   = now_utc.isoformat().replace("+00:00", "Z")
    start_iso = (now_utc - timedelta(days=args.days)).isoformat().replace("+00:00", "Z")

    accounts = ACCOUNTS if args.accounts.upper() == "ALL" else [
        a.strip().upper() for a in args.accounts.split(",") if a.strip()
    ]

    print(f"Returns window: {start_iso[:10]} → {end_iso[:10]} ({args.days}d)")
    print(f"Accounts: {accounts}")

    frames = []
    for acct in accounts:
        try:
            df = pull_returns(acct, start_iso, end_iso)
        except RuntimeError as e:
            print(f"  [{acct}] FAILED: {e}")
            df = pd.DataFrame()
        if not df.empty:
            print(f"  [{acct}] {len(df)} return rows")
        frames.append(df)

    if not any(len(f) for f in frames):
        print("⚠ No returns pulled from any account — exiting.")
        return

    combined = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    master_asin = load_master_asin_lookup()
    matched, unmatched = annotate_and_filter(combined, master_asin)

    # Split per account
    per_account: dict[str, pd.DataFrame] = {}
    for acct in accounts:
        df_a = matched[matched["seller_account"] == acct].copy()
        per_account[acct] = df_a

    print(f"\nWriting per-account returns files (last {args.days} days):")
    write_per_account(per_account, unmatched)
    print("\n✅ DONE")


if __name__ == "__main__":
    main()
