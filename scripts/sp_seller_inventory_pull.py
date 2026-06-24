"""SP-API Seller FBA Inventory pull — automated replacement for the
operator's manual `Inventory Snapshot.xlsx` upload for the Amazon
seller side.

Flow mirrors sp_seller_sales_pull.py:
    1. For each of the 5 seller accounts, exchange LWA refresh_token
       → access_token.
    2. Submit GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA report.  This
       is a TSV report containing every FBA SKU's current inventory
       (afn-fulfillable-quantity, inbound, reserved, unfulfillable,
       researching).  Equivalent to the Manage FBA Inventory page
       export in Seller Central.  Same data path AA works with via
       the direct API; CR/NEX/VIOMI/WM can only access it through
       the Reports route (their tokens get 403 on /fba/inventory/v1/
       but the report works fine).
    3. Download + parse TSV; collect all rows across the 5 accounts.
    4. Resolve SKU → ASIN via sku_master (raw FBA inv report does
       NOT carry ASIN — operator's manual export had the same gap).
    5. Master-filter and tag each row with Brand + Model.
    6. Per-brand split → write `data/raw/inventory/Week NN/<Brand>/
       Seller FBA Inventory (SP-API).xlsx`.

The xlsx mirrors the legacy Inventory Snapshot.xlsx layout enough
that the existing inventory_model_snapshot.py ETL can adopt this
file when the operator's ready to switch.

Sun..Sat week convention same as the rest of the pipeline.

Required env (.env):
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
RAW_INV_DIR   = REPO_ROOT / "data" / "raw" / "inventory"

LWA_URL    = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"   # IN marketplace = EU region
IN_MKT     = "A21TJRUUN4KGV"

REPORT_TYPE = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"

SELLER_ACCOUNTS = [
    "AUDIOARRAY",
    "CAMBIUMRETAIL",
    "NEXLEV",
    "VIOMI",
    "WHITEMULBERRY",
]

BRAND_FOLDER = {
    "Audio Array":    "Audio_Array",
    "Fossil":         "Fossil",
    "Nexlev":         "Nexlev",
    "Tonor":          "Tonor",
    "White Mulberry": "White_Mulberry",
}

OUTPUT_COLS = [
    "SKU", "ASIN", "Brand", "Model",
    "Inventory",
    "afn_fulfillable_quantity",
    "afn_inbound_working_quantity",
    "afn_inbound_shipped_quantity",
    "afn_inbound_receiving_quantity",
    "afn_reserved_quantity",
    "afn_total_quantity",
    "afn_unsellable_quantity",
    "afn_researching_quantity",
    "Seller Account",
    "SnapshotDate",
    "Week",
]


# ─────────────────────────────────────────────────────────────────────
# Date helpers — Sun..Sat
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
# Auth + Report submission
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
        raise RuntimeError(f"LWA exchange failed for {account}: HTTP {r.status_code}")
    return r.json()["access_token"]


def pull_fba_inventory(account: str) -> pd.DataFrame:
    """Submit GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA + poll + download.
    Returns the report as a DataFrame (raw TSV → pandas)."""
    tok = get_access_token(account)
    H = {"x-amz-access-token": tok, "Content-Type": "application/json"}
    body = {
        "reportType":     REPORT_TYPE,
        "marketplaceIds": [IN_MKT],
    }
    print(f"  [{account}] submitting {REPORT_TYPE}…")
    r = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=H, timeout=30)
    if r.status_code != 202:
        print(f"  [{account}] create failed: HTTP {r.status_code} {r.text[:200]}")
        return pd.DataFrame()
    rid = r.json()["reportId"]

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
    else:
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
# Master-join + per-brand split
# ─────────────────────────────────────────────────────────────────────
def load_sku_master() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build two lookups from sku_master:

      * by ASIN — canonical SKU/Brand/Model/category per ASIN
      * by SKU — fallback when the FBA report carries an ASIN not in master
        (rare, but possible for ASIN-less rows or new listings)

    Per the ASIN > SKU > Model alignment rule, ASIN match wins:
    even when a report ships SKU=FBS66974 (not in master),
    if asin=B007NM84PQ IS in master we still tag the row Fossil.
    """
    if not SKU_MASTER.exists():
        raise SystemExit(f"sku_master.xlsx missing at {SKU_MASTER}")
    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()

    by_asin_rows = []
    by_sku_rows  = []
    for _, r in m.iterrows():
        asin  = str(r.get("ASIN", "") or "").strip()
        brand = str(r.get("Brand", "") or "").strip()
        model = str(r.get("Model", "") or "").strip()
        if asin and asin.lower() not in ("nan", "none"):
            by_asin_rows.append({
                "asin": asin, "brand": brand, "model": model,
                "canonical_sku": str(r.get("FBA SKU", "") or "").strip(),
            })
        for sku_col in ("FBA SKU", "Original SKU"):
            sku = str(r.get(sku_col, "") or "").strip()
            if sku and sku.lower() not in ("nan", "none"):
                by_sku_rows.append({
                    "sku": sku, "asin": asin, "brand": brand, "model": model,
                })
    by_asin = (pd.DataFrame(by_asin_rows)
                 .drop_duplicates(subset=["asin"], keep="first")
                 .reset_index(drop=True))
    by_sku  = (pd.DataFrame(by_sku_rows)
                 .drop_duplicates(subset=["sku"], keep="first")
                 .reset_index(drop=True))
    return by_asin, by_sku


def merge_and_split(
    all_df: pd.DataFrame,
    master_by_asin: pd.DataFrame,
    master_by_sku: pd.DataFrame,
    week_no: int,
    snapshot_date: str,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    if all_df.empty:
        return {}, pd.DataFrame()

    # The TSV report's SKU column is "sku" (lowercase).  Sometimes called
    # "merchant-sku" or "seller-sku" in older variants — fall back if needed.
    sku_col = next((c for c in ("sku", "seller-sku", "merchant-sku") if c in all_df.columns), None)
    if not sku_col:
        raise SystemExit(f"FBA inventory report missing SKU column. Got: {list(all_df.columns)[:8]}")
    asin_col = next((c for c in ("asin", "ASIN") if c in all_df.columns), None)

    # Numeric columns we want to keep.  Names normalized lowercase-hyphen.
    rename = {
        "afn-fulfillable-quantity":      "afn_fulfillable_quantity",
        "afn-inbound-working-quantity":  "afn_inbound_working_quantity",
        "afn-inbound-shipped-quantity":  "afn_inbound_shipped_quantity",
        "afn-inbound-receiving-quantity":"afn_inbound_receiving_quantity",
        "afn-reserved-quantity":         "afn_reserved_quantity",
        "afn-total-quantity":            "afn_total_quantity",
        "afn-unsellable-quantity":       "afn_unsellable_quantity",
        "afn-researching-quantity":      "afn_researching_quantity",
    }
    all_df = all_df.rename(columns=rename)
    for c in rename.values():
        if c not in all_df.columns:
            all_df[c] = 0
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce").fillna(0).astype(int)

    all_df = all_df.rename(columns={sku_col: "sku"})
    all_df["sku"] = all_df["sku"].astype(str).str.strip()
    if asin_col and asin_col != "asin":
        all_df = all_df.rename(columns={asin_col: "asin"})
    if "asin" not in all_df.columns:
        all_df["asin"] = ""
    all_df["asin"] = all_df["asin"].astype(str).str.strip()

    # Aggregate by (seller_account, sku, asin) — usually 1 row per SKU per
    # account already, but defensive in case any account splits SKU by
    # condition.
    agg = (
        all_df.groupby(["seller_account", "sku", "asin"], as_index=False)
              .agg(**{c: (c, "sum") for c in rename.values()})
    )

    # ASIN-first master resolution — canonical per the alignment rule.
    # If the report's ASIN is in sku_master we take Brand/Model from there
    # even when the SKU on the report (e.g. FBS66974) is a fulfillment-center
    # variant not present in the master.  SKU is only a fallback.
    asin_lookup = master_by_asin.set_index("asin")[["brand", "model"]]
    sku_lookup  = master_by_sku.set_index("sku")[["asin", "brand", "model"]]

    def resolve(row):
        a = row["asin"]
        if a and a in asin_lookup.index:
            r = asin_lookup.loc[a]
            return pd.Series({"brand": r["brand"], "model": r["model"], "matched_asin": a})
        s = row["sku"]
        if s in sku_lookup.index:
            r = sku_lookup.loc[s]
            return pd.Series({"brand": r["brand"], "model": r["model"], "matched_asin": r["asin"]})
        return pd.Series({"brand": "", "model": "", "matched_asin": a})

    resolved = agg.apply(resolve, axis=1)
    annotated = pd.concat([agg, resolved], axis=1)
    annotated["asin"] = annotated["matched_asin"].where(
        annotated["matched_asin"].astype(str).str.strip().ne(""), annotated["asin"]
    )
    annotated = annotated.drop(columns=["matched_asin"])

    matched   = annotated[annotated["brand"].astype(str).str.strip().ne("")].copy()
    unmatched = annotated[annotated["brand"].astype(str).str.strip().eq("") |
                          annotated["brand"].isna()].copy()

    per_brand: dict[str, pd.DataFrame] = {}
    for brand, brand_df in matched.groupby("brand"):
        folder = BRAND_FOLDER.get(brand)
        if not folder:
            continue
        inventory_qty = (
            brand_df["afn_total_quantity"].fillna(0).astype(int)
            - brand_df["afn_unsellable_quantity"].fillna(0).astype(int)
        )
        out = pd.DataFrame({
            "SKU":                            brand_df["sku"],
            "ASIN":                           brand_df["asin"],
            "Brand":                          brand_df["brand"],
            "Model":                          brand_df["model"],
            # Operator inventory definition: total AFN minus unsellable.
            # Matches the manual Manage FBA Inventory export totals.
            "Inventory":                      inventory_qty,
            "afn_fulfillable_quantity":       brand_df["afn_fulfillable_quantity"],
            "afn_inbound_working_quantity":   brand_df["afn_inbound_working_quantity"],
            "afn_inbound_shipped_quantity":   brand_df["afn_inbound_shipped_quantity"],
            "afn_inbound_receiving_quantity": brand_df["afn_inbound_receiving_quantity"],
            "afn_reserved_quantity":          brand_df["afn_reserved_quantity"],
            "afn_total_quantity":             brand_df["afn_total_quantity"],
            "afn_unsellable_quantity":        brand_df["afn_unsellable_quantity"],
            "afn_researching_quantity":       brand_df["afn_researching_quantity"],
            "Seller Account":                 brand_df["seller_account"],
            "SnapshotDate":                   snapshot_date,
            "Week":                           week_no,
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
    snapshot_date: str,
) -> None:
    week_dir = RAW_INV_DIR / f"Week {week_no}"
    week_dir.mkdir(parents=True, exist_ok=True)

    for folder, df in sorted(per_brand.items()):
        brand_dir = week_dir / folder
        brand_dir.mkdir(parents=True, exist_ok=True)
        out_path = brand_dir / "Seller FBA Inventory (SP-API).xlsx"
        df.to_excel(out_path, index=False)
        inv  = int(df["Inventory"].sum())
        ful  = int(df["afn_fulfillable_quantity"].sum())
        tot  = int(df["afn_total_quantity"].sum())
        uns  = int(df["afn_unsellable_quantity"].sum())
        inb  = int(df["afn_inbound_shipped_quantity"].sum() + df["afn_inbound_working_quantity"].sum())
        print(
            f"  -> {out_path.relative_to(REPO_ROOT)}  "
            f"({len(df)} rows, Inventory={inv}, total_afn={tot}, unsellable={uns}, "
            f"fulfillable={ful}, inbound={inb})"
        )

    if not unmatched.empty:
        audit_dir = week_dir / "_audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        unm_out = audit_dir / "seller_fba_inv_unmatched_sku.csv"
        cols = ["seller_account", "sku", "asin", "afn_fulfillable_quantity",
                "afn_total_quantity", "afn_inbound_shipped_quantity",
                "afn_reserved_quantity"]
        cols = [c for c in cols if c in unmatched.columns]
        unmatched[cols].sort_values("afn_total_quantity", ascending=False).to_csv(unm_out, index=False)
        print(
            f"  -> {unm_out.relative_to(REPO_ROOT)}  "
            f"({len(unmatched)} SKUs missing from sku_master, "
            f"total fulfillable={int(unmatched['afn_fulfillable_quantity'].sum())})"
        )


# ─────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", default="ALL",
                    help="Comma-separated subset of seller accounts (default: ALL 5).")
    ap.add_argument("--week-end", default=None,
                    help="Saturday ISO date (YYYY-MM-DD).  Default = last full Sun-Sat week.")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    if args.week_end:
        end_iso = args.week_end
    else:
        _, end_iso = last_full_sun_sat_week()
    week_no = sun_sat_week_number(end_iso)
    snapshot_date = end_iso  # Saturday close — same convention as Vendor SOH

    accounts = SELLER_ACCOUNTS if args.accounts.upper() == "ALL" else [
        a.strip().upper() for a in args.accounts.split(",") if a.strip()
    ]
    print(f"Snapshot date: {snapshot_date} (Sat) → W{week_no}")
    print(f"Accounts: {accounts}")

    frames = []
    for acct in accounts:
        try:
            df = pull_fba_inventory(acct)
            if df.empty:
                print(f"  [{acct}] no rows")
                continue
            print(f"  [{acct}] {len(df)} SKU rows")
            frames.append(df)
        except Exception as e:
            print(f"  [{acct}] FAILED: {type(e).__name__}: {e}")

    if not frames:
        print("⚠ No data pulled from any account — exiting.")
        return

    combined = pd.concat(frames, ignore_index=True)
    master_by_asin, master_by_sku = load_sku_master()
    per_brand, unmatched = merge_and_split(
        combined, master_by_asin, master_by_sku, week_no, snapshot_date
    )

    if not per_brand:
        print("⚠ No master-matched SKUs — nothing to write.")
        return

    print(f"\nWriting per-brand FBA inventory outputs for W{week_no}:")
    write_outputs(per_brand, unmatched, week_no, snapshot_date)
    print("\n✅ DONE")


if __name__ == "__main__":
    main()
