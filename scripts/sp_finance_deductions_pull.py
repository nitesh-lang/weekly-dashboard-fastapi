"""Actual Amazon deductions from the Finances API (settlement truth).

Pulls listFinancialEvents for a month and aggregates the deduction types the
fee-estimate APIs can't see:
  * RefundEventList        -> refund admin ("RefundCommission"), reversed fees,
                              refunded principal — per SKU
  * ServiceFeeEventList    -> storage / removal / prep / misc service fees
  * AdjustmentEventList    -> reimbursements (credits back to us)
  * RemovalShipmentEventList -> removal order charges

Output (append/replace per account+month):
  data/processed/amazon_deductions_snapshot.csv
    account, month, event_group, fee_type, sku, asin, amount, qty
Amounts keep Amazon's sign convention: negative = charged to us, positive =
credited. GST on fees appears as separate *Tax fee types (input-credit eligible).

Usage:
  python scripts/sp_finance_deductions_pull.py --account NEXLEV                # last full month
  python scripts/sp_finance_deductions_pull.py --account NEXLEV --month 2026-08
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_CSV = REPO_ROOT / "data" / "processed" / "amazon_deductions_snapshot.csv"
MASTER = REPO_ROOT / "data" / "master" / "sku_master.xlsx"
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"

SELLER_ACCOUNTS = ["AUDIOARRAY", "CAMBIUMRETAIL", "NEXLEV", "VIOMI", "WHITEMULBERRY"]


def _lwa(account: str) -> str:
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": os.environ[f"SP_REFRESH_TOKEN_{account}"],
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _amt(node) -> float:
    try:
        return float((node or {}).get("CurrencyAmount") or 0)
    except Exception:
        return 0.0


def pull_month(account: str, month: str) -> pd.DataFrame:
    y, m = map(int, month.split("-"))
    start = f"{date(y, m, 1)}T00:00:00Z"
    end_d = date(y + (m == 12), (m % 12) + 1, 1)
    end = f"{end_d}T00:00:00Z"
    tok = _lwa(account)
    H = {"x-amz-access-token": tok}
    rows: list[dict] = []
    token = None
    page = 0
    while True:
        params = {"PostedAfter": start, "PostedBefore": end, "MaxResultsPerPage": "100"}
        if token:
            params = {"NextToken": token}
        r = requests.get(f"{SPAPI_HOST}/finances/v0/financialEvents",
                         params=params, headers=H, timeout=60)
        if r.status_code == 429:
            time.sleep(3)
            continue
        r.raise_for_status()
        j = r.json().get("payload", {})
        ev = j.get("FinancialEvents", {})
        page += 1

        for rf in ev.get("RefundEventList") or []:
            for it in rf.get("ShipmentItemAdjustmentList") or []:
                sku = it.get("SellerSKU", "")
                qty = it.get("QuantityShipped") or 0
                for fee in it.get("ItemFeeAdjustmentList") or []:
                    rows.append({"event_group": "Refund", "fee_type": fee.get("FeeType", "?"),
                                 "sku": sku, "amount": _amt(fee.get("FeeAmount")), "qty": qty})
                for chg in it.get("ItemChargeAdjustmentList") or []:
                    rows.append({"event_group": "RefundCharge", "fee_type": chg.get("ChargeType", "?"),
                                 "sku": sku, "amount": _amt(chg.get("ChargeAmount")), "qty": qty})

        for sf in ev.get("ServiceFeeEventList") or []:
            reason = sf.get("FeeReason") or sf.get("FeeDescription") or "?"
            sku = sf.get("SellerSKU", "") or ""
            asin = sf.get("ASIN", "") or ""
            for fee in sf.get("FeeList") or []:
                rows.append({"event_group": "ServiceFee", "fee_type": f"{reason}:{fee.get('FeeType','?')}",
                             "sku": sku, "asin": asin, "amount": _amt(fee.get("FeeAmount")), "qty": 0})

        for ad in ev.get("AdjustmentEventList") or []:
            at = ad.get("AdjustmentType", "?")
            items = ad.get("AdjustmentItemList") or [{}]
            for it in items:
                rows.append({"event_group": "Adjustment", "fee_type": at,
                             "sku": it.get("SellerSKU", "") or "", "asin": it.get("ASIN", "") or "",
                             "amount": _amt(it.get("TotalAmount")) or _amt(ad.get("AdjustmentAmount")),
                             "qty": float(it.get("Quantity") or 0)})

        for rm in ev.get("RemovalShipmentEventList") or []:
            for it in rm.get("RemovalShipmentItemList") or []:
                rows.append({"event_group": "Removal", "fee_type": rm.get("TransactionType", "REMOVAL"),
                             "sku": it.get("FulfillmentNetworkSKU", "") or "",
                             "amount": _amt(it.get("FeeAmount")) or -_amt(it.get("Revenue")),
                             "qty": float(it.get("Quantity") or 0)})

        token = j.get("NextToken")
        if page % 20 == 0:
            print(f"    …page {page}, {len(rows)} deduction rows so far")
        if not token:
            break
        time.sleep(0.6)   # ~0.5 r/s throttle headroom
    print(f"    {page} pages, {len(rows)} deduction rows")
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "asin" not in df.columns:
        df["asin"] = ""
    df["asin"] = df["asin"].fillna("")
    # map SKU -> ASIN via master where the event didn't carry one
    try:
        mast = pd.read_excel(MASTER)
        scol = next(c for c in mast.columns if c.strip().lower() == "sku")
        acol = next(c for c in mast.columns if c.strip().lower() == "asin")
        amap = dict(zip(mast[scol].astype(str).str.strip().str.upper(),
                        mast[acol].astype(str).str.strip().str.upper()))
        need = (df["asin"] == "") & df["sku"].astype(str).ne("")
        df.loc[need, "asin"] = df.loc[need, "sku"].astype(str).str.strip().str.upper().map(amap).fillna("")
    except Exception as e:
        print(f"    WARN: sku->asin map failed: {e}")
    agg = (df.groupby(["event_group", "fee_type", "sku", "asin"], as_index=False)
             .agg(amount=("amount", "sum"), qty=("qty", "sum")))
    agg["account"] = account
    agg["month"] = month
    return agg


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", choices=SELLER_ACCOUNTS, required=True)
    ap.add_argument("--month", default=None)
    args = ap.parse_args()
    load_dotenv(REPO_ROOT / ".env")
    if args.month:
        month = args.month
    else:
        e = date.today().replace(day=1) - timedelta(days=1)
        month = f"{e.year}-{e.month:02d}"
    print(f"=== {args.account} finances {month} ===")
    df = pull_month(args.account, month)
    if df.empty:
        print("No deduction events — nothing written.")
        sys.exit(1)
    df = df[["account", "month", "event_group", "fee_type", "sku", "asin", "amount", "qty"]]
    if OUT_CSV.exists():
        old = pd.read_csv(OUT_CSV, dtype={"month": str})
        old = old[~((old["month"] == month) & (old["account"] == args.account))]
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(OUT_CSV, index=False)
    cur = df[(df["month"] == month) & (df["account"] == args.account)]
    print("\nBy group (negative = charged to us):")
    print(cur.groupby("event_group")["amount"].sum().round(0).to_string())
    print("\nTop fee types:")
    print(cur.groupby(["event_group", "fee_type"])["amount"].sum().round(0)
          .sort_values().head(12).to_string())
    print(f"\nWROTE {OUT_CSV.name} ({len(df)} rows total)")


if __name__ == "__main__":
    main()
