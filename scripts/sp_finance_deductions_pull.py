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
# Per-SKU money Amazon ACTUALLY charged on shipments (vs the fee-preview
# estimates the margin tool uses) + affordability (no-cost-EMI) expense,
# which carries no SKU so it lands as one account-level row.
FEES_CSV = REPO_ROOT / "data" / "processed" / "amazon_charged_fees_snapshot.csv"
AFFORD_SKU = "__AFFORDABILITY__"
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
    ship: dict = {}
    afford = 0.0
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

        # ── Shipments: the fees Amazon really charged, per SKU ──
        for se in ev.get("ShipmentEventList") or []:
            # Multi-Channel Fulfilment rows carry an FBA fee but ZERO Amazon
            # revenue - the sale happened on another channel. Counting their
            # units here loaded their landed cost onto Amazon's margin
            # (418 units = Rs6.27L of phantom COGS in Aug 2026).
            if (se.get("MarketplaceName") or "") != "Amazon.in":
                continue
            for it in se.get("ShipmentItemList") or []:
                sku = (it.get("SellerSKU") or "").strip()
                if not sku:
                    continue
                d = ship.setdefault(sku, {"units": 0, "principal": 0.0, "tax": 0.0,
                                          "promo": 0.0, "referral": 0.0,
                                          "fulfilment": 0.0, "closing": 0.0,
                                          "other_fees": 0.0})
                d["units"] += int(it.get("QuantityShipped") or 0)
                for c in it.get("ItemChargeList") or []:
                    t = c.get("ChargeType", "")
                    if t == "Principal":
                        d["principal"] += _amt(c.get("ChargeAmount"))
                    elif t == "Tax":
                        d["tax"] += _amt(c.get("ChargeAmount"))
                for f in it.get("ItemFeeList") or []:
                    t = f.get("FeeType", "")
                    v = _amt(f.get("FeeAmount"))
                    if t in ("Commission", "GiftwrapCommission"):
                        d["referral"] += v
                    elif t.startswith("FBA"):
                        d["fulfilment"] += v
                    elif "ClosingFee" in t:
                        d["closing"] += v
                    else:
                        d["other_fees"] += v
                for pr in it.get("PromotionList") or []:
                    d["promo"] += _amt(pr.get("PromotionAmount"))

        for ae in ev.get("AffordabilityExpenseEventList") or []:
            afford += _amt(ae.get("TotalExpense"))
        for ar in ev.get("AffordabilityExpenseReversalEventList") or []:
            afford += _amt(ar.get("TotalExpense"))   # Amazon signs credits positive

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
        acol = next(c for c in mast.columns if c.strip().lower() == "asin")
        # sku_master keys SKUs under "FBA SKU" (+ "Original SKU" for the
        # pre-FBA code) — there is no plain "SKU" column. Build the map from
        # EVERY sku-ish column so settlement rows key onto their ASIN.
        skucols = [c for c in mast.columns if "sku" in c.strip().lower()]
        amap: dict[str, str] = {}
        for sc in skucols:
            amap.update({k: v for k, v in zip(
                mast[sc].astype(str).str.strip().str.upper(),
                mast[acol].astype(str).str.strip().str.upper()) if k and k != "NAN"})
        need = (df["asin"] == "") & df["sku"].astype(str).ne("")
        df.loc[need, "asin"] = (df.loc[need, "sku"].astype(str).str.strip().str.upper()
                                .map(amap).fillna(""))
        hit = int((df.loc[need, "asin"] != "").sum())
        print(f"    sku->asin map: {len(amap)} keys, matched {hit}/{int(need.sum())} rows")
    except Exception as e:
        print(f"    WARN: sku->asin map failed: {e!r}")
    agg = (df.groupby(["event_group", "fee_type", "sku", "asin"], as_index=False)
             .agg(amount=("amount", "sum"), qty=("qty", "sum")))
    agg["account"] = account
    agg["month"] = month

    # ── charged-fees snapshot (per SKU) + affordability (account level) ──
    if ship or afford:
        f = pd.DataFrame([{"sku": k, **v} for k, v in ship.items()])
        if afford:
            f = pd.concat([f, pd.DataFrame([{"sku": AFFORD_SKU, "units": 0,
                                             "principal": 0.0, "tax": 0.0, "promo": 0.0,
                                             "referral": 0.0, "fulfilment": 0.0,
                                             "closing": 0.0, "other_fees": afford}])],
                          ignore_index=True)
        f["asin"] = ""
        try:
            mast = pd.read_excel(MASTER)
            acol = next(c for c in mast.columns if c.strip().lower() == "asin")
            amap: dict[str, str] = {}
            for sc in [c for c in mast.columns if "sku" in c.strip().lower()]:
                amap.update({k: v for k, v in zip(
                    mast[sc].astype(str).str.strip().str.upper(),
                    mast[acol].astype(str).str.strip().str.upper()) if k and k != "NAN"})
            f["asin"] = f["sku"].astype(str).str.strip().str.upper().map(amap).fillna("")
        except Exception as e:
            print(f"    WARN: charged-fees sku->asin map failed: {e!r}")
        f["account"] = account
        f["month"] = month
        cols = ["account", "month", "sku", "asin", "units", "principal", "tax",
                "promo", "referral", "fulfilment", "closing", "other_fees"]
        f = f[cols]
        if FEES_CSV.exists():
            old = pd.read_csv(FEES_CSV, dtype={"month": str})
            old = old[~((old["month"] == month) & (old["account"] == account))]
            f = pd.concat([old, f], ignore_index=True)
        f.to_csv(FEES_CSV, index=False)
        cur = f[(f["month"] == month) & (f["account"] == account)]
        real = cur[cur["sku"] != AFFORD_SKU]
        print(f"    charged fees: {len(real)} SKUs, {int(real['units'].sum())} units, "
              f"referral Rs {real['referral'].sum():,.0f}, fulfilment Rs {real['fulfilment'].sum():,.0f}, "
              f"promo Rs {real['promo'].sum():,.0f} | affordability Rs {afford:,.0f}")
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
