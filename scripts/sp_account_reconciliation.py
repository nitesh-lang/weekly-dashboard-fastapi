"""Full-account reconciliation for one seller account + month, from the
Finances API. Answers: what did we sell, what came back, what did Amazon
charge, and what should land in the bank.

Design rules
------------
* NOTHING IS SILENTLY DROPPED. Every event list Amazon returns is either
  classified into a bucket or reported under "UNCLASSIFIED" with its value,
  so a missed money type can never hide.
* FEES ARE SHOWN EX-GST *AND* GST SEPARATELY. Settlement reports fees
  GST-inclusive (verified 10/09/26: SC-01 referral Rs202.08/unit = 5.28% of
  gross, /1.18 = Rs171.26 = 4.47% = Amazon's published 4.5%). GST on fees is
  input-tax-creditable, so margin must use the ex-GST figure; the GST column
  is what you reclaim.
* Units ordered vs shipped comes from the weekly sales snapshot, so
  cancellations are visible (ordered but never shipped).

    python scripts/sp_account_reconciliation.py --account NEXLEV --month 2026-08
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
SPAPI_HOST = "https://sellingpartnerapi-eu.amazon.com"
SALES_CSV = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
AMS_CSV = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
GST = 1.18

# Fee types that are charged to us WITH GST on top (India). Everything in the
# fee lists is GST-inclusive; we split it back out for the margin basis.
def _split_gst(v: float) -> tuple[float, float]:
    """-> (ex_gst, gst_component). v is the GST-inclusive amount."""
    ex = v / GST
    return ex, v - ex


def _amt(node) -> float:
    try:
        return float((node or {}).get("CurrencyAmount") or 0)
    except Exception:
        return 0.0


def _token(account: str) -> str:
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": os.environ[f"SP_REFRESH_TOKEN_{account}"],
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def sweep(account: str, month: str) -> dict:
    y, m = map(int, month.split("-"))
    start = f"{date(y, m, 1)}T00:00:00Z"
    end = f"{date(y + (m == 12), (m % 12) + 1, 1)}T00:00:00Z"
    tok = _token(account)
    H = {"x-amz-access-token": tok}

    B = defaultdict(float)          # money buckets
    F = defaultdict(float)          # fee detail (GST-inclusive as charged)
    U = defaultdict(int)            # unit counters
    unclassified = defaultdict(float)
    seen_lists = defaultdict(int)
    token, pages = None, 0

    while True:
        params = {"NextToken": token} if token else {
            "PostedAfter": start, "PostedBefore": end, "MaxResultsPerPage": "100"}
        r = requests.get(f"{SPAPI_HOST}/finances/v0/financialEvents",
                         params=params, headers=H, timeout=60)
        if r.status_code == 429:
            time.sleep(3)
            continue
        r.raise_for_status()
        payload = r.json().get("payload", {})
        ev = payload.get("FinancialEvents", {})
        pages += 1
        for k, v in ev.items():
            if isinstance(v, list) and v:
                seen_lists[k] += len(v)

        # ── SALES ───────────────────────────────────────────────────────
        for se in ev.get("ShipmentEventList") or []:
            for it in se.get("ShipmentItemList") or []:
                U["units_shipped"] += int(it.get("QuantityShipped") or 0)
                for c in it.get("ItemChargeList") or []:
                    t, a = c.get("ChargeType", "?"), _amt(c.get("ChargeAmount"))
                    if t == "Principal":
                        B["sales_ex_gst"] += a
                    elif t == "Tax":
                        B["gst_collected"] += a
                    elif t in ("ShippingCharge", "ShippingTax", "GiftWrap", "GiftWrapTax"):
                        B["shipping_giftwrap_collected"] += a
                    elif t.startswith("TCS"):
                        B["tcs_withheld"] += a
                    else:
                        unclassified[f"ShipmentCharge:{t}"] += a
                for f in it.get("ItemFeeList") or []:
                    F[f.get("FeeType", "?")] += _amt(f.get("FeeAmount"))
                for pr in it.get("PromotionList") or []:
                    B["promo_funded"] += _amt(pr.get("PromotionAmount"))
                for tw in it.get("ItemTaxWithheldList") or []:
                    for tx in tw.get("TaxesWithheld") or []:
                        B["tax_withheld"] += _amt(tx.get("ChargeAmount"))

        # ── REFUNDS ─────────────────────────────────────────────────────
        for rf in ev.get("RefundEventList") or []:
            for it in rf.get("ShipmentItemAdjustmentList") or []:
                U["units_refunded"] += abs(int(it.get("QuantityShipped") or 0))
                for c in it.get("ItemChargeAdjustmentList") or []:
                    t, a = c.get("ChargeType", "?"), _amt(c.get("ChargeAmount"))
                    if t == "Principal":
                        B["refund_principal"] += a
                    elif t == "Tax":
                        B["refund_gst"] += a
                    elif t.startswith("TCS"):
                        B["tcs_reversed"] += a
                    else:
                        B["refund_other"] += a
                for f in it.get("ItemFeeAdjustmentList") or []:
                    F["REFUND:" + f.get("FeeType", "?")] += _amt(f.get("FeeAmount"))

        # ── SERVICE FEES (storage, removals, misc) ──────────────────────
        for sf in ev.get("ServiceFeeEventList") or []:
            reason = sf.get("FeeReason") or sf.get("FeeDescription") or "ServiceFee"
            for f in sf.get("FeeList") or []:
                # FeeType names the real charge (storage, LTSF, refund admin,
                # coupon redemption, removal). Without it the bucket is one
                # opaque number and fee creep hides inside it.
                label = f.get("FeeType") or reason
                F["SERVICE:" + str(reason) + "|" + str(label)] += _amt(f.get("FeeAmount"))

        # ── ADJUSTMENTS (reimbursements etc.) ───────────────────────────
        for ad in ev.get("AdjustmentEventList") or []:
            B[f"adj_{ad.get('AdjustmentType','other')}"] += _amt(ad.get("AdjustmentAmount"))

        # ── AFFORDABILITY (no-cost EMI) ─────────────────────────────────
        for ae in ev.get("AffordabilityExpenseEventList") or []:
            B["affordability"] += _amt(ae.get("TotalExpense"))
        for ar in ev.get("AffordabilityExpenseReversalEventList") or []:
            # Amazon already SIGNS reversals positive (a credit back). Adding
            # them is correct; subtracting double-counted the credit and made
            # every settlement tie-out miss by exactly 2x the reversals.
            B["affordability"] += _amt(ar.get("TotalExpense"))

        # ── ANYTHING ELSE WITH MONEY IN IT ──────────────────────────────
        handled = {"ShipmentEventList", "RefundEventList", "ServiceFeeEventList",
                   "AdjustmentEventList", "AffordabilityExpenseEventList",
                   "AffordabilityExpenseReversalEventList"}
        for k, v in ev.items():
            if k in handled or not isinstance(v, list) or not v:
                continue
            tot = 0.0
            def walk(o):
                nonlocal tot
                if isinstance(o, dict):
                    if "CurrencyAmount" in o:
                        tot += _amt(o)
                    else:
                        for x in o.values():
                            walk(x)
                elif isinstance(o, list):
                    for x in o:
                        walk(x)
            walk(v)
            unclassified[k] += tot

        token = payload.get("NextToken")
        if not token:
            break
        time.sleep(0.5)

    return {"buckets": dict(B), "fees": dict(F), "units": dict(U),
            "unclassified": dict(unclassified), "event_counts": dict(seen_lists),
            "pages": pages, "month": month, "account": account}


def ordered_units(month: str, brand_hint: str) -> tuple[int, float]:
    """Units ORDERED (vs shipped) from the weekly snapshot, so cancellations
    show up as the gap."""
    if not SALES_CSV.exists():
        return 0, 0.0
    s = pd.read_csv(SALES_CSV, usecols=["week", "brand", "channel", "units_sold", "gross_sales"])
    s = s[s["channel"] == "Amazon"]
    s = s[s["brand"].astype(str).str.lower().str.replace("_", " ") == brand_hint.lower()]
    wn = pd.to_numeric(s["week"].astype(str).str.extract(r"(\d+)", expand=False), errors="coerce")
    y, m = map(int, month.split("-"))
    sun = pd.Timestamp("2026-08-09") + pd.to_timedelta((wn - 33) * 7, unit="D")
    sel = (sun.dt.month == m) & (sun.dt.year == y)
    return int(s.loc[sel, "units_sold"].sum()), float(s.loc[sel, "gross_sales"].sum())


def ad_spend(month: str, brand_hint: str) -> float:
    if not AMS_CSV.exists():
        return 0.0
    a = pd.read_csv(AMS_CSV, usecols=["brand", "week", "Spend"])
    a = a[a["brand"].astype(str).str.lower() == brand_hint.lower()]
    wn = pd.to_numeric(a["week"], errors="coerce")
    y, m = map(int, month.split("-"))
    sun = pd.Timestamp("2026-08-09") + pd.to_timedelta((wn - 33) * 7, unit="D")
    return float(a.loc[(sun.dt.month == m) & (sun.dt.year == y), "Spend"].sum())


def report(res: dict, brand_hint: str) -> pd.DataFrame:
    """Controller-reviewed schedule (10/09/26).

    Fixes applied after review:
      * output GST is a LIABILITY held in trust, never part of the bottom line
      * TCS u/s 52 is a PREPAYMENT ASSET, TDS u/s 194-O is RECOVERABLE — neither
        is a cost, though both reduce the cash Amazon sends
      * fees are stated ex-GST (the margin basis); the GST on them is ITC
      * service fees are broken out by fee type instead of one opaque bucket
      * the bottom line is labelled CONTRIBUTION BEFORE COGS, because no cost
        of goods is in this file at all
    """
    B, F, U = res["buckets"], res["fees"], res["units"]
    rows = []

    def add(sec, item, val, note=""):
        rows.append({"Section": sec, "Item": item, "Amount": round(val, 2), "Note": note})

    ordered, _ = ordered_units(res["month"], brand_hint)
    spend = ad_spend(res["month"], brand_hint)
    shipped = U.get("units_shipped", 0)

    add("1. SALES", "Units ordered (weekly report, order-date basis)", ordered,
        "different date basis from shipped")
    add("1. SALES", "Units shipped (what Amazon paid on)", shipped, "financial basis")
    add("1. SALES", "Ordered but not shipped", ordered - shipped,
        "NOT all cancellations - month-end timing + pending orders; needs All-Orders report to split")
    add("1. SALES", "Product sales (ex-GST)", B.get("sales_ex_gst", 0), "P&L revenue")
    add("1. SALES", "Shipping and gift wrap collected", B.get("shipping_giftwrap_collected", 0), "")

    ru = U.get("units_refunded", 0)
    add("2. RETURNS", "Units refunded", ru, "")
    add("2. RETURNS", "Return rate % (in-month, mixed cohorts)",
        round(ru / shipped * 100, 2) if shipped else 0,
        "these refunds mostly belong to earlier months' shipments")
    add("2. RETURNS", "Sale value refunded (ex-GST)", B.get("refund_principal", 0), "")
    for k in sorted(F):
        if k.startswith("REFUND:") and abs(F[k]) > 0.5:
            ex, _g = _split_gst(F[k])
            add("2. RETURNS", k.replace("REFUND:", "") + " on refunds (ex-GST)", ex,
                "given back to us" if F[k] > 0 else "Amazon kept this")

    groups = {"Commission (referral)": ["Commission", "GiftwrapCommission"],
              "FBA fulfilment": ["FBAWeightBasedFee", "FBAPerUnitFulfillmentFee"],
              "Closing fee": ["FixedClosingFee", "VariableClosingFee"],
              "Other selling fees": ["ShippingChargeback", "GiftwrapChargeback",
                                     "TechnologyFee", "ShippingHB"]}
    used = set()
    fee_gst_total = 0.0
    for label, keys in groups.items():
        v = sum(F.get(k, 0) for k in keys)
        used.update(keys)
        if v:
            ex, gst = _split_gst(v)
            fee_gst_total += gst
            add("3. AMAZON FEES (ex-GST)", label, ex, "")
    svc = {k: v for k, v in F.items() if k.startswith("SERVICE:")}
    for k in sorted(svc, key=lambda x: svc[x]):
        ex, gst = _split_gst(svc[k])
        fee_gst_total += gst
        add("3. AMAZON FEES (ex-GST)", k.split("|")[-1], ex,
            k.replace("SERVICE:", "").split("|")[0])
    for k, v in F.items():
        if k in used or k.startswith(("SERVICE:", "REFUND:")) or not v:
            continue
        ex, gst = _split_gst(v)
        fee_gst_total += gst
        add("3. AMAZON FEES (ex-GST)", k + " (unmapped)", ex, "CHECK ME")

    add("4. WE FUNDED", "Coupons / promotions", B.get("promo_funded", 0), "")
    add("4. WE FUNDED", "No-cost EMI / bank offers", B.get("affordability", 0), "")
    add("4. WE FUNDED", "Advertising (from our AMS data)", -spend,
        "billed outside settlement - CONFIRM if GST-inclusive")

    credits = 0.0
    for k, v in B.items():
        if k.startswith("adj_"):
            credits += v
            note = ("fee credit - carries GST, reverses ITC" if "ommission" in k
                    else "compensation - no GST")
            add("5. CREDITS", k.replace("adj_", "").replace("_", " ").title(), v, note)

    out_gst = B.get("gst_collected", 0)
    ref_gst = B.get("refund_gst", 0)
    tcs = B.get("tcs_withheld", 0) + B.get("tcs_reversed", 0)
    tds = B.get("tax_withheld", 0)
    add("6. GST AND TAX (not profit)", "Output GST collected from customers", out_gst,
        "held in trust - never ours")
    add("6. GST AND TAX (not profit)", "Output GST reversed on refunds", ref_gst, "")
    add("6. GST AND TAX (not profit)", "ITC on Amazon fees (reclaimable)", -fee_gst_total,
        "tie to Amazon's tax invoice AND GSTR-2B")
    add("6. GST AND TAX (not profit)", "TCS withheld u/s 52 (0.5% of net sales)", tcs,
        "PREPAYMENT ASSET - accept monthly in the GST portal or it is stranded")
    add("6. GST AND TAX (not profit)", "TDS withheld u/s 194-O (0.1% of gross)", tds,
        "RECOVERABLE in ITR - charged on gross, so returns are a permanent drag")
    net_gst_payable = out_gst + ref_gst - fee_gst_total + tcs
    add("6. GST AND TAX (not profit)", "Net GST still to remit in cash", -net_gst_payable, "")

    settle = (B.get("sales_ex_gst", 0) + out_gst + B.get("shipping_giftwrap_collected", 0)
              + credits + B.get("refund_principal", 0) + ref_gst + B.get("refund_other", 0)
              + sum(F.values()) + B.get("promo_funded", 0) + B.get("affordability", 0)
              + tcs + tds)
    add("7. BOTTOM LINE", "Amazon should settle (cash basis, GST-inclusive)", settle,
        "compare with deposits - cycles straddle the month end")
    add("7. BOTTOM LINE", "less: net GST to remit to government", -net_gst_payable, "")
    add("7. BOTTOM LINE", "= Marketplace contribution BEFORE COGS", settle - net_gst_payable, "")
    add("7. BOTTOM LINE", "= After advertising, BEFORE COGS", settle - net_gst_payable - spend,
        "NOT profit - landed cost of goods is not in this file")

    for k, v in res["unclassified"].items():
        add("8. NOT CLASSIFIED", k, v, "money not bucketed - investigate")
    add("9. COMPLETENESS", "Event lists Amazon returned with data", len(res["event_counts"]),
        ", ".join(sorted(res["event_counts"])))
    add("9. COMPLETENESS", "Event lists returned EMPTY", 33 - len(res["event_counts"]),
        "incl. ShipmentSettleEventList - must stay empty or revenue double-counts")
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="NEXLEV")
    ap.add_argument("--month", default=None, help="YYYY-MM (default: last full month)")
    ap.add_argument("--brand", default=None, help="brand name in the weekly snapshot")
    args = ap.parse_args()
    load_dotenv(ROOT / ".env")
    month = args.month or (lambda e: f"{e.year}-{e.month:02d}")(
        date.today().replace(day=1) - timedelta(days=1))
    brand = args.brand or args.account.lower()

    print(f"=== {args.account} reconciliation {month} ===")
    res = sweep(args.account, month)
    print(f"  {res['pages']} pages; event types seen: "
          + ", ".join(f"{k}={v}" for k, v in sorted(res["event_counts"].items())))
    df = report(res, brand)
    out = ROOT / "data" / "processed" / f"reconciliation_{args.account}_{month}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        df.to_excel(xw, "Reconciliation", index=False)
        pd.DataFrame([{"fee_type": k, "amount_incl_gst": v} for k, v in
                      sorted(res["fees"].items(), key=lambda kv: kv[1])]).to_excel(
            xw, "Fee detail", index=False)
    print()
    for sec, g in df.groupby("Section", sort=True):
        print(sec)
        for _, r in g.iterrows():
            print(f"    {str(r['Item'])[:46]:48} {r['Amount']:>14,.0f}  {r['Note']}")
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
