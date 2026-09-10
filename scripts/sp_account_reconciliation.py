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
    C = defaultdict(float)          # what the classifier actually picked up
    leaf_seen = defaultdict(float)  # every rupee Amazon put in the two big lists
    all_lists: set[str] = set()     # every list key returned, empty ones included
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
        for k in ev:
            all_lists.add(k)
        for k, v in ev.items():
            if isinstance(v, list) and v:
                seen_lists[k] += len(v)

        # LEAK TEST. "Nothing was silently dropped" is only worth saying if it
        # is measured: sum every leaf CurrencyAmount in the two big lists and
        # compare against what the classifier below actually picked up. The
        # residual is the money we failed to see. It found Rs23,399 of promo
        # adjustments that had been invisible since this script was written.
        for _lk in ("ShipmentEventList", "RefundEventList"):
            leaf_seen[_lk] += _leaf_sum(ev.get(_lk) or [])

        # ── SALES ───────────────────────────────────────────────────────
        for se in ev.get("ShipmentEventList") or []:
            # Multi-Channel Fulfilment: Amazon ships OUR order from OUR FBA
            # stock for a fee, and the sale happened on some other channel.
            # Those units have an FBA fee here but ZERO revenue here, so
            # counting them as Amazon 3P units loads their landed cost onto
            # Amazon's margin. 418 units in Aug 2026 = Rs6.27L of phantom COGS.
            mcf = (se.get("MarketplaceName") or "") != "Amazon.in"
            for it in se.get("ShipmentItemList") or []:
                if mcf:
                    U["units_mcf"] += int(it.get("QuantityShipped") or 0)
                    for f in it.get("ItemFeeList") or []:
                        _a = _amt(f.get("FeeAmount"))
                        C["ShipmentEventList"] += _a
                        F["MCF:" + f.get("FeeType", "?")] += _a
                    continue
                U["units_shipped"] += int(it.get("QuantityShipped") or 0)
                for c in it.get("ItemChargeList") or []:
                    t, a = c.get("ChargeType", "?"), _amt(c.get("ChargeAmount"))
                    C["ShipmentEventList"] += a
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
                    _a = _amt(f.get("FeeAmount"))
                    C["ShipmentEventList"] += _a
                    F[f.get("FeeType", "?")] += _a
                for pr in it.get("PromotionList") or []:
                    _a = _amt(pr.get("PromotionAmount"))
                    C["ShipmentEventList"] += _a
                    B["promo_funded"] += _a
                for tw in it.get("ItemTaxWithheldList") or []:
                    for tx in tw.get("TaxesWithheld") or []:
                        _a = _amt(tx.get("ChargeAmount"))
                        C["ShipmentEventList"] += _a
                        B["tax_withheld"] += _a

        # ── REFUNDS ─────────────────────────────────────────────────────
        for rf in ev.get("RefundEventList") or []:
            for it in rf.get("ShipmentItemAdjustmentList") or []:
                U["units_refunded"] += abs(int(it.get("QuantityShipped") or 0))
                for c in it.get("ItemChargeAdjustmentList") or []:
                    t, a = c.get("ChargeType", "?"), _amt(c.get("ChargeAmount"))
                    C["RefundEventList"] += a
                    if t == "Principal":
                        B["refund_principal"] += a
                    elif t == "Tax":
                        B["refund_gst"] += a
                    elif t.startswith("TCS"):
                        B["tcs_reversed"] += a
                    else:
                        B["refund_other"] += a
                for f in it.get("ItemFeeAdjustmentList") or []:
                    _a = _amt(f.get("FeeAmount"))
                    C["RefundEventList"] += _a
                    F["REFUND:" + f.get("FeeType", "?")] += _a
                # Coupon funding comes BACK when the discounted order is
                # refunded. Missing this overstated coupon cost by Rs23,399
                # in Aug and left the settlement short by the same amount.
                for pr in it.get("PromotionAdjustmentList") or []:
                    _a = _amt(pr.get("PromotionAmount"))
                    C["RefundEventList"] += _a
                    B["promo_funded"] += _a

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
        _agst = lambda e: (_amt(e.get("TaxTypeCGST")) + _amt(e.get("TaxTypeSGST"))
                           + _amt(e.get("TaxTypeIGST")))
        for ae in ev.get("AffordabilityExpenseEventList") or []:
            B["affordability"] += _amt(ae.get("TotalExpense"))
            B["affordability_gst"] += _agst(ae)
        for ar in ev.get("AffordabilityExpenseReversalEventList") or []:
            # Amazon already SIGNS reversals positive (a credit back). Adding
            # them is correct; subtracting double-counted the credit and made
            # every settlement tie-out miss by exactly 2x the reversals.
            B["affordability"] += _amt(ar.get("TotalExpense"))
            B["affordability_gst"] += _agst(ar)

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

    leak = {k: round(leaf_seen[k] - C[k], 2) for k in leaf_seen}
    return {"buckets": dict(B), "fees": dict(F), "units": dict(U),
            "leak": leak, "lists_returned": len(all_lists),
            "unclassified": dict(unclassified), "event_counts": dict(seen_lists),
            "pages": pages, "month": month, "account": account}


def _leaf_sum(o) -> float:
    """Every CurrencyAmount in the tree, counted once. Stops descending at a
    money node so a parent total and its children are never both added."""
    if isinstance(o, dict):
        if "CurrencyAmount" in o:
            return _amt(o)
        return sum(_leaf_sum(x) for x in o.values())
    if isinstance(o, list):
        return sum(_leaf_sum(x) for x in o)
    return 0.0


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
    """Ad spend for the CALENDAR month, day-prorated out of Sun-Sat weeks.

    Keeping whole weeks whose Sunday fell in the month put 35 days of spend
    against a 31-day financial month: w36 (Aug 30 - Sep 5) is five-sevenths
    September, and w31's one August day was dropped. That mis-stated August
    ads by Rs136,839 (9.1%). Every other number here comes from a strict
    calendar-month API window, so ads has to match it.
    """
    if not AMS_CSV.exists():
        raise FileNotFoundError(
            f"{AMS_CSV} is missing - ad spend is a Rs15L line and must not "
            "silently come through as zero")
    a = pd.read_csv(AMS_CSV, usecols=["brand", "week", "Spend"])
    a = a[a["brand"].astype(str).str.lower() == brand_hint.lower()].copy()
    a["wn"] = pd.to_numeric(a["week"], errors="coerce")
    a["Spend"] = pd.to_numeric(a["Spend"], errors="coerce").fillna(0)
    # week 33 of 2026 starts Sunday 09/08/2026
    a["start"] = pd.Timestamp("2026-08-09") + pd.to_timedelta((a["wn"] - 33) * 7, unit="D")
    y, m = map(int, month.split("-"))
    total = 0.0
    for _, r in a.iterrows():
        if pd.isna(r["start"]):
            continue
        days = [r["start"] + pd.Timedelta(days=d) for d in range(7)]
        inside = sum(1 for d in days if d.month == m and d.year == y)
        if inside:
            total += float(r["Spend"]) * inside / 7
    return total


def orders_bridge(account: str, month: str) -> dict:
    """Ordered -> cancelled / pending / unfulfillable / not-yet-shipped -> shipped,
    from GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL for the calendar
    month. Replaces the old proxy (weekly-report units minus shipped units),
    which mixed a Sun-Sat week grid with a calendar month and made ordinary
    month-end timing look like a 15% cancellation rate. Real rate: ~2.7%.
    """
    import gzip
    import io

    cache = ROOT / "data" / "raw" / f"_allorders_{account}_{month}.csv"
    if cache.exists():
        df = pd.read_csv(cache, dtype=str)
    else:
        y, m = map(int, month.split("-"))
        end = date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)
        tok = _token(account)
        H = {"x-amz-access-token": tok, "content-type": "application/json"}
        body = {"reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
                "marketplaceIds": ["A21TJRUUN4KGV"],
                "dataStartTime": f"{date(y, m, 1)}T00:00:00Z",
                "dataEndTime": f"{end}T23:59:59Z"}
        c = requests.post(f"{SPAPI_HOST}/reports/2021-06-30/reports",
                          json=body, headers=H, timeout=30)
        if c.status_code != 202:
            print(f"  All-Orders report unavailable: HTTP {c.status_code}")
            return {}
        rid = c.json()["reportId"]
        st = "?"
        for _ in range(40):
            j = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
                             headers={"x-amz-access-token": tok}, timeout=30).json()
            st = j.get("processingStatus")
            if st in ("DONE", "FATAL", "CANCELLED"):
                break
            time.sleep(12)
        if st != "DONE":
            print(f"  All-Orders report: {st}")
            return {}
        d = requests.get(f"{SPAPI_HOST}/reports/2021-06-30/documents/{j['reportDocumentId']}",
                         headers={"x-amz-access-token": tok}, timeout=30).json()
        raw = requests.get(d["url"], timeout=120).content
        if d.get("compressionAlgorithm") == "GZIP":
            raw = gzip.decompress(raw)
        df = pd.read_csv(io.BytesIO(raw), sep="\t", dtype=str)
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache, index=False)

    if df.empty or "quantity" not in df.columns:
        return {}
    q = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    istat = df.get("item-status", pd.Series([""] * len(df))).fillna("")
    ostat = df.get("order-status", pd.Series([""] * len(df))).fillna("")
    ordered = int(q.sum())
    shipped = int(q[istat.eq("Shipped")].sum())
    cancelled = int(q[ostat.eq("Cancelled")].sum())
    pending = int(q[ostat.eq("Pending")].sum())
    unfulfillable = int(q[istat.eq("Unfulfillable")].sum())
    later = ordered - shipped - cancelled - pending - unfulfillable
    return {"ordered": ordered, "shipped": shipped, "cancelled": cancelled,
            "pending": pending, "unfulfillable": unfulfillable, "later": later}


ACCOUNT_BRAND = {"NEXLEV": "Nexlev", "AUDIOARRAY": "Audio Array",
                 "WHITEMULBERRY": "White Mulberry"}
RETURN_RECOVERY = 0.875   # repack + restarted storage clock, same as the margin tool


def cogs_for(account: str, month: str) -> dict:
    """Landed cost of what we actually shipped, from the margin tool's master
    (the same DP the calculator uses), less the stock that came back sellable.

    Per-SKU units come from the settlement pull, so this is what Amazon
    actually shipped and paid on — not an order-report estimate.
    """
    brand = ACCOUNT_BRAND.get(account)
    fees_csv = ROOT / "data" / "processed" / "amazon_charged_fees_snapshot.csv"
    returns_csv = ROOT / "data" / "processed" / "returns_snapshot.csv"
    if not brand or not fees_csv.exists():
        return {}
    try:
        from weekly_app.etl.margin_snapshot import MARGIN_TOOL_MASTERS, _global_params
        path = MARGIN_TOOL_MASTERS.get(brand)
        if not path or not path.exists():
            return {}
        xls = pd.ExcelFile(path)
        m = xls.parse(xls.sheet_names[0])
        g = _global_params(xls)
    except Exception as e:
        print(f"  COGS: master unavailable ({e!r})")
        return {}

    usd = float(g.get("usd_rate") or 0)
    sur = float(g.get("surcharge_pct") or 0)

    def _dp(r) -> float:
        fob = float(r.get("Latest FOB") or 0) * usd
        fr = float(r.get("Freight+Clearance") or 0) * usd
        duty = (fob + fr) * float(r.get("Import Duty %") or 0) / 100
        return fob + fr + duty + duty * sur / 100 + float(r.get("Additional Cost") or 0)

    m["_DP"] = m.apply(_dp, axis=1)
    up = lambda s: s.astype(str).str.strip().str.upper()
    dpmap: dict[str, float] = {}
    for c in [c for c in m.columns if "sku" in str(c).lower()]:
        dpmap.update(dict(zip(up(m[c]), m["_DP"])))
    if "ASIN" in m.columns:
        dpmap.update(dict(zip(up(m["ASIN"]), m["_DP"])))

    f = pd.read_csv(fees_csv, dtype={"month": str})
    f = f[(f["account"] == account) & (f["month"] == month) &
          (f["sku"] != "__AFFORDABILITY__")].copy()
    if f.empty:
        return {}
    f["units"] = pd.to_numeric(f["units"], errors="coerce").fillna(0)
    f["dp"] = up(f["sku"]).map(dpmap)
    f.loc[f["dp"].isna(), "dp"] = up(f["asin"].fillna("")).map(dpmap)
    cov = f[f["dp"].notna()]
    units_all = float(f["units"].sum())
    units_cov = float(cov["units"].sum())
    if units_cov <= 0:
        return {}
    measured = float((cov["units"] * cov["dp"]).sum())
    avg = measured / units_cov
    gross = avg * units_all          # gross up the few SKUs with no master row

    sellable = 0.0
    try:
        rr = pd.read_csv(returns_csv)
        rr = rr[rr["brand"].astype(str).str.lower() == brand.lower()]
        ru = pd.to_numeric(rr["returns_3p"], errors="coerce").fillna(0)
        sp = pd.to_numeric(rr["sellable_pct"], errors="coerce").fillna(0)
        if ru.sum() > 0:
            sellable = float((ru * sp).sum() / ru.sum())
    except Exception:
        pass
    return {"overhead_pct": float(g.get("overhead_pct") or 0),
            "finance_pct": float(g.get("finance_pct") or 0),
            "gross": gross, "avg": avg, "units_all": units_all,
            "units_cov": units_cov, "coverage": units_cov / units_all * 100,
            "sellable_pct": sellable, "recovery_factor": RETURN_RECOVERY,
            "brand": brand}


def report(res: dict, brand_hint: str, bridge: dict | None = None,
           cogs: dict | None = None) -> pd.DataFrame:
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

    if bridge:
        b = bridge
        add("01. SALES", "Units ordered in the month", b["ordered"], "All-Orders report, order date")
        add("01. SALES", "  less cancelled", -b["cancelled"],
            f"{b['cancelled'] / b['ordered'] * 100:.1f}% of orders - the REAL cancellation rate"
            if b["ordered"] else "")
        add("01. SALES", "  less pending (payment not authorised)", -b["pending"], "")
        add("01. SALES", "  less unfulfillable", -b["unfulfillable"], "")
        add("01. SALES", "  less not shipped by month end", -b["later"], "ships next month")
        add("01. SALES", "= Shipped from this month's orders", b["shipped"], "")
        add("01. SALES", "Units shipped (what Amazon paid on)", shipped,
            f"financial basis, Amazon.in only; the {b['shipped'] - shipped} gap to the "
            "line above is orders shipped across the month boundary")
        if U.get("units_mcf"):
            add("01. SALES", "Units shipped for other channels (MCF)", U["units_mcf"],
                "Amazon shipped these from our FBA stock but the sale was elsewhere - "
                "no revenue here, so no cost here either")
    else:
        add("01. SALES", "Units ordered (weekly report, order-date basis)", ordered,
            "different date basis from shipped")
        add("01. SALES", "Units shipped (what Amazon paid on)", shipped, "financial basis")
    add("01. SALES", "Product sales (GST NOT included)", B.get("sales_ex_gst", 0), "P&L revenue")
    add("01. SALES", "Shipping and gift wrap collected", B.get("shipping_giftwrap_collected", 0), "")

    fee_gst_total = 0.0
    ru = U.get("units_refunded", 0)
    add("02. RETURNS", "Units refunded", ru, "")
    add("02. RETURNS", "Return rate % (in-month, mixed cohorts)",
        round(ru / shipped * 100, 2) if shipped else 0,
        "on Amazon-only units; these refunds mostly belong to earlier months")
    add("02. RETURNS", "Sale value refunded (GST NOT included)", B.get("refund_principal", 0), "")
    for k in sorted(F):
        if k.startswith("REFUND:") and abs(F[k]) > 0.5:
            ex, _g = _split_gst(F[k])
            # when Amazon returns commission it returns the GST on it too, so
            # the credit we already claimed has to come back down
            fee_gst_total += _g
            add("02. RETURNS", k.replace("REFUND:", "") + " on refunds (ex-GST)", ex,
                "given back to us" if F[k] > 0 else "Amazon kept this")

    groups = {"Commission (referral)": ["Commission", "GiftwrapCommission"],
              "FBA fulfilment": ["FBAWeightBasedFee", "FBAPerUnitFulfillmentFee"],
              "Closing fee": ["FixedClosingFee", "VariableClosingFee"],
              "Other selling fees": ["ShippingChargeback", "GiftwrapChargeback",
                                     "TechnologyFee", "ShippingHB"]}
    used = set()
    for label, keys in groups.items():
        v = sum(F.get(k, 0) for k in keys)
        used.update(keys)
        if v:
            ex, gst = _split_gst(v)
            fee_gst_total += gst
            add("03. AMAZON FEES (ex-GST)", label, ex, "")
    svc = {k: v for k, v in F.items() if k.startswith("SERVICE:")}
    for k in sorted(svc, key=lambda x: svc[x]):
        ex, gst = _split_gst(svc[k])
        fee_gst_total += gst
        add("03. AMAZON FEES (ex-GST)", k.split("|")[-1], ex,
            k.replace("SERVICE:", "").split("|")[0])
    mcf_fee = sum(v for k, v in F.items() if k.startswith("MCF:"))
    if mcf_fee:
        ex, gst = _split_gst(mcf_fee)
        fee_gst_total += gst
        add("03. AMAZON FEES (ex-GST)", "Multi-Channel Fulfilment (other channels)", ex,
            f"{U.get('units_mcf', 0)} units Amazon shipped for orders placed elsewhere - "
            "real cash, but it belongs to that channel, not to Amazon 3P")
    for k, v in F.items():
        if k in used or k.startswith(("SERVICE:", "REFUND:", "MCF:")) or not v:
            continue
        ex, gst = _split_gst(v)
        fee_gst_total += gst
        add("03. AMAZON FEES (ex-GST)", k + " (unmapped)", ex, "CHECK ME")

    add("04. WE FUNDED", "Coupons / promotions", B.get("promo_funded", 0), "")
    add("04. WE FUNDED", "No-cost EMI / bank offers", B.get("affordability", 0), "")
    # Amazon Ads bills GST-INCLUSIVE (operator confirmed 10/09/26), so the 18%
    # inside the spend is input tax credit, not cost - same treatment as fee GST.
    ads_ex, ads_gst = _split_gst(spend)
    add("04. WE FUNDED", "Advertising (billed with GST)", -spend,
        "what Amazon Ads actually charged us, GST included")
    add("04. WE FUNDED", "  of which GST we get back as credit", ads_gst,
        "reclaimed below - the real cost of ads is the ex-GST figure")
    add("04. WE FUNDED", "  real cost of advertising (ex-GST)", -ads_ex,
        f"{ads_ex / B['sales_ex_gst'] * 100:.1f}% of sales" if B.get("sales_ex_gst") else "")

    credits = 0.0
    for k, v in B.items():
        if k.startswith("adj_"):
            credits += v
            note = ("fee credit - carries GST, reverses ITC" if "ommission" in k
                    else "compensation - no GST")
            add("05. CREDITS", k.replace("adj_", "").replace("_", " ").title(), v, note)

    out_gst = B.get("gst_collected", 0)
    ref_gst = B.get("refund_gst", 0)
    tcs = B.get("tcs_withheld", 0) + B.get("tcs_reversed", 0)
    tds = B.get("tax_withheld", 0)
    add("06. GST AND TAX (not profit)", "Output GST collected from customers", out_gst,
        "held in trust - never ours")
    add("06. GST AND TAX (not profit)", "Output GST reversed on refunds", ref_gst, "")
    add("06. GST AND TAX (not profit)", "ITC on Amazon fees (reclaimable)", -fee_gst_total,
        "tie to Amazon's tax invoice AND GSTR-2B")
    emi_gst = B.get("affordability_gst", 0)
    add("06. GST AND TAX (not profit)", "ITC on no-cost EMI (reclaimable)", -emi_gst,
        "the EMI expense Amazon bills carries GST as well")
    add("06. GST AND TAX (not profit)", "ITC on advertising (reclaimable)", ads_gst,
        "ad invoices carry GST too - claim it in GSTR-2B or you pay 18% twice")
    add("06. GST AND TAX (not profit)", "TCS withheld u/s 52 (0.5% of net sales)", tcs,
        "PREPAYMENT ASSET - accept monthly in the GST portal or it is stranded")
    add("06. GST AND TAX (not profit)", "TDS withheld u/s 194-O (0.1% of gross)", tds,
        "RECOVERABLE in ITR - charged on gross, so returns are a permanent drag")
    # INPUT TAX CREDIT REDUCES what we remit (operator caught this 10/09/26 —
    # the sign was inverted, overstating the GST bill by 2x the ITC = Rs616,196
    # on August). fee_gst_total and tcs are already negative.
    net_gst_payable = out_gst + ref_gst + fee_gst_total + tcs - ads_gst + emi_gst
    add("06. GST AND TAX (not profit)", "Net GST still to remit in cash", -net_gst_payable, "")

    # THE FULL WALK — every component, in order, so the total can be followed.
    sales_ex = B.get("sales_ex_gst", 0)
    ship_col = B.get("shipping_giftwrap_collected", 0)
    ref_prin = B.get("refund_principal", 0)
    ref_oth = B.get("refund_other", 0)
    fees_incl = sum(v for k, v in F.items() if not k.startswith("REFUND:"))
    fee_back = sum(v for k, v in F.items() if k.startswith("REFUND:"))
    promo = B.get("promo_funded", 0)
    afford = B.get("affordability", 0)

    add("07. HOW THE TOTAL IS BUILT", "Product sales (GST not included)", sales_ex, "what we sold")
    add("07. HOW THE TOTAL IS BUILT", "plus GST collected from customers", out_gst,
        "comes in with the sale, goes out to the government")
    add("07. HOW THE TOTAL IS BUILT", "plus shipping and gift wrap", ship_col, "")
    add("07. HOW THE TOTAL IS BUILT", "less refunds to customers", ref_prin + ref_gst + ref_oth,
        "sale value + GST returned")
    add("07. HOW THE TOTAL IS BUILT", "plus fees Amazon returned on refunds", fee_back,
        "commission and closing come back; FBA fee does not")
    add("07. HOW THE TOTAL IS BUILT", "less Amazon fees (including GST on fees)", fees_incl,
        "commission, FBA, closing, storage, removals")
    add("07. HOW THE TOTAL IS BUILT", "less coupons and promotions", promo, "")
    add("07. HOW THE TOTAL IS BUILT", "less no-cost EMI and bank offers", afford, "")
    add("07. HOW THE TOTAL IS BUILT", "plus reimbursements and corrections", credits, "")
    add("07. HOW THE TOTAL IS BUILT", "less TCS withheld", tcs, "you reclaim this in GST")
    add("07. HOW THE TOTAL IS BUILT", "less TDS withheld", tds, "you reclaim this in your ITR")

    settle = (sales_ex + out_gst + ship_col + credits + ref_prin + ref_gst + ref_oth
              + fees_incl + fee_back + promo + afford + tcs + tds)
    add("08. BOTTOM LINE", "AMAZON SHOULD PAY US", settle,
        "add up everything above - this is the settlement")
    add("08. BOTTOM LINE", "less GST we owe the government", -net_gst_payable,
        "output GST, less refunds, less credit on fee GST and ad GST, less TCS withheld")
    add("08. BOTTOM LINE", "MARKETPLACE CONTRIBUTION BEFORE COGS", settle - net_gst_payable,
        "what the marketplace actually left us")
    add("08. BOTTOM LINE", "less advertising (GST-inclusive cash)", -spend,
        "billed separately by Amazon Ads; its GST is credited back in the line above")
    after_ads = settle - net_gst_payable - spend
    add("08. BOTTOM LINE", "AFTER ADVERTISING, BEFORE COGS", after_ads,
        "the landed cost of the goods still has to come off")

    add("08. BOTTOM LINE", "add back TDS withheld (recoverable in your ITR)", -tds,
        "reduces the cash Amazon sends, but it is not a cost")
    after_ads = after_ads - tds

    if cogs:
        refunded = U.get("units_refunded", 0)
        recovered = refunded * (cogs["sellable_pct"] / 100) * cogs["avg"] * cogs["recovery_factor"]
        net_cogs = cogs["gross"] - recovered
        add("09. COST OF GOODS", "Landed cost of units shipped", -cogs["gross"],
            f"{cogs['units_all']:.0f} units at Rs {cogs['avg']:,.0f} average "
            f"({cogs['coverage']:.0f}% priced from the {cogs['brand']} master, rest at that average)")
        add("09. COST OF GOODS", "less stock recovered from returns", recovered,
            f"{refunded} refunds, {cogs['sellable_pct']:.0f}% came back sellable, "
            f"valued at {cogs['recovery_factor'] * 100:.0f}% (repack + restarted storage clock)")
        add("09. COST OF GOODS", "NET COST OF GOODS", -net_cogs, "")
        # HONEST LABELLING: this is the Amazon channel's contribution, NOT company
        # net profit - salaries, warehousing, software, interest and tax still come
        # off. A CFO acting on a line called "net profit" would read it wrong.
        pl = after_ads - net_cogs
        _rev = B.get("sales_ex_gst", 0)
        pc = lambda v: (f"{v / _rev * 100:.1f}% of sales" if _rev else "")
        add("10. PROFIT", "AMAZON CHANNEL CONTRIBUTION", pl,
            pc(pl) + " - before company overhead, interest and tax")
        # BASIS MATTERS: the margin calculator charges overhead and finance as a
        # % of NLC (landed cost), not of sales - the master's own computed
        # columns prove it (NLC 1,259.86 -> overhead 62.99 = exactly 5%). Using
        # sales here would have charged Rs9.30L where the tool charges Rs4.73L
        # and made this statement disagree with the calculator it feeds from.
        oh = cogs["gross"] * cogs.get("overhead_pct", 0) / 100
        fin = cogs["gross"] * cogs.get("finance_pct", 0) / 100
        if oh or fin:
            add("10. PROFIT", "less business overhead", -oh,
                f"{cogs['overhead_pct']:.0f}% of landed cost - same basis as the margin calculator")
            add("10. PROFIT", "less cost of finance", -fin,
                f"{cogs['finance_pct']:.0f}% of landed cost - working capital tied up in stock")
            pbt = pl - oh - fin
            tax = pbt * 0.25168 if pbt > 0 else 0.0
            add("10. PROFIT", "PROFIT BEFORE TAX", pbt, pc(pbt))
            add("10. PROFIT", "less income tax provision", -tax,
                "25.17% under section 115BAA")
            add("10. PROFIT", "PROFIT AFTER TAX", pbt - tax, pc(pbt - tax))

    for k, v in res["unclassified"].items():
        add("11. NOT CLASSIFIED", k, v, "money not bucketed - investigate")
    add("12. COMPLETENESS", "Event lists Amazon returned with data", len(res["event_counts"]),
        ", ".join(sorted(res["event_counts"])))
    add("12. COMPLETENESS", "Event lists returned EMPTY",
        res.get("lists_returned", 0) - len(res["event_counts"]),
        "incl. ShipmentSettleEventList - must stay empty or revenue double-counts")
    for _k, _v in (res.get("leak") or {}).items():
        add("12. COMPLETENESS", f"Money in {_k} we did not classify", _v,
            "MUST BE ZERO - this is measured, not asserted" if abs(_v) < 0.5
            else "LEAK - investigate before trusting the total")
    return pd.DataFrame(rows)




def summary_sheet(df: pd.DataFrame, res: dict, brand_hint: str) -> pd.DataFrame:
    """One page anyone can read: what came in, what Amazon took, what is left."""
    val = lambda item: float(df.loc[df["Item"] == item, "Amount"].sum())
    sec = lambda name: float(df.loc[df["Section"] == name, "Amount"].sum())
    B, U = res["buckets"], res["units"]
    sales = val("Product sales (GST NOT included)")
    pct = lambda v: (abs(v) / sales * 100) if sales else 0

    fees = df[df["Section"] == "03. AMAZON FEES (ex-GST)"]["Amount"].sum()
    funded = df[df["Section"] == "04. WE FUNDED"]["Amount"].sum()
    credits = df[df["Section"] == "05. CREDITS"]["Amount"].sum()
    refunds = val("Sale value refunded (GST NOT included)")
    ref_fee_back = df[(df["Section"] == "02. RETURNS") &
                      (df["Item"].str.contains("on refunds", na=False))]["Amount"].sum()
    ads = val("Advertising (billed with GST)")
    tcs = val("TCS withheld u/s 52 (0.5% of net sales)")
    tds = val("TDS withheld u/s 194-O (0.1% of gross)")
    net_gst = val("Net GST still to remit in cash")
    settle = val("AMAZON SHOULD PAY US")

    rows = [
        ("WHAT WE SOLD", "", "", ""),
        ("  Units ordered", val("Units ordered in the month"), "", "orders placed this month"),
        ("  of which cancelled", val("  less cancelled"), "", "the real cancellation rate"),
        ("  of which not shipped by month end", val("  less not shipped by month end"), "", "ships next month - timing, not lost"),
        ("  Units Amazon shipped and paid on", val("Units shipped (what Amazon paid on)"), "", "the financial basis"),
        ("  Product sales - GST NOT included", sales, "100%", "our revenue; every % below is against this"),
        ("  GST collected from customers", val("Output GST collected from customers"), f"{pct(val('Output GST collected from customers')):.1f}%", "held in trust - never ours"),
        ("  What customers actually paid", sales + val("Output GST collected from customers"), "", "sales + GST = the money that came in"),
        ("", "", "", ""),
        ("WHAT CAME BACK", "", "", ""),
        ("  Units refunded", val("Units refunded"), f"{val('Return rate % (in-month, mixed cohorts)'):.1f}%", "of units shipped"),
        ("  Sale value refunded", refunds, f"{pct(refunds):.1f}%", "of sales"),
        ("  Fees Amazon gave back on those refunds", ref_fee_back, "", "commission + closing returned; FBA fee never is"),
        ("", "", "", ""),
        ("WHAT AMAZON DEDUCTED (before GST on fees)", "", "", ""),
        ("  Commission (referral)", val("Commission (referral)"), f"{pct(val('Commission (referral)')):.1f}%", ""),
        ("  FBA fulfilment (pick, pack, deliver)", val("FBA fulfilment"), f"{pct(val('FBA fulfilment')):.1f}%", ""),
        ("  Closing fee", val("Closing fee"), f"{pct(val('Closing fee')):.1f}%", ""),
        ("  Storage", val("FBAStorageFee"), f"{pct(val('FBAStorageFee')):.1f}%", ""),
        ("  Long-term storage (aged stock)", val("FBALongTermStorageFee"), f"{pct(val('FBALongTermStorageFee')):.1f}%", "stock sitting too long"),
        ("  Removals and other service fees",
         val("FBARemovalFee") + val("MFNPostageFee") + val("Other selling fees"), "", ""),
        ("  TOTAL AMAZON FEES", fees, f"{pct(fees):.1f}%", "of sales, before GST on fees"),
        ("", "", "", ""),
        ("WHAT WE PAID FOR OURSELVES", "", "", ""),
        ("  Coupons and promotions", val("Coupons / promotions"), f"{pct(val('Coupons / promotions')):.1f}%", ""),
        ("  No-cost EMI / bank offers", val("No-cost EMI / bank offers"), f"{pct(val('No-cost EMI / bank offers')):.1f}%", ""),
        ("  Advertising", ads, f"{pct(ads):.1f}%", "billed separately, not in settlement"),
        ("", "", "", ""),
        ("WHAT AMAZON PAID BACK", credits, f"{pct(credits):.1f}%", "reimbursements for lost/damaged stock, fee corrections"),
        ("", "", "", ""),
        ("TAX HELD BACK (not a cost - you get it back)", "", "", ""),
        ("  TCS at 0.5% of net sales", tcs, "", "goes to your GST cash ledger - ACCEPT IT MONTHLY on the portal"),
        ("  TDS at 0.1% of gross sales", tds, "", "claim in your income tax return"),
        ("", "", "", ""),
        ("THE BOTTOM LINE", "", "", ""),
        ("  Amazon should pay us", settle, "", "cash, including the GST we must pass on"),
        ("  less GST we owe the government", net_gst, "", "output GST less refunds, less credit on fee GST, less TCS"),
        ("  CONTRIBUTION BEFORE COST OF GOODS", settle + net_gst, f"{pct(settle + net_gst):.1f}%", "of sales"),
        ("  After advertising", settle + net_gst + ads, f"{pct(settle + net_gst + ads):.1f}%", "before cost of goods"),
        ("  less cost of goods (net of returns recovered)", val("NET COST OF GOODS"), f"{pct(val('NET COST OF GOODS')):.1f}%", "landed cost from the margin master"),
        ("  AMAZON CHANNEL CONTRIBUTION", val("AMAZON CHANNEL CONTRIBUTION"), f"{pct(val('AMAZON CHANNEL CONTRIBUTION')):.1f}%", "before company overhead, interest and tax"),
        ("  less business overhead and finance", val("less business overhead") + val("less cost of finance"), "", "rates from the margin master"),
        ("  PROFIT BEFORE TAX", val("PROFIT BEFORE TAX"), f"{pct(val('PROFIT BEFORE TAX')):.1f}%", ""),
        ("  PROFIT AFTER TAX", val("PROFIT AFTER TAX"), f"{pct(val('PROFIT AFTER TAX')):.1f}%", "Amazon 3P only, this month"),
        ("", "", "", ""),
        ("PROOF THIS IS COMPLETE", "", "", ""),
        ("  Event types Amazon reported", val("Event lists Amazon returned with data"), "", "every one classified above"),
        ("  Event types that were empty", val("Event lists returned EMPTY"), "", "nothing ignored"),
        ("  Money in Amazon's events we could not classify",
         round(sum((res.get("leak") or {}).values()), 2), "",
         "measured leaf-by-leaf against the payload, not asserted"),
    ]
    return pd.DataFrame(rows, columns=["Item", "Amount", "% of sales", "What it means"])


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
    print("  All-Orders bridge (ordered -> shipped)...")
    ob = orders_bridge(args.account, month)
    cg = cogs_for(args.account, month)
    df = report(res, brand, ob, cg)
    # Also append to a long-format snapshot the dashboard serves, so the UI
    # never has to re-hit the API (a full month is ~56 paginated calls).
    snap = ROOT / "data" / "processed" / "reconciliation_snapshot.csv"
    keep = df.copy()
    keep.insert(0, "account", args.account)
    keep.insert(1, "month", month)
    keep["pulled_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    if snap.exists():
        old_df = pd.read_csv(snap, dtype={"month": str})
        old_df = old_df[~((old_df["account"] == args.account) & (old_df["month"] == month))]
        keep = pd.concat([old_df, keep], ignore_index=True)
    keep.to_csv(snap, index=False)
    print(f"-> {snap.name} ({len(keep)} rows across all accounts/months)")

    out = ROOT / "data" / "processed" / f"reconciliation_{args.account}_{month}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        summary_sheet(df, res, brand).to_excel(xw, "Summary", index=False)
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
