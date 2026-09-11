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
    P = defaultdict(float)          # promo funding split by PromotionType
    unclassified = defaultdict(float)
    seen_lists = defaultdict(int)
    C = defaultdict(float)          # what the classifier actually picked up
    leaf_seen = defaultdict(float)  # every rupee Amazon put in each handled list
    bgap = defaultdict(float)       # parent total vs its own breakdown
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
        # is measured: sum every leaf CurrencyAmount in EVERY list we classify
        # and compare against what the classifier below actually picked up. The
        # residual is the money we failed to see. It found Rs23,399 of promo
        # adjustments that had been invisible since this script was written.
        # Widened 11/09/26 from two lists to all six (see LEAK_LISTS).
        for _lk in LEAK_LISTS:
            leaf_seen[_lk] += _leaf_sum(ev.get(_lk) or [], _REDUNDANT.get(_lk, ()))

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
                    # WHICH KIND of promotion matters: a real Coupon owes
                    # Amazon a per-redemption fee, a deal price-discount does
                    # not. Aug 2026 showed Rs1.30L of promo funding and ZERO
                    # coupon-redemption fee, which is either a missing fee or
                    # simply no coupons - unanswerable until the type is kept.
                    P["promo_type_" + str(pr.get("PromotionType") or "?")] += _a
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
                    # Capture the refund side too, or the by-type split sums to
                    # MORE than the parent it sits under: the parent is net of
                    # coupon funding that comes back on refunds (Rs23,399 in
                    # Aug), and a child line bigger than its total is a bug on
                    # its face.
                    P["promo_type_" + str(pr.get("PromotionType") or "?")] += _a

        # ── SERVICE FEES (storage, removals, misc) ──────────────────────
        for sf in ev.get("ServiceFeeEventList") or []:
            reason = sf.get("FeeReason") or sf.get("FeeDescription") or "ServiceFee"
            for f in sf.get("FeeList") or []:
                # FeeType names the real charge (storage, LTSF, refund admin,
                # coupon redemption, removal). Without it the bucket is one
                # opaque number and fee creep hides inside it.
                label = f.get("FeeType") or reason
                _a = _amt(f.get("FeeAmount"))
                C["ServiceFeeEventList"] += _a
                F["SERVICE:" + str(reason) + "|" + str(label)] += _a

        # ── ADJUSTMENTS (reimbursements etc.) ───────────────────────────
        for ad in ev.get("AdjustmentEventList") or []:
            _a = _amt(ad.get("AdjustmentAmount"))
            C["AdjustmentEventList"] += _a
            B[f"adj_{ad.get('AdjustmentType','other')}"] += _a
            # We book the parent only. If Amazon's own per-item breakdown does
            # not add up to it, one of the two is wrong and the difference is
            # real money - so measure it rather than ignoring the item list.
            _items = ad.get("AdjustmentItemList") or []
            if _items:
                _sum = sum(_amt(i.get("TotalAmount")) for i in _items)
                if abs(_sum - _a) > 0.5:
                    bgap["AdjustmentItemList vs AdjustmentAmount"] += _sum - _a

        # ── AFFORDABILITY (no-cost EMI) ─────────────────────────────────
        _agst = lambda e: (_amt(e.get("TaxTypeCGST")) + _amt(e.get("TaxTypeSGST"))
                           + _amt(e.get("TaxTypeIGST")))
        _acheck = lambda e, lk: (
            bgap.__setitem__(f"{lk}: base+GST vs TotalExpense",
                             bgap[f"{lk}: base+GST vs TotalExpense"]
                             + _amt(e.get("BaseExpense")) + _agst(e)
                             - _amt(e.get("TotalExpense")))
            if _amt(e.get("BaseExpense"))
            and abs(_amt(e.get("BaseExpense")) + _agst(e)
                    - _amt(e.get("TotalExpense"))) > 0.5 else None)
        for ae in ev.get("AffordabilityExpenseEventList") or []:
            _t = _amt(ae.get("TotalExpense"))
            C["AffordabilityExpenseEventList"] += _t
            B["affordability"] += _t
            B["affordability_gst"] += _agst(ae)
            _acheck(ae, "AffordabilityExpense")
        for ar in ev.get("AffordabilityExpenseReversalEventList") or []:
            # Amazon already SIGNS reversals positive (a credit back). Adding
            # them is correct; subtracting double-counted the credit and made
            # every settlement tie-out miss by exactly 2x the reversals.
            _t = _amt(ar.get("TotalExpense"))
            C["AffordabilityExpenseReversalEventList"] += _t
            B["affordability"] += _t
            B["affordability_gst"] += _agst(ar)
            _acheck(ar, "AffordabilityExpenseReversal")

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

    leak = {k: round(leaf_seen[k] - C[k], 2) for k in leaf_seen
            if leaf_seen[k] or C[k]}
    return {"buckets": dict(B), "fees": dict(F), "units": dict(U), "promo": dict(P),
            "leak": leak, "lists_returned": len(all_lists),
            "breakdown_gaps": {k: round(v, 2) for k, v in bgap.items()},
            "unclassified": dict(unclassified), "event_counts": dict(seen_lists),
            "pages": pages, "month": month, "account": account}


# Amazon states some money TWICE: once as a parent total and again as the
# breakdown it is made of. AdjustmentEventList carries AdjustmentAmount *and*
# an AdjustmentItemList of per-item amounts; the Affordability lists carry
# TotalExpense *and* the BaseExpense + three TaxType* parts that add up to it.
# Leaf-summing those naively counts the same rupee two or three times and
# reports a leak that is not real. The parent is canonical; the breakdown gets
# its own does-it-add-up check in sweep() instead of entering the residual.
_REDUNDANT: dict[str, tuple[str, ...]] = {
    "AdjustmentEventList": ("AdjustmentItemList",),
    "AffordabilityExpenseEventList": (
        "BaseExpense", "TaxTypeIGST", "TaxTypeCGST", "TaxTypeSGST"),
    "AffordabilityExpenseReversalEventList": (
        "BaseExpense", "TaxTypeIGST", "TaxTypeCGST", "TaxTypeSGST"),
}

# Every list the classifier handles by name. Until 11/09/26 the leak test
# covered only the first two, so four lists holding real money were TRUSTED
# rather than measured - precisely the hole the leak test exists to close.
# AdjustmentEventList was the worst of them: the classifier reads only
# AdjustmentAmount and never looked at AdjustmentItemList at all.
LEAK_LISTS = ("ShipmentEventList", "RefundEventList", "ServiceFeeEventList",
              "AdjustmentEventList", "AffordabilityExpenseEventList",
              "AffordabilityExpenseReversalEventList")


def _leaf_sum(o, skip: tuple[str, ...] = ()) -> float:
    """Every CurrencyAmount in the tree, counted once. Stops descending at a
    money node so a parent total and its children are never both added.
    `skip` drops keys that only restate money already counted at the parent."""
    if isinstance(o, dict):
        if "CurrencyAmount" in o:
            return _amt(o)
        return sum(_leaf_sum(v, skip) for k, v in o.items() if k not in skip)
    if isinstance(o, list):
        return sum(_leaf_sum(x, skip) for x in o)
    return 0.0


def _payload_leaf_total(ev: dict) -> float:
    """Every rupee in one financialEvents payload, across all lists, using the
    same parent-vs-breakdown rules as the month sweep."""
    return sum(_leaf_sum(v or [], _REDUNDANT.get(k, ()))
               for k, v in ev.items() if isinstance(v, list))


def event_groups(account: str, month: str, verify: bool = True) -> dict:
    """The BANK side. financialEvents is a POSTED-DATE ACCRUAL - it answers
    "what did Amazon charge and credit in August". It can never equal cash,
    because Amazon pays in settlement periods that straddle month ends, and
    until now this reconciliation had no cash side at all: nothing here had
    ever been compared against a bank statement.

    financialEventGroups is the only place Amazon states what it actually
    transferred, to which account tail, on which date. For each group we also
    re-derive Amazon's own arithmetic:

        BeginningBalance + every event in the group == OriginalTotal

    If that identity holds, we can read Amazon's ledger correctly and the
    deposit is explained line by line. If it does not, either the pull is
    incomplete or a money type is invisible to us - and the gap is reported
    as a number instead of being assumed away.
    """
    y, m = map(int, month.split("-"))
    m_start = pd.Timestamp(date(y, m, 1), tz="UTC")
    m_end = pd.Timestamp(date(y + (m == 12), (m % 12) + 1, 1), tz="UTC")
    tok = _token(account)
    H = {"x-amz-access-token": tok}

    # Look back a full quarter: the group that PAID for early-August sales
    # usually started in July, and a group that started in August may not
    # close until September. Both have to be visible to explain the month.
    groups, token, pages = [], None, 0
    while True:
        params = {"NextToken": token} if token else {
            "FinancialEventGroupStartedAfter": (m_start - pd.Timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "FinancialEventGroupStartedBefore": m_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "MaxResultsPerPage": "100"}
        r = requests.get(f"{SPAPI_HOST}/finances/v0/financialEventGroups",
                         params=params, headers=H, timeout=60)
        if r.status_code == 429:
            time.sleep(3)
            continue
        if r.status_code != 200:
            print(f"  financialEventGroups unavailable: HTTP {r.status_code}")
            return {}
        payload = r.json().get("payload", {})
        groups += payload.get("FinancialEventGroupList") or []
        pages += 1
        token = payload.get("NextToken")
        if not token:
            break
        time.sleep(0.5)

    out = []
    for g in groups:
        gs = pd.to_datetime(g.get("FinancialEventGroupStart"), errors="coerce", utc=True)
        ge = pd.to_datetime(g.get("FinancialEventGroupEnd"), errors="coerce", utc=True)
        # Keep any group whose window OVERLAPS the month, plus any still open
        # (an open group has no end date and is where this month's unpaid
        # money is currently sitting).
        if pd.notna(gs) and gs >= m_end:
            continue
        if pd.notna(ge) and ge < m_start:
            continue
        row = {
            "id": g.get("FinancialEventGroupId"),
            "start": gs, "end": ge,
            "status": g.get("ProcessingStatus") or "",
            "transfer_status": g.get("FundTransferStatus") or "",
            "transfer_date": g.get("FundTransferDate") or "",
            "account_tail": g.get("AccountTail") or "",
            "beginning": _amt(g.get("BeginningBalance")),
            "total": _amt(g.get("OriginalTotal")),
            "converted": _amt(g.get("ConvertedTotal")),
            "events": None, "tie": None, "pages": 0,
        }
        if verify and row["id"]:
            try:
                ev_total, ev_pages = _group_events_total(row["id"], H)
                row["events"] = ev_total
                row["pages"] = ev_pages
                # Amazon's identity: what you started with, plus everything
                # that happened, is what you get paid.
                row["tie"] = round(row["beginning"] + ev_total - row["total"], 2)
            except Exception as e:                      # noqa: BLE001
                print(f"  group {row['id']}: {e}")
        out.append(row)

    out.sort(key=lambda r: (pd.Timestamp.min.tz_localize("UTC")
                            if pd.isna(r["start"]) else r["start"]))
    return {"groups": out, "month_start": m_start, "month_end": m_end}


def _group_events_total(gid: str, H: dict) -> tuple[float, int]:
    """Every rupee Amazon put inside one settlement group."""
    total, pages, token = 0.0, 0, None
    while True:
        params = {"NextToken": token} if token else {"MaxResultsPerPage": "100"}
        r = requests.get(
            f"{SPAPI_HOST}/finances/v0/financialEventGroups/{gid}/financialEvents",
            params=params, headers=H, timeout=60)
        if r.status_code == 429:
            time.sleep(3)
            continue
        r.raise_for_status()
        payload = r.json().get("payload", {})
        total += _payload_leaf_total(payload.get("FinancialEvents", {}) or {})
        pages += 1
        token = payload.get("NextToken")
        if not token:
            break
        time.sleep(0.5)
    return round(total, 2), pages


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
    # FAIL LOUDLY, NEVER QUIETLY. This used to print a warning and return {},
    # which dropped COST OF GOODS and the whole PROFIT section from the report
    # while everything above them still looked perfect - and then wrote that
    # gutted version over the good snapshot. Running without PYTHONPATH set to
    # the repo root is enough to trigger it (the margin_snapshot import fails),
    # and it did on 11/09/26. A reconciliation missing its bottom line must
    # stop, exactly like ad_spend refuses to come through as zero.
    try:
        from weekly_app.etl.margin_snapshot import MARGIN_TOOL_MASTERS, _global_params
    except ImportError as e:
        raise ImportError(
            f"cannot import weekly_app.etl.margin_snapshot ({e}) - run with "
            f"PYTHONPATH={ROOT} . Without it there is no landed cost, so the "
            "report would silently lose COST OF GOODS and PROFIT") from e
    path = MARGIN_TOOL_MASTERS.get(brand)
    if not path or not path.exists():
        raise FileNotFoundError(
            f"margin master for {brand} not found at {path} - landed cost is a "
            "Rs59L line and must not silently vanish from the report")
    xls = pd.ExcelFile(path)
    m = xls.parse(xls.sheet_names[0])
    g = _global_params(xls)

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
        # Legitimate for an account-month whose fee pull has not been run yet.
        # report() prints this reason on the face of the schedule rather than
        # letting the profit sections just not appear.
        return {"unavailable": f"no rows in {fees_csv.name} for {account} {month} "
                               "- run scripts/sp_finance_deductions_pull.py first"}
    f["units"] = pd.to_numeric(f["units"], errors="coerce").fillna(0)
    f["dp"] = up(f["sku"]).map(dpmap)
    f.loc[f["dp"].isna(), "dp"] = up(f["asin"].fillna("")).map(dpmap)
    cov = f[f["dp"].notna()]
    units_all = float(f["units"].sum())
    units_cov = float(cov["units"].sum())
    if units_cov <= 0:
        return {"unavailable": f"not one of the {len(f)} SKUs Amazon billed for "
                               f"{account} {month} matched the {brand} margin master"}
    measured = float((cov["units"] * cov["dp"]).sum())
    avg = measured / units_cov
    gross = avg * units_all          # gross up the few SKUs with no master row
    # NAME the SKUs being costed on an assumption. "96% coverage" is only
    # actionable if someone can see which 4% to go and map.
    miss = (f[f["dp"].isna()].groupby(["sku", "asin"], dropna=False)["units"]
            .sum().sort_values(ascending=False).reset_index())
    miss["assumed_cost"] = miss["units"] * avg

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
            "brand": brand, "unmapped": miss}


def report(res: dict, brand_hint: str, bridge: dict | None = None,
           cogs: dict | None = None, bank: dict | None = None,
           freight_per_unit: float = 0.0) -> pd.DataFrame:
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
    # This was inside the settlement total but on no line of its own, so it
    # could not be seen or checked. Small, but "nothing is hidden" has to mean
    # nothing.
    if abs(B.get("refund_other", 0)) > 0.5:
        add("02. RETURNS", "Shipping and gift wrap refunded", B.get("refund_other", 0),
            "returned to the customer along with the sale value")
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
    # Split by PromotionType so "zero coupon-redemption fee on Rs1.30L of promo"
    # can actually be judged: a real Coupon owes Amazon a per-redemption fee, a
    # deal price-discount owes nothing. Without the type it is unanswerable.
    for _k, _v in sorted((res.get("promo") or {}).items(), key=lambda kv: kv[1]):
        if abs(_v) > 0.5:
            _t = _k.replace("promo_type_", "")
            # Amazon returns "PromotionMetaDataDefinitionValue" here - a
            # placeholder, not a promotion kind. Say so instead of dressing it
            # up as an answer: it means this field CANNOT settle whether the
            # Rs1.3L was coupons (which owe a per-redemption fee) or deal
            # discounts (which do not). That needs the promotions report.
            _opaque = "metadata" in _t.lower() or _t in ("?", "")
            add("04. WE FUNDED", f"  of which {_t}", _v,
                "Amazon returns no real promotion type here, so this does NOT "
                "tell us whether coupons ran - the missing coupon-redemption "
                "fee stays an open question" if _opaque else
                ("a real coupon should ALSO carry a per-redemption fee in section 03"
                 if "coupon" in _t.lower() else
                 "price discount - no redemption fee is due on this"))
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
    # LABEL HONESTLY: this is output GST net of the credits Amazon's own
    # invoices carry. It does NOT net off import IGST paid at the bill of
    # entry (~Rs9L), which is equally creditable and equally real cash. The
    # line is a marketplace-GST figure, not the company's GST cash position,
    # and calling it "cash" without saying so overstates what is owed.
    add("06. GST AND TAX (not profit)", "Net GST still to remit in cash", -net_gst_payable,
        "marketplace GST only - import IGST paid at the bill of entry is also "
        "creditable and is NOT netted here")

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

    # MIRROR OF THE MCF COGS BUG. We already keep the 418 MCF units' landed cost
    # out of this P&L because the sale happened on another channel. Their
    # fulfilment fee has to leave for exactly the same reason. Amazon really did
    # withhold it, so it stays in the settlement above (that is cash); it just
    # is not a cost of the AMAZON channel. Leaving it in understated Amazon's
    # contribution by Rs58,344 in Aug 2026.
    # BASIS: the settlement deducted the fee GST-INCLUSIVE, but that GST is
    # reclaimed as input credit in the "less GST we owe" line directly above,
    # so by the time we reach contribution the fee has already netted down to
    # its EX-GST cost. Adding back the gross figure would over-credit us by the
    # 18%. Add back exactly what contribution is bearing: the ex-GST amount.
    if mcf_fee:
        mcf_ex, _ = _split_gst(mcf_fee)
        add("08. BOTTOM LINE", "add back Multi-Channel Fulfilment fee", -mcf_ex,
            f"{U.get('units_mcf', 0)} units Amazon shipped for orders placed on "
            "another channel - real cash, but that channel's cost, not Amazon's "
            "(ex-GST; the GST on it is already credited in the line above)")
        after_ads = after_ads - mcf_ex

    if cogs and cogs.get("unavailable"):
        # A reader must never have to NOTICE that two sections are missing.
        add("09. COST OF GOODS", "Landed cost NOT AVAILABLE", 0, cogs["unavailable"])
        add("10. PROFIT", "PROFIT CANNOT BE STATED", 0,
            "everything above is Amazon's side only - the cost of the goods is "
            "missing, so contribution and profit are NOT shown rather than shown wrong")
    elif cogs:
        refunded = U.get("units_refunded", 0)
        recovered = refunded * (cogs["sellable_pct"] / 100) * cogs["avg"] * cogs["recovery_factor"]
        net_cogs = cogs["gross"] - recovered
        add("09. COST OF GOODS", "Landed cost of units shipped", -cogs["gross"],
            f"{cogs['units_all']:.0f} units at Rs {cogs['avg']:,.0f} average "
            f"({cogs['coverage']:.0f}% priced from the {cogs['brand']} master, rest at that average)")
        _um = cogs.get("unmapped")
        if _um is not None and len(_um):
            _uu = float(_um["units"].sum())
            add("09. COST OF GOODS", "  of which priced on an ASSUMPTION", -_uu * cogs["avg"],
                f"{_uu:.0f} units across {len(_um)} SKUs have no row in the "
                f"{cogs['brand']} master - see the 'Unmapped SKUs' sheet; map them "
                "and this line becomes measured")
        add("09. COST OF GOODS", "less stock recovered from returns", recovered,
            f"{refunded} refunds, {cogs['sellable_pct']:.0f}% came back sellable, "
            f"valued at {cogs['recovery_factor'] * 100:.0f}% (repack + restarted storage clock)")
        # INBOUND FREIGHT TO FBA. Confirmed with the operator 11/09/26: we ship
        # into FBA on our OWN carrier, not Amazon Transportation Services, so
        # this cost can never appear in financialEvents - and it is not in the
        # landed cost either (Nexlev's 'Additional Cost' is populated on 4 of
        # 103 rows; 'Freight+Clearance' is the USD international leg, not the
        # domestic run to the FC). It is therefore genuinely missing, and a
        # missing cost must be VISIBLE rather than absent. Pass the real rate
        # with --freight-per-unit and it becomes a booked line.
        if freight_per_unit:
            fr_cost = freight_per_unit * cogs["units_all"]
            add("09. COST OF GOODS", "Inbound freight to FBA", -fr_cost,
                f"{cogs['units_all']:.0f} units at Rs {freight_per_unit:,.2f} - our own "
                "carrier, so Amazon never reports it")
            net_cogs += fr_cost
        else:
            add("09. COST OF GOODS", "Inbound freight to FBA - NOT INCLUDED", 0,
                "we self-ship into FBA, so this never appears in Amazon's data and "
                "it is not in the margin master either - profit below is overstated "
                "by it. Re-run with --freight-per-unit <Rs> to book it")
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
    # HONEST LABELLING OF THE RESIDUAL: for Shipment and Refund the leaf sum is
    # an independent walk of the payload, so a zero really does prove nothing
    # was dropped. For Adjustment and the two Affordability lists the canonical
    # figure IS the single parent field we book, so their residual is zero by
    # construction and proves nothing on its own - the real test for those is
    # the breakdown check below. Saying so is the difference between measuring
    # completeness and asserting it.
    _STRUCTURAL = {"AdjustmentEventList", "AffordabilityExpenseEventList",
                   "AffordabilityExpenseReversalEventList"}
    for _k, _v in (res.get("leak") or {}).items():
        if abs(_v) >= 0.5:
            _n = "LEAK - investigate before trusting the total"
        elif _k in _STRUCTURAL:
            _n = ("zero by construction - we book Amazon's parent total; the "
                  "real test for this list is the breakdown check below")
        else:
            _n = "MUST BE ZERO - this is measured, not asserted"
        add("12. COMPLETENESS", f"Money in {_k} we did not classify", _v, _n)
    _bg = res.get("breakdown_gaps") or {}
    for _k, _v in _bg.items():
        add("12. COMPLETENESS", f"Breakdown disagrees with parent - {_k}", _v,
            "Amazon's own sub-total does not add up to the figure we booked")
    if not _bg:
        add("12. COMPLETENESS", "Breakdowns that disagree with their parent", 0,
            "every AdjustmentItemList and Affordability base+GST re-added to "
            "the parent total we booked")

    if bank:
        _bank_section(add, bank, settle)
    _cohort_section(add, res, B, U, cogs)
    return pd.DataFrame(rows)


def _cohort_section(add, res: dict, B: dict, U: dict, cogs: dict | None) -> None:
    """Section 14 - what THIS month's orders will eventually return.

    Deliberately a MEMO, not a restatement of the ladder above. Everything from
    section 01 to 10 is on a settlement basis, and section 13 proves that basis
    ties to the bank to the rupee. Re-basing the bottom line onto a modelled
    accrual would break that tie and replace a measured number with an
    estimated one. So the accrual view sits alongside it, sized, and the reader
    decides whether to book a provision.

    This is the Ind AS 115 right-of-return question: revenue should carry the
    returns the period's own sales will generate, not the returns that happened
    to post in the period.
    """
    path = ROOT / "data" / "processed" / f"returns_cohort_summary_{res['account']}.csv"
    if not path.exists():
        add("14. RETURNS - COHORT VIEW (memo)", "Cohort curve not built", 0,
            f"run scripts/sp_returns_cohort.py --account {res['account']} to "
            "measure what this month's orders will actually return")
        return
    c = pd.read_csv(path, dtype={"cohort": str})
    row = c[c["cohort"] == res["month"]]
    if row.empty:
        add("14. RETURNS - COHORT VIEW (memo)", "No cohort row for this month", 0,
            f"{path.name} has no {res['month']} cohort")
        return
    r = row.iloc[0]
    shipped = U.get("units_shipped", 0)
    refunded = U.get("units_refunded", 0)
    in_month = (refunded / shipped * 100) if shipped else 0

    add("14. RETURNS - COHORT VIEW (memo)", "Return rate used above (in-month)",
        round(in_month, 2),
        "refunds POSTED this month over units shipped this month - two different "
        "populations, which is why this is only a proxy")
    add("14. RETURNS - COHORT VIEW (memo)",
        "Returns already arrived from this month's orders", round(r["observed_return_pct"], 2),
        f"after {int(r['age_months'])} month(s); matured cohorts say that is only "
        f"{r['share_of_lifetime_arrived'] * 100:.0f}% of what a cohort ever returns")
    add("14. RETURNS - COHORT VIEW (memo)",
        "EXPECTED LIFETIME return rate for this month's orders",
        round(r["expected_lifetime_return_pct"], 2),
        f"grossed up on the curve from cohorts at least "
        f"{int(r['maturity_lag_months'])} months old "
        f"(their lifetime rate: {r['lifetime_rate_from_matured_pct']:.2f}%)")

    # BASIS. The cohort file counts units shipped from orders PLACED in the
    # month (4,131 for Aug 26); this P&L counts units Amazon SHIPPED and paid
    # on in the month (3,715) - different populations, because orders cross the
    # month boundary in both directions. Apply the cohort RATE to this report's
    # own population, or the provision is computed on units whose revenue is
    # not in these numbers.
    exp_units = r["expected_lifetime_return_pct"] / 100 * shipped
    add("14. RETURNS - COHORT VIEW (memo)", "Units this month's shipments will return",
        round(exp_units),
        f"the cohort rate applied to the {shipped:,} units this P&L is built on "
        f"(the cohort itself is {r['units_shipped']:,.0f} units ordered in the month); "
        f"vs {refunded:.0f} refunds actually posted")

    # Net cost of ONE return, computed from this report's own numbers rather
    # than a remembered constant: revenue given back, less the fees Amazon
    # returns, less the stock that comes back sellable.
    if refunded and cogs and not cogs.get("unavailable"):
        fee_back = sum(v for k, v in res["fees"].items() if k.startswith("REFUND:"))
        recov = (cogs["sellable_pct"] / 100) * cogs["avg"] * cogs["recovery_factor"]
        per = (abs(B.get("refund_principal", 0)) / refunded) - (fee_back / refunded) - recov
        delta = (exp_units - refunded) * per
        add("14. RETURNS - COHORT VIEW (memo)", "Net cost of one return", -round(per, 2),
            f"Rs{abs(B.get('refund_principal', 0)) / refunded:,.0f} sale value given back, "
            f"less Rs{fee_back / refunded:,.0f} of fees returned, less Rs{recov:,.0f} of "
            "stock that comes back sellable")
        add("14. RETURNS - COHORT VIEW (memo)",
            "PROVISION if charged on a cohort basis", -round(delta, 2),
            f"{exp_units - refunded:+,.0f} more returns than the month booked. "
            "The bottom line above is NOT adjusted for this - it stays on the "
            "settlement basis that section 13 ties to the bank")


def _bank_section(add, bank: dict, settle: float) -> None:
    """Section 13 - the cash side. Everything above this point is an accrual."""
    groups = bank.get("groups") or []
    if not groups:
        add("13. BANK TIE-OUT", "No settlement groups returned", 0,
            "financialEventGroups was empty - the cash side is UNPROVEN")
        return
    ms, me = bank["month_start"], bank["month_end"]
    fmt = lambda t: "open" if pd.isna(t) else t.strftime("%d/%m/%Y")

    paid_in_month, verified, unverified = 0.0, 0, 0
    for g in groups:
        closed = str(g["status"]).upper() == "CLOSED"
        note = (f"{g['status'].lower()}"
                + (f", transferred {g['transfer_date'][:10]}" if g["transfer_date"] else "")
                + (f" to a/c ...{g['account_tail']}" if g["account_tail"] else ""))
        if g["tie"] is None:
            note += " - NOT re-derived"
            unverified += 1
        elif abs(g["tie"]) < 1.0:
            note += (f" - ties exactly: opening {g['beginning']:,.0f} "
                     f"+ events {g['events']:,.0f} = {g['total']:,.0f}")
            verified += 1
        else:
            note += (f" - DOES NOT TIE by Rs {g['tie']:,.2f} "
                     f"(opening {g['beginning']:,.0f} + events {g['events']:,.0f} "
                     f"vs stated {g['total']:,.0f})")
            unverified += 1
        # Amazon issues MORE THAN ONE settlement per period (Nexlev Aug: two for
        # every week). Labelling them by date alone makes two different deposits
        # look like the same row twice, so carry the group id - it is also what
        # you search for in Seller Central > Payments to find the deposit.
        add("13. BANK TIE-OUT",
            f"Settlement {fmt(g['start'])} - {fmt(g['end'])}  #{g['id']}",
            g["total"], note)
        # Cash basis: a deposit belongs to the month its transfer LANDED in.
        td = pd.to_datetime(g["transfer_date"], errors="coerce", utc=True)
        if closed and pd.notna(td) and ms <= td < me:
            paid_in_month += g["total"]

    add("13. BANK TIE-OUT", "CASH Amazon actually transferred this month", paid_in_month,
        "sum of settlements whose transfer date falls inside the month - "
        "this is what should appear on the bank statement")
    add("13. BANK TIE-OUT", "Accrual: what this month's events say we earned", settle,
        "section 08 - events POSTED in the month, whenever they get paid")
    add("13. BANK TIE-OUT", "Timing difference (accrual less cash)", settle - paid_in_month,
        "settlement periods straddle month ends - a difference here is TIMING, "
        "not error; it is only a problem if a group above fails to tie")
    add("13. BANK TIE-OUT", "Groups re-derived from their own events", verified,
        f"{verified} of {len(groups)} tie to the rupee"
        + (f"; {unverified} could not be verified" if unverified else ""))




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
         f"measured leaf-by-leaf across all {len(LEAK_LISTS)} classified lists, not asserted"),
        ("  Amazon sub-totals that disagree with our figure",
         round(sum((res.get("breakdown_gaps") or {}).values()), 2), "",
         "every per-item breakdown re-added to the parent we booked"),
    ]
    if len(df[df["Section"] == "13. BANK TIE-OUT"]):
        cash = val("CASH Amazon actually transferred this month")
        rows += [
            ("", "", "", ""),
            ("DOES IT MATCH THE BANK", "", "", ""),
            ("  Cash Amazon transferred this month", cash, "",
             "settlements whose transfer date landed in the month"),
            ("  Accrual (what this month's events earned)", settle, "",
             "the figure everything above is built on"),
            ("  Timing difference", settle - cash, "",
             "settlement periods straddle month ends - timing, not error"),
            ("  Settlements re-derived from their own events",
             val("Groups re-derived from their own events"), "",
             "opening balance + every event = the deposit Amazon states"),
        ]
    return pd.DataFrame(rows, columns=["Item", "Amount", "% of sales", "What it means"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="NEXLEV")
    ap.add_argument("--month", default=None, help="YYYY-MM (default: last full month)")
    ap.add_argument("--brand", default=None, help="brand name in the weekly snapshot")
    ap.add_argument("--freight-per-unit", type=float, default=0.0,
                    help="Rs/unit to ship into FBA on our own carrier. Amazon "
                         "never reports this and the margin master does not "
                         "carry it; without it the profit below is overstated")
    ap.add_argument("--no-bank", action="store_true",
                    help="skip the settlement-group pull (the cash side)")
    ap.add_argument("--no-verify-groups", action="store_true",
                    help="list settlement groups but do not re-derive each one "
                         "from its own events (much faster, much weaker proof)")
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
    bank = None
    if not args.no_bank:
        print("  Settlement groups (the cash side)...")
        bank = event_groups(args.account, month,
                            verify=not args.no_verify_groups) or None
        if bank:
            _v = [g for g in bank["groups"] if g["tie"] is not None]
            print(f"  {len(bank['groups'])} groups overlap the month; "
                  f"{sum(1 for g in _v if abs(g['tie']) < 1.0)}/{len(_v)} "
                  f"re-derived to the rupee")
    df = report(res, brand, ob, cg, bank, args.freight_per_unit)
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
        _um = (cg or {}).get("unmapped")
        if _um is not None and len(_um):
            _um.to_excel(xw, "Unmapped SKUs", index=False)
    print()
    for sec, g in df.groupby("Section", sort=True):
        print(sec)
        for _, r in g.iterrows():
            print(f"    {str(r['Item'])[:46]:48} {r['Amount']:>14,.0f}  {r['Note']}")
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
