"""1P (vendor) reconciliation for the Cocoblu profile — Audio Array + Tonor.

Why this is a different animal from the 3P one
----------------------------------------------
There is no financialEvents for vendors. Amazon does not hand a seller-style
settlement; it issues purchase orders, receives goods, and pays against its OWN
count of what arrived. So the 1P chain has to be built from the PO funnel:

    ORDERED -> ACCEPTED (we confirmed) -> RECEIVED (Amazon scanned it in)

and the money leak is the gap between what we accepted and what Amazon says it
received. That difference is a shortage, and it is paid at netCost per unit or
not at all. Nobody is told when it happens.

What this can and cannot prove
------------------------------
CAN: every PO line, at netCost, from order to receipt, per brand, with the
unreceived value aged so that a PO raised last week is never called a loss.
CANNOT: tie to cash. Vendor remittance lives in Vendor Central > Payments and
is not exposed by SP-API at all (directFulfillment/payments is 403 here and
would be the wrong programme anyway). The cash leg needs a remittance export.
That limit is stated on the face of the report rather than glossed.

    python scripts/sp_vendor_reconciliation.py --month 2026-08
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
HOST = "https://sellingpartnerapi-eu.amazon.com"
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"

# A PO raised days ago that Amazon has not booked in yet is IN TRANSIT, not a
# shortage. Only lines older than this are treated as settled enough to call a
# gap real. Both buckets are always reported, so the choice stays visible.
SHORTAGE_AGE_DAYS = 30


def _token(env_key: str) -> str:
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": os.environ[env_key],
        "client_id": os.environ["SP_LWA_CLIENT_ID"],
        "client_secret": os.environ["SP_LWA_CLIENT_SECRET"]}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _qty(node) -> float:
    """Amazon nests quantity as {amount, unitOfMeasure, unitSize}. unitSize is
    the number of eaches in the unit, so a case of 6 is 2 x 6 = 12 eaches; not
    multiplying it undercounts every case-packed line."""
    if not isinstance(node, dict):
        return 0.0
    a = node.get("amount")
    if a is None:
        return 0.0
    try:
        return float(a) * float(node.get("unitSize") or 1)
    except (TypeError, ValueError):
        return 0.0


def _amt(node) -> float:
    try:
        return float((node or {}).get("amount") or 0)
    except (TypeError, ValueError):
        return 0.0


def pull_po_status(token: str, start: str, end: str) -> list[dict]:
    """Every PO line with its order / accept / receive counts."""
    H = {"x-amz-access-token": token}
    rows, nxt, pages = [], None, 0
    while True:
        params = ({"nextToken": nxt} if nxt else
                  {"createdAfter": start, "createdBefore": end, "limit": 100})
        r = requests.get(f"{HOST}/vendor/orders/v1/purchaseOrdersStatus",
                         params=params, headers=H, timeout=60)
        if r.status_code == 429:
            time.sleep(3)
            continue
        r.raise_for_status()
        pay = r.json().get("payload", {})
        pages += 1
        for o in pay.get("ordersStatus") or []:
            po_date = pd.to_datetime(o.get("purchaseOrderDate"), errors="coerce", utc=True)
            for it in o.get("itemStatus") or []:
                ack = it.get("acknowledgementStatus") or {}
                rec = it.get("receivingStatus") or {}
                rows.append({
                    "po": o.get("purchaseOrderNumber"),
                    "po_status": o.get("purchaseOrderStatus"),
                    "po_date": po_date,
                    "ship_to": (o.get("shipToParty") or {}).get("partyId"),
                    "asin": it.get("buyerProductIdentifier"),
                    "net_cost": _amt(it.get("netCost")),
                    "ordered": _qty((it.get("orderedQuantity") or {}).get("orderedQuantity")),
                    "accepted": _qty(ack.get("acceptedQuantity")),
                    "rejected": _qty(ack.get("rejectedQuantity")),
                    "confirm": ack.get("confirmationStatus") or "",
                    "received": _qty(rec.get("receivedQuantity")),
                    "receive_status": rec.get("receiveStatus") or "",
                })
        nxt = (pay.get("pagination") or {}).get("nextToken")
        if not nxt:
            break
        time.sleep(0.5)
    print(f"  {pages} pages, {len(rows)} PO lines")
    return rows


def brand_map() -> dict:
    if not MASTER.exists():
        return {}
    m = pd.read_excel(MASTER)
    up = lambda s: s.astype(str).str.upper().str.strip()
    return dict(zip(up(m["ASIN"]), m["Brand"].astype(str)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default=None, help="YYYY-MM (default: last full month)")
    ap.add_argument("--token-env", default="SP_API_VENDOR_REFRESH_TOKEN_AUDIOARRAY")
    ap.add_argument("--label", default="AUDIOARRAY_1P")
    args = ap.parse_args()
    load_dotenv(ROOT / ".env")

    last = date.today().replace(day=1) - timedelta(days=1)
    month = args.month or f"{last.year}-{last.month:02d}"
    y, m = map(int, month.split("-"))
    start = f"{date(y, m, 1)}T00:00:00Z"
    end = f"{date(y + (m == 12), (m % 12) + 1, 1)}T00:00:00Z"

    print(f"=== {args.label} vendor reconciliation {month} ===")
    print("  purchase orders...")
    rows = pull_po_status(_token(args.token_env), start, end)
    if not rows:
        raise SystemExit("no PO lines in the window")
    d = pd.DataFrame(rows)
    d["brand"] = d["asin"].astype(str).str.upper().str.strip().map(brand_map())
    d["brand"] = d["brand"].fillna("(unmapped)")

    # Value every leg at the PO's own netCost - that is the price Amazon
    # agreed to pay us, so it is the only honest way to price a shortage.
    for leg in ("ordered", "accepted", "rejected", "received"):
        d[f"{leg}_val"] = d[leg] * d["net_cost"]
    d["short"] = (d["accepted"] - d["received"]).clip(lower=0)
    d["short_val"] = d["short"] * d["net_cost"]
    # Amazon sometimes books MORE than it accepted. Track it: it offsets the
    # gap in aggregate and a line-level count that never goes negative would
    # quietly hide it.
    d["over"] = (d["received"] - d["accepted"]).clip(lower=0)
    d["over_val"] = d["over"] * d["net_cost"]

    now = pd.Timestamp.utcnow()
    d["age_days"] = (now - d["po_date"]).dt.days
    d["settled"] = d["age_days"] >= SHORTAGE_AGE_DAYS

    tot = lambda c, f=None: float((d if f is None else d[f])[c].sum())
    print()
    print(f"  PO FUNNEL ({len(d)} lines across {d['po'].nunique()} POs)")
    print(f"    ordered              {tot('ordered'):>10,.0f} u   Rs {tot('ordered_val'):>14,.0f}")
    print(f"    rejected by us       {tot('rejected'):>10,.0f} u   Rs {tot('rejected_val'):>14,.0f}")
    print(f"    accepted             {tot('accepted'):>10,.0f} u   Rs {tot('accepted_val'):>14,.0f}")
    print(f"    received by Amazon   {tot('received'):>10,.0f} u   Rs {tot('received_val'):>14,.0f}")
    print(f"    accepted NOT received{tot('short'):>10,.0f} u   Rs {tot('short_val'):>14,.0f}")
    st, un = d["settled"], ~d["settled"]
    # DO NOT call this a shortage. "Accepted but not received" means one of:
    # we never shipped it, we shipped it and Amazon has not booked it, or
    # Amazon received less than we sent. Only the last two are money owed, and
    # telling them apart needs the ASN / shipment confirmations we do not have
    # here. Naming it honestly is the difference between a finding and a guess.
    print(f"      aged >= {SHORTAGE_AGE_DAYS}d - unfulfilled OR unbooked "
          f"{tot('short', st):>8,.0f} u   Rs {tot('short_val', st):>12,.0f}")
    print(f"      recent - still in the pipeline     "
          f"{tot('short', un):>8,.0f} u   Rs {tot('short_val', un):>12,.0f}")
    if tot("over"):
        print(f"    received MORE than accepted"
              f"{tot('over'):>7,.0f} u   Rs {tot('over_val'):>14,.0f}  "
              "(offsets the gap above)")

    print()
    print("  BY BRAND (accepted vs received, settled POs only)")
    g = (d[st].groupby("brand")
         .agg(lines=("po", "size"), accepted=("accepted", "sum"),
              received=("received", "sum"), accepted_val=("accepted_val", "sum"),
              short_val=("short_val", "sum")))
    g["fill_%"] = (g["received"] / g["accepted"] * 100).round(1)
    print(g.to_string())

    print()
    print("  RECEIVE STATUS MIX")
    print(d.groupby("receive_status").agg(lines=("po", "size"),
                                          units=("accepted", "sum")).to_string())

    # ── snapshot the page reads ─────────────────────────────────────────
    # Long format, one row per (scope, metric), so /1p-reconciliation never
    # has to re-hit Amazon. `scope` is ALL or a brand name; the page needs
    # units AND value for every step, which is why this is not the 3P
    # reconciliation's Section/Item shape.
    def _row(scope, metric, units, value):
        return {"label": args.label, "month": month, "scope": scope,
                "metric": metric, "units": round(float(units), 2),
                "value": round(float(value), 2)}

    snap_rows = [
        _row("ALL", "asked", tot("ordered"), tot("ordered_val")),
        _row("ALL", "could_not_send", tot("rejected"), tot("rejected_val")),
        _row("ALL", "promised", tot("accepted"), tot("accepted_val")),
        _row("ALL", "arrived", tot("received"), tot("received_val")),
        _row("ALL", "not_arrived", tot("short"), tot("short_val")),
        _row("ALL", "travelling", tot("short", un), tot("short_val", un)),
        _row("ALL", "missing", tot("short", st), tot("short_val", st)),
        _row("ALL", "extra", tot("over"), tot("over_val")),
        _row("ALL", "pos", d["po"].nunique(), len(d)),
    ]
    for b, r in g.iterrows():
        snap_rows += [
            _row(b, "promised", r["accepted"], r["accepted_val"]),
            _row(b, "arrived", r["received"], r["accepted_val"] - r["short_val"]),
            _row(b, "missing", r["accepted"] - r["received"], r["short_val"]),
        ]
    snap = ROOT / "data" / "processed" / "vendor_reconciliation_snapshot.csv"
    keep = pd.DataFrame(snap_rows)
    keep["pulled_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    keep["shortage_age_days"] = SHORTAGE_AGE_DAYS
    if snap.exists():
        old = pd.read_csv(snap, dtype={"month": str})
        old = old[~((old["label"] == args.label) & (old["month"] == month))]
        keep = pd.concat([old, keep], ignore_index=True)
    keep.to_csv(snap, index=False)
    print(f"-> {snap.name} ({len(keep)} rows)")

    out = ROOT / "data" / "processed" / f"vendor_reconciliation_{args.label}_{month}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        d.drop(columns=["po_date"]).assign(
            po_date=d["po_date"].dt.strftime("%Y-%m-%d")).to_excel(xw, "PO lines", index=False)
        g.reset_index().to_excel(xw, "By brand", index=False)
        short = (d[st & (d["short"] > 0)]
                 .groupby(["brand", "asin", "po"])
                 .agg(accepted=("accepted", "sum"), received=("received", "sum"),
                      short=("short", "sum"), short_val=("short_val", "sum"))
                 .sort_values("short_val", ascending=False).reset_index())
        short.to_excel(xw, "Shortages", index=False)
    print(f"\n-> {out.name}")
    print("\n  NOT COVERED: what Amazon actually PAID. Vendor remittance is not in")
    print("  SP-API at all - it lives in Vendor Central > Payments. Everything above")
    print("  is what Amazon OWES on its own receipt count, not what landed in the bank.")


if __name__ == "__main__":
    main()
