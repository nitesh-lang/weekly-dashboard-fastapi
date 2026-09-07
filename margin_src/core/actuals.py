"""Realized actuals for the margin calculator, read from the WEEKLY
project's processed snapshots (sanctioned cross-read, same as ams_tacos):

  * asp_for(asin)     — realized ASP on the Amazon 3P channel over the last
                        ~3 months (13 weeks): sum(gross_sales)/sum(units).
                        GROSS basis (incl. GST) so it compares 1:1 with the
                        calculator's BAU SP.
  * returns_for(asin) — actual return rate from returns_snapshot.csv
                        (30d units basis) + how much comes back sellable.

Both loaders cache a small per-ASIN roll-up keyed on file mtime — never the
full frame (512MB box; see the AMS-Trend 502 incident).
"""
from __future__ import annotations

import threading
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
SALES_CSV = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
RETURNS_CSV = ROOT / "data" / "processed" / "returns_snapshot.csv"
STORAGE_CSV = ROOT / "data" / "processed" / "storage_fees_snapshot.csv"

# The weekly 3P pull nets GST out at 18% flat (house rule); we reverse the
# exact same flat factor to get back to the customer-facing price.
GST_RATE = 0.18

_lock = threading.Lock()
_cache: dict = {"sales": (0.0, None), "returns": (0.0, None), "storage": (0.0, None)}


def _sales_agg() -> pd.DataFrame | None:
    if not SALES_CSV.exists():
        return None
    mt = SALES_CSV.stat().st_mtime
    with _lock:
        if _cache["sales"][0] == mt:
            return _cache["sales"][1]
    df = pd.read_csv(SALES_CSV, usecols=["week", "channel", "asin", "sku",
                                         "units_sold", "gross_sales"])
    df = df[df["channel"] == "Amazon"].copy()      # 3P only — same lane the calc models
    df["wn"] = pd.to_numeric(df["week"].astype(str).str.extract(r"(\d+)", expand=False),
                             errors="coerce")
    df = df.dropna(subset=["wn"])
    df["wn"] = df["wn"].astype(int)
    df["asin"] = df["asin"].fillna("").astype(str).str.strip().str.upper()
    df["sku"] = df["sku"].fillna("").astype(str).str.strip().str.upper()
    agg = (df.groupby(["asin", "sku", "wn"], as_index=False)[["units_sold", "gross_sales"]]
             .sum())
    with _lock:
        _cache["sales"] = (mt, agg)
    return agg


def asp_for(asin: str, sku: str = "", weeks: int = 13) -> dict:
    """Match by ASIN when we have one; fall back to SKU for master rows whose
    ASIN column is blank/'-' (they still sell — the snapshot knows them by SKU)."""
    asin = (asin or "").strip().upper()
    sku = (sku or "").strip().upper()
    agg = _sales_agg()
    if agg is None or agg.empty:
        return {"available": False, "reason": "sales snapshot missing"}
    latest = int(agg["wn"].max())          # anchor on the dataset, not the ASIN
    lo = latest - weeks + 1
    if len(asin) == 10:
        mask = agg["asin"] == asin
    elif sku:
        mask = agg["sku"] == sku
    else:
        return {"available": False, "reason": "no ASIN or SKU to match"}
    a = agg[mask & (agg["wn"].between(lo, latest))]
    units = float(a["units_sold"].sum())
    net_sales = float(a["gross_sales"].sum())
    if units <= 0:
        return {"available": False, "reason": f"no Amazon 3P sales W{lo}-W{latest}"}
    # The weekly seller pull stores Amazon 3P sales NET of GST by design
    # (sp_seller_sales_pull.py: net = gross / 1.18). The calculator's SP is
    # GST-INCLUSIVE, so re-gross here — otherwise the ladder strips GST a
    # second time and the "actual" ASP lands ~15% low (operator caught this
    # on SC-01: ₹3,317 net shown where ₹3,914 gross was right).
    net_asp = net_sales / units
    return {"available": True,
            "asp": round(net_asp * (1 + GST_RATE), 2),
            "net_asp": round(net_asp, 2),
            "gst_factor": round(1 + GST_RATE, 2),
            "units": int(units), "net_sales": round(net_sales, 0),
            "weeks_with_sales": int(a["wn"].nunique()),
            "window": f"W{lo}-W{latest}"}


def _returns_agg() -> pd.DataFrame | None:
    if not RETURNS_CSV.exists():
        return None
    mt = RETURNS_CSV.stat().st_mtime
    with _lock:
        if _cache["returns"][0] == mt:
            return _cache["returns"][1]
    df = pd.read_csv(RETURNS_CSV, usecols=["asin", "sku", "returns_3p",
                                           "sellable_pct"])
    df["asin"] = df["asin"].fillna("").astype(str).str.strip().str.upper()
    df["sku"] = df["sku"].fillna("").astype(str).str.strip().str.upper()
    for c in ("returns_3p", "sellable_pct"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # sellable% weighted by return volume so multi-SKU ASINs blend honestly
    df["_sell_w"] = df["sellable_pct"].fillna(0) * df["returns_3p"].fillna(0)
    agg = df.groupby(["asin", "sku"], as_index=False).agg(
        returns_3p=("returns_3p", "sum"),
        _sell_w=("_sell_w", "sum"))
    with _lock:
        _cache["returns"] = (mt, agg)
    return agg


def _storage_agg() -> pd.DataFrame | None:
    if not STORAGE_CSV.exists():
        return None
    mt = STORAGE_CSV.stat().st_mtime
    with _lock:
        if _cache["storage"][0] == mt:
            return _cache["storage"][1]
    df = pd.read_csv(STORAGE_CSV, dtype={"month": str})
    df["asin"] = df["asin"].fillna("").astype(str).str.strip().str.upper()
    for c in ("storage_fee", "ltsf_fee"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    with _lock:
        _cache["storage"] = (mt, df)
    return df


def warehousing_for(asin: str, sku: str = "") -> dict:
    """Latest month's Amazon storage + LTSF for the ASIN, expressed per unit
    at the average monthly sales rate (13wk Amazon-3P units / 3). Slow movers
    correctly look expensive — that is the point of this line."""
    asin = (asin or "").strip().upper()
    df = _storage_agg()
    if df is None or df.empty:
        return {"available": False, "reason": "storage fee snapshot missing — run sp_fba_storage_fees_pull.py"}
    if len(asin) != 10:
        return {"available": False, "reason": "no ASIN (storage report is ASIN-keyed)"}
    month = df["month"].max()
    r = df[(df["asin"] == asin) & (df["month"] == month)]
    if r.empty:
        return {"available": False, "reason": f"no storage rows for ASIN in {month}"}
    storage = float(r["storage_fee"].sum())
    ltsf = float(r["ltsf_fee"].sum())
    sales = _sales_agg()
    monthly_units = None
    if sales is not None and not sales.empty:
        latest = int(sales["wn"].max())
        u13 = float(sales[(sales["asin"] == asin)
                          & (sales["wn"].between(latest - 12, latest))]["units_sold"].sum())
        if u13 > 0:
            monthly_units = u13 / 3.0
    per_unit = round((storage + ltsf) / monthly_units, 2) if monthly_units else None
    return {"available": True, "month": month,
            "storage_fee": round(storage, 0), "ltsf_fee": round(ltsf, 0),
            "monthly_units": round(monthly_units, 0) if monthly_units else None,
            "per_unit": per_unit,
            "avg_qty_on_hand": round(float(r["avg_qty_on_hand"].sum()), 0)}


def returns_for(asin: str, sku: str = "") -> dict:
    asin = (asin or "").strip().upper()
    sku = (sku or "").strip().upper()
    agg = _returns_agg()
    if agg is None or agg.empty:
        return {"available": False, "reason": "returns snapshot missing"}
    if len(asin) == 10:
        r = agg[agg["asin"] == asin]
    elif sku:
        r = agg[agg["sku"] == sku]
    else:
        return {"available": False, "reason": "no ASIN or SKU to match"}
    if r.empty:
        return {"available": False, "reason": "no returns rows for ASIN/SKU"}
    ret = float(r["returns_3p"].sum())
    # MATCHED WINDOWS (audit 07/09/26): returns_3p covers the seller returns
    # pull's 90-day window; the snapshot's units_sold_30d is 4 weeks of
    # all-channel sales — that mismatch overstated the rate up to ~3x.
    # Denominator here = Amazon-3P units over 13 weeks (~91 days) from the
    # same sales agg the ASP uses. 3P returns ÷ 3P sales, same window.
    sales = _sales_agg()
    if sales is None or sales.empty:
        return {"available": False, "reason": "sales snapshot missing"}
    latest = int(sales["wn"].max())
    lo = latest - 12
    smask = (sales["asin"] == asin) if len(asin) == 10 else (sales["sku"] == sku)
    sold = float(sales[smask & (sales["wn"].between(lo, latest))]["units_sold"].sum())
    if sold <= 0:
        return {"available": False, "reason": f"no Amazon 3P sales W{lo}-W{latest}"}
    sell_pct = (float(r["_sell_w"].sum()) / ret) if ret > 0 else None
    return {"available": True,
            "rate_pct": round(ret / sold * 100, 2),
            "return_units": int(ret), "units_sold_13w": int(sold),
            "window": f"W{lo}-W{latest}",
            "sellable_pct": round(sell_pct, 1) if sell_pct is not None else None}
