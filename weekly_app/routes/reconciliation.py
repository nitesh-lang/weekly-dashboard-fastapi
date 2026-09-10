"""Account reconciliation — serves what scripts/sp_account_reconciliation.py
already computed from Amazon's Finances API.

The pull itself is ~56 paginated API calls per account-month, so the page
NEVER recomputes on request: the script writes
data/processed/reconciliation_snapshot.csv and this route reads it.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query

from weekly_app.core.json_utils import clean_nan

router = APIRouter(prefix="/api/reconciliation", tags=["reconciliation"])
SNAP = Path("data/processed/reconciliation_snapshot.csv")


def _load() -> pd.DataFrame:
    if not SNAP.exists():
        return pd.DataFrame()
    df = pd.read_csv(SNAP, dtype={"month": str})
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    for c in ("Section", "Item", "Note", "account", "month"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    return df


@router.get("")
@router.get("/")
def reconciliation(account: Optional[str] = Query(None),
                   month: Optional[str] = Query(None)):
    df = _load()
    if df.empty:
        return clean_nan({"error": "No reconciliation pulled yet — run "
                                   "scripts/sp_account_reconciliation.py",
                          "accounts": [], "months": [], "rows": []})
    accounts = sorted(df["account"].unique())
    acct = account if account in accounts else accounts[0]
    sub = df[df["account"] == acct]
    months = sorted(sub["month"].unique(), reverse=True)
    mon = month if month in months else (months[0] if months else "")
    sub = sub[sub["month"] == mon]

    val = lambda item: float(sub.loc[sub["Item"] == item, "Amount"].sum())
    sales = val("Product sales (GST NOT included)")
    settle = val("AMAZON SHOULD PAY US")
    net_gst = val("Net GST still to remit in cash")
    ads = val("Advertising (from our AMS data)")
    fees = float(sub.loc[sub["Section"] == "3. AMAZON FEES (ex-GST)", "Amount"].sum())
    contribution = settle + net_gst

    kpis = {
        "sales": sales,
        "units_shipped": val("Units shipped (what Amazon paid on)"),
        "return_rate": val("Return rate % (in-month, mixed cohorts)"),
        "amazon_fees": fees,
        "amazon_fees_pct": (abs(fees) / sales * 100) if sales else 0,
        "ads": ads,
        "settle": settle,
        "net_gst": net_gst,
        "contribution": contribution,
        "contribution_pct": (contribution / sales * 100) if sales else 0,
        "after_ads": contribution + ads,
        "after_ads_pct": ((contribution + ads) / sales * 100) if sales else 0,
    }
    rows = (sub[["Section", "Item", "Amount", "Note"]]
            .to_dict("records"))
    pulled = sub["pulled_at"].iloc[0] if "pulled_at" in sub.columns and len(sub) else ""
    return clean_nan({"accounts": accounts, "months": months,
                      "account": acct, "month": mon, "pulled_at": pulled,
                      "kpis": kpis, "rows": rows})
