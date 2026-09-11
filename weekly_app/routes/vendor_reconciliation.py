"""1P (vendor) reconciliation — serves what scripts/sp_vendor_reconciliation.py
already computed from the Amazon Vendor PO APIs.

Same rule as the 3P page: the pull is slow and paginated, so this route NEVER
recomputes on request. It reads data/processed/vendor_reconciliation_snapshot.csv.

Access is restricted on the same basis as /reconciliation — this is PO-level
cost data, which is what Amazon pays us per unit.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from weekly_app.core.json_utils import clean_nan

router = APIRouter(prefix="/api/1p-reconciliation", tags=["1p-reconciliation"])
SNAP = Path("data/processed/vendor_reconciliation_snapshot.csv")

_ALLOWED = {e.strip().lower() for e in
            os.getenv("RECONCILIATION_EMAILS", "unmeshat@gmail.com").split(",") if e.strip()}


def _guard(request: Request) -> None:
    email = (request.session.get("user_email") or "").strip().lower()
    if not email:
        raise HTTPException(401, "Not signed in")
    if email in _ALLOWED:
        return
    try:
        from weekly_app.core.auth_users import get_role
        if (get_role(email) or "").lower() == "admin":
            return
    except Exception:
        pass
    raise HTTPException(403, "Reconciliation is restricted")


@router.get("")
@router.get("/")
def one_p(request: Request,
          label: Optional[str] = Query(None),
          month: Optional[str] = Query(None)):
    _guard(request)
    if not SNAP.exists():
        return clean_nan({"error": "No 1P pull yet — run "
                                   "scripts/sp_vendor_reconciliation.py",
                          "labels": [], "months": [], "steps": {}, "brands": []})
    df = pd.read_csv(SNAP, dtype={"month": str})
    for c in ("label", "scope", "metric"):
        df[c] = df[c].fillna("").astype(str)
    for c in ("units", "value"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    labels = sorted(df["label"].unique())
    lab = label if label in labels else (labels[0] if labels else "")
    sub = df[df["label"] == lab]
    months = sorted(sub["month"].unique(), reverse=True)
    mon = month if month in months else (months[0] if months else "")
    sub = sub[sub["month"] == mon]

    def cell(scope: str, metric: str) -> dict:
        r = sub[(sub["scope"] == scope) & (sub["metric"] == metric)]
        if r.empty:
            return {"units": 0.0, "value": 0.0}
        return {"units": float(r["units"].iloc[0]), "value": float(r["value"].iloc[0])}

    steps = {m: cell("ALL", m) for m in
             ("asked", "could_not_send", "promised", "arrived",
              "not_arrived", "travelling", "missing", "extra", "pos")}

    brands = []
    for scope in sorted(x for x in sub["scope"].unique() if x != "ALL"):
        p, a, m = cell(scope, "promised"), cell(scope, "arrived"), cell(scope, "missing")
        brands.append({
            "brand": scope,
            "promised_units": p["units"], "promised_value": p["value"],
            "arrived_units": a["units"], "arrived_value": a["value"],
            "missing_units": m["units"], "missing_value": m["value"],
            # "How much of our promise landed" — the one number that says
            # whether this brand is behaving.
            "fill_pct": (a["units"] / p["units"] * 100) if p["units"] else 0.0,
        })
    brands.sort(key=lambda b: b["missing_value"], reverse=True)

    return clean_nan({
        "labels": labels, "months": months, "label": lab, "month": mon,
        "pulled_at": (sub["pulled_at"].iloc[0] if "pulled_at" in sub.columns and len(sub) else ""),
        "age_days": int(sub["shortage_age_days"].iloc[0])
        if "shortage_age_days" in sub.columns and len(sub) else 30,
        "steps": steps, "brands": brands,
    })
