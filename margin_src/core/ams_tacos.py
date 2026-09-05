"""Actual TACOS (last 3 months) per ASIN, from the weekly AMS trend data.

Read-only dependency on the Weekly lane's
`data/ams_weekly_data/processed_ads/business_ads_joined.csv` — the same file
the AMS Trend page serves, so the calculator quotes the number the operator
already trusts instead of a second derivation (sanctioned in CLAUDE.md
boundary #2; no code import, no writes).

TACOS is aggregated as **sum(spend) / sum(gmv)** over the window — never the
mean of weekly TACOS, which over-weights small weeks.

Weeks are stored as bare integers (operator's Sun–Sat convention), so they
are mapped to dates off the same anchor the OMS scripts use: W33 = Sun
2026-08-09. That gives each week a real month for the 3-month split.
"""
from __future__ import annotations

import datetime as dt
import threading
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
AMS_FILE = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"

_ANCHOR_WEEK, _ANCHOR_SUN = 33, dt.date(2026, 8, 9)

_lock = threading.Lock()
_cache: tuple[float, pd.DataFrame] | None = None


def _week_start(week: int) -> dt.date:
    return _ANCHOR_SUN + dt.timedelta(days=7 * (int(week) - _ANCHOR_WEEK))


def _load() -> pd.DataFrame:
    """Cached by file mtime — a weekly refresh is picked up automatically."""
    global _cache
    if not AMS_FILE.exists():
        return pd.DataFrame()
    mtime = AMS_FILE.stat().st_mtime
    with _lock:
        if _cache and _cache[0] == mtime:
            return _cache[1]
    df = pd.read_csv(AMS_FILE, low_memory=False)
    for c in ("Spend", "gmv", "attributed_sales", "week"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["week"])
    df["_start"] = df["week"].map(_week_start)
    df["_month"] = df["_start"].map(lambda d: d.strftime("%b %Y"))
    for c in ("asin", "child_asin"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    with _lock:
        _cache = (mtime, df)
    return df


def _pct(spend: float, gmv: float):
    return round(100 * spend / gmv, 2) if gmv else None


def tacos_for(asin: str, months: int = 3) -> dict:
    """Actual TACOS for an ASIN: per-month for the last `months`, plus the
    combined window figure. Empty months are returned with nulls rather than
    dropped, so the UI can show that a period genuinely had no data."""
    asin = (asin or "").strip().upper()
    df = _load()
    if df.empty or not asin:
        return {"asin": asin, "available": False, "reason": "AMS trend data not found",
                "months": [], "window": None}

    mask = df["asin"].eq(asin)
    if "child_asin" in df.columns:
        mask = mask | df["child_asin"].eq(asin)
    rows = df[mask]
    if rows.empty:
        return {"asin": asin, "available": False,
                "reason": "no AMS rows for this ASIN", "months": [], "window": None}

    # last N distinct months present in the FILE (not calendar-relative), so a
    # lagging weekly refresh still shows the three most recent real months.
    order = (df[["_month", "_start"]].groupby("_month")["_start"].max()
             .sort_values().index.tolist())
    wanted = order[-months:]

    out = []
    for m in wanted:
        sub = rows[rows["_month"] == m]
        spend = float(sub["Spend"].fillna(0).sum())
        gmv = float(sub["gmv"].fillna(0).sum())
        out.append({"month": m, "spend": round(spend, 2), "gmv": round(gmv, 2),
                    "tacos_pct": _pct(spend, gmv),
                    "weeks": sorted(int(w) for w in sub["week"].unique())})

    win = rows[rows["_month"].isin(wanted)]
    w_spend = float(win["Spend"].fillna(0).sum())
    w_gmv = float(win["gmv"].fillna(0).sum())
    return {
        "asin": asin,
        "available": True,
        "months": out,
        "window": {"label": f"{wanted[0]} – {wanted[-1]}" if wanted else "",
                   "spend": round(w_spend, 2), "gmv": round(w_gmv, 2),
                   "tacos_pct": _pct(w_spend, w_gmv)},
        "brand": (rows["brand"].dropna().iloc[0] if "brand" in rows and not rows["brand"].dropna().empty else ""),
        "model": (rows["model"].dropna().iloc[0] if "model" in rows and not rows["model"].dropna().empty else ""),
    }
