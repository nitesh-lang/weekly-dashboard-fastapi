"""Senior PPC analyst insight engine for the AMS Trend page.

Takes the same row-level DataFrame the /api/ams/trend endpoint
produces (ASIN × week × metrics) and emits structured, opinionated
performance insights — the kind a 15-year Amazon Ads consultant
would write on a Monday review call.

Design rules (so the output never reads like AI slop):

1. EVERY insight cites a real number from the data — no vague
   "spend is up" without telling you BY HOW MUCH and AGAINST WHAT
   BASELINE.

2. Always frame cause -> effect -> implication.  "ACOS jumped from
   12% to 18% — CPC rose 25% while CVR stayed flat — bid management
   got too aggressive, not the auction" beats "ACOS went up".

3. Skip rather than fabricate.  If the selected window has <2 weeks
   the WoW commentary is dropped entirely; if there's no CVR
   denominator a CVR insight is dropped.  Better silence than a
   false signal.

4. Numbers in INR with Cr / L / K shorthand the operator already
   uses elsewhere on the dashboard.

5. Output is ranked, capped at 12 insights total (the operator has
   a budget of ~30 seconds of attention before they scroll).

Returns a dict shaped like:

    {
      "window":   {"weeks": [...], "weeks_count": int, "label": "W22-W25"},
      "headline": [{kind, text}, ...]   # 1-3 items
      "efficiency": [...]                # 0-3
      "movers":     [...]                # 0-3
      "anomalies":  [...]                # 0-2
      "inventory":  [...]                # 0-2
      "trajectory": [...]                # 0-2
      "actions":    [...]                # 0-3
    }

Each insight item is shaped:
    {
      "kind":      "positive" | "negative" | "warning" | "neutral" | "action",
      "text":      "Human-readable sentence with numbers baked in.",
      "metric":    "acos" | "tacos" | "roas" | "spend" | "cpc" | "cvr" | "ctr" | None,
      "delta_pct": float | None,         # signed; positive = up
      "weight":    float                  # 0-1; downstream UI sorting
    }
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Senior-PPC thresholds.  These are CATEGORY-AGNOSTIC defaults — we
# don't have per-category margin data, so we use the conventional
# Amazon Ads consultant guardrails.  Override-friendly if the
# operator later tags categories with margin floors.
# ---------------------------------------------------------------------
ACOS_HEALTHY_MAX = 0.22    # 22%
ACOS_WARNING_MAX = 0.30    # 30%
ACOS_CRITICAL    = 0.40    # 40%

TACOS_HEALTHY_MAX = 0.08   # 8% — share of total revenue spent on ads
TACOS_WARNING_MAX = 0.12   # 12%

ROAS_HEALTHY_MIN = 4.0     # 1/0.25 = 4x → ACOS 25%
ROAS_WARNING_MIN = 3.0     # → ACOS 33%
ROAS_CRITICAL    = 2.5     # → ACOS 40%

CTR_FLOOR        = 0.0030  # 0.30% — below this ad creative usually broken
CVR_FLOOR        = 0.05    # 5% sessions->units; below this CR is poor on Amazon

# Movement bands — "small" / "moderate" / "large" deltas
DELTA_MOVING_PCT   = 0.05  # 5%  — worth mentioning
DELTA_LARGE_PCT    = 0.15  # 15% — substantive
DELTA_HUGE_PCT     = 0.30  # 30% — anomaly territory

# Concentration: a single model controlling >X% of spend gets called out
SPEND_CONCENTRATION_FLAG = 0.40


# ---------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------
def _inr(n: float | int | None) -> str:
    """₹ X.XX Cr / L / K depending on magnitude.  Mirrors the JS
    fmtINR used in dashboard.html so insights read consistently
    with the rest of the page."""
    if n is None or pd.isna(n):
        return "₹0"
    n = float(n)
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e7:
        return f"{sign}₹{n / 1e7:.2f} Cr"
    if n >= 1e5:
        return f"{sign}₹{n / 1e5:.2f} L"
    if n >= 1e3:
        return f"{sign}₹{n / 1e3:.1f} K"
    return f"{sign}₹{n:,.0f}"


def _pct(n: float | None, decimals: int = 1) -> str:
    if n is None or pd.isna(n) or np.isinf(n):
        return "—"
    return f"{n * 100:.{decimals}f}%"


def _bps(prev: float | None, curr: float | None) -> str:
    """Delta in basis points, signed.  Useful for ACOS/TACOS movements."""
    if any(v is None or pd.isna(v) for v in (prev, curr)):
        return "—"
    return f"{(curr - prev) * 10000:+.0f}bps"


def _delta_pct(prev: float | None, curr: float | None) -> Optional[float]:
    """Return signed pct change.  None if undefined."""
    if prev is None or curr is None or pd.isna(prev) or pd.isna(curr):
        return None
    if prev == 0:
        return None
    return float(curr - prev) / abs(float(prev))


def _arrow(delta: float | None) -> str:
    if delta is None:
        return "—"
    if delta > DELTA_MOVING_PCT:
        return "↑"
    if delta < -DELTA_MOVING_PCT:
        return "↓"
    return "→"


# ---------------------------------------------------------------------
# Weekly aggregation
# ---------------------------------------------------------------------
def _weekly_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Roll the row-level df to one row per week with the headline metrics."""
    if df.empty:
        return pd.DataFrame()
    g = (df.groupby("week", as_index=False)
            .agg(spend=("ad_spend", "sum"),
                 attr_sales=("attributed_sales", "sum"),
                 gmv=("gmv", "sum"),
                 clicks=("clicks", "sum"),
                 impressions=("impressions", "sum"),
                 orders=("ams_orders", "sum"),
                 units=("units", "sum"),
                 sessions=("sessions", "sum"))
            .sort_values("week")
            .reset_index(drop=True))
    g["acos"]  = np.where(g["attr_sales"] > 0, g["spend"] / g["attr_sales"],   np.nan)
    g["tacos"] = np.where(g["gmv"] > 0,        g["spend"] / g["gmv"],          np.nan)
    g["roas"] = np.where(g["spend"] > 0,       g["attr_sales"] / g["spend"],   np.nan)
    g["cpc"]  = np.where(g["clicks"] > 0,      g["spend"] / g["clicks"],       np.nan)
    g["ctr"]  = np.where(g["impressions"] > 0, g["clicks"] / g["impressions"], np.nan)
    g["cvr_sess"] = np.where(g["sessions"] > 0, g["units"] / g["sessions"],    np.nan)
    g["cvr_click"]= np.where(g["clicks"] > 0,   g["orders"] / g["clicks"],     np.nan)
    return g


def _model_totals_for_week(df: pd.DataFrame, wk: int) -> pd.DataFrame:
    """Roll up by Model for a single week — used for movers/anomalies."""
    w = df[df["week"] == wk].copy()
    if w.empty:
        return pd.DataFrame()
    g = (w.groupby("Model", as_index=False)
           .agg(spend=("ad_spend", "sum"),
                attr_sales=("attributed_sales", "sum"),
                gmv=("gmv", "sum"),
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
                orders=("ams_orders", "sum"),
                units=("units", "sum")))
    g["acos"] = np.where(g["attr_sales"] > 0, g["spend"] / g["attr_sales"],     np.nan)
    g["roas"] = np.where(g["spend"] > 0,      g["attr_sales"] / g["spend"],     np.nan)
    g["cpc"]  = np.where(g["clicks"] > 0,     g["spend"] / g["clicks"],         np.nan)
    return g


# ---------------------------------------------------------------------
# Builders for each insight category.  Each returns a list (possibly
# empty) of insight dicts.
# ---------------------------------------------------------------------
def _build_headline(weekly: pd.DataFrame) -> list[dict]:
    """1-3 headline lines describing the latest week + WoW context."""
    if weekly.empty:
        return []
    latest = weekly.iloc[-1]
    prev   = weekly.iloc[-2] if len(weekly) >= 2 else None
    out: list[dict] = []

    # Line 1 — the topline.  Always present.
    if prev is not None:
        spend_d = _delta_pct(prev["spend"], latest["spend"])
        sales_d = _delta_pct(prev["attr_sales"], latest["attr_sales"])
        gmv_d   = _delta_pct(prev["gmv"], latest["gmv"])
        text = (f"W{int(latest['week'])}: ad spend {_inr(latest['spend'])} "
                f"({_arrow(spend_d)} {_pct(spend_d)} WoW), "
                f"attributed sales {_inr(latest['attr_sales'])} "
                f"({_arrow(sales_d)} {_pct(sales_d)}), "
                f"total GMV {_inr(latest['gmv'])} "
                f"({_arrow(gmv_d)} {_pct(gmv_d)}).")
        out.append({"kind": "neutral", "text": text, "metric": None,
                    "delta_pct": spend_d, "weight": 1.0})
    else:
        text = (f"W{int(latest['week'])}: ad spend {_inr(latest['spend'])}, "
                f"attributed sales {_inr(latest['attr_sales'])}, "
                f"total GMV {_inr(latest['gmv'])}. "
                f"Single-week view — pick ≥2 weeks for trend context.")
        out.append({"kind": "neutral", "text": text, "metric": None,
                    "delta_pct": None, "weight": 1.0})

    # Line 2 — efficiency frame.  ACOS vs TACOS vs target.
    acos  = latest["acos"]
    tacos = latest["tacos"]
    if pd.notna(acos) and pd.notna(tacos):
        kind = "positive"
        if acos > ACOS_CRITICAL or tacos > TACOS_WARNING_MAX:
            kind = "negative"
        elif acos > ACOS_WARNING_MAX or tacos > TACOS_HEALTHY_MAX:
            kind = "warning"
        text = (f"Efficiency: ACOS {_pct(acos)}, TACOS {_pct(tacos)}, "
                f"ROAS {latest['roas']:.2f}x — "
                + ("inside the healthy band (ACOS ≤22%, TACOS ≤8%)."
                   if kind == "positive"
                   else "approaching the warning band — review highest-spend models."
                   if kind == "warning"
                   else "outside the healthy band; cut bids on low-ROAS models before next week."))
        out.append({"kind": kind, "text": text, "metric": "acos",
                    "delta_pct": None, "weight": 0.95})
    return out


def _build_efficiency(weekly: pd.DataFrame) -> list[dict]:
    """Direction-of-travel reads on ACOS / TACOS / ROAS / CPC / CTR / CVR."""
    if len(weekly) < 2:
        return []
    out: list[dict] = []
    latest = weekly.iloc[-1]
    prev   = weekly.iloc[-2]
    trail  = weekly.iloc[-min(4, len(weekly) - 1):-1] if len(weekly) >= 3 else None

    def _trail_mean(col):
        if trail is None or trail[col].dropna().empty:
            return None
        return float(trail[col].mean())

    # ACOS movement, framed
    if pd.notna(latest["acos"]) and pd.notna(prev["acos"]):
        d_bps = (latest["acos"] - prev["acos"]) * 10000
        if abs(d_bps) >= 100:    # ≥100bps = ≥1pp = worth noting
            kind = "negative" if d_bps > 0 else "positive"
            text = (f"ACOS moved {_pct(prev['acos'])} → {_pct(latest['acos'])} "
                    f"({d_bps:+.0f}bps). ")
            tm = _trail_mean("acos")
            if tm is not None:
                text += f"4-week trailing mean: {_pct(tm)}. "
            # Diagnose drivers
            cpc_d = _delta_pct(prev["cpc"], latest["cpc"])
            cvr_d = _delta_pct(prev["cvr_click"], latest["cvr_click"])
            if cpc_d is not None and cvr_d is not None:
                if cpc_d > DELTA_MOVING_PCT and cvr_d < -DELTA_MOVING_PCT:
                    text += (f"Driver: CPC {_arrow(cpc_d)} {_pct(cpc_d)} AND "
                             f"click→order CVR {_arrow(cvr_d)} {_pct(cvr_d)} — "
                             f"both legs hurting, prioritise.")
                elif cpc_d > DELTA_MOVING_PCT:
                    text += (f"Driver is CPC ({_arrow(cpc_d)} {_pct(cpc_d)}), "
                             f"CVR roughly stable. Auction pressure or bid creep.")
                elif cvr_d < -DELTA_MOVING_PCT:
                    text += (f"Driver is conversion ({_arrow(cvr_d)} {_pct(cvr_d)}); "
                             f"CPC stable. Listing / price / OOS issue likely.")
            out.append({"kind": kind, "text": text, "metric": "acos",
                        "delta_pct": d_bps / 100, "weight": 0.9})

    # TACOS movement
    if pd.notna(latest["tacos"]) and pd.notna(prev["tacos"]):
        d_bps = (latest["tacos"] - prev["tacos"]) * 10000
        if abs(d_bps) >= 50:
            kind = "negative" if d_bps > 0 else "positive"
            tm = _trail_mean("tacos")
            tm_part = f", 4w mean {_pct(tm)}" if tm is not None else ""
            text = (f"TACOS {_pct(prev['tacos'])} → {_pct(latest['tacos'])} "
                    f"({d_bps:+.0f}bps{tm_part}). "
                    + ("Healthy band intact (≤8%)." if latest['tacos'] <= TACOS_HEALTHY_MAX else
                       "Warning band (8-12%) — efficient growth getting harder." if latest['tacos'] <= TACOS_WARNING_MAX else
                       "Above 12% — paid is doing too much of the revenue lifting; build organic."))
            out.append({"kind": kind, "text": text, "metric": "tacos",
                        "delta_pct": d_bps / 100, "weight": 0.85})

    # CPC trend
    if pd.notna(latest["cpc"]) and pd.notna(prev["cpc"]):
        d = _delta_pct(prev["cpc"], latest["cpc"])
        if d is not None and abs(d) >= DELTA_LARGE_PCT:
            kind = "warning" if d > 0 else "positive"
            text = (f"CPC moved {_inr(prev['cpc'])} → {_inr(latest['cpc'])} "
                    f"({_pct(d)} WoW). "
                    + ("Either competitive heat picked up or your bid strategy got more aggressive — pull the 4w CPC by model to triangulate."
                       if d > 0 else "Bid relief — verify you're not also losing impression share."))
            out.append({"kind": kind, "text": text, "metric": "cpc",
                        "delta_pct": d, "weight": 0.75})

    # CTR trend
    if pd.notna(latest["ctr"]) and pd.notna(prev["ctr"]):
        d = _delta_pct(prev["ctr"], latest["ctr"])
        if d is not None and abs(d) >= DELTA_LARGE_PCT:
            kind = "positive" if d > 0 else "warning"
            text = (f"CTR moved {_pct(latest['ctr'], 2)} (from {_pct(prev['ctr'], 2)}, "
                    f"{_pct(d)} WoW). "
                    + ("Creative or relevance signal improving — expect CPC to soften with a lag."
                       if d > 0 else
                       "Drop in ad relevance — check main image / price competitiveness / search-term match."))
            if latest['ctr'] < CTR_FLOOR:
                text += f" Absolute CTR is below the 0.30% floor — symptomatic of poor ad quality regardless of trend."
                kind = "negative"
            out.append({"kind": kind, "text": text, "metric": "ctr",
                        "delta_pct": d, "weight": 0.7})

    return out


def _build_movers(df: pd.DataFrame, weekly: pd.DataFrame) -> list[dict]:
    """Identify the 1-3 models actually moving the needle — winners and drainers."""
    if weekly.empty:
        return []
    out: list[dict] = []
    latest_wk = int(weekly.iloc[-1]["week"])
    mdl = _model_totals_for_week(df, latest_wk)
    if mdl.empty:
        return out

    mdl = mdl[mdl["spend"] > 0].copy()
    if mdl.empty:
        return out
    total_spend = mdl["spend"].sum()
    total_attr  = mdl["attr_sales"].sum()

    # Concentration check — is one model swallowing the budget?
    top = mdl.sort_values("spend", ascending=False).head(1).iloc[0]
    top_share_spend = top["spend"] / total_spend if total_spend > 0 else 0
    if total_attr > 0:
        top_share_sales = top["attr_sales"] / total_attr
    else:
        top_share_sales = 0
    if top_share_spend >= SPEND_CONCENTRATION_FLAG:
        gap_pp = (top_share_spend - top_share_sales) * 100
        kind = "warning" if gap_pp > 5 else "neutral"
        text = (f"Model **{top['Model']}** consumed {top_share_spend*100:.0f}% of W{latest_wk} ad spend "
                f"({_inr(top['spend'])}) but produced {top_share_sales*100:.0f}% of attributed sales "
                f"({_inr(top['attr_sales'])})"
                + (f" — {gap_pp:+.0f}pp efficiency drag, this single model is the primary lever."
                   if gap_pp > 5 else " — concentrated but efficiency is balanced."))
        out.append({"kind": kind, "text": text, "metric": "spend",
                    "delta_pct": gap_pp / 100, "weight": 0.92})

    # Winners — ROAS ≥2x brand median for the week, with non-trivial spend
    median_roas = mdl["roas"].dropna().median() if not mdl["roas"].dropna().empty else None
    if median_roas is not None and median_roas > 0:
        winners = mdl[(mdl["roas"] >= 2 * median_roas) & (mdl["spend"] >= 5000)].copy()
        winners = winners.sort_values("attr_sales", ascending=False).head(2)
        for _, w in winners.iterrows():
            text = (f"Top performer: **{w['Model']}** "
                    f"— ROAS {w['roas']:.2f}x on {_inr(w['spend'])} spend "
                    f"(vs brand median {median_roas:.2f}x). "
                    f"Room to push bids 10-15% before efficiency degrades.")
            out.append({"kind": "positive", "text": text, "metric": "roas",
                        "delta_pct": None, "weight": 0.8})

    # Drainers — ROAS <2 on ≥₹10k spend, OR ≥₹5k spend with zero attributed sales
    drainers = mdl[((mdl["roas"] < ROAS_CRITICAL) & (mdl["spend"] >= 10000))
                   | ((mdl["attr_sales"] == 0) & (mdl["spend"] >= 5000))].copy()
    drainers = drainers.sort_values("spend", ascending=False).head(2)
    for _, d in drainers.iterrows():
        if d["attr_sales"] == 0:
            text = (f"Drainer: **{d['Model']}** spent {_inr(d['spend'])} with "
                    f"ZERO attributed sales — pause or rewrite ad copy/targeting immediately.")
        else:
            text = (f"Drainer: **{d['Model']}** — ROAS {d['roas']:.2f}x "
                    f"({_inr(d['spend'])} spent, {_inr(d['attr_sales'])} sales). "
                    f"Cut bids 20% or move budget to higher-ROAS models.")
        out.append({"kind": "negative", "text": text, "metric": "roas",
                    "delta_pct": None, "weight": 0.85})

    return out


def _build_anomalies(df: pd.DataFrame, weekly: pd.DataFrame) -> list[dict]:
    """Per-model spikes / odd metrics that warrant a deeper look."""
    if len(weekly) < 2:
        return []
    latest_wk = int(weekly.iloc[-1]["week"])
    prev_wk   = int(weekly.iloc[-2]["week"])
    cur  = _model_totals_for_week(df, latest_wk)
    prv  = _model_totals_for_week(df, prev_wk)
    if cur.empty or prv.empty:
        return []
    j = cur.merge(prv, on="Model", how="inner", suffixes=("_cur", "_prv"))
    if j.empty:
        return []
    out: list[dict] = []

    # Models where CPC jumped >50% AND spend was material in both weeks
    j["cpc_d"] = (j["cpc_cur"] - j["cpc_prv"]) / j["cpc_prv"].replace(0, np.nan)
    spikes = j[(j["cpc_d"].abs() >= DELTA_HUGE_PCT)
               & (j["spend_cur"] >= 3000)
               & (j["spend_prv"] >= 3000)].copy()
    spikes = spikes.sort_values("spend_cur", ascending=False).head(2)
    for _, s in spikes.iterrows():
        text = (f"CPC anomaly on **{s['Model']}**: "
                f"{_inr(s['cpc_prv'])} → {_inr(s['cpc_cur'])} ({_pct(s['cpc_d'])} WoW). "
                f"Investigate before it propagates to the rest of the campaign group.")
        out.append({"kind": "warning", "text": text, "metric": "cpc",
                    "delta_pct": float(s["cpc_d"]), "weight": 0.75})

    # Models where spend doubled but attributed sales stayed flat
    j["spend_d"] = (j["spend_cur"] - j["spend_prv"]) / j["spend_prv"].replace(0, np.nan)
    j["sales_d"] = (j["attr_sales_cur"] - j["attr_sales_prv"]) / j["attr_sales_prv"].replace(0, np.nan)
    waste = j[(j["spend_d"] >= 0.5) & (j["sales_d"].fillna(0) <= 0.1)
              & (j["spend_cur"] >= 5000)].head(1)
    for _, w in waste.iterrows():
        text = (f"Diminishing returns on **{w['Model']}**: spend {_pct(w['spend_d'])} WoW "
                f"({_inr(w['spend_prv'])} → {_inr(w['spend_cur'])}) but attributed sales "
                f"only {_pct(w['sales_d'])}. You've passed the elastic point on this model.")
        out.append({"kind": "negative", "text": text, "metric": "spend",
                    "delta_pct": float(w["spend_d"]), "weight": 0.8})
    return out


def _build_inventory(df: pd.DataFrame, weekly: pd.DataFrame) -> list[dict]:
    """High-ROAS but low-cover combinations (wasted ad spend risk)
    + high-spend on OOS models (immediate waste)."""
    if weekly.empty:
        return []
    out: list[dict] = []
    latest_wk = int(weekly.iloc[-1]["week"])
    w = df[df["week"] == latest_wk].copy()
    if w.empty or "inventory_total_amazon" not in w.columns:
        return out

    mdl = (w.groupby("Model", as_index=False)
             .agg(spend=("ad_spend", "sum"),
                  attr_sales=("attributed_sales", "sum"),
                  units=("units", "sum"),
                  inv=("inventory_total_amazon", "sum")))
    mdl["roas"] = np.where(mdl["spend"] > 0, mdl["attr_sales"] / mdl["spend"], np.nan)
    # Days of cover = inventory / (units sold this week / 7)
    mdl["days_cover"] = np.where(mdl["units"] > 0,
                                 mdl["inv"] / (mdl["units"] / 7.0),
                                 np.nan)

    # High-ROAS + low-cover combo — wasted traffic risk
    risky = mdl[(mdl["roas"] >= ROAS_HEALTHY_MIN)
                & (mdl["days_cover"].between(0, 14))
                & (mdl["spend"] >= 3000)].copy()
    risky = risky.sort_values("attr_sales", ascending=False).head(2)
    for _, r in risky.iterrows():
        text = (f"**{r['Model']}** is converting well (ROAS {r['roas']:.2f}x) "
                f"but only **{r['days_cover']:.0f} days of cover** ({int(r['inv'])} units). "
                f"Either inbound stock fast or throttle bids to avoid driving "
                f"traffic to an OOS listing in 2 weeks.")
        out.append({"kind": "warning", "text": text, "metric": "roas",
                    "delta_pct": None, "weight": 0.7})

    # OOS but still spending
    oos = mdl[(mdl["inv"] == 0) & (mdl["spend"] >= 1000)].copy()
    oos = oos.sort_values("spend", ascending=False).head(1)
    for _, o in oos.iterrows():
        text = (f"**{o['Model']}** is at ZERO Amazon-side inventory but "
                f"still spending {_inr(o['spend'])} on ads. Pause campaigns now — "
                f"every click is wasted spend.")
        out.append({"kind": "negative", "text": text, "metric": "spend",
                    "delta_pct": None, "weight": 0.95})
    return out


def _build_trajectory(weekly: pd.DataFrame) -> list[dict]:
    """Multi-week slope reads.  Need ≥4 weeks to be statistically interesting."""
    if len(weekly) < 4:
        return []
    out: list[dict] = []

    # TACOS trajectory — first vs last over selected window
    first = weekly.iloc[0]
    last  = weekly.iloc[-1]
    if pd.notna(first["tacos"]) and pd.notna(last["tacos"]):
        d_bps = (last["tacos"] - first["tacos"]) * 10000
        if abs(d_bps) >= 100:
            text = (f"TACOS trajectory across the selected window: "
                    f"W{int(first['week'])} {_pct(first['tacos'])} → "
                    f"W{int(last['week'])} {_pct(last['tacos'])} "
                    f"({d_bps:+.0f}bps over {len(weekly)} weeks). "
                    + ("Efficiency eroding — paid is doing more revenue lifting per week."
                       if d_bps > 0 else "Efficiency improving — momentum on organic share."))
            kind = "negative" if d_bps > 0 else "positive"
            out.append({"kind": kind, "text": text, "metric": "tacos",
                        "delta_pct": d_bps / 100, "weight": 0.6})

    # CVR trajectory
    if pd.notna(first["cvr_sess"]) and pd.notna(last["cvr_sess"]):
        d = _delta_pct(first["cvr_sess"], last["cvr_sess"])
        if d is not None and abs(d) >= 0.15:
            text = (f"Session→unit CVR moved {_pct(first['cvr_sess'])} → "
                    f"{_pct(last['cvr_sess'])} across {len(weekly)} weeks "
                    f"({_pct(d)}). "
                    + ("Better traffic quality OR listing improvements landing — "
                       "look for price changes, image refreshes, or relevance wins."
                       if d > 0 else
                       "Traffic quality degrading — search terms may have widened, "
                       "or competitive pricing changed. Audit search-term reports."))
            kind = "positive" if d > 0 else "warning"
            out.append({"kind": kind, "text": text, "metric": "cvr",
                        "delta_pct": d, "weight": 0.55})
    return out


def _build_actions(df: pd.DataFrame, weekly: pd.DataFrame,
                   movers: list[dict], anomalies: list[dict],
                   inventory: list[dict]) -> list[dict]:
    """Concrete next-step recommendations — at most 3, prioritised by impact."""
    out: list[dict] = []

    # Action 1 — biggest drainer becomes a "cut bids" action
    for m in movers:
        if m["kind"] == "negative" and "Drainer" in m["text"]:
            # Extract the model name from the bold-marked first ** ** pair
            try:
                model = m["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Cut bids 20% on {model}** this week and re-check next Monday — "
                            f"every rupee saved here is high-margin freed up for ROAS-positive models.",
                    "metric": None, "delta_pct": None, "weight": 0.95,
                })
                break
            except IndexError:
                continue

    # Action 2 — biggest winner becomes a "lean in" action
    for m in movers:
        if m["kind"] == "positive" and "Top performer" in m["text"]:
            try:
                model = m["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Lean into {model}** — raise bids 10-15% or expand match types. "
                            f"Currently under-spent for its ROAS profile.",
                    "metric": None, "delta_pct": None, "weight": 0.85,
                })
                break
            except IndexError:
                continue

    # Action 3 — OOS / low-cover risk becomes a "pause" or "inbound" action
    for inv in inventory:
        if inv["kind"] == "negative":
            try:
                model = inv["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Pause {model} campaigns** until inventory restocks. "
                            f"Spend on an OOS listing converts at zero.",
                    "metric": None, "delta_pct": None, "weight": 1.0,
                })
                break
            except IndexError:
                continue
        elif inv["kind"] == "warning" and "days of cover" in inv["text"]:
            try:
                model = inv["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Inbound stock for {model}** before next week — "
                            f"you're 2 weeks from running ads against an OOS listing.",
                    "metric": None, "delta_pct": None, "weight": 0.9,
                })
                break
            except IndexError:
                continue

    return out[:3]


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------
def build_insights(df: pd.DataFrame,
                   selected_weeks: list[int] | None = None) -> dict[str, Any]:
    """Turn a filtered AMS Trend DataFrame into structured insights.

    Args:
        df: row-level AMS data (already brand/category/model filtered)
        selected_weeks: optional explicit week filter — by default use
            every week present in df.

    Returns:
        Insights payload (see module docstring).
    """
    if df is None or df.empty:
        return {
            "window": {"weeks": [], "weeks_count": 0, "label": ""},
            "headline": [], "efficiency": [], "movers": [],
            "anomalies": [], "inventory": [], "trajectory": [],
            "actions": [],
            "empty_reason": "No data after filters — widen the selection.",
        }

    df = df.copy()
    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df = df[df["week"].notna()]

    # Drop rows where Model is missing or stringified-NaN — these are
    # master-alignment gaps; they'd dominate "top spender" calls with
    # a meaningless "Model NAN" label. Aggregate-level WoW math is
    # unaffected because the spend still flows into weekly totals
    # via _weekly_totals which doesn't group by Model.
    if "Model" in df.columns:
        m = df["Model"].astype(str).str.strip()
        df = df[m.ne("") & m.str.lower().ne("nan") & m.str.lower().ne("none")]

    weeks_in_data = sorted(df["week"].dropna().unique().astype(int).tolist())
    if selected_weeks:
        weeks_used = [w for w in selected_weeks if w in weeks_in_data]
        if not weeks_used:
            weeks_used = weeks_in_data
    else:
        weeks_used = weeks_in_data

    # Cap at 12 — same convention as /api/ams/trend's default window
    weeks_used = weeks_used[-12:]
    df = df[df["week"].isin(weeks_used)]

    weekly = _weekly_totals(df)
    if weekly.empty:
        return {
            "window": {"weeks": weeks_used,
                       "weeks_count": len(weeks_used),
                       "label": f"W{weeks_used[0]}-W{weeks_used[-1]}" if weeks_used else ""},
            "headline": [], "efficiency": [], "movers": [],
            "anomalies": [], "inventory": [], "trajectory": [],
            "actions": [],
            "empty_reason": "No weekly aggregates — check the data slice.",
        }

    headline   = _build_headline(weekly)
    efficiency = _build_efficiency(weekly)
    movers     = _build_movers(df, weekly)
    anomalies  = _build_anomalies(df, weekly)
    inventory  = _build_inventory(df, weekly)
    trajectory = _build_trajectory(weekly)
    actions    = _build_actions(df, weekly, movers, anomalies, inventory)

    weeks_label = (f"W{weeks_used[0]}-W{weeks_used[-1]}"
                   if len(weeks_used) >= 2 else
                   f"W{weeks_used[0]}" if weeks_used else "")

    return {
        "window": {
            "weeks": weeks_used,
            "weeks_count": len(weeks_used),
            "label": weeks_label,
        },
        "headline":   headline,
        "efficiency": efficiency,
        "movers":     movers,
        "anomalies":  anomalies,
        "inventory":  inventory,
        "trajectory": trajectory,
        "actions":    actions,
    }
