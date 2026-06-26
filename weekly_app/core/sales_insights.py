"""Sales Trend insight engine — facts-only weekly read.

Mirrors the payload shape of ams_insights so the React panel UI stays
structurally identical, but every line is a number-grounded statement
from the row-level sales df. No speculation ("check the listing",
"verify pricing"), no fabricated drivers ("auction pressure"), no
domain-foreign advice ("push via search/deals" — that's ads, not sales).

Rule of writing:
  - cite the metric, the value, and the baseline (prior week or 4w mean)
  - skip the section rather than say something we can't prove from data
  - inventory + cover claims must be derived from inventory_by_model
  - actions are limited to those a sales-only view can justify
    (reorder when cover < lead time, liquidate when dead stock)

Sections produced (kept for UI parity with ams_insights):
  headline    latest-week totals + WoW
  efficiency  intentionally empty (no ROAS/ACOS analogue in sales)
  movers      top WoW unit growers + drainers, numbers only
  anomalies   sudden-zero events, numbers only
  inventory   low-cover hot sellers + dead stock, derived from inv data
  trajectory  first-vs-last GMV / units slope, numbers only
  actions     reorder + liquidate (only when triggered by data)
"""
from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------
DELTA_MOVING_PCT = 0.05
LOW_COVER_DAYS   = 14    # cover ≤ 14d on active sales = reorder window closing
DEAD_STOCK_INV_MIN = 50  # need ≥ this much inventory to bother calling it out


# ---------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------
def _inr(n) -> str:
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


def _pct(n, decimals: int = 1) -> str:
    if n is None or pd.isna(n) or np.isinf(n):
        return "—"
    return f"{n * 100:.{decimals}f}%"


def _delta_pct(prev, curr) -> Optional[float]:
    if prev is None or curr is None or pd.isna(prev) or pd.isna(curr):
        return None
    if prev == 0:
        return None
    return float(curr - prev) / abs(float(prev))


def _arrow(d: Optional[float]) -> str:
    if d is None:
        return "—"
    if d > DELTA_MOVING_PCT:
        return "↑"
    if d < -DELTA_MOVING_PCT:
        return "↓"
    return "→"


def _label(row) -> str:
    """`Brand / MODEL`. Brand is stored lowercase in the loader, so we
    title-case it for display. Falls back to bare Model when brand is
    missing or unknown."""
    model = str(row.get("model", "")).strip()
    brand = str(row.get("brand", "")).strip()
    if brand and brand.lower() not in ("nan", "none", "unknown", ""):
        return f"{brand.title()} / {model}"
    return model


# ---------------------------------------------------------------------
# Aggregators
# ---------------------------------------------------------------------
def _weekly_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    return (df.groupby("week_num", as_index=False)
              .agg(sales=("sales", "sum"),
                   units=("units", "sum"))
              .sort_values("week_num")
              .reset_index(drop=True))


def _model_totals_for_week(df: pd.DataFrame, wk: int) -> pd.DataFrame:
    w = df[df["week_num"] == wk]
    if w.empty:
        return pd.DataFrame()
    agg = dict(sales=("sales", "sum"), units=("units", "sum"))
    if "brand" in w.columns:
        agg["brand"] = ("brand", "first")
    return w.groupby("model", as_index=False).agg(**agg)


# ---------------------------------------------------------------------
# Builders — facts only
# ---------------------------------------------------------------------
def _build_headline(weekly: pd.DataFrame) -> list[dict]:
    if weekly.empty:
        return []
    out: list[dict] = []
    latest = weekly.iloc[-1]
    prev   = weekly.iloc[-2] if len(weekly) >= 2 else None

    if prev is not None:
        gmv_d   = _delta_pct(prev["sales"], latest["sales"])
        units_d = _delta_pct(prev["units"], latest["units"])
        text = (f"W{int(latest['week_num'])}: GMV {_inr(latest['sales'])} "
                f"({_arrow(gmv_d)} {_pct(gmv_d)} WoW), "
                f"{int(latest['units']):,} units "
                f"({_arrow(units_d)} {_pct(units_d)}).")
        kind = "positive" if (gmv_d or 0) >= 0 else "warning"
        out.append({"kind": kind, "text": text, "metric": "gmv",
                    "delta_pct": gmv_d, "weight": 1.0})

        if len(weekly) >= 5:
            trail = weekly.iloc[-5:-1]
            base_gmv   = float(trail["sales"].mean())
            base_units = float(trail["units"].mean())
            vs_gmv   = _delta_pct(base_gmv, latest["sales"])
            vs_units = _delta_pct(base_units, latest["units"])
            text2 = (f"vs prior 4-week mean: GMV {_inr(base_gmv)} "
                     f"({_arrow(vs_gmv)} {_pct(vs_gmv)}), "
                     f"units {int(round(base_units)):,} "
                     f"({_arrow(vs_units)} {_pct(vs_units)}).")
            out.append({"kind": "neutral", "text": text2, "metric": "gmv",
                        "delta_pct": vs_gmv, "weight": 0.9})
    else:
        text = (f"W{int(latest['week_num'])}: GMV {_inr(latest['sales'])}, "
                f"{int(latest['units']):,} units. "
                f"Single-week view — pick ≥2 weeks for WoW.")
        out.append({"kind": "neutral", "text": text, "metric": "gmv",
                    "delta_pct": None, "weight": 1.0})
    return out


def _build_movers(df: pd.DataFrame, weekly: pd.DataFrame) -> list[dict]:
    if len(weekly) < 2 or df.empty:
        return []
    out: list[dict] = []
    latest_wk = int(weekly.iloc[-1]["week_num"])
    prev_wk   = int(weekly.iloc[-2]["week_num"])
    cur = _model_totals_for_week(df, latest_wk)
    prv = _model_totals_for_week(df, prev_wk)
    if cur.empty or prv.empty:
        return []
    j = cur.merge(prv, on="model", how="outer", suffixes=("_cur", "_prv")).fillna({
        "sales_cur": 0, "sales_prv": 0, "units_cur": 0, "units_prv": 0,
    })
    bc = j.get("brand_cur", pd.Series([""] * len(j)))
    bp = j.get("brand_prv", pd.Series([""] * len(j)))
    j["brand"] = bc.where(bc.astype(str).str.len() > 0, bp).fillna("")

    j["units_d_abs"] = j["units_cur"] - j["units_prv"]
    j["units_d_pct"] = np.where(j["units_prv"] > 0,
                                j["units_d_abs"] / j["units_prv"],
                                np.nan)

    # Growers — ≥10 units gained AND ≥20% WoW AND ≥10 units this week
    growers = j[(j["units_d_abs"] >= 10)
                & (j["units_d_pct"].fillna(0) >= 0.2)
                & (j["units_cur"] >= 10)].copy()
    growers = growers.sort_values("units_d_abs", ascending=False).head(4)
    for _, g in growers.iterrows():
        text = (f"Top grower: **{_label(g)}** — units "
                f"{int(g['units_prv'])} → {int(g['units_cur'])} "
                f"({_pct(g['units_d_pct'])} WoW), GMV "
                f"{_inr(g['sales_prv'])} → {_inr(g['sales_cur'])}.")
        out.append({"kind": "positive", "text": text, "metric": "units",
                    "delta_pct": float(g["units_d_pct"]) if pd.notna(g["units_d_pct"]) else None,
                    "weight": 0.85})

    # Drainers — ≥10 units lost AND ≤-20% WoW AND ≥10 units prev
    drainers = j[(j["units_d_abs"] <= -10)
                 & (j["units_d_pct"].fillna(0) <= -0.2)
                 & (j["units_prv"] >= 10)].copy()
    drainers = drainers.sort_values("units_d_abs").head(4)
    for _, d in drainers.iterrows():
        text = (f"Drainer: **{_label(d)}** — units "
                f"{int(d['units_prv'])} → {int(d['units_cur'])} "
                f"({_pct(d['units_d_pct'])} WoW), GMV "
                f"{_inr(d['sales_prv'])} → {_inr(d['sales_cur'])}.")
        out.append({"kind": "negative", "text": text, "metric": "units",
                    "delta_pct": float(d["units_d_pct"]) if pd.notna(d["units_d_pct"]) else None,
                    "weight": 0.85})
    return out


def _row_brand(r) -> dict:
    return {"model": r["model"],
            "brand": r.get("brand_cur") or r.get("brand_prv") or ""}


def _build_anomalies(df: pd.DataFrame, weekly: pd.DataFrame) -> list[dict]:
    """Only flag what the data itself proves: a model that was selling
    last week went to zero this week.  Spike-style 'verify the feed'
    flags removed — they're data-quality assertions, not insights."""
    if len(weekly) < 2 or df.empty:
        return []
    out: list[dict] = []
    latest_wk = int(weekly.iloc[-1]["week_num"])
    prev_wk   = int(weekly.iloc[-2]["week_num"])
    cur = _model_totals_for_week(df, latest_wk)
    prv = _model_totals_for_week(df, prev_wk)
    if cur.empty or prv.empty:
        return []
    j = cur.merge(prv, on="model", how="inner", suffixes=("_cur", "_prv"))
    if j.empty:
        return []

    sudden = j[(j["units_prv"] >= 10) & (j["units_cur"] == 0)].copy()
    sudden = sudden.sort_values("units_prv", ascending=False).head(4)
    for _, s in sudden.iterrows():
        text = (f"**{_label(_row_brand(s))}** went from "
                f"{int(s['units_prv'])} units (W{prev_wk}) to "
                f"0 units (W{latest_wk}).")
        out.append({"kind": "warning", "text": text, "metric": "units",
                    "delta_pct": -1.0, "weight": 0.85})
    return out


def _build_inventory(df: pd.DataFrame, weekly: pd.DataFrame,
                     inventory_by_model: dict) -> list[dict]:
    """inventory_by_model: dict[(brand_lower, MODEL_UPPER)] → units.
    Empty / None drops the whole section — we don't speculate without
    real inventory numbers."""
    if weekly.empty or not inventory_by_model:
        return []
    out: list[dict] = []
    latest_wk = int(weekly.iloc[-1]["week_num"])
    w = df[df["week_num"] == latest_wk]
    if w.empty:
        return out

    agg = dict(units=("units", "sum"), sales=("sales", "sum"))
    if "brand" in w.columns:
        agg["brand"] = ("brand", "first")
    mdl_cur = w.groupby("model", as_index=False).agg(**agg)
    if mdl_cur.empty:
        return out

    last4_weeks = weekly.tail(4)["week_num"].tolist()
    win = df[df["week_num"].isin(last4_weeks)]
    mdl_4w = (win.groupby("model", as_index=False)
                 .agg(units_4w=("units", "sum")))
    mdl = mdl_cur.merge(mdl_4w, on="model", how="left")
    mdl["units_4w"] = mdl["units_4w"].fillna(0)

    def _inv(row):
        brand = str(row.get("brand", "")).strip().lower()
        model = str(row["model"]).upper().strip()
        return int(inventory_by_model.get((brand, model), 0))

    mdl["inv"] = mdl.apply(_inv, axis=1)
    n_weeks = max(len(last4_weeks), 1)
    mdl["weekly_velocity"] = mdl["units_4w"] / n_weeks
    mdl["days_cover"] = np.where(mdl["weekly_velocity"] > 0,
                                 mdl["inv"] / (mdl["weekly_velocity"] / 7.0),
                                 np.nan)

    # Hot seller × low cover — pure derived fact
    risky = mdl[(mdl["units_4w"] >= 20)
                & (mdl["days_cover"].between(0, LOW_COVER_DAYS))
                & (mdl["inv"] > 0)].copy()
    risky = risky.sort_values("units_4w", ascending=False).head(4)
    for _, r in risky.iterrows():
        text = (f"**{_label(r)}** — {int(r['units_4w'])} units sold across "
                f"last {n_weeks}w, {int(r['inv'])} units on hand → "
                f"**{int(r['days_cover'])} days of cover** at current velocity.")
        out.append({"kind": "warning", "text": text, "metric": "inventory",
                    "delta_pct": None, "weight": 0.85})

    # Dead stock — sitting capital with no recent movement
    dead = mdl[(mdl["units_4w"] == 0)
               & (mdl["inv"] >= DEAD_STOCK_INV_MIN)].copy()
    dead = dead.sort_values("inv", ascending=False).head(4)
    for _, d in dead.iterrows():
        text = (f"**{_label(d)}** — {int(d['inv'])} units on hand, "
                f"0 units sold last {n_weeks}w.")
        out.append({"kind": "negative", "text": text, "metric": "inventory",
                    "delta_pct": None, "weight": 0.8})
    return out


def _build_trajectory(weekly: pd.DataFrame) -> list[dict]:
    if len(weekly) < 4:
        return []
    out: list[dict] = []
    first = weekly.iloc[0]
    last  = weekly.iloc[-1]

    d_gmv = _delta_pct(first["sales"], last["sales"])
    if d_gmv is not None and abs(d_gmv) >= 0.10:
        kind = "positive" if d_gmv > 0 else "warning"
        text = (f"GMV across the window: W{int(first['week_num'])} "
                f"{_inr(first['sales'])} → W{int(last['week_num'])} "
                f"{_inr(last['sales'])} ({_pct(d_gmv)} over {len(weekly)} weeks).")
        out.append({"kind": kind, "text": text, "metric": "gmv",
                    "delta_pct": d_gmv, "weight": 0.65})

    d_u = _delta_pct(first["units"], last["units"])
    if d_u is not None and abs(d_u) >= 0.10:
        kind = "positive" if d_u > 0 else "warning"
        text = (f"Units across the window: {int(first['units']):,} → "
                f"{int(last['units']):,} ({_pct(d_u)} over {len(weekly)} weeks).")
        out.append({"kind": kind, "text": text, "metric": "units",
                    "delta_pct": d_u, "weight": 0.55})
    return out


def _build_actions(inventory: list[dict]) -> list[dict]:
    """Only data-derivable actions.  Restock and liquidate are both
    direct consequences of inventory + sales velocity — no external
    context (margin, capacity, marketing budget) required.

    'Push the top grower' and similar were removed: sales-only data
    can't justify spending money to amplify."""
    out: list[dict] = []

    for inv in inventory:
        if inv["kind"] == "warning" and "days of cover" in inv["text"]:
            try:
                label = inv["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Reorder {label}** — current cover is below 14 days at observed velocity.",
                    "metric": None, "delta_pct": None, "weight": 1.0,
                })
                break
            except IndexError:
                pass

    for inv in inventory:
        if inv["kind"] == "negative" and "0 units sold last" in inv["text"]:
            try:
                label = inv["text"].split("**")[1]
                out.append({
                    "kind": "action",
                    "text": f"**Liquidate {label}** — inventory on hand with zero sales in the trailing window.",
                    "metric": None, "delta_pct": None, "weight": 0.8,
                })
                break
            except IndexError:
                pass

    return out[:3]


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------
def build_sales_insights(df: pd.DataFrame,
                         selected_weeks: list[int] | None = None,
                         inventory_by_model: dict | None = None) -> dict[str, Any]:
    """Turn a row-level sales DataFrame into structured insights.

    Args:
        df: rows with at least `model`, `week_num`, `units`, `sales`
            (and optionally `brand`).
        selected_weeks: explicit week_num filter; default = all weeks in df.
        inventory_by_model: dict[(brand_lower, MODEL_UPPER)] → units at
            the latest selected week.  Empty / None drops the inventory
            section entirely (we don't speculate without real numbers).
    """
    if inventory_by_model is None:
        inventory_by_model = {}

    if df is None or df.empty:
        return {
            "window": {"weeks": [], "weeks_count": 0, "label": ""},
            "headline": [], "efficiency": [], "movers": [],
            "anomalies": [], "inventory": [], "trajectory": [],
            "actions": [],
            "empty_reason": "No sales data after filters — widen the selection.",
        }

    df = df.copy()
    df["week_num"] = pd.to_numeric(df["week_num"], errors="coerce")
    df = df[df["week_num"].notna()]

    if "model" in df.columns:
        m = df["model"].astype(str).str.strip()
        df = df[m.ne("") & m.str.lower().ne("nan") & m.str.lower().ne("none")]

    weeks_in_data = sorted(df["week_num"].dropna().unique().astype(int).tolist())
    if selected_weeks:
        weeks_used = [w for w in selected_weeks if w in weeks_in_data]
        if not weeks_used:
            weeks_used = weeks_in_data
    else:
        weeks_used = weeks_in_data
    weeks_used = weeks_used[-12:]
    df = df[df["week_num"].isin(weeks_used)]

    weekly = _weekly_totals(df)
    label = (f"W{weeks_used[0]}-W{weeks_used[-1]}"
             if len(weeks_used) >= 2 else
             f"W{weeks_used[0]}" if weeks_used else "")

    if weekly.empty:
        return {
            "window": {"weeks": weeks_used, "weeks_count": len(weeks_used), "label": label},
            "headline": [], "efficiency": [], "movers": [],
            "anomalies": [], "inventory": [], "trajectory": [],
            "actions": [],
            "empty_reason": "No weekly aggregates — check the data slice.",
        }

    headline   = _build_headline(weekly)
    movers     = _build_movers(df, weekly)
    anomalies  = _build_anomalies(df, weekly)
    inventory  = _build_inventory(df, weekly, inventory_by_model)
    trajectory = _build_trajectory(weekly)
    actions    = _build_actions(inventory)

    return {
        "window": {"weeks": weeks_used, "weeks_count": len(weeks_used), "label": label},
        "headline":   headline,
        "efficiency": [],   # no analogue in sales-only data; kept empty for UI shape parity
        "movers":     movers,
        "anomalies":  anomalies,
        "inventory":  inventory,
        "trajectory": trajectory,
        "actions":    actions,
    }
