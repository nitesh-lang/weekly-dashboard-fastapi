"""Key Points engine — the ranked "what actually matters this week" layer.

One shared computation feeding BOTH surfaces the operator opens:
  * the top of weekly_brief.md (/api/insights page)
  * ai_context.json["key_points"] (AI chat + any consumer)

House rules (feedback_real_insights_only): every point cites a real number
against a real baseline (prior week / trailing weeks); a section with no
qualifying signal is SKIPPED, never padded; Fossil is excluded at this
presentation layer like every other route; ₹ renders as Cr / L / K.

Ranking is by absolute ₹ impact — a ₹12L channel swing outranks a ₹40K
model move regardless of percentage drama.  Max 7 points so "key" stays
meaningful.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
SALES_CSV = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
INV_CSV   = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
AMS_CSV   = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"

EXCLUDED_BRANDS = ("fossil",)
MAX_POINTS = 7


def _inr(v: float) -> str:
    v = float(v or 0)
    a = abs(v)
    if a >= 1e7:
        return f"₹{v/1e7:.2f} Cr"
    if a >= 1e5:
        return f"₹{v/1e5:.1f} L"
    if a >= 1e3:
        return f"₹{v/1e3:.0f}K"
    return f"₹{v:.0f}"


def _pct(v: Optional[float]) -> str:
    return f"{v:+.1f}%" if v is not None and np.isfinite(v) else "n/a"


def _wn(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.extract(r"(\d+)", expand=False),
                         errors="coerce")


def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        from weekly_app.core.df_cache import load_csv_cached
        df = load_csv_cached(path)
    except Exception:
        df = pd.read_csv(path)
    if "brand" in df.columns:
        bn = df["brand"].astype(str).str.strip().str.lower()
        df = df[~bn.isin(EXCLUDED_BRANDS)].copy()
    if "week" in df.columns:
        df["wn"] = _wn(df["week"])
        df = df.dropna(subset=["wn"])
        df["wn"] = df["wn"].astype(int)
    return df


def compute_key_points() -> List[Dict[str, Any]]:
    """Returns points sorted by |₹ impact| desc, capped at MAX_POINTS.
    Each: {icon, kind, text, impact_inr}.  Empty list when snapshots are
    missing — callers render nothing rather than something invented."""
    s = _load(SALES_CSV)
    if s.empty or "wn" not in s.columns:
        return []
    latest = int(s["wn"].max())
    prev = latest - 1
    cur, prv = s[s.wn == latest], s[s.wn == prev]
    if cur.empty or prv.empty:
        return []

    pts: List[Dict[str, Any]] = []

    def add(icon: str, kind: str, text: str, impact: float) -> None:
        pts.append({"icon": icon, "kind": kind, "text": text,
                    "impact_inr": round(float(impact), 0)})

    # 1 — GMV WoW with the channel that actually drove it
    g_c, g_p = cur["gmv"].sum(), prv["gmv"].sum()
    if g_p > 0:
        ch_delta = (cur.groupby("channel")["gmv"].sum()
                    .sub(prv.groupby("channel")["gmv"].sum(), fill_value=0))
        drv = ch_delta.abs().idxmax()
        add("🟢" if g_c >= g_p else "🔴", "gmv_wow",
            f"GMV {_inr(g_c)} ({_pct((g_c-g_p)/g_p*100)} WoW, {_inr(g_c-g_p)}); "
            f"largest driver: {drv} {_inr(ch_delta[drv])}",
            g_c - g_p)

    # 2 — best-in-trailing-12-weeks record
    trail = s[s.wn.between(latest - 11, latest)].groupby("wn")["gmv"].sum()
    if len(trail) >= 4 and trail.idxmax() == latest:
        add("🏆", "record",
            f"Best GMV week of the last {len(trail)}: {_inr(g_c)} "
            f"(previous best {_inr(trail.drop(index=latest).max())})",
            g_c - trail.drop(index=latest).max())

    # 3 — biggest model gainer and drainer in ₹
    md = (cur.groupby(["brand", "model"])["gmv"].sum()
          .sub(prv.groupby(["brand", "model"])["gmv"].sum(), fill_value=0)
          .sort_values())
    if not md.empty:
        (b_lo, m_lo), d_lo = md.index[0], md.iloc[0]
        (b_hi, m_hi), d_hi = md.index[-1], md.iloc[-1]
        cur_hi = cur[(cur.brand == b_hi) & (cur.model == m_hi)]["gmv"].sum()
        cur_lo = cur[(cur.brand == b_lo) & (cur.model == m_lo)]["gmv"].sum()
        if d_hi > 0:
            add("🟢", "top_gainer",
                f"{b_hi} {m_hi} added {_inr(d_hi)} WoW → {_inr(cur_hi)}", d_hi)
        if d_lo < 0:
            add("🔴", "top_drainer",
                f"{b_lo} {m_lo} lost {_inr(abs(d_lo))} WoW → {_inr(cur_lo)}", d_lo)

    # 4 — channel that vanished (sold last week, zero this week)
    gone = (prv.groupby("channel")["gmv"].sum()
            .loc[lambda x: x > 10_000]
            .drop(index=cur.groupby("channel")["gmv"].sum()
                  .loc[lambda x: x > 0].index, errors="ignore"))
    for ch, val in gone.sort_values(ascending=False).head(2).items():
        add("⚠️", "channel_missing",
            f"{ch} is ZERO this week (was {_inr(val)} in W{prev}) — "
            f"real drop or missing upload?", -val)

    # 5 — ads efficiency: TACoS move + wasted spend
    a = _load(AMS_CSV)
    if not a.empty and {"spend", "wn"}.issubset(a.columns):
        sp_c, sp_p = a[a.wn == latest]["spend"].sum(), a[a.wn == prev]["spend"].sum()
        if sp_c > 0 and g_c > 0 and sp_p > 0 and g_p > 0:
            t_c, t_p = sp_c / g_c * 100, sp_p / g_p * 100
            if abs(t_c - t_p) >= 1.0:
                add("🟢" if t_c < t_p else "🟡", "tacos",
                    f"TACoS {t_c:.1f}% (was {t_p:.1f}%) on {_inr(sp_c)} spend",
                    -(t_c - t_p) / 100 * g_c)
        acur = a[a.wn == latest]
        if {"sales", "model", "brand"}.issubset(acur.columns):
            waste = acur[(acur["sales"].fillna(0) == 0) & (acur["spend"] > 1000)]
            wsum = waste["spend"].sum()
            if wsum > 5000:
                top = waste.sort_values("spend", ascending=False).iloc[0]
                add("🔴", "wasted_spend",
                    f"{_inr(wsum)} ad spend on {len(waste)} zero-sale models "
                    f"(worst: {top['brand']} {top['model']} {_inr(top['spend'])})",
                    -wsum)

    # 6 — stock-out risk on a top seller: cover < 3 weeks
    inv = _load(INV_CSV)
    if not inv.empty and {"inventory_units", "model", "wn"}.issubset(inv.columns):
        vel = cur.groupby(["brand", "model"]).agg(u=("units_sold", "sum"),
                                                  g=("gmv", "sum"))
        onh = (inv[inv.wn == inv.wn.max()]
               .groupby(["brand", "model"])["inventory_units"].sum())
        j = vel.join(onh, how="inner").dropna()
        j = j[(j.u > 10)]
        if not j.empty:
            j["cover"] = j["inventory_units"] / j["u"]
            risk = j[j.cover < 3].sort_values("g", ascending=False)
            if not risk.empty:
                (b, m), r = risk.index[0], risk.iloc[0]
                add("⏳", "stock_risk",
                    f"{b} {m} has {r.cover:.1f} wks cover "
                    f"({int(r['inventory_units'])} u vs {int(r.u)} u/wk) while doing "
                    f"{_inr(r.g)}/wk — restock or lose it",
                    -r.g)

    pts.sort(key=lambda p: abs(p["impact_inr"]), reverse=True)
    return pts[:MAX_POINTS]


def render_md(points: List[Dict[str, Any]]) -> str:
    """Markdown block for the top of weekly_brief.md.  Empty string when
    there is nothing worth saying."""
    if not points:
        return ""
    lines = ["## ⚡ Key Points", ""]
    lines += [f"- {p['icon']} {p['text']}" for p in points]
    lines.append("")
    return "\n".join(lines)
