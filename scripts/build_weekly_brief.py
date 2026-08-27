"""Weekly brief generator — deterministic, template-based, NO LLM call.

Reads the operator's processed snapshots (sales / inventory / ads) and
writes a detailed Markdown brief to data/processed/weekly_brief.md.

Run standalone:
    python scripts/build_weekly_brief.py

Or via the weekly cron — the workflow runs this after step 4 finishes,
so every refresh produces a fresh brief without an API key.

Sections (in this order):
    Headline           — portfolio GMV / units / spend / ROAS this week
    What went well     — top WoW risers + brand standouts
    What to watch      — top WoW fallers + low-stock + spend efficiency risks
    Suggested actions  — concrete moves driven by the signals above
    Brand briefs       — one detailed paragraph per brand
    Channel + category — mix + movers
    Alerts             — anything the existing alerts engine flagged

Output is Markdown the Insights page renders as-is.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SALES_CSV   = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
INV_CSV     = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
AMS_CSV     = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
INBOUND_CSV = ROOT / "data" / "processed" / "inbound_snapshot.csv"
OUT_FILE    = ROOT / "data" / "processed" / "weekly_brief.md"

# Brands operator wants in the brief.  Fossil is excluded ENTIRELY —
# its ad performance is non-comparable (no AMS account) and its sales
# rhythm is different enough that mixing it into portfolio rollups
# distorts the weekly story.
EXCLUDED_BRANDS = ("fossil",)
ALL_BRANDS      = ("Audio Array", "Nexlev", "White Mulberry", "Tonor")
AMS_BRANDS      = ALL_BRANDS

# Thresholds — kept consistent with the existing alerts engine.
LOW_COVER_DAYS = 14
HIGH_ACOS      = 0.40   # 40%
LOW_ROAS       = 2.0


# ─── Helpers ─────────────────────────────────────────────────────────
def _wn(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.extract(r"(\d+)")[0], errors="coerce")

def fmt_inr(v: float) -> str:
    if v is None or pd.isna(v) or not pd.notnull(v):
        return "—"
    v = float(v)
    if abs(v) >= 1e7: return f"₹{v / 1e7:.2f}Cr"
    if abs(v) >= 1e5: return f"₹{v / 1e5:.2f}L"
    if abs(v) >= 1e3: return f"₹{v / 1e3:.1f}K"
    return f"₹{v:.0f}"

def fmt_int(v) -> str:
    if v is None or pd.isna(v): return "—"
    return f"{int(v):,}"

def fmt_pct(v, sign: bool = True) -> str:
    if v is None or pd.isna(v): return "—"
    s = "+" if (sign and v > 0) else ""
    return f"{s}{v:.1f}%"

def trend_arrow(v: Optional[float]) -> str:
    if v is None or pd.isna(v): return "·"
    if v >= 5:  return "🟢"
    if v <= -5: return "🔴"
    return "🟡"

def wow_pct(curr: float, prev: float) -> Optional[float]:
    if prev is None or prev == 0 or pd.isna(prev): return None
    return round((curr - prev) / prev * 100, 1)


# ─── Section builders ────────────────────────────────────────────────
def headline_section(s: pd.DataFrame, a: pd.DataFrame, latest_wn: int) -> str:
    prev_wn = latest_wn - 1
    cur = s[s["wn"] == latest_wn]
    prv = s[s["wn"] == prev_wn]
    gmv_cur, gmv_prv = float(cur["gmv"].sum()), float(prv["gmv"].sum())
    u_cur,   u_prv   = float(cur["units_sold"].sum()), float(prv["units_sold"].sum())

    # 4-week trailing for context
    last4 = s[s["wn"].between(latest_wn - 3, latest_wn)]
    prior4 = s[s["wn"].between(latest_wn - 7, latest_wn - 4)]
    gmv_4   = float(last4["gmv"].sum())
    gmv_p4  = float(prior4["gmv"].sum())

    a_cur = a[pd.to_numeric(a["week"], errors="coerce") == latest_wn]
    a_prv = a[pd.to_numeric(a["week"], errors="coerce") == prev_wn]
    spend_cur, spend_prv = float(a_cur["Spend"].sum()), float(a_prv["Spend"].sum())
    attr_cur,  attr_prv  = float(a_cur["attributed_sales"].sum()), float(a_prv["attributed_sales"].sum())
    gmv_ams_cur = float(a_cur["gmv"].sum())

    roas_cur  = (attr_cur / spend_cur) if spend_cur > 0 else None
    acos_cur  = (spend_cur / attr_cur) if attr_cur > 0 else None
    tacos_cur = (spend_cur / gmv_ams_cur) if gmv_ams_cur > 0 else None

    # Pick the biggest single contributor to write a narrative
    by_brand = cur.groupby("brand").agg(gmv=("gmv","sum"), u=("units_sold","sum")).sort_values("gmv", ascending=False)
    top_brand = by_brand.index[0] if not by_brand.empty else ""
    top_share = (by_brand.iloc[0]["gmv"] / max(gmv_cur, 1)) * 100 if not by_brand.empty else 0

    lines = []
    lines.append(f"## This week at a glance")
    lines.append("")

    wow_gmv   = wow_pct(gmv_cur, gmv_prv)
    wow_units = wow_pct(u_cur, u_prv)
    wow_4w    = wow_pct(gmv_4, gmv_p4)
    direction = "a recovery week" if (wow_gmv or 0) > 5 else (
                "a soft week"     if (wow_gmv or 0) < -5 else
                "a steady week")
    narr = (
        f"Week {latest_wn} closed at **{fmt_inr(gmv_cur)} GMV** "
        f"({fmt_pct(wow_gmv)} WoW, {fmt_pct(wow_4w)} vs prior 4w) on "
        f"**{fmt_int(u_cur)} units** ({fmt_pct(wow_units)} WoW). "
        f"This was {direction}, led by **{top_brand}** at "
        f"{top_share:.0f}% of portfolio GMV."
    )
    lines.append(narr)
    lines.append("")

    # Ad efficiency single-line
    lines.append(
        f"Ad spend **{fmt_inr(spend_cur)}** ({fmt_pct(wow_pct(spend_cur, spend_prv))}) — "
        f"ROAS **{roas_cur:.2f}x** · "
        f"ACOS **{(acos_cur*100):.1f}%** · "
        f"TACOS **{(tacos_cur*100):.1f}%**."
        if roas_cur is not None else f"Ad spend **{fmt_inr(spend_cur)}** this week."
    )
    lines.append("")

    # Realised margin — snapshot carries sales_nlc (landed cost of sold
    # units), so contribution margin is computable, not estimated.  Only
    # rendered when cost coverage is meaningful (>60% of GMV has NLC).
    if "sales_nlc" in cur.columns:
        nlc_cur, nlc_prv = float(cur["sales_nlc"].sum()), float(prv["sales_nlc"].sum())
        cov = cur[cur["sales_nlc"] > 0]["gmv"].sum() / max(gmv_cur, 1)
        if nlc_cur > 0 and cov > 0.6:
            mg_c = (gmv_cur - nlc_cur) / gmv_cur * 100
            mg_p = (gmv_prv - nlc_prv) / gmv_prv * 100 if gmv_prv > 0 and nlc_prv > 0 else None
            lines.append(
                f"Realised margin **{mg_c:.1f}%** ({fmt_inr(gmv_cur - nlc_cur)} over landed cost)"
                + (f" vs {mg_p:.1f}% last week"
                   f" ({'+' if mg_c >= mg_p else ''}{mg_c - mg_p:.1f} pt)." if mg_p is not None else ".")
            )
            lines.append("")

    # Channel pulse — the single best and worst ₹ movers, so the glance
    # answers "where did the week happen" without opening the tables.
    ch_d = (cur.groupby("channel")["gmv"].sum()
            .sub(prv.groupby("channel")["gmv"].sum(), fill_value=0))
    if len(ch_d) >= 2:
        best, worst = ch_d.idxmax(), ch_d.idxmin()
        if ch_d[best] > 0 or ch_d[worst] < 0:
            bits = []
            if ch_d[best] > 0:
                bits.append(f"**{best}** {fmt_inr(ch_d[best])} up")
            if ch_d[worst] < 0 and worst != best:
                bits.append(f"**{worst}** {fmt_inr(abs(ch_d[worst]))} down")
            lines.append("Channel pulse: " + " · ".join(bits) + " WoW.")
            lines.append("")
    return "\n".join(lines)


def movers_sections(s: pd.DataFrame, latest_wn: int) -> str:
    """Top WoW risers + fallers at the (brand × model) grain.
    Filtered to models with at least 10 units last week to avoid noise."""
    prev_wn = latest_wn - 1
    cur = (s[s["wn"] == latest_wn]
           .groupby(["brand", "model"]).agg(units=("units_sold","sum"), gmv=("gmv","sum")).reset_index())
    prv = (s[s["wn"] == prev_wn]
           .groupby(["brand", "model"]).agg(prev_units=("units_sold","sum"), prev_gmv=("gmv","sum")).reset_index())
    m = cur.merge(prv, on=["brand", "model"], how="outer").fillna(0)
    m = m[(m["units"] >= 10) | (m["prev_units"] >= 10)]
    m["wow_units"]  = m.apply(lambda r: wow_pct(r["units"], r["prev_units"]) if r["prev_units"] > 0 else None, axis=1)
    m["wow_gmv"]    = m.apply(lambda r: wow_pct(r["gmv"],   r["prev_gmv"])   if r["prev_gmv"] > 0   else None, axis=1)
    m["delta_gmv"]  = m["gmv"] - m["prev_gmv"]

    risers = m.dropna(subset=["wow_gmv"]).sort_values("delta_gmv", ascending=False).head(5)
    fallers = m.dropna(subset=["wow_gmv"]).sort_values("delta_gmv", ascending=True).head(5)

    out = []
    out.append("## What went well")
    out.append("")
    if risers.empty:
        out.append("- Nothing notable above the noise threshold this week.")
    else:
        for _, r in risers.iterrows():
            arr = trend_arrow(r["wow_gmv"])
            out.append(
                f"- {arr} **{r['brand']} / {r['model']}** — "
                f"{fmt_int(r['units'])} units ({fmt_pct(r['wow_units'])} WoW), "
                f"GMV {fmt_inr(r['gmv'])} ({fmt_pct(r['wow_gmv'])})."
            )
    out.append("")
    out.append("## What to watch")
    out.append("")
    if fallers.empty:
        out.append("- No material declines this week.")
    else:
        for _, r in fallers.iterrows():
            arr = trend_arrow(r["wow_gmv"])
            out.append(
                f"- {arr} **{r['brand']} / {r['model']}** — "
                f"{fmt_int(r['units'])} units ({fmt_pct(r['wow_units'])} WoW), "
                f"GMV {fmt_inr(r['gmv'])} ({fmt_pct(r['wow_gmv'])})."
            )
    out.append("")

    # ── Momentum streaks — ≥3 consecutive weeks moving the same way.
    # A streak is a stronger signal than any single WoW: it separates a
    # trend from noise using only real weekly totals.
    wk_model = (s[s["wn"].between(latest_wn - 5, latest_wn)]
                .groupby(["brand", "model", "wn"])["gmv"].sum().reset_index())
    streaks_up, streaks_dn = [], []
    for (b, mo), g in wk_model.groupby(["brand", "model"]):
        g = g.sort_values("wn")
        if len(g) < 4 or g["gmv"].iloc[-1] < 20_000:
            continue
        diffs = g["gmv"].diff().dropna()
        run = 0
        for d in reversed(diffs.tolist()):
            if run >= 0 and d > 0:
                run = run + 1 if run >= 0 else run
            elif run <= 0 and d < 0:
                run = run - 1
            else:
                break
        cum = g["gmv"].iloc[-1] - g["gmv"].iloc[max(-1 - abs(run), -len(g))]
        if run >= 3:
            streaks_up.append((b, mo, run, cum, g["gmv"].iloc[-1]))
        elif run <= -3:
            streaks_dn.append((b, mo, run, cum, g["gmv"].iloc[-1]))
    if streaks_up or streaks_dn:
        out.append("### Momentum (3+ week streaks)")
        out.append("")
        for b, mo, run, cum, now in sorted(streaks_up, key=lambda x: -x[3])[:4]:
            out.append(f"- 📈 **{b} / {mo}** — up {run} weeks straight, "
                       f"{fmt_inr(cum)} added over the run → {fmt_inr(now)}/wk.")
        for b, mo, run, cum, now in sorted(streaks_dn, key=lambda x: x[3])[:4]:
            out.append(f"- 📉 **{b} / {mo}** — down {abs(run)} weeks straight, "
                       f"{fmt_inr(abs(cum))} lost over the run → {fmt_inr(now)}/wk.")
        out.append("")

    # ── Price realisation — ASP (gmv/units) moves ≥7% WoW on material
    # models.  Catches silent discounting (or price recovery) that a pure
    # GMV view hides: units up + GMV flat = you paid for the growth.
    pr = m[(m["units"] >= 10) & (m["prev_units"] >= 10) & (m["gmv"] > 20_000)].copy()
    if not pr.empty:
        pr["asp"] = pr["gmv"] / pr["units"]
        pr["prev_asp"] = pr["prev_gmv"] / pr["prev_units"]
        pr["asp_pct"] = (pr["asp"] - pr["prev_asp"]) / pr["prev_asp"] * 100
        moved = pr[abs(pr["asp_pct"]) >= 7].sort_values("asp_pct")
        if not moved.empty:
            out.append("### Price realisation")
            out.append("")
            for _, r in moved.head(5).iterrows():
                icon = "🏷️" if r["asp_pct"] < 0 else "💎"
                out.append(
                    f"- {icon} **{r['brand']} / {r['model']}** — ASP "
                    f"{fmt_inr(r['asp'])} vs {fmt_inr(r['prev_asp'])} last week "
                    f"({fmt_pct(r['asp_pct'])}); units {fmt_int(r['units'])} "
                    f"({fmt_pct(r['wow_units'])})."
                )
            out.append("")
    return "\n".join(out)


def inventory_section(inv: pd.DataFrame, s: pd.DataFrame, a: pd.DataFrame,
                      latest_wn: int) -> str:
    """Low-stock and dead-stock signals from the inventory snapshot
    against last-4w burn from the sales snapshot.  Ads-aware: a low-cover
    model that is also being advertised gets an explicit pause / scale-down
    directive — restocking advice alone hides that spend is accelerating
    the stock-out."""
    inv_l = inv[inv["wn"] == latest_wn].copy()
    # Normalised model key — sales has "AI-04 Red", inventory has
    # "AI-04 RED"; a strict-string merge would orphan the rows and
    # falsely flag healthy stock as dead.  Compare upper+stripped,
    # keep the inventory casing for display.
    inv_l["_mk"] = inv_l["model"].astype(str).str.strip().str.upper()
    inv_by_model = (inv_l.groupby(["brand","model","_mk"])["inventory_units"]
                      .sum().reset_index().rename(columns={"inventory_units":"stock"}))

    s4 = s[s["wn"].between(latest_wn - 3, latest_wn)].copy()
    s4["_mk"] = s4["model"].astype(str).str.strip().str.upper()
    burn4 = (s4.groupby(["brand","_mk"])["units_sold"].sum().reset_index()
             .rename(columns={"units_sold":"u4w"}))
    burn4["avg_weekly"] = burn4["u4w"] / 4.0
    j = inv_by_model.merge(burn4, on=["brand","_mk"], how="left").fillna({"u4w": 0, "avg_weekly": 0})
    j["cover_weeks"] = j.apply(
        lambda r: (r["stock"] / r["avg_weekly"]) if r["avg_weekly"] > 0 else None, axis=1
    )

    # This week's ad spend per model — powers the pause / scale-down calls.
    spend = {}
    if not a.empty and {"week", "brand", "model", "Spend"}.issubset(a.columns):
        ac = a[pd.to_numeric(a["week"], errors="coerce") == latest_wn]
        if not ac.empty:
            sp = ac.groupby(["brand", "model"])["Spend"].sum()
            spend = {(str(b).strip().lower(), str(m).strip().upper()): v
                     for (b, m), v in sp.items() if v > 0}

    def _spend_of(r) -> float:
        return spend.get((str(r["brand"]).strip().lower(), r["_mk"]), 0.0)

    # Low cover — has stock < 2 weeks AND moves at least 5 units/week
    low = j[(j["avg_weekly"] >= 5) & (j["cover_weeks"].notna()) & (j["cover_weeks"] <= 2.0)] \
            .sort_values("cover_weeks").head(8)

    # Watch band — 2-4 weeks cover WITH active ad spend: not yet a crisis,
    # but full-throttle ads will turn it into one before the reorder lands.
    watch = j[(j["avg_weekly"] >= 5) & (j["cover_weeks"].notna())
              & (j["cover_weeks"] > 2.0) & (j["cover_weeks"] <= 4.0)].copy()
    if not watch.empty:
        watch["_sp"] = watch.apply(_spend_of, axis=1)
        watch = watch[watch["_sp"] >= 500].sort_values("cover_weeks").head(6)

    # Dead stock — stock > 0, no sales in last 4w
    dead = j[(j["stock"] >= 30) & (j["avg_weekly"] == 0)] \
             .sort_values("stock", ascending=False).head(8)

    out = ["## Inventory health", ""]
    if not low.empty:
        out.append("**Low cover (≤ 2 weeks at current burn):**")
        for _, r in low.iterrows():
            cov = r["cover_weeks"]
            sp = _spend_of(r)
            ads = (f" **Ads: PAUSE** ({fmt_inr(sp)}/wk running — spend on a model "
                   f"you can't ship burns the budget and the rank it bought)."
                   if sp >= 500 else "")
            out.append(
                f"- 🔴 **{r['brand']} / {r['model']}** — {fmt_int(r['stock'])} units on hand, "
                f"~{r['avg_weekly']:.1f} u/week burn ({cov:.1f} weeks cover). Reorder.{ads}"
            )
        out.append("")
    if not watch.empty:
        out.append("**Scale ads down (2-4 weeks cover, ads still running):**")
        for _, r in watch.iterrows():
            out.append(
                f"- 🟠 **{r['brand']} / {r['model']}** — {r['cover_weeks']:.1f} weeks cover with "
                f"{fmt_inr(r['_sp'])}/wk ad spend. Halve spend until the reorder lands, "
                f"or stock runs out mid-campaign."
            )
        out.append("")
    if not dead.empty:
        out.append("**Dead stock (no sales last 4w, ≥30 units):**")
        for _, r in dead.iterrows():
            out.append(
                f"- 🟡 **{r['brand']} / {r['model']}** — {fmt_int(r['stock'])} units sitting idle."
            )
        out.append("")
    if low.empty and watch.empty and dead.empty:
        out.append("- Inventory levels look healthy across the portfolio this week.")
        out.append("")
    return "\n".join(out)


def ads_efficiency_section(a: pd.DataFrame, latest_wn: int) -> str:
    """Flag bad ROAS / high ACOS / spend-without-sales."""
    cur = a[pd.to_numeric(a["week"], errors="coerce") == latest_wn].copy()
    # Drop Fossil — no ad account
    cur = cur[cur["brand"].str.strip().str.lower() != "fossil"]
    by_m = (cur.groupby(["brand","model"])
              .agg(spend=("Spend","sum"), attr=("attributed_sales","sum"),
                   gmv=("gmv","sum"), units=("units","sum"))
              .reset_index())
    by_m["roas"]  = by_m.apply(lambda r: r["attr"] / r["spend"] if r["spend"] > 0 else None, axis=1)
    by_m["acos"]  = by_m.apply(lambda r: r["spend"] / r["attr"] if r["attr"] > 0 else None, axis=1)
    by_m["tacos"] = by_m.apply(lambda r: r["spend"] / r["gmv"] if r["gmv"] > 0 else None, axis=1)

    bad_roas    = by_m[(by_m["spend"] >= 500) & (by_m["roas"].notna()) & (by_m["roas"] < LOW_ROAS)] \
                   .sort_values("spend", ascending=False).head(6)
    high_acos   = by_m[(by_m["spend"] >= 500) & (by_m["acos"].notna()) & (by_m["acos"] > HIGH_ACOS)] \
                   .sort_values("acos", ascending=False).head(6)
    spend_only  = by_m[(by_m["spend"] >= 200) & (by_m["attr"] < 1)] \
                   .sort_values("spend", ascending=False).head(6)
    good_low_share = by_m[(by_m["roas"].notna()) & (by_m["roas"] >= 4) & (by_m["spend"] < 1000)] \
                     .sort_values("roas", ascending=False).head(5)

    out = ["## Ad efficiency this week", ""]
    if not good_low_share.empty:
        out.append("**ROAS strong, spend share low — room to bid up:**")
        for _, r in good_low_share.iterrows():
            out.append(
                f"- 🟢 **{r['brand']} / {r['model']}** — ROAS {r['roas']:.2f}x on only "
                f"{fmt_inr(r['spend'])} spend. Consider raising bid / budget."
            )
        out.append("")
    if not bad_roas.empty:
        out.append("**ROAS below threshold (< 2x):**")
        for _, r in bad_roas.iterrows():
            out.append(
                f"- 🔴 **{r['brand']} / {r['model']}** — ROAS {r['roas']:.2f}x on "
                f"{fmt_inr(r['spend'])} spend. Investigate bids / negatives."
            )
        out.append("")
    if not high_acos.empty:
        out.append("**ACOS > 40% (margin risk):**")
        for _, r in high_acos.iterrows():
            out.append(
                f"- 🔴 **{r['brand']} / {r['model']}** — ACOS {(r['acos']*100):.1f}% on "
                f"{fmt_inr(r['spend'])} spend."
            )
        out.append("")
    if not spend_only.empty:
        out.append("**Spending with no attribution:**")
        for _, r in spend_only.iterrows():
            out.append(
                f"- 🔴 **{r['brand']} / {r['model']}** — {fmt_inr(r['spend'])} spend, 0 attributed sales. Pause."
            )
        out.append("")
    if bad_roas.empty and high_acos.empty and spend_only.empty and good_low_share.empty:
        out.append("- Ad performance looks balanced — nothing flagged.")
        out.append("")
    return "\n".join(out)


def brand_briefs_section(s: pd.DataFrame, inv: pd.DataFrame, a: pd.DataFrame, latest_wn: int) -> str:
    prev_wn = latest_wn - 1
    out = ["## Brand briefs", ""]
    for brand in ALL_BRANDS:
        s_cur = s[(s["wn"] == latest_wn) & (s["brand"] == brand)]
        s_prv = s[(s["wn"] == prev_wn)  & (s["brand"] == brand)]
        if s_cur.empty and s_prv.empty:
            continue
        gmv_c, gmv_p = float(s_cur["gmv"].sum()), float(s_prv["gmv"].sum())
        u_c,   u_p   = float(s_cur["units_sold"].sum()), float(s_prv["units_sold"].sum())

        # Top model this week within the brand
        top = (s_cur.groupby("model").agg(gmv=("gmv","sum"), u=("units_sold","sum"))
               .sort_values("gmv", ascending=False))
        top_model = top.index[0] if not top.empty else None
        top_gmv   = top.iloc[0]["gmv"] if not top.empty else 0

        # Ads (skip Fossil)
        ads_line = ""
        if brand in AMS_BRANDS and not a.empty:
            ac = a[(pd.to_numeric(a["week"], errors="coerce") == latest_wn) & (a["brand"] == brand)]
            sp = float(ac["Spend"].sum())
            at = float(ac["attributed_sales"].sum())
            roas = (at / sp) if sp > 0 else None
            acos = (sp / at) if at > 0 else None
            if sp > 0:
                ads_line = (
                    f" Ad spend {fmt_inr(sp)} → "
                    f"ROAS {roas:.2f}x" + (f", ACOS {(acos*100):.1f}%." if acos is not None else ".")
                )

        # Inventory cover
        inv_b = inv[(inv["wn"] == latest_wn) & (inv["brand"] == brand)]
        stock = int(inv_b["inventory_units"].sum())

        arrow = trend_arrow(wow_pct(gmv_c, gmv_p))
        out.append(
            f"**{brand}** {arrow} — "
            f"GMV {fmt_inr(gmv_c)} ({fmt_pct(wow_pct(gmv_c, gmv_p))} WoW) on "
            f"{fmt_int(u_c)} units ({fmt_pct(wow_pct(u_c, u_p))}). "
            + (f"Top contributor **{top_model}** at {fmt_inr(top_gmv)}. " if top_model else "")
            + f"Total stock {fmt_int(stock)} units." + ads_line
        )
        out.append("")
    return "\n".join(out)


def channel_category_section(s: pd.DataFrame, latest_wn: int) -> str:
    prev_wn = latest_wn - 1
    by_ch_cur = s[s["wn"] == latest_wn].groupby("channel")["gmv"].sum().sort_values(ascending=False)
    by_ch_prv = s[s["wn"] == prev_wn].groupby("channel")["gmv"].sum()

    out = ["## Channel mix", ""]
    total = by_ch_cur.sum()
    for ch, gmv in by_ch_cur.head(6).items():
        share = (gmv / total * 100) if total > 0 else 0
        wow = wow_pct(gmv, float(by_ch_prv.get(ch, 0)))
        arrow = trend_arrow(wow)
        out.append(f"- {arrow} **{ch}** — {fmt_inr(gmv)} ({share:.1f}% of portfolio, {fmt_pct(wow)} WoW)")
    out.append("")

    cat = (s[s["wn"] == latest_wn].groupby("category_l0")
           .agg(gmv=("gmv","sum"), u=("units_sold","sum")).sort_values("gmv", ascending=False))
    out.append("## Top categories")
    out.append("")
    for catname, row in cat.head(5).iterrows():
        if not catname or pd.isna(catname): continue
        out.append(f"- **{catname}** — {fmt_inr(row['gmv'])} on {fmt_int(row['u'])} units.")
    out.append("")
    return "\n".join(out)


def expert_directions(s: pd.DataFrame, inv: pd.DataFrame, a: pd.DataFrame, latest_wn: int) -> str:
    """Cross-signal directives — each joins ≥2 of {sales trend, inventory
    cover, ads efficiency, ASIN type, category} and states the ₹
    consequence.  This is the "what would a good Amazon operator actually
    do" layer; single-signal observations stay in the sections above.
    Every play is guarded: no qualifying data → the play is skipped."""
    out: list[str] = []

    # Shared frames ------------------------------------------------------
    cur  = s[s["wn"] == latest_wn]
    s4   = s[s["wn"].between(latest_wn - 3, latest_wn)]
    if cur.empty:
        return ""
    gmv_now = cur.groupby(["brand", "model"])["gmv"].sum()

    # Weekly GMV per model for streaks
    wkm = (s[s["wn"].between(latest_wn - 3, latest_wn)]
           .groupby(["brand", "model", "wn"])["gmv"].sum().unstack("wn"))

    # Inventory cover at current burn
    inv_l = inv[inv["wn"] == inv["wn"].max()] if not inv.empty else pd.DataFrame()
    cover = pd.Series(dtype=float)
    if not inv_l.empty:
        onh = inv_l.groupby(["brand", "model"])["inventory_units"].sum()
        burn = s4.groupby(["brand", "model"])["units_sold"].sum() / 4.0
        cover = (onh / burn.replace(0, np.nan)).dropna()

    # Ads at model grain, this week + portfolio baselines
    A = pd.DataFrame()
    port_roas = port_tacos = None
    if not a.empty and "week" in a.columns:
        ac = a[pd.to_numeric(a["week"], errors="coerce") == latest_wn].copy()
        if not ac.empty and {"Spend", "attributed_sales"}.issubset(ac.columns):
            A = ac.groupby(["brand", "model"]).agg(
                spend=("Spend", "sum"), attr=("attributed_sales", "sum"),
                agmv=("gmv", "sum")).reset_index()
            tsp, tat, tg = A["spend"].sum(), A["attr"].sum(), A["agmv"].sum()
            port_roas  = tat / tsp if tsp > 0 else None
            port_tacos = tsp / tg if tg > 0 else None
            A = A.set_index(["brand", "model"])

    # Play 1 — scale into strength: rising 3wk + stock to absorb + ROAS
    # above portfolio.  The trifecta almost nobody checks together.
    if not wkm.empty and not A.empty and port_roas:
        for key in wkm.index:
            row = wkm.loc[key].dropna()
            if len(row) < 3 or not row.is_monotonic_increasing:
                continue
            if key not in A.index or key not in cover.index:
                continue
            sp, at = A.loc[key, "spend"], A.loc[key, "attr"]
            cv = cover.loc[key]
            g = gmv_now.get(key, 0)
            if sp < 500 or at / sp < port_roas * 1.3 or cv < 5 or g < 50_000:
                continue
            b, m = key
            out.append(
                f"- 🚀 **Scale {b} / {m}** — GMV up 3 straight wks to {fmt_inr(g)}, "
                f"ROAS {at/sp:.1f}x vs portfolio {port_roas:.1f}x, and {cv:.0f} wks of "
                f"stock to absorb growth. Lift spend {fmt_inr(sp)} → {fmt_inr(sp*1.5)}; "
                f"the constraint is budget, not demand or supply."
            )
            if sum(1 for l in out if l.startswith("- 🚀")) >= 3:
                break

    # Play 2 — cut ads before OOS: advertising a model you cannot ship is
    # paying to crash your own rank twice.
    if not A.empty and not cover.empty:
        risky = [(k, A.loc[k, "spend"], cover.loc[k])
                 for k in A.index.intersection(cover.index)
                 if A.loc[k, "spend"] > 2000 and cover.loc[k] <= 2.0]
        for (b, m), sp, cv in sorted(risky, key=lambda x: -x[1])[:3]:
            burn_w = s4[(s4.brand == b) & (s4.model == m)]["units_sold"].sum() / 4.0
            need = int(round(6 * burn_w))
            out.append(
                f"- ✂️ **Cut ads on {b} / {m} until restocked** — {fmt_inr(sp)}/wk spend "
                f"against {cv:.1f} wks cover. OOS mid-campaign burns the spend AND the "
                f"organic rank it bought; pause, order ~{need} u, resume at arrival."
            )

    # Play 3 — harvest Core, it should not need subsidy: Core-type models
    # whose TACoS runs ≥2x the portfolio while GMV is flat/down.
    if not A.empty and port_tacos and "asin_type" in cur.columns:
        core_models = set(
            map(tuple, cur[cur["asin_type"].astype(str).str.strip().str.lower() == "core"]
                [["brand", "model"]].drop_duplicates().itertuples(index=False, name=None)))
        prev_g = s[s["wn"] == latest_wn - 1].groupby(["brand", "model"])["gmv"].sum()
        picks = []
        for k in A.index:
            if k not in core_models:
                continue
            sp, ag = A.loc[k, "spend"], A.loc[k, "agmv"]
            if sp < 2000 or ag <= 0:
                continue
            t = sp / ag
            if t >= port_tacos * 2 and gmv_now.get(k, 0) <= prev_g.get(k, 1e18):
                picks.append((k, sp, t))
        for (b, m), sp, t in sorted(picks, key=lambda x: -x[1])[:3]:
            save = sp * 0.3
            out.append(
                f"- 🌾 **Harvest {b} / {m}** — a Core ASIN running TACoS {t*100:.0f}% vs "
                f"portfolio {port_tacos*100:.0f}% with GMV flat WoW. Mature listings keep "
                f"rank on organic momentum; trim ~30% ({fmt_inr(save)}/wk) and watch rank, "
                f"not ROAS."
            )

    # Play 4 — pricing didn't buy demand: ASP cut ≥7% that moved units ≤5%.
    # Give the discount back, GMV barely notices.
    curm = cur.groupby(["brand", "model"]).agg(u=("units_sold", "sum"), g=("gmv", "sum"))
    prvm = s[s["wn"] == latest_wn - 1].groupby(["brand", "model"]).agg(
        pu=("units_sold", "sum"), pg=("gmv", "sum"))
    pj = curm.join(prvm, how="inner")
    pj = pj[(pj.u >= 10) & (pj.pu >= 10) & (pj.g > 30_000)]
    if not pj.empty:
        pj["asp"], pj["pasp"] = pj.g / pj.u, pj.pg / pj.pu
        pj["dp"] = (pj.asp - pj.pasp) / pj.pasp * 100
        pj["du"] = (pj.u - pj.pu) / pj.pu * 100
        bad = pj[(pj.dp <= -7) & (pj.du <= 5)].sort_values("dp")
        for (b, m), r in bad.head(2).iterrows():
            back = (r.pasp - r.asp) * r.u
            out.append(
                f"- 💰 **Restore price on {b} / {m}** — ASP cut {abs(r.dp):.0f}% "
                f"({fmt_inr(r.pasp)} → {fmt_inr(r.asp)}) bought only {r.du:+.0f}% units. "
                f"The discount isn't converting; reverting recovers ~{fmt_inr(back)}/wk "
                f"margin at current volume."
            )

    # Play 5 — category tilt: put money where the category is moving.
    if "category_l0" in s.columns and not A.empty:
        cg = s4.groupby(["category_l0", "wn"])["gmv"].sum().unstack("wn")
        if cg.shape[1] >= 4:
            growth = (cg.iloc[:, -1] - cg.iloc[:, 0]) / cg.iloc[:, 0].replace(0, np.nan) * 100
            cat_of = cur.drop_duplicates(["brand", "model"]).set_index(["brand", "model"])["category_l0"]
            Asp = A.join(cat_of, how="left")
            cat_spend = Asp.groupby("category_l0")["spend"].sum()
            cat_gmv = cur.groupby("category_l0")["gmv"].sum()
            tot_sp, tot_g = cat_spend.sum(), cat_gmv.sum()
            if tot_sp > 0 and tot_g > 0:
                for cat in growth.dropna().sort_values(ascending=False).head(2).index:
                    gr = growth[cat]
                    sh_g = cat_gmv.get(cat, 0) / tot_g * 100
                    sh_s = cat_spend.get(cat, 0) / tot_sp * 100
                    if gr > 20 and sh_g - sh_s > 8 and cat_gmv.get(cat, 0) > 200_000:
                        out.append(
                            f"- 🧭 **Tilt budget toward {cat}** — category GMV {gr:+.0f}% over "
                            f"4 wks, now {sh_g:.0f}% of sales but only {sh_s:.0f}% of ad spend. "
                            f"The market is moving there faster than your budget is."
                        )

    if not out:
        return ""
    return "\n".join(["## Operator playbook (cross-signal)", "",
                      *out[:7], ""])


def suggested_actions(s: pd.DataFrame, inv: pd.DataFrame, a: pd.DataFrame, latest_wn: int) -> str:
    """Synthesise 3-7 concrete operator moves from the same signals."""
    actions = []

    # 1) Low cover → reorder.  Uses an upper-cased model key to merge
    # because sales + inventory casing diverge ("AI-04 Red" vs
    # "AI-04 RED") and a strict-string join would silently miss them.
    inv_l = inv[inv["wn"] == latest_wn].copy()
    inv_l["_mk"] = inv_l["model"].astype(str).str.strip().str.upper()
    inv_l = inv_l.groupby(["brand","model","_mk"])["inventory_units"].sum().reset_index()
    s4 = s[s["wn"].between(latest_wn - 3, latest_wn)].copy()
    s4["_mk"] = s4["model"].astype(str).str.strip().str.upper()
    burn4 = (s4.groupby(["brand","_mk"])["units_sold"].sum() / 4.0).reset_index().rename(columns={"units_sold":"avg_w"})
    j = inv_l.merge(burn4, on=["brand","_mk"], how="left").fillna({"avg_w":0})
    j["cover"] = j.apply(lambda r: r["inventory_units"]/r["avg_w"] if r["avg_w"]>0 else None, axis=1)
    # ₹/week at risk ranks the reorders by money, and the suggested qty
    # (6-week target cover minus on-hand) turns "reorder" into a number
    # the operator can act on without opening another tab.
    gmv4 = (s4.groupby(["brand","_mk"])["gmv"].sum() / 4.0).reset_index().rename(columns={"gmv":"gmv_w"})
    j = j.merge(gmv4, on=["brand","_mk"], how="left").fillna({"gmv_w":0})
    crit = j[(j["avg_w"]>=5) & (j["cover"].notna()) & (j["cover"]<=2.0)] \
             .sort_values("gmv_w", ascending=False).head(3)
    for _, r in crit.iterrows():
        need = max(int(round(6*r["avg_w"] - r["inventory_units"])), 0)
        actions.append(
            f"**Reorder {r['brand']} / {r['model']}** — {r['cover']:.1f} wks cover at "
            f"{r['avg_w']:.0f} u/wk ({fmt_inr(r['gmv_w'])}/wk at risk); "
            f"~{need} u brings it to 6 wks."
        )

    # 2) High ROAS low spend → bid up
    cur = a[pd.to_numeric(a["week"], errors="coerce") == latest_wn].copy()
    cur = cur[cur["brand"].str.strip().str.lower() != "fossil"]
    by = cur.groupby(["brand","model"]).agg(spend=("Spend","sum"), attr=("attributed_sales","sum")).reset_index()
    by["roas"] = by.apply(lambda r: r["attr"]/r["spend"] if r["spend"]>0 else None, axis=1)
    bid_up = by[(by["roas"].notna()) & (by["roas"]>=4) & (by["spend"]<1000)] \
              .sort_values("roas", ascending=False).head(2)
    for _, r in bid_up.iterrows():
        actions.append(
            f"**Bid up {r['brand']} / {r['model']}** — ROAS {r['roas']:.2f}x on only "
            f"{fmt_inr(r['spend'])} spend; room to scale."
        )

    # 3) Spend with no attribution → pause
    dead_spend = by[(by["spend"]>=200) & (by["attr"]<1)].sort_values("spend", ascending=False).head(2)
    for _, r in dead_spend.iterrows():
        actions.append(
            f"**Pause {r['brand']} / {r['model']}** ad spend — "
            f"{fmt_inr(r['spend'])} this week with no attributed sales."
        )

    # 4) Top decliner → investigate
    cur_brand = s[s["wn"] == latest_wn].groupby("brand")["gmv"].sum()
    prv_brand = s[s["wn"] == latest_wn - 1].groupby("brand")["gmv"].sum()
    wow_b = pd.Series({b: wow_pct(cur_brand.get(b, 0), prv_brand.get(b, 0)) for b in cur_brand.index})
    biggest_drop = wow_b.dropna().sort_values().head(1)
    for brand, v in biggest_drop.items():
        if v < -10:
            actions.append(
                f"**Investigate {brand}** — GMV down {abs(v):.1f}% WoW; check listing health, OOS, and ad pacing."
            )

    out = ["## Suggested actions", ""]
    if actions:
        for a_ in actions[:7]:
            out.append(f"- {a_}")
    else:
        out.append("- No urgent moves flagged. Keep current pacing and review again next week.")
    out.append("")
    return "\n".join(out)


# ─── Main ────────────────────────────────────────────────────────────
def _drop_excluded(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "brand" not in df.columns:
        return df
    bn = df["brand"].astype(str).str.strip().str.lower()
    return df[~bn.isin(EXCLUDED_BRANDS)].copy()


def build_brief(week: Optional[int] = None,
                brand: Optional[str] = None,
                asin_type: Optional[str] = None,
                category: Optional[str] = None,
                subcategory: Optional[str] = None,
                model: Optional[str] = None,
                last_n: Optional[int] = None) -> str:
    """Build the brief for a slice.  EVERY section recomputes on the
    filtered frames — week / brand / ASIN-type / category / model briefs
    are genuinely different documents, not the global one with lines hidden.
    No args = the canonical all-brands latest-week brief the cron stores."""
    s = pd.read_csv(SALES_CSV)
    s["wn"] = _wn(s["week"])
    s = s.dropna(subset=["wn"])
    s["wn"] = s["wn"].astype(int)
    s = _drop_excluded(s)

    inv = pd.read_csv(INV_CSV)
    inv["wn"] = _wn(inv["week"])
    inv = inv.dropna(subset=["wn"])
    inv["wn"] = inv["wn"].astype(int)
    inv = _drop_excluded(inv)

    a = pd.read_csv(AMS_CSV) if AMS_CSV.exists() else pd.DataFrame()
    a = _drop_excluded(a)

    scope_bits = []
    if brand and brand.strip().lower() != "all":
        bl = brand.strip().lower()
        s = s[s["brand"].astype(str).str.strip().str.lower() == bl]
        if "brand" in inv.columns:
            inv = inv[inv["brand"].astype(str).str.strip().str.lower() == bl]
        if not a.empty and "brand" in a.columns:
            a = a[a["brand"].astype(str).str.strip().str.lower() == bl]
        scope_bits.append(brand.strip())

    pair_scoped = False   # any filter sales carries but inv/ads don't
    if asin_type and asin_type.strip().lower() != "all":
        tl = asin_type.strip().lower()
        if "asin_type" in s.columns:
            s = s[s["asin_type"].astype(str).str.strip().str.lower() == tl]
            pair_scoped = True
            scope_bits.append(f"{asin_type.strip()} ASINs")

    if category and category.strip().lower() != "all":
        cl = category.strip().lower()
        if "category_l0" in s.columns:
            s = s[s["category_l0"].astype(str).str.strip().str.lower() == cl]
            pair_scoped = True
            scope_bits.append(category.strip())

    if subcategory and subcategory.strip().lower() != "all":
        cl = subcategory.strip().lower()
        if "category_l1" in s.columns:
            s = s[s["category_l1"].astype(str).str.strip().str.lower() == cl]
            pair_scoped = True
            scope_bits.append(subcategory.strip())

    if model and model.strip().lower() != "all":
        ml = model.strip().upper()
        if "model" in s.columns:
            s = s[s["model"].astype(str).str.strip().str.upper() == ml]
            pair_scoped = True
            scope_bits.append(model.strip())

    if pair_scoped:
        # inv + ads don't carry asin_type / category — scope them through
        # the (brand, model) pairs that sales says belong to this slice.
        pairs = set(zip(s["brand"].astype(str).str.strip().str.lower(),
                        s["model"].astype(str).str.strip().str.upper()))
        def _in_pairs(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty or not {"brand", "model"}.issubset(df.columns):
                return df
            k = list(zip(df["brand"].astype(str).str.strip().str.lower(),
                         df["model"].astype(str).str.strip().str.upper()))
            return df[[p in pairs for p in k]]
        inv, a = _in_pairs(inv), _in_pairs(a)

    if s.empty:
        return (f"# Weekly Brief\n\nNo sales rows match this slice"
                f" ({' · '.join(scope_bits) or 'all'}) — nothing to report.")

    latest_wn = int(week) if week and (s["wn"] == int(week)).any() else int(s["wn"].max())

    # Window mode — "last N weeks": every section computes on the window
    # (anchored at the selected/latest week), and a window summary compares
    # it to the preceding N weeks under the SAME brand/category/model slice.
    window_md = ""
    if last_n and int(last_n) > 0:
        n = int(last_n)
        lo = latest_wn - n + 1
        cur_w = s[s["wn"].between(lo, latest_wn)]
        prv_w = s[s["wn"].between(lo - n, lo - 1)]
        if cur_w.empty:
            return (f"# Weekly Brief\n\nNo sales rows in W{lo}-W{latest_wn}"
                    f" for this slice ({' · '.join(scope_bits) or 'all'}).")
        g1, u1 = cur_w["gmv"].sum(), cur_w["units_sold"].sum()
        g0 = prv_w["gmv"].sum()
        ww = wow_pct(g1, g0)
        wk_avg = g1 / max(cur_w["wn"].nunique(), 1)
        window_md = (
            f"## Window — last {n} weeks (W{lo}-W{latest_wn})\n\n"
            f"- **GMV {fmt_inr(g1)}** over {cur_w['wn'].nunique()} wks · "
            f"{fmt_int(u1)} units · avg {fmt_inr(wk_avg)}/wk\n"
            + (f"- vs preceding {n} wks (W{lo-n}-W{lo-1}): {fmt_inr(g0)} "
               f"({fmt_pct(ww)}) {trend_arrow(ww)}\n" if g0 > 0 else
               f"- no sales in the preceding {n}-wk window to compare against\n")
        )
        s = cur_w
        if "wn" in inv.columns:
            inv = inv[inv["wn"].between(lo, latest_wn)]
        if not a.empty and "week" in a.columns:
            a = a[pd.to_numeric(a["week"], errors="coerce").between(lo, latest_wn)]
        scope_bits.append(f"Last {n} wks")

    gen_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    title_scope = f" — {' · '.join(scope_bits)}" if scope_bits else ""
    parts = []
    parts.append(f"# Weekly Brief — Week {latest_wn}{title_scope}")
    parts.append(f"*Generated {gen_ts} from data through Week {int(s['wn'].max())}*")
    parts.append("")
    if window_md:
        parts.append(window_md)
    # Ranked ₹-impact key points first — the "so what" before the tables.
    # Shared engine with ai_context.json; recomputed on this exact slice.
    try:
        from weekly_app.core.key_points import compute_key_points, render_md
        kp_md = render_md(compute_key_points(week=latest_wn, brand=brand,
                                             asin_type=asin_type,
                                             category=category,
                                             subcategory=subcategory, model=model))
        if kp_md:
            parts.append(kp_md)
    except Exception as e:
        print(f"⚠ key points skipped: {e}")
    parts.append(headline_section(s, a, latest_wn))
    parts.append(movers_sections(s, latest_wn))
    parts.append(inventory_section(inv, s, a, latest_wn))
    parts.append(ads_efficiency_section(a, latest_wn) if not a.empty else "")
    parts.append(expert_directions(s, inv, a, latest_wn))
    parts.append(suggested_actions(s, inv, a, latest_wn))
    parts.append(brand_briefs_section(s, inv, a, latest_wn))
    parts.append(channel_category_section(s, latest_wn))
    return "\n".join(p for p in parts if p)


def main() -> int:
    if not SALES_CSV.exists() or not INV_CSV.exists():
        print(f"❌ Required snapshots missing: {SALES_CSV.exists()=} {INV_CSV.exists()=}")
        return 2
    md = build_brief()
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(md, encoding="utf-8")
    print(f"✅ Wrote {len(md):,} chars → {OUT_FILE.relative_to(ROOT)}")
    # Tiny preview
    print("\n── Preview ──")
    for line in md.splitlines()[:14]:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
