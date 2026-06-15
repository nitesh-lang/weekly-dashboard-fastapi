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
    return "\n".join(out)


def inventory_section(inv: pd.DataFrame, s: pd.DataFrame, latest_wn: int) -> str:
    """Low-stock and dead-stock signals from the inventory snapshot
    against last-4w burn from the sales snapshot."""
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

    # Low cover — has stock < 2 weeks AND moves at least 5 units/week
    low = j[(j["avg_weekly"] >= 5) & (j["cover_weeks"].notna()) & (j["cover_weeks"] <= 2.0)] \
            .sort_values("cover_weeks").head(8)

    # Dead stock — stock > 0, no sales in last 4w
    dead = j[(j["stock"] >= 30) & (j["avg_weekly"] == 0)] \
             .sort_values("stock", ascending=False).head(8)

    out = ["## Inventory health", ""]
    if not low.empty:
        out.append("**Low cover (≤ 2 weeks at current burn):**")
        for _, r in low.iterrows():
            cov = r["cover_weeks"]
            out.append(
                f"- 🔴 **{r['brand']} / {r['model']}** — {fmt_int(r['stock'])} units on hand, "
                f"~{r['avg_weekly']:.1f} u/week burn ({cov:.1f} weeks cover). Reorder."
            )
        out.append("")
    if not dead.empty:
        out.append("**Dead stock (no sales last 4w, ≥30 units):**")
        for _, r in dead.iterrows():
            out.append(
                f"- 🟡 **{r['brand']} / {r['model']}** — {fmt_int(r['stock'])} units sitting idle."
            )
        out.append("")
    if low.empty and dead.empty:
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
    crit = j[(j["avg_w"]>=5) & (j["cover"].notna()) & (j["cover"]<=2.0)].sort_values("cover").head(3)
    for _, r in crit.iterrows():
        actions.append(
            f"**Reorder {r['brand']} / {r['model']}** — only {r['cover']:.1f} weeks of cover left at current burn."
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


def build_brief() -> str:
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

    latest_wn = int(s["wn"].max())
    gen_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    parts = []
    parts.append(f"# Weekly Brief — Week {latest_wn}")
    parts.append(f"*Generated {gen_ts} from data through Week {latest_wn}*")
    parts.append("")
    parts.append(headline_section(s, a, latest_wn))
    parts.append(movers_sections(s, latest_wn))
    parts.append(inventory_section(inv, s, latest_wn))
    parts.append(ads_efficiency_section(a, latest_wn) if not a.empty else "")
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
