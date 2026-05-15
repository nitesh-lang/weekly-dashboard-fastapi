"""
Analytics — cross-module visualizations + Claude-powered insights.

Routes:
- GET /analytics                        Page (HTML)
- GET /api/analytics/insights           AI insights as Markdown (JSON-wrapped)

Data sources reused from existing routes:
- weekly_sales_snapshot.csv             sales (week × brand × model × channel × sku)
- ams_weekly_fact_with_category.csv     AMS metrics (week × asin)
- inventory_dashboard.load_all_inventory  rich SKU+channel inventory (lazy)
"""
import os
import re
import time
import json
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader

router = APIRouter()
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
AMS_FILE = Path("data/ams_weekly_data/processed_ads/business_ads_joined.csv")

# In-memory insights cache. Key: (frozenset of weeks, brand, view). 10-min TTL.
_insight_cache: Dict[Any, Any] = {}
_INSIGHT_TTL = 600


# =====================================================
# UTILS
# =====================================================
def _wk_n(w):
    m = re.search(r"\d+", str(w))
    return int(m.group()) if m else 0


def _fix_week(w: str) -> str:
    w = str(w).strip()
    if w.startswith("Week") and " " not in w:
        w = w.replace("Week", "Week ")
    return w


def _pct_change(cur, prev):
    if prev in (0, None):
        return None
    return round(((cur - prev) / prev) * 100, 1)


# =====================================================
# ANALYTICS-SPECIFIC BUSINESS RULES
# Applied at load time so every chart, KPI, top mover, and the AI
# insights all see the same filtered data.
#
#  - Fossil is excluded from analytics. (Other pages still show it.)
#  - "1P Sales" stays as its own row in the channel heatmap (separate
#    channel in the data). It's Amazon Vendor Central; the "Amazon"
#    channel is Amazon Seller Central. The AI prompt is told to group
#    them when summarizing Amazon performance, but the chart keeps
#    them visually separate so the operator can see the SC vs VC mix.
# =====================================================
EXCLUDED_BRANDS = {"fossil"}

# Channels that should be treated as Amazon when computing aggregates
# such as "Amazon GMV" or in AI summaries. NOT used to rename channels
# in the heatmap.
AMAZON_CHANNELS_LOWER = {"amazon", "1p sales", "1p", "1psales"}


def _load_sales() -> pd.DataFrame:
    df = pd.read_csv(SALES_FILE)
    df.columns = df.columns.str.strip().str.lower()
    df["week"] = df["week"].astype(str).str.strip()
    df["sku"] = df["sku"].astype(str)
    df["channel"] = df["channel"].astype(str).str.strip()
    df["brand"] = df["brand"].astype(str).str.strip()
    if "model" in df.columns:
        df["model"] = df["model"].astype(str).str.strip()
    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Drop excluded brands
    df = df[~df["brand"].str.lower().isin(EXCLUDED_BRANDS)]

    return df


def _load_ams() -> pd.DataFrame:
    if not AMS_FILE.exists():
        return pd.DataFrame()
    df = pd.read_csv(AMS_FILE)
    df.columns = df.columns.str.strip()
    if "week" in df.columns:
        df["week"] = df["week"].astype(str).str.strip()
        df["week"] = df["week"].str.replace(r"\.0$", "", regex=True)
    if "brand" in df.columns:
        df = df[~df["brand"].astype(str).str.lower().isin(EXCLUDED_BRANDS)]
    return df


# =====================================================
# DATA LAYER (shared by page render + AI insights)
# =====================================================
def compute_analytics(
    active_weeks: List[str],
    brands: List[str],
    view: str,
) -> Dict[str, Any]:
    """Aggregate everything the analytics page (and AI insights) need.

    `brands` is a list. Empty list = no brand restriction.
    """
    sales = _load_sales()
    all_weeks = sorted(sales["week"].dropna().unique().tolist(), key=_wk_n)

    if not active_weeks and all_weeks:
        active_weeks = [all_weeks[-1]]

    brands_list = sorted(sales["brand"].dropna().unique().tolist())
    brands_lower = [b.strip().lower() for b in (brands or []) if b and b.strip()]

    # Current window
    cur = sales.copy()
    cur = cur[cur["week"].isin(active_weeks)]
    if brands_lower:
        cur = cur[cur["brand"].str.lower().isin(brands_lower)]
    if view == "mapped" and "sku_status" in cur.columns:
        cur = cur[cur["sku_status"] == "MAPPED"]

    # Prior window — same length, immediately preceding
    n = len(active_weeks)
    prior_weeks: List[str] = []
    if n > 0 and len(all_weeks) > n:
        try:
            min_idx = min(all_weeks.index(w) for w in active_weeks if w in all_weeks)
            prior_weeks = all_weeks[max(0, min_idx - n):min_idx]
        except ValueError:
            prior_weeks = []

    prior = sales.copy()
    if prior_weeks:
        prior = prior[prior["week"].isin(prior_weeks)]
    else:
        prior = prior.iloc[0:0]
    if brands_lower and not prior.empty:
        prior = prior[prior["brand"].str.lower().isin(brands_lower)]
    if view == "mapped" and "sku_status" in prior.columns:
        prior = prior[prior["sku_status"] == "MAPPED"]

    # ─── HERO ─────────────────────────────────────────────
    cur_gmv = float(cur["gross_sales"].sum())
    cur_units = int(cur["units_sold"].sum())
    cur_skus = int(cur[cur["units_sold"] > 0]["sku"].nunique())
    cur_nlc = float(cur["sales_nlc"].sum()) if "sales_nlc" in cur.columns else 0.0

    prior_gmv = float(prior["gross_sales"].sum())
    prior_units = int(prior["units_sold"].sum())
    prior_nlc = float(prior["sales_nlc"].sum()) if "sales_nlc" in prior.columns else 0.0

    # AMS for the current window
    ams = _load_ams()
    ams_window = ams.copy()
    if not ams_window.empty:
        active_week_nums = [str(_wk_n(w)) for w in active_weeks]
        ams_window = ams_window[ams_window["week"].astype(str).isin(active_week_nums)]
        if brands_lower and "brand" in ams_window.columns:
            ams_window = ams_window[
                ams_window["brand"].astype(str).str.lower().isin(brands_lower)
            ]

    def _sum(col):
        if ams_window.empty or col not in ams_window.columns:
            return 0.0
        return float(pd.to_numeric(ams_window[col], errors="coerce").fillna(0).sum())

    spend = _sum("Spend") or _sum("spend") or _sum("ad_spend")
    attr_sales = _sum("attributed_sales")
    sessions = _sum("sessions")
    ams_units = _sum("units")
    clicks = _sum("Clicks") or _sum("clicks")
    impressions = _sum("Impressions") or _sum("impressions")

    acos = round((spend / attr_sales * 100), 2) if attr_sales > 0 else 0.0
    tacos = round((spend / cur_gmv * 100), 2) if cur_gmv > 0 else 0.0
    conv = round((ams_units / sessions * 100), 2) if sessions > 0 else 0.0
    roas = round((attr_sales / spend), 2) if spend > 0 else 0.0

    hero = {
        "gmv":         {"value": int(cur_gmv),       "delta": _pct_change(cur_gmv, prior_gmv),     "label": "GMV",          "fmt": "inr",  "tint": "purple"},
        "units":       {"value": cur_units,           "delta": _pct_change(cur_units, prior_units), "label": "Units Sold",   "fmt": "int",  "tint": "blue"},
        "active_skus": {"value": cur_skus,            "delta": None,                                "label": "Active SKUs",  "fmt": "int",  "tint": "indigo"},
        "nlc":         {"value": int(cur_nlc),        "delta": _pct_change(cur_nlc, prior_nlc),     "label": "NLC Sold",     "fmt": "inr",  "tint": "amber"},
        "spend":       {"value": int(spend),          "delta": None,                                "label": "Ad Spend",     "fmt": "inr",  "tint": "rose"},
        "attr_sales":  {"value": int(attr_sales),     "delta": None,                                "label": "Attributed",   "fmt": "inr",  "tint": "green"},
        "acos":        {"value": acos,                "delta": None,                                "label": "ACOS %",       "fmt": "pct",  "tint": "red"},
        "tacos":       {"value": tacos,               "delta": None,                                "label": "TACOS %",      "fmt": "pct",  "tint": "teal"},
    }
    extra = {"sessions": int(sessions), "conv": conv, "roas": roas, "clicks": int(clicks), "impressions": int(impressions)}

    # ─── SALES PULSE (12 weeks) ───────────────────────────
    pulse_base = sales.copy()
    if brands_lower:
        pulse_base = pulse_base[pulse_base["brand"].str.lower().isin(brands_lower)]
    if view == "mapped" and "sku_status" in pulse_base.columns:
        pulse_base = pulse_base[pulse_base["sku_status"] == "MAPPED"]
    pulse = (
        pulse_base.groupby("week", as_index=False)
        .agg(gmv=("gross_sales", "sum"), units=("units_sold", "sum"))
    )
    pulse["_n"] = pulse["week"].apply(_wk_n)
    pulse = pulse.sort_values("_n").tail(12)
    pulse_data = (
        pulse[["week", "gmv", "units"]]
        .assign(gmv=lambda d: d["gmv"].round(0).astype(int),
                units=lambda d: d["units"].round(0).astype(int))
        .to_dict("records")
    )

    # ─── CHANNEL HEATMAP (channel × last 12 weeks) ───────
    heat_base = pulse_base
    last_12 = sorted(heat_base["week"].unique().tolist(), key=_wk_n)[-12:]
    heat_base = heat_base[heat_base["week"].isin(last_12)]
    if heat_base.empty:
        heatmap = {"channels": [], "weeks": [], "values": []}
    else:
        pv = heat_base.pivot_table(
            index="channel", columns="week", values="gross_sales",
            aggfunc="sum", fill_value=0,
        )
        # Order columns by week number, rows by latest column desc
        pv = pv[sorted(pv.columns, key=_wk_n)]
        if len(pv.columns) > 0:
            pv = pv.sort_values(pv.columns[-1], ascending=False)
        heatmap = {
            "channels": [str(c) for c in pv.index.tolist()],
            "weeks": [str(c) for c in pv.columns.tolist()],
            "values": [[round(float(pv.loc[ch, w])) for w in pv.columns] for ch in pv.index],
        }

    # ─── CATEGORY PERFORMANCE ────────────────────────────
    category_share: List[Dict[str, Any]] = []
    category_growth: List[Dict[str, Any]] = []

    if "category_l0" in cur.columns and not cur.empty:
        cs_base = cur.copy()
        cs_base["category_l0"] = (
            cs_base["category_l0"].astype(str).str.strip()
            .replace({"": "Uncategorized", "nan": "Uncategorized"})
        )
        cs = cs_base.groupby("category_l0", as_index=False).agg(gmv=("gross_sales", "sum"))
        cs = cs[cs["gmv"] > 0]
        total = cs["gmv"].sum() or 1
        cs["pct"] = (cs["gmv"] / total * 100).round(2)
        cs = cs.sort_values("gmv", ascending=False).head(10)
        category_share = [
            {"category": r["category_l0"], "gmv": int(r["gmv"]), "pct": float(r["pct"])}
            for _, r in cs.iterrows()
        ]

        if not prior.empty and "category_l0" in prior.columns:
            cur_c = cs_base.groupby("category_l0", as_index=False).agg(gmv=("gross_sales", "sum"))
            pr_base = prior.copy()
            pr_base["category_l0"] = (
                pr_base["category_l0"].astype(str).str.strip()
                .replace({"": "Uncategorized", "nan": "Uncategorized"})
            )
            pr_c = pr_base.groupby("category_l0", as_index=False).agg(gmv=("gross_sales", "sum"))
            mg = cur_c.merge(pr_c, on="category_l0", how="outer", suffixes=("_cur", "_prior")).fillna(0)
            mg["pct"] = mg.apply(
                lambda r: ((r["gmv_cur"] - r["gmv_prior"]) / r["gmv_prior"] * 100)
                if r["gmv_prior"] > 0 else None,
                axis=1,
            )
            mg = mg.dropna(subset=["pct"]).sort_values("pct", ascending=False).head(10)
            category_growth = [
                {"category": r["category_l0"], "pct": round(float(r["pct"]), 1),
                 "gmv_cur": int(r["gmv_cur"]), "gmv_prior": int(r["gmv_prior"])}
                for _, r in mg.iterrows()
            ]

    # ─── BRAND PERFORMANCE ───────────────────────────────
    # Show brand breakdown only when no specific brand filter is active.
    brand_share: List[Dict[str, Any]] = []
    brand_growth: List[Dict[str, Any]] = []
    if not brands_lower:
        # Share for current window
        if not cur.empty:
            bs = cur.groupby("brand", as_index=False).agg(gmv=("gross_sales", "sum"))
            bs = bs[bs["gmv"] > 0]
            total = bs["gmv"].sum() or 1
            bs["pct"] = (bs["gmv"] / total * 100).round(2)
            bs = bs.sort_values("gmv", ascending=False)
            brand_share = [
                {"brand": r["brand"], "gmv": int(r["gmv"]), "pct": float(r["pct"])}
                for _, r in bs.iterrows()
            ]

        # Growth: cur vs prior
        if not cur.empty and not prior.empty:
            cur_b = cur.groupby("brand", as_index=False).agg(gmv=("gross_sales", "sum"))
            pr_b = prior.groupby("brand", as_index=False).agg(gmv=("gross_sales", "sum"))
            mg = cur_b.merge(pr_b, on="brand", how="outer", suffixes=("_cur", "_prior")).fillna(0)
            mg["pct"] = mg.apply(
                lambda r: ((r["gmv_cur"] - r["gmv_prior"]) / r["gmv_prior"] * 100)
                if r["gmv_prior"] > 0 else None,
                axis=1,
            )
            mg = mg.dropna(subset=["pct"]).sort_values("pct", ascending=False)
            brand_growth = [
                {"brand": r["brand"], "pct": round(float(r["pct"]), 1),
                 "gmv_cur": int(r["gmv_cur"]), "gmv_prior": int(r["gmv_prior"])}
                for _, r in mg.iterrows()
            ]

    # ─── AMAZON FUNNEL ───────────────────────────────────
    ams_funnel: Optional[Dict[str, Any]] = None
    if not ams_window.empty:
        ams_funnel = {
            "impressions": extra["impressions"],
            "clicks": extra["clicks"],
            "sessions": int(sessions),
            "units": int(ams_units),
            "attr_sales": int(attr_sales),
            "spend": int(spend),
        }

    # ─── TOP MOVERS (cur vs prior) ───────────────────────
    # Include brand so we can drill into the right /viewer/sales filter
    keys = ["sku", "model"] + (["brand"] if "brand" in cur.columns else [])
    cur_sku = cur.groupby(keys, as_index=False).agg(gmv=("gross_sales", "sum"))
    pr_sku = prior.groupby(keys, as_index=False).agg(gmv=("gross_sales", "sum")) if not prior.empty else cur_sku.iloc[0:0].copy()
    mv = cur_sku.merge(pr_sku, on=keys, how="outer", suffixes=("_cur", "_prior")).fillna(0)
    mv["delta"] = mv["gmv_cur"] - mv["gmv_prior"]
    mv["pct"] = mv.apply(
        lambda r: ((r["gmv_cur"] - r["gmv_prior"]) / r["gmv_prior"] * 100)
        if r["gmv_prior"] > 0 else None,
        axis=1,
    )
    top_up = (
        mv[mv["gmv_cur"] > 0].sort_values("delta", ascending=False).head(5)
        .to_dict("records")
    )
    top_down = (
        mv[mv["gmv_prior"] > 0].sort_values("delta", ascending=True).head(5)
        .to_dict("records")
    )

    def _movers(rows):
        out = []
        for r in rows:
            out.append({
                "sku": str(r["sku"]),
                "model": str(r["model"]),
                "brand": str(r.get("brand", "")),
                "gmv_cur": int(r["gmv_cur"]),
                "gmv_prior": int(r["gmv_prior"]),
                "delta": int(r["delta"]),
                "pct": round(float(r["pct"]), 1) if r["pct"] is not None and pd.notna(r["pct"]) else None,
            })
        return out

    return {
        "all_weeks": all_weeks,
        "active_weeks": active_weeks,
        "prior_weeks": prior_weeks,
        "brands_list": brands_list,
        "hero": hero,
        "extra": extra,
        "pulse": pulse_data,
        "heatmap": heatmap,
        "brand_share": brand_share,
        "brand_growth": brand_growth,
        "category_share": category_share,
        "category_growth": category_growth,
        "ams_funnel": ams_funnel,
        "top_up": _movers(top_up),
        "top_down": _movers(top_down),
    }


# =====================================================
# PAGE ROUTE
# =====================================================
@router.get("/analytics", response_class=HTMLResponse)
def analytics_page(
    request: Request,
    week: Optional[str] = None,
    weeks: List[str] = Query(default=[]),
    brand: Optional[str] = None,                  # legacy single-brand
    brands: List[str] = Query(default=[]),        # multi-brand checkboxes
    view: str = "mapped",
):
    if weeks:
        active_weeks = [_fix_week(w) for w in weeks if str(w).strip()]
    elif week:
        active_weeks = [_fix_week(week)]
    else:
        active_weeks = []

    # Resolve effective brand list: multi wins over legacy single.
    eff_brands = [b for b in (brands or []) if b and b.strip()]
    if not eff_brands and brand:
        eff_brands = [brand]

    data = compute_analytics(active_weeks, eff_brands, view)

    selected = {
        "weeks": data["active_weeks"],
        "weeks_display": ", ".join(data["active_weeks"]) if data["active_weeks"] else "All Weeks",
        "brands": eff_brands,
        "view": view,
    }

    from datetime import datetime as _dt
    generated_at = _dt.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    return HTMLResponse(_env.get_template("analytics.html").render(
        request=request,
        selected=selected,
        weeks=data["all_weeks"],
        brands=data["brands_list"],
        hero=data["hero"],
        extra=data["extra"],
        pulse=data["pulse"],
        heatmap=data["heatmap"],
        brand_share=data["brand_share"],
        brand_growth=data["brand_growth"],
        category_share=data["category_share"],
        category_growth=data["category_growth"],
        ams_funnel=data["ams_funnel"],
        top_up=data["top_up"],
        top_down=data["top_down"],
        prior_period_label=", ".join(data["prior_weeks"]) if data["prior_weeks"] else None,
        generated_at=generated_at,
    ))


# =====================================================
# AI INSIGHTS API
# =====================================================
_SYSTEM_PROMPT = """You are a senior retail / e-commerce analytics consultant working
with an Indian D2C and Amazon-first business. Given the JSON data below from a weekly
dashboard, produce concise, actionable intelligence for the operator.

Important data conventions (read before analyzing):
- The channels "Amazon" and "1P Sales" are BOTH Amazon — "Amazon" is
  Amazon Seller Central and "1P Sales" is Amazon Vendor Central. When
  you talk about "Amazon performance" or "Amazon GMV", you must
  combine BOTH channels' numbers. Treat them as one Amazon business
  in the narrative; only call them out separately if there's a
  meaningful SC-vs-VC story.
- The brand "Fossil" has been deliberately excluded from this view.
  Don't mention it.

Format your response as Markdown with TWO sections:

## 📊 Observations
3 to 5 bullet points covering:
- Specific week-over-week changes (cite numbers)
- Notable trends — rising or falling brands, channels, SKUs
- Anomalies, surprising swings, or risk signals
- Performance vs the prior comparable period

## 🎯 Recommendations
2 actionable next steps the operator can take this week. Be specific and concrete
("Audit the top 3 ASINs by ACOS this week", not "review marketing").

Rules:
- Cite ACTUAL numbers from the data — no vague qualifiers.
- Use ₹ for INR, % for percentages, comma-separated thousands.
- Be direct. No hedging, no preamble, no platitudes, no apologies.
- If a data section is missing or empty, skip the bullet for it.
- Total response under 350 words.
- Never invent data the JSON doesn't contain.
"""


@router.get("/api/analytics/insights")
def analytics_insights(
    request: Request,
    week: Optional[str] = None,
    weeks: List[str] = Query(default=[]),
    brand: Optional[str] = None,                  # legacy single-brand
    brands: List[str] = Query(default=[]),        # multi-brand checkboxes
    view: str = "mapped",
    refresh: bool = False,
):
    if weeks:
        active_weeks = [_fix_week(w) for w in weeks if str(w).strip()]
    elif week:
        active_weeks = [_fix_week(week)]
    else:
        active_weeks = []

    # Resolve effective brand list: multi wins over legacy single.
    eff_brands = [b for b in (brands or []) if b and b.strip()]
    if not eff_brands and brand:
        eff_brands = [brand]

    cache_key = (
        tuple(sorted(active_weeks)),
        tuple(sorted(b.strip().lower() for b in eff_brands)),
        view,
    )

    if not refresh and cache_key in _insight_cache:
        ts, content = _insight_cache[cache_key]
        if time.time() - ts < _INSIGHT_TTL:
            return JSONResponse({
                "insights": content,
                "cached": True,
                "age_sec": int(time.time() - ts),
            })

    data = compute_analytics(active_weeks, eff_brands, view)

    # Pre-compute a combined Amazon GMV across "Amazon" + "1P Sales" so the
    # AI doesn't have to add the channels itself (and can't accidentally
    # report Amazon as just one of them).
    heat = data["heatmap"]
    amazon_total_by_week: dict = {}
    amazon_total_period: int = 0
    for ch_name, vals in zip(heat["channels"], heat["values"]):
        if ch_name.strip().lower() in AMAZON_CHANNELS_LOWER:
            for wk, v in zip(heat["weeks"], vals):
                amazon_total_by_week[wk] = amazon_total_by_week.get(wk, 0) + int(v)
                amazon_total_period += int(v)

    # Compact summary for the prompt — only the fields the model needs.
    payload = {
        "period": {
            "current": data["active_weeks"],
            "prior":   data["prior_weeks"],
            "filter_brand": brand or None,
            "filter_view":  view,
        },
        "data_conventions": {
            "amazon_is_combined_of": ["Amazon", "1P Sales"],
            "amazon_combined_gmv_period": amazon_total_period,
            "amazon_combined_gmv_by_week": amazon_total_by_week,
            "excluded_brands": ["Fossil"],
        },
        "headline": {k: v["value"] for k, v in data["hero"].items()},
        "headline_deltas": {k: v["delta"] for k, v in data["hero"].items() if v["delta"] is not None},
        "extra_metrics": data["extra"],
        "weekly_trend": data["pulse"],
        "channel_heatmap_top": (
            [{"channel": ch, "by_week": dict(zip(data["heatmap"]["weeks"], vals))}
             for ch, vals in zip(data["heatmap"]["channels"][:5], data["heatmap"]["values"][:5])]
        ),
        "brand_share": data["brand_share"][:8],
        "brand_growth_top": data["brand_growth"][:5],
        "brand_growth_bottom": data["brand_growth"][-5:] if len(data["brand_growth"]) > 5 else [],
        "category_share": data["category_share"][:8],
        "category_growth_top": data["category_growth"][:5],
        "category_growth_bottom": data["category_growth"][-5:] if len(data["category_growth"]) > 5 else [],
        "amazon_funnel": data["ams_funnel"],
        "top_movers_up": data["top_up"],
        "top_movers_down": data["top_down"],
    }

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return JSONResponse({
            "insights": "_AI Insights unavailable — `ANTHROPIC_API_KEY` env var is not set._",
            "cached": False,
            "error": "missing_api_key",
        })

    # Model is env-configurable so you can swap to Opus (premium quality) or
    # Haiku (cheap/fast) without a code change. Defaults to Sonnet 4.6 — the
    # right balance for retail-analytics insights.
    model_id = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model_id,
            max_tokens=1500,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {"role": "user", "content": json.dumps(payload, indent=2)}
            ],
        )
        markdown = "".join(
            block.text for block in msg.content if getattr(block, "type", "") == "text"
        ) or "_No content returned by AI._"
    except Exception as e:
        return JSONResponse({
            "insights": f"_AI Insights failed: {type(e).__name__}: {e}_",
            "cached": False,
            "error": str(e),
        })

    _insight_cache[cache_key] = (time.time(), markdown)
    return JSONResponse({
        "insights": markdown,
        "cached": False,
        "age_sec": 0,
        "model": model_id,
    })
