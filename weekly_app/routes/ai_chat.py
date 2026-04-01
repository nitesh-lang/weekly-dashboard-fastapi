"""
AI Chat route — page-aware, multi-CSV, last-4-weeks context.
"""

import os
import json
import anthropic
import pandas as pd
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/ai", tags=["ai-chat"])

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

SALES_CSV     = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV       = Path("data/processed/business_ads_joined.csv")
INVENTORY_CSV = Path("data/processed/inventory_model_snapshot.csv")


# ── Request schema ──────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    question: str
    week: str = "All Weeks"
    brand: str = "All"
    view: str = "all"
    page: str = "dashboard"   # "dashboard" | "ams-trend" | "inventory" | "sales-trend"


# ── Helpers ─────────────────────────────────────────────────────────────────
def get_last_n_weeks(df: pd.DataFrame, week_col: str, n: int = 4) -> list:
    """Return the last N week values found in the dataframe."""
    try:
        weeks = sorted(df[week_col].dropna().unique(), key=lambda w: int("".join(filter(str.isdigit, str(w)))))
        return weeks[-n:]
    except Exception:
        return df[week_col].dropna().unique().tolist()


def fmt(n) -> str:
    try:
        n = float(n)
        if abs(n) >= 1e7: return f"₹{n/1e7:.2f} Cr"
        if abs(n) >= 1e5: return f"₹{n/1e5:.2f} L"
        if abs(n) >= 1e3: return f"₹{n/1e3:.1f} K"
        return f"₹{n:,.0f}"
    except Exception:
        return str(n)


# ── Sales context ────────────────────────────────────────────────────────────
def load_sales_context(week: str, brand: str) -> dict:
    if not SALES_CSV.exists():
        return {"error": "Sales CSV not found"}

    df = pd.read_csv(SALES_CSV, low_memory=False)
    context = {}

    # Brand filter
    if brand and brand not in ("All", ""):
        if "Brand" in df.columns:
            df = df[df["Brand"] == brand]

    # Always load last 4 weeks for trend; also load selected week for KPIs
    last4 = get_last_n_weeks(df, "week", 4) if "week" in df.columns else []
    context["last_4_weeks"] = last4

    df4 = df[df["week"].isin(last4)] if "week" in df.columns and last4 else df

    # Selected week KPIs
    if week and week not in ("All Weeks", ""):
        dfw = df[df["week"] == week] if "week" in df.columns else df
    else:
        dfw = df4

    def safe_sum(d, col):
        return round(float(d[col].sum()), 2) if col in d.columns else 0

    context["selected_week_kpis"] = {
        "week": week,
        "units": int(dfw["units_sold"].sum()) if "units_sold" in dfw.columns else 0,
        "gmv": safe_sum(dfw, "gmv"),
        "nlc": safe_sum(dfw, "sales_nlc"),
    }

    # Week-over-week trend (last 4)
    if "week" in df4.columns and "gmv" in df4.columns:
        trend = (
            df4.groupby("week")
            .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
            .reset_index().sort_values("week")
        )
        context["weekly_trend"] = trend.to_dict(orient="records")

    # Top SKUs by GMV (last 4 weeks)
    sku_col = "FBA SKU" if "FBA SKU" in df4.columns else None
    if sku_col and "gmv" in df4.columns:
        grp_cols = [sku_col]
        if "Model" in df4.columns: grp_cols.append("Model")
        if "category_l0" in df4.columns: grp_cols.append("category_l0")
        top = (
            df4.groupby(grp_cols)
            .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
            .sort_values("gmv", ascending=False).head(20).reset_index()
        )
        context["top_skus"] = top.to_dict(orient="records")

    # Channel breakdown
    if "channel" in df4.columns and "gmv" in df4.columns:
        ch = (
            df4.groupby("channel")
            .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
            .sort_values("gmv", ascending=False).reset_index()
        )
        context["channels"] = ch.to_dict(orient="records")

    return context


# ── AMS context ──────────────────────────────────────────────────────────────
def load_ams_context(week: str, brand: str) -> dict:
    if not AMS_CSV.exists():
        return {"error": "AMS CSV not found"}

    df = pd.read_csv(AMS_CSV, low_memory=False)
    context = {}

    # Brand filter
    if brand and brand not in ("All", ""):
        if "brand" in df.columns:
            df = df[df["brand"].str.lower() == brand.lower()]

    # Always include last 4 weeks
    last4 = get_last_n_weeks(df, "week", 4) if "week" in df.columns else []
    context["last_4_weeks"] = last4
    df4 = df[df["week"].isin(last4)] if "week" in df.columns and last4 else df

    # Overall AMS summary (last 4 weeks)
    def safe_sum(d, col):
        return round(float(d[col].sum()), 2) if col in d.columns else 0

    context["ams_summary_last4"] = {
        "total_spend": safe_sum(df4, "Spend"),
        "total_attributed_sales": safe_sum(df4, "attributed_sales"),
        "total_gmv": safe_sum(df4, "gmv"),
        "total_clicks": int(df4["Clicks"].sum()) if "Clicks" in df4.columns else 0,
        "total_impressions": int(df4["Impressions"].sum()) if "Impressions" in df4.columns else 0,
        "avg_acos": round(float(df4["acos"].mean()), 4) if "acos" in df4.columns else None,
        "avg_roas": round(float(df4["roas"].mean()), 4) if "roas" in df4.columns else None,
    }

    # Week-over-week AMS trend
    if "week" in df4.columns:
        agg = {}
        for col in ["Spend", "attributed_sales", "gmv", "Clicks", "Impressions"]:
            if col in df4.columns:
                agg[col] = (col, "sum")
        if agg:
            trend = df4.groupby("week").agg(**agg).reset_index().sort_values("week")
            context["ams_weekly_trend"] = trend.to_dict(orient="records")

    # Per-model breakdown (last 4 weeks) — this answers "how is model X trending?"
    if "model" in df4.columns:
        grp = ["model"]
        if "asin" in df4.columns: grp.append("asin")
        if "category_l0" in df4.columns: grp.append("category_l0")

        agg = {}
        for col in ["Spend", "attributed_sales", "gmv", "Clicks", "Impressions", "units"]:
            if col in df4.columns:
                agg[col] = (col, "sum")

        model_perf = (
            df4.groupby(grp).agg(**agg)
            .reset_index()
            .sort_values("gmv" if "gmv" in df4.columns else grp[0], ascending=False)
            .head(30)
        )

        # Add computed ACOS and ROAS per model
        if "Spend" in model_perf.columns and "attributed_sales" in model_perf.columns:
            model_perf["acos"] = (model_perf["Spend"] / model_perf["attributed_sales"].replace(0, float("nan"))).round(4)
            model_perf["roas"] = (model_perf["attributed_sales"] / model_perf["Spend"].replace(0, float("nan"))).round(2)

        context["model_performance"] = model_perf.to_dict(orient="records")

    # Per-model per-week trend (for "last 4 weeks" trend questions on a specific model)
    if "model" in df4.columns and "week" in df4.columns:
        grp = ["model", "week"]
        agg = {}
        for col in ["Spend", "attributed_sales", "gmv", "units"]:
            if col in df4.columns:
                agg[col] = (col, "sum")
        if agg:
            model_week = df4.groupby(grp).agg(**agg).reset_index().sort_values(["model", "week"])
            context["model_weekly_trend"] = model_week.to_dict(orient="records")

    return context


# ── Inventory context ────────────────────────────────────────────────────────
def load_inventory_context(brand: str) -> dict:
    if not INVENTORY_CSV.exists():
        return {"error": "Inventory CSV not found"}
    try:
        df = pd.read_csv(INVENTORY_CSV, low_memory=False)
        if brand and brand not in ("All", "") and "Brand" in df.columns:
            df = df[df["Brand"] == brand]
        return {"inventory": df.head(50).to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e)}


# ── System prompt ─────────────────────────────────────────────────────────────
def build_system_prompt(context: dict, week: str, brand: str, page: str) -> str:
    ctx_str = json.dumps(context, indent=2, default=str)

    page_desc = {
        "dashboard":   "the main Sales Dashboard (units, GMV, NLC, channel mix, SKU table)",
        "ams-trend":   "the AMS / Ads Trend page (ad spend, ROAS, ACOS, attributed sales, impressions, clicks per model/ASIN)",
        "inventory":   "the Inventory Dashboard (stock levels, sell-through, days of stock)",
        "sales-trend": "the Sales Trend page (SKU/model level weekly sales)",
    }.get(page, "the dashboard")

    return f"""You are an expert e-commerce business analyst embedded in a Weekly Unified Dashboard for a watches/electronics brand selling on Amazon India and other channels.

USER IS CURRENTLY ON: {page_desc}
ACTIVE FILTERS — Week: {week} | Brand: {brand if brand not in ('All','') else 'All Brands'}

LIVE DATA (JSON):
{ctx_str}

INSTRUCTIONS:
- Answer using ONLY the data above. Never invent numbers.
- Use ₹ for currency with Indian units: Cr (crore), L (lakh), K (thousand).
- For trend questions, use model_weekly_trend or ams_weekly_trend to show week-by-week movement.
- For "last 4 weeks" questions, the last_4_weeks field shows which weeks are included.
- For model/ASIN questions, use model_performance and model_weekly_trend.
- For AMS: highlight Spend, ROAS, ACOS, attributed sales. Flag high ACOS (>50%) as a concern.
- For sales: highlight GMV, units, NLC, top SKUs, channel mix.
- Keep answers to 4–7 sentences. Use a short table or bullet list only if comparing 3+ items.
- If data is insufficient, say so clearly and suggest what filter to change."""


# ── Main endpoint ─────────────────────────────────────────────────────────────
@router.post("/ai-chat")
async def ai_chat(request: Request, body: ChatRequest):
    # Load context based on current page
    if body.page == "inventory":
        context = load_inventory_context(body.brand)
    else:
        # Always load both sales + full AMS on every page
        context = load_sales_context(body.week, body.brand)
        ams = load_ams_context(body.week, body.brand)
        if "error" not in ams:
            context["ams_summary_last4"]    = ams.get("ams_summary_last4", {})
            context["ams_weekly_trend"]     = ams.get("ams_weekly_trend", [])
            context["model_performance"]    = ams.get("model_performance", [])
            context["model_weekly_trend"]   = ams.get("model_weekly_trend", [])

    system_prompt = build_system_prompt(context, body.week, body.brand, body.page)

    def stream_response():
        try:
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                system=system_prompt,
                messages=[{"role": "user", "content": body.question}],
            ) as stream:
                for chunk in stream.text_stream:
                    yield f"data: {json.dumps({'text': chunk})}\n\n"
            yield "data: [DONE]\n\n"
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'Invalid API key.'})}\n\n"
        except anthropic.RateLimitError:
            yield f"data: {json.dumps({'error': 'Rate limit hit. Try again shortly.'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        stream_response(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Health check ──────────────────────────────────────────────────────────────
@router.get("/ai-chat/health")
async def ai_chat_health():
    return {"status": "ok", "api_key_set": bool(os.environ.get("ANTHROPIC_API_KEY"))}
