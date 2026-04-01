"""
AI Chat route — reads pre-computed ai_context.json (fast).
Falls back to live CSV processing if JSON not found.
Supports multi-turn conversation history.
"""

import os
import json
import anthropic
import pandas as pd
import numpy as np
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

router = APIRouter(prefix="/api/ai", tags=["ai-chat"])

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

AI_CONTEXT_JSON = Path("data/processed/ai_context.json")
SALES_CSV       = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV = Path("data/ams_weekly_data/processed_ads/business_ads_joined.csv")
INVENTORY_CSV   = Path("data/processed/inventory_model_snapshot.csv")


class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    question: str
    history: List[Message] = []
    week: str = "All Weeks"
    brand: str = "All"
    view: str = "all"
    page: str = "dashboard"


# ── Smart context trimmer ─────────────────────────────────────────────────────
def cap_list(lst, n):
    """Hard cap any list to n items."""
    return lst[:n] if isinstance(lst, list) else lst

def trim_context_for_question(context: dict, question: str) -> dict:
    """
    Filter context to only what's relevant for the question.
    Reduces tokens and improves answer quality.
    """
    q = question.lower()
    trimmed = {
        "generated_at":       context.get("generated_at"),
        "alerts":             context.get("alerts", []),
        "alert_count":        context.get("alert_count", 0),
        "high_alerts":        context.get("high_alerts", 0),
        "all_weeks":          context.get("all_weeks", []),
        "last_4_weeks":       context.get("last_4_weeks", []),
        "latest_week":        context.get("latest_week"),
        "latest_week_kpis":   context.get("latest_week_kpis", {}),
        "last4_kpis":         context.get("last4_kpis", {}),
        "ams_last4_kpis":     context.get("ams_last4_kpis", {}),
        "ams_latest_week_kpis": context.get("ams_latest_week_kpis", {}),
    }

    is_overview  = any(w in q for w in ["overview","overall","full","summary","analysis","everything","business","picture"])
    is_channel  = any(w in q for w in ["channel","amazon","flipkart","offline","b2b","b2c"])
    is_category = any(w in q for w in ["category","watch","microphone","headphone","earphone"])
    is_ams      = any(w in q for w in ["ams","ads","spend","roas","acos","tacos","asin","attributed","campaign"])
    is_trend    = any(w in q for w in ["trend","week","wow","growth","decline","last 4","history"])
    is_model    = any(w in q for w in ["model","sku","asin","k1","k2","fs","es","jr","mk","me"])
    is_inventory= any(w in q for w in ["inventory","stock","oos","out of stock","days","sell-through","sell through"])
    is_alert    = any(w in q for w in ["alert","risk","issue","problem","concern","flag","anomal"])

    if is_overview or is_channel:
        trimmed["channel_breakdown"]     = cap_list(context.get("channel_breakdown", []), 10)
        trimmed["category_breakdown"]    = cap_list(context.get("category_breakdown", []), 8)
        trimmed["weekly_sales_trend"]    = cap_list(context.get("weekly_sales_trend", []), 13)
        trimmed["ams_weekly_trend"]      = cap_list(context.get("ams_weekly_trend", []), 13)
        trimmed["top_models_last4"]      = cap_list(context.get("top_models_last4", []), 15)
        trimmed["model_ams_performance"] = cap_list(context.get("model_ams_performance", []), 15)

    if is_category:
        trimmed["category_breakdown"] = cap_list(context.get("category_breakdown", []), 8)

    if is_ams or is_overview:
        trimmed["model_ams_performance"]  = cap_list(context.get("model_ams_performance", []), 15)
        trimmed["asin_ams_performance"]   = cap_list(context.get("asin_ams_performance", []), 15)
        trimmed["ams_weekly_trend"]       = cap_list(context.get("ams_weekly_trend", []), 13)
        if is_trend or is_model:
            trimmed["model_ams_weekly_trend"] = cap_list(context.get("model_ams_weekly_trend", []), 60)

    if is_trend or is_overview:
        trimmed["weekly_sales_trend"] = cap_list(context.get("weekly_sales_trend", []), 13)
        trimmed["ams_weekly_trend"]   = cap_list(context.get("ams_weekly_trend", []), 13)

    if is_model or is_trend:
        trimmed["top_models_latest_week"] = cap_list(context.get("top_models_latest_week", []), 15)
        trimmed["top_models_last4"]       = cap_list(context.get("top_models_last4", []), 15)
        trimmed["model_weekly_sales"]     = cap_list(context.get("model_weekly_sales", []), 60)
        trimmed["model_ams_weekly_trend"] = cap_list(context.get("model_ams_weekly_trend", []), 60)
        trimmed["model_ams_performance"]  = cap_list(context.get("model_ams_performance", []), 15)

    if is_inventory or is_alert or is_overview:
        trimmed["inventory"] = cap_list(context.get("inventory", []), 20)
        trimmed["alerts"]    = cap_list(context.get("alerts", []), 10)

    if not any([is_overview, is_channel, is_category, is_ams, is_trend, is_model, is_inventory, is_alert]):
        trimmed["top_models_last4"]      = cap_list(context.get("top_models_last4", []), 10)
        trimmed["channel_breakdown"]     = cap_list(context.get("channel_breakdown", []), 8)
        trimmed["model_ams_performance"] = cap_list(context.get("model_ams_performance", []), 10)

    return trimmed


# ── Load context (JSON first, CSV fallback) ───────────────────────────────────
def load_context(week: str, brand: str) -> dict:
    if AI_CONTEXT_JSON.exists():
        try:
            with open(AI_CONTEXT_JSON) as f:
                ctx = json.load(f)
            # If brand filter active, note it (full re-filter would require CSV)
            ctx["active_brand_filter"] = brand if brand not in ("All","") else "All Brands"
            ctx["active_week_filter"]  = week
            return ctx
        except Exception:
            pass

    # Fallback: live CSV processing (slower)
    print("⚠️  ai_context.json not found — falling back to live CSV processing")
    from weekly_app.etl.build_ai_context import build_sales_context, build_ams_context, build_inventory_context, detect_anomalies
    sales_ctx     = build_sales_context(brand)
    ams_ctx       = build_ams_context(brand)
    inventory_ctx = build_inventory_context(brand)
    alerts        = detect_anomalies(sales_ctx, ams_ctx, inventory_ctx)
    return {**sales_ctx, **ams_ctx, **inventory_ctx, "alerts": alerts}


# ── System prompt ─────────────────────────────────────────────────────────────
def build_system_prompt(context: dict, week: str, brand: str, page: str) -> str:
    ctx_str = json.dumps(context, indent=2, default=str)

    # Alert summary for top of prompt
    alerts = context.get("alerts", [])
    high   = [a for a in alerts if a.get("severity") == "high"]
    alert_summary = ""
    if alerts:
        alert_summary = f"\n⚠️  ACTIVE ALERTS ({len(alerts)} total, {len(high)} high priority):\n"
        for a in alerts[:6]:
            icon = "🔴" if a.get("severity") == "high" else "🟡"
            alert_summary += f"  {icon} {a.get('message','')}\n"

    page_focus = {
        "ams-trend":   "User is on AMS/Ads page — lead with ad performance insights.",
        "inventory":   "User is on Inventory page — lead with stock and sell-through insights.",
        "sales-trend": "User is on Sales Trend page — lead with model/SKU trend insights.",
        "dashboard":   "User is on main Dashboard — lead with overall GMV and channel insights.",
    }.get(page, "")

    return f"""You are an advanced e-commerce business analyst AI inside a Weekly Unified Dashboard for a brand selling watches and electronics on Amazon India and other channels (Fossil, Nexlev, Audio_Array, White_Mulberry, Tonor).

YOU HAVE COMPLETE ACCESS TO ALL DATA:
- Sales: GMV, units, NLC by SKU/model/channel/category/week — WITH week-over-week % deltas pre-calculated
- AMS/Ads: Spend, ROAS, ACOS, TACOS, attributed sales, clicks, impressions by model/ASIN/week — WITH WoW deltas
- Inventory: stock levels, sell-through — JOINED with AMS to flag spend-on-OOS risks
- Anomaly alerts: pre-detected issues flagged automatically
{alert_summary}
FILTERS — Week: {week} | Brand: {brand if brand not in ('All','') else 'All Brands'}
{page_focus}

DATA:
{ctx_str}

HOW TO ANSWER:
- Use ONLY the data above. Never invent numbers.
- WoW delta fields like gmv_wow_pct are pre-calculated — use them directly.
- Currency: ₹ with Cr (crore=10M), L (lakh=100K), K (thousand).
- For "overall analysis" or "full picture": structure as:
  **Sales Overview** → **Channel Mix** → **Top Models** → **AMS Performance** → **Alerts & Risks**
- For model/ASIN trends: use model_ams_weekly_trend and model_weekly_sales — show week-by-week numbers.
- For alerts: explain what they mean and what action to take.
- Flag: ACOS > 60% = high risk. ROAS < 2 = low return. Stock < 14 days = reorder urgently.
- Short questions: 3-5 sentences. Deep analysis: bold headers + bullets.
- You have conversation history — reference earlier answers naturally."""


# ── Main endpoint ─────────────────────────────────────────────────────────────
@router.post("/ai-chat")
async def ai_chat(request: Request, body: ChatRequest):
    full_context    = load_context(body.week, body.brand)
    trimmed_context = trim_context_for_question(full_context, body.question)
    system_prompt   = build_system_prompt(trimmed_context, body.week, body.brand, body.page)

    messages = [{"role": m.role, "content": m.content} for m in body.history[-10:]]
    messages.append({"role": "user", "content": body.question})

    def stream_response():
        try:
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                system=system_prompt,
                messages=messages,
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
    ctx_exists = AI_CONTEXT_JSON.exists()
    ctx_age    = None
    alert_count = 0
    if ctx_exists:
        try:
            with open(AI_CONTEXT_JSON) as f:
                ctx = json.load(f)
            ctx_age     = ctx.get("generated_at")
            alert_count = ctx.get("alert_count", 0)
        except Exception:
            pass
    return {
        "status":          "ok",
        "api_key_set":     bool(os.environ.get("ANTHROPIC_API_KEY")),
        "context_json":    ctx_exists,
        "context_built_at": ctx_age,
        "active_alerts":   alert_count,
        "sales_csv":       SALES_CSV.exists(),
        "ams_csv":         AMS_CSV.exists(),
        "inventory_csv":   INVENTORY_CSV.exists(),
    }


# ── Manual rebuild trigger ────────────────────────────────────────────────────
@router.post("/ai-chat/rebuild-context")
async def rebuild_context():
    """Manually trigger context rebuild. Also called automatically after ETL."""
    try:
        from weekly_app.etl.build_ai_context import build_ai_context
        ctx = build_ai_context()
        return {
            "status":      "ok",
            "alerts":      ctx.get("alert_count", 0),
            "high_alerts": ctx.get("high_alerts", 0),
            "built_at":    ctx.get("generated_at"),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
