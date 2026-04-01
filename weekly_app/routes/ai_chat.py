"""
AI Chat route — loads ALL data (sales + AMS + inventory) on every request.
Supports multi-turn conversation history.
"""

import os
import json
import anthropic
import pandas as pd
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

router = APIRouter(prefix="/api/ai", tags=["ai-chat"])

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

SALES_CSV     = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV       = Path("data/processed/business_ads_joined.csv")
INVENTORY_CSV = Path("data/processed/inventory_model_snapshot.csv")


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


def safe_sum(df, col):
    return round(float(df[col].sum()), 2) if col in df.columns else 0

def safe_mean(df, col):
    v = df[col].replace([float("inf"), float("-inf")], pd.NA).dropna()
    return round(float(v.mean()), 4) if len(v) > 0 else None

def get_last_n_weeks(df, week_col="week", n=4):
    try:
        weeks = sorted(df[week_col].dropna().unique(), key=lambda w: int("".join(filter(str.isdigit, str(w)))))
        return weeks[-n:]
    except Exception:
        return list(df[week_col].dropna().unique())

def filter_brand(df, brand, col="Brand"):
    if brand and brand not in ("All", "") and col in df.columns:
        return df[df[col].str.lower() == brand.lower()]
    return df


def load_sales_context(week: str, brand: str) -> dict:
    if not SALES_CSV.exists():
        return {"sales_error": "weekly_sales_snapshot.csv not found"}

    df = pd.read_csv(SALES_CSV, low_memory=False)
    df = filter_brand(df, brand, "Brand")

    all_weeks = get_last_n_weeks(df, "week", 99) if "week" in df.columns else []
    last4 = all_weeks[-4:]
    df4 = df[df["week"].isin(last4)] if "week" in df.columns else df
    dfw = df[df["week"] == week] if week and week not in ("All Weeks", "") and "week" in df.columns else df

    ctx = {
        "all_weeks_available": all_weeks,
        "last_4_weeks": last4,
        "selected_week": week,
        "kpis": {
            "units": int(dfw["units_sold"].sum()) if "units_sold" in dfw.columns else 0,
            "gmv": safe_sum(dfw, "gmv"),
            "nlc": safe_sum(dfw, "sales_nlc"),
        },
    }

    if "week" in df.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df.columns}
        if agg:
            ctx["weekly_sales_trend"] = df.groupby("week").agg(**agg).reset_index().sort_values("week").to_dict(orient="records")

    if "channel" in df4.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df4.columns}
        if agg:
            ctx["channel_breakdown"] = df4.groupby("channel").agg(**agg).sort_values("gmv", ascending=False).reset_index().to_dict(orient="records")

    cat_col = next((c for c in ["category_l0","Category","category"] if c in df4.columns), None)
    if cat_col:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv")] if v in df4.columns}
        if agg:
            ctx["category_breakdown"] = df4.groupby(cat_col).agg(**agg).sort_values("gmv", ascending=False).reset_index().to_dict(orient="records")

    sku_col = next((c for c in ["FBA SKU","SKU","sku"] if c in df4.columns), None)
    model_col = next((c for c in ["Model","model"] if c in df4.columns), None)
    grp = [c for c in [sku_col, model_col, cat_col] if c]
    if grp and "gmv" in df4.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df4.columns}
        ctx["top_skus_models"] = df4.groupby(grp).agg(**agg).sort_values("gmv", ascending=False).head(25).reset_index().to_dict(orient="records")

    if model_col and "week" in df.columns and "gmv" in df.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv")] if v in df.columns}
        ctx["model_weekly_sales"] = df.groupby([model_col, "week"]).agg(**agg).reset_index().sort_values([model_col, "week"]).to_dict(orient="records")

    return ctx


def load_ams_context(week: str, brand: str) -> dict:
    if not AMS_CSV.exists():
        return {"ams_error": "business_ads_joined.csv not found"}

    df = pd.read_csv(AMS_CSV, low_memory=False)
    df = filter_brand(df, brand, "brand")

    all_weeks = get_last_n_weeks(df, "week", 99) if "week" in df.columns else []
    last4 = all_weeks[-4:]
    df4 = df[df["week"].isin(last4)] if "week" in df.columns else df

    ctx = {
        "ams_last_4_weeks": last4,
        "ams_overall": {
            "total_spend": safe_sum(df4, "Spend"),
            "total_attributed_sales": safe_sum(df4, "attributed_sales"),
            "total_gmv": safe_sum(df4, "gmv"),
            "total_clicks": int(df4["Clicks"].sum()) if "Clicks" in df4.columns else 0,
            "total_impressions": int(df4["Impressions"].sum()) if "Impressions" in df4.columns else 0,
            "avg_acos": safe_mean(df4, "acos"),
            "avg_roas": safe_mean(df4, "roas"),
            "avg_tacos": safe_mean(df4, "tacos"),
        },
    }

    if "week" in df.columns:
        agg = {col: (col, "sum") for col in ["Spend","attributed_sales","gmv","Clicks","Impressions"] if col in df.columns}
        if agg:
            ctx["ams_weekly_trend"] = df.groupby("week").agg(**agg).reset_index().sort_values("week").to_dict(orient="records")

    if "model" in df4.columns:
        grp = [c for c in ["model","asin","brand","category_l0"] if c in df4.columns]
        agg = {col: (col, "sum") for col in ["Spend","attributed_sales","gmv","Clicks","Impressions","units"] if col in df4.columns}
        mp = df4.groupby(grp).agg(**agg).reset_index()
        if "Spend" in mp.columns and "attributed_sales" in mp.columns:
            mp["acos"] = (mp["Spend"] / mp["attributed_sales"].replace(0, float("nan"))).round(4)
            mp["roas"] = (mp["attributed_sales"] / mp["Spend"].replace(0, float("nan"))).round(2)
        ctx["model_ams_performance"] = mp.sort_values("gmv" if "gmv" in mp.columns else "Spend", ascending=False).head(30).to_dict(orient="records")

    if "model" in df.columns and "week" in df.columns:
        agg = {col: (col, "sum") for col in ["Spend","attributed_sales","gmv","units","Clicks"] if col in df.columns}
        if agg:
            ctx["model_ams_weekly_trend"] = df.groupby(["model","week"]).agg(**agg).reset_index().sort_values(["model","week"]).to_dict(orient="records")

    if "asin" in df4.columns:
        agg = {col: (col, "sum") for col in ["Spend","attributed_sales","gmv","Clicks"] if col in df4.columns}
        if agg:
            ap = df4.groupby("asin").agg(**agg).reset_index()
            if "Spend" in ap.columns and "attributed_sales" in ap.columns:
                ap["acos"] = (ap["Spend"] / ap["attributed_sales"].replace(0, float("nan"))).round(4)
                ap["roas"] = (ap["attributed_sales"] / ap["Spend"].replace(0, float("nan"))).round(2)
            ctx["asin_ams_performance"] = ap.sort_values("gmv" if "gmv" in ap.columns else "Spend", ascending=False).head(20).to_dict(orient="records")

    return ctx


def load_inventory_context(brand: str) -> dict:
    if not INVENTORY_CSV.exists():
        return {}
    try:
        df = pd.read_csv(INVENTORY_CSV, low_memory=False)
        df = filter_brand(df, brand, "Brand")
        return {"inventory": df.head(40).to_dict(orient="records")}
    except Exception as e:
        return {"inventory_error": str(e)}


def build_system_prompt(context: dict, week: str, brand: str, page: str) -> str:
    ctx_str = json.dumps(context, indent=2, default=str)

    page_focus = {
        "ams-trend":   "User is on AMS/Ads page — lead with ad performance insights.",
        "inventory":   "User is on Inventory page — lead with stock and sell-through insights.",
        "sales-trend": "User is on Sales Trend page — lead with model/SKU trend insights.",
        "dashboard":   "User is on main Dashboard — lead with overall GMV and channel insights.",
    }.get(page, "")

    return f"""You are an advanced e-commerce business analyst AI inside a Weekly Unified Dashboard for a brand selling watches and electronics on Amazon India and other channels (brands include Fossil, Nexlev, Audio_Array, White_Mulberry, Tonor).

YOU HAVE COMPLETE ACCESS TO ALL DATA:
- Sales: GMV, units, NLC by SKU/model/channel/category/week
- AMS/Ads: Spend, ROAS, ACOS, TACOS, attributed sales, clicks, impressions by model/ASIN/week  
- Inventory: stock levels, sell-through rates

FILTERS — Week: {week} | Brand: {brand if brand not in ('All','') else 'All Brands'}
{page_focus}

COMPLETE DATASET:
{ctx_str}

HOW TO ANSWER:
- Use ONLY the data above. Never invent numbers.
- Currency: ₹ with Cr (crore), L (lakh), K (thousand).
- For trend questions: pull from model_ams_weekly_trend, model_weekly_sales, ams_weekly_trend — show week-by-week numbers.
- For model/ASIN: use model_ams_performance, asin_ams_performance, top_skus_models.
- For channels: use channel_breakdown.
- Flag: ACOS > 50% = high spend risk. ROAS < 2 = low return. Zero sales SKUs.
- For "overall analysis" or "give me a full picture": structure your answer as:
  **Sales Overview** → **Top Channels** → **Top Models** → **AMS Performance** → **Risks & Recommendations**
- Short questions: 3-5 sentences. Deep analysis: use bold headers and bullet points.
- You remember the conversation — reference earlier questions naturally."""


@router.post("/ai-chat")
async def ai_chat(request: Request, body: ChatRequest):
    context = {}
    context.update(load_sales_context(body.week, body.brand))
    context.update(load_ams_context(body.week, body.brand))
    context.update(load_inventory_context(body.brand))

    system_prompt = build_system_prompt(context, body.week, body.brand, body.page)

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


@router.get("/ai-chat/health")
async def ai_chat_health():
    return {
        "status": "ok",
        "api_key_set": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "sales_csv": SALES_CSV.exists(),
        "ams_csv": AMS_CSV.exists(),
        "inventory_csv": INVENTORY_CSV.exists(),
    }
