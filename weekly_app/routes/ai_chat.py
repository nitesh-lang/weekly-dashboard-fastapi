"""
AI Chat route for Weekly Unified Dashboard.
Fetches live data from your existing API endpoints,
then calls the Anthropic API to answer user questions.

Add to main.py:
    from routes.ai_chat import router as ai_chat_router
    app.include_router(ai_chat_router)
"""

import os
import json
import httpx
import anthropic
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/ai", tags=["ai-chat"])

# ── Anthropic client ────────────────────────────────────────────────────────
# Set ANTHROPIC_API_KEY in your environment (Render dashboard → Environment)
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


# ── Request schema ──────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    question: str
    week: str = "Week 13"
    brand: str = "All"
    view: str = "all"


# ── Internal: fetch your existing dashboard data ────────────────────────────
async def fetch_dashboard_context(base_url: str, week: str, brand: str, view: str) -> dict:
    """
    Reads directly from the weekly_sales_snapshot.csv produced by your ETL.
    """
    import pandas as pd
    context = {}

    try:
        df = pd.read_csv("data/processed/weekly_sales_snapshot.csv", low_memory=False)

        # Filter by brand if specified
        if brand and brand != "All" and brand != "":
            if "Brand" in df.columns:
                df = df[df["Brand"] == brand]

        # Filter by week if specified (skip if "All Weeks")
        if week and week != "All Weeks" and "week" in df.columns:
            df = df[df["week"] == week]

        # Summary KPIs
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        context["summary"] = {
            "total_units": int(df["units_sold"].sum()) if "units_sold" in df.columns else 0,
            "total_gmv": round(float(df["gmv"].sum()), 2) if "gmv" in df.columns else 0,
            "total_nlc": round(float(df["sales_nlc"].sum()), 2) if "sales_nlc" in df.columns else 0,
            "weeks_included": df["week"].unique().tolist() if "week" in df.columns else [],
            "brands_included": df["Brand"].unique().tolist() if "Brand" in df.columns else [],
        }

        # Top SKUs by GMV
        if "FBA SKU" in df.columns and "gmv" in df.columns:
            top_skus = (
                df.groupby("FBA SKU")
                .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
                .sort_values("gmv", ascending=False)
                .head(15)
                .reset_index()
            )
            context["top_skus"] = top_skus.to_dict(orient="records")

        # Channel breakdown
        if "channel" in df.columns and "gmv" in df.columns:
            channels = (
                df.groupby("channel")
                .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
                .sort_values("gmv", ascending=False)
                .reset_index()
            )
            context["channels"] = channels.to_dict(orient="records")

        # Week-over-week trend
        if "week" in df.columns and "gmv" in df.columns:
            trend = (
                df.groupby("week")
                .agg(units=("units_sold", "sum"), gmv=("gmv", "sum"))
                .reset_index()
                .sort_values("week")
            )
            context["sales_trend"] = trend.to_dict(orient="records")

    except Exception as e:
        context["error"] = f"Could not load sales data: {str(e)}"

    return context


def build_system_prompt(context: dict, week: str, brand: str) -> str:
    """
    Constructs the system prompt with the dashboard data as context.
    """
    ctx_str = json.dumps(context, indent=2, default=str)

    return f"""You are an expert e-commerce business analyst assistant embedded in a Weekly Unified Dashboard used by a consumer electronics / watches brand selling on Amazon India and other channels.

CURRENT FILTERS:
- Week: {week}
- Brand: {brand}

LIVE DASHBOARD DATA (JSON):
{ctx_str}

YOUR JOB:
- Answer questions about sales numbers, trends, AMS/ads performance, and SKU-level insights using ONLY the data provided above.
- Be concise and direct. Use ₹ for currency, include units (Cr, L, K) for large numbers.
- When comparing periods, highlight % change and whether it's positive or negative.
- If the data doesn't contain enough information to answer, say so clearly.
- Format numbers cleanly: ₹1.20 Cr, ₹40.38 L, 3,621 units, 4.03% contribution.
- For AMS questions, cover: spend, ROAS, ACOS, impressions, clicks where available.
- For SKU questions, highlight top performers and flag any zero-sales SKUs.
- Keep responses to 3–6 sentences unless a detailed breakdown is explicitly requested.
- Do NOT make up numbers. Only use what is in the data above."""


# ── Main endpoint ───────────────────────────────────────────────────────────
@router.post("/ai-chat")
async def ai_chat(request: Request, body: ChatRequest):
    """
    Streams an AI answer to the user's question using live dashboard data as context.
    """
    # Build the base URL for internal API calls (same server)
    base_url = str(request.base_url).rstrip("/")

    # Fetch live data from your existing endpoints
    context = await fetch_dashboard_context(base_url, body.week, body.brand, body.view)

    system_prompt = build_system_prompt(context, body.week, body.brand)

    def stream_response():
        """Generator that yields SSE-formatted chunks."""
        try:
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                system=system_prompt,
                messages=[{"role": "user", "content": body.question}],
            ) as stream:
                for text_chunk in stream.text_stream:
                    # SSE format: data: <chunk>\n\n
                    yield f"data: {json.dumps({'text': text_chunk})}\n\n"

            yield "data: [DONE]\n\n"

        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'Invalid API key. Set ANTHROPIC_API_KEY in your environment.'})}\n\n"
        except anthropic.RateLimitError:
            yield f"data: {json.dumps({'error': 'Rate limit reached. Please wait a moment and try again.'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': f'Error: {str(e)}'})}\n\n"

    return StreamingResponse(
        stream_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Disable Nginx buffering on Render
        },
    )


# ── Health check ────────────────────────────────────────────────────────────
@router.get("/ai-chat/health")
async def ai_chat_health():
    """Quick check that the route is registered and API key is set."""
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    return {"status": "ok", "api_key_set": has_key}
