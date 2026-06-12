"""
Insights brief — narrative, prose-style weekly summary for operators
who don't want to read tables of numbers.

Reads the same `ai_context.json` the AI chat uses, hands it to Claude
with a structured prompt, and returns Markdown.  Cached per
(ai_context.json mtime, brand filter) so opening the page is instant
after the first generation; only changes to underlying data force a
re-run, keeping LLM cost trivial.
"""
from __future__ import annotations

import os
import json
import time
from pathlib import Path
from typing import Optional, Tuple

import anthropic
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/insights", tags=["insights"])

AI_CONTEXT_JSON = Path("data/processed/ai_context.json")

# Lazy-init so the import works in environments without the key set
# (e.g. CI smoke-tests).  The endpoint itself will surface a clean 503
# if the key is missing at request time.
_client: Optional[anthropic.Anthropic] = None
def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    return _client


# Cache: { (mtime_int, brand): (timestamp, markdown_text) }
# Cheap in-process cache; survives restart on Render only via
# regeneration on first request.  That's fine — first viewer pays
# ~5s on a cold restart, everyone after that gets instant.
_CACHE: dict = {}


def _slim_context(ctx: dict) -> dict:
    """Trim ai_context.json down to what the brief actually needs.

    Full context is ~150KB; we don't need every model's per-week trend
    to write 6 bullet points.  Cherry-picking keeps the prompt small
    and the LLM focused on the headline signal."""
    return {
        "generated_at":      ctx.get("generated_at"),
        "latest_week":       ctx.get("latest_week"),
        "prev_week":         ctx.get("prev_week"),
        "all_weeks":         ctx.get("all_weeks", [])[-12:],  # last 12w window
        "last_4_weeks":      ctx.get("last_4_weeks", []),
        "latest_week_kpis":  ctx.get("latest_week_kpis", {}),
        "last4_kpis":        ctx.get("last4_kpis", {}),
        "ams_latest_week_kpis": ctx.get("ams_latest_week_kpis", {}),
        "ams_last4_kpis":    ctx.get("ams_last4_kpis", {}),
        "weekly_sales_trend": ctx.get("weekly_sales_trend", [])[-12:],
        "ams_weekly_trend":   ctx.get("ams_weekly_trend", [])[-12:],
        "channel_breakdown":  ctx.get("channel_breakdown", [])[:8],
        "category_breakdown": ctx.get("category_breakdown", [])[:6],
        # The risers/fallers — what the brief is built around
        "top_models_latest_week": ctx.get("top_models_latest_week", [])[:12],
        "top_models_last4":       ctx.get("top_models_last4", [])[:12],
        "model_ams_performance":  ctx.get("model_ams_performance", [])[:10],
        # Inventory + alerts → "what to watch"
        "inventory": ctx.get("inventory", [])[:15],
        "alerts":    ctx.get("alerts", [])[:12],
        "alert_count": ctx.get("alert_count", 0),
    }


SYSTEM_PROMPT = """You are the chief-of-staff briefing the founder of a multi-brand Amazon-India business (brands: Fossil, Nexlev, Audio Array, White Mulberry, Tonor).

The founder doesn't want a numbers dump — they want a 5-minute Monday-morning read that tells them what to think about this week.  Write the brief in operator-friendly prose, like you're speaking, not like a spreadsheet.

OUTPUT — strict Markdown, exactly these sections in this order:

## This week at a glance
One paragraph (3-4 sentences).  Was it a recovery week, a slowdown, a launch ramp, an ad-spend spike?  Name the brand(s) driving the headline, and ONE specific number to anchor it (e.g. "led by Audio Array's AM-S1 launch, up 78%").  Don't list every brand.

## What went well
3 bullets, each ONE sentence.  Each bullet must name a specific model, brand, or channel and explain *why* it's good.  Examples:
- "AM-S1 hit 322 units in its first full week — Wireless Lapel category is now Audio Array's #1 contributor."
- "Nexlev's accessories ROAS climbed back above 4x after two weeks below threshold."

## What to watch
3 bullets, each ONE sentence.  Surface real risks, not generic worries.  Tie each to a specific model/SKU/brand if possible.  Examples:
- "Tonor declined for the third straight week — primarily B2B; check if a key reseller paused."
- "AM-C13 inventory cover is 11 days at current burn; reorder by Saturday or risk OOS."

## Suggested actions
3-5 bullets.  Each must be a concrete operator move starting with a verb: *Raise bid*, *Reorder*, *Pause*, *Investigate*, *Promote*.  Tie to a specific ASIN / model / brand whenever the data supports it.

## Brand briefs
One short paragraph per brand (1-2 sentences each).  Skip a brand if the data shows nothing notable.  Example:
- **Audio Array** — Recovered after two weak weeks.  AM-S1 + AA-22 driving 60% of brand growth; ROAS holding 4.1x.

STYLE RULES
- Write like a human.  No "WoW", "MoM", "TACOS" jargon dumps — translate to plain English where you can.
- Use specific numbers ONLY when they sharpen the point.  Currency in ₹ with L/Cr.  Never quote a full table.
- Never say "data shows" or "based on the data" — just say it.
- No emoji except optional 🟢/🟡/🔴 prefix on bullets to mark good/watch/risk.
- Don't pad — if there's no real story for "What went well", say so briefly rather than invent one.
"""


def _build_brief(ctx: dict, brand: Optional[str]) -> str:
    slim = _slim_context(ctx)
    brand_hint = (
        f"\nBRAND FILTER: focus only on {brand}. Skip Brand briefs for other brands."
        if brand and brand.lower() not in ("all", "")
        else ""
    )
    user_prompt = (
        f"Here is the weekly context JSON.{brand_hint}\n\n"
        f"```json\n{json.dumps(slim, indent=2, default=str)}\n```\n\n"
        f"Write the brief following the structure above."
    )
    msg = _get_client().messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1800,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return msg.content[0].text if msg.content else ""


def _cache_key(brand: Optional[str]) -> Tuple[int, str]:
    mtime = int(AI_CONTEXT_JSON.stat().st_mtime) if AI_CONTEXT_JSON.exists() else 0
    return (mtime, (brand or "all").strip().lower())


@router.get("/brief")
def get_brief(
    brand: Optional[str] = Query(None, description="Restrict the brief to one brand"),
    force: bool = Query(False, description="Bypass cache and regenerate"),
):
    if not AI_CONTEXT_JSON.exists():
        return JSONResponse(
            {"error": "ai_context.json not built — run the weekly ETL first."},
            status_code=503,
        )
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return JSONResponse(
            {"error": "ANTHROPIC_API_KEY not configured on this environment."},
            status_code=503,
        )

    key = _cache_key(brand)
    if not force and key in _CACHE:
        ts, md = _CACHE[key]
        return {
            "markdown": md,
            "cached":   True,
            "generated_at": ts,
            "context_mtime": key[0],
            "brand": brand or "all",
        }

    try:
        ctx = json.loads(AI_CONTEXT_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"ai_context.json unreadable: {e}")

    try:
        md = _build_brief(ctx, brand)
    except anthropic.AuthenticationError:
        raise HTTPException(503, "Anthropic API key rejected")
    except anthropic.RateLimitError:
        raise HTTPException(429, "Rate limit hit — try again in a moment")
    except Exception as e:
        raise HTTPException(500, f"Brief generation failed: {e}")

    now = int(time.time())
    _CACHE[key] = (now, md)
    return {
        "markdown": md,
        "cached":   False,
        "generated_at": now,
        "context_mtime": key[0],
        "brand": brand or "all",
    }


@router.get("/brands")
def get_brand_list():
    """Brand names available in the current ai_context for the picker."""
    if not AI_CONTEXT_JSON.exists():
        return {"brands": []}
    try:
        ctx = json.loads(AI_CONTEXT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"brands": []}
    seen = set()
    for entry in ctx.get("top_models_last4", []):
        b = (entry.get("brand") or "").strip()
        if b: seen.add(b)
    return {"brands": sorted(seen)}
