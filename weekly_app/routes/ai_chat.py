"""
AI Chat route — reads pre-computed ai_context.json (fast).
Falls back to live CSV processing if JSON not found.
Supports multi-turn conversation history.

When the user's question references a specific model (e.g. "AM-S1",
"K1", "JR1401"), we ALSO do a live snapshot lookup for that model and
inject a structured per-channel dossier (inventory by channel, sales by
week, ads by week) into the prompt.  This guarantees per-channel
accuracy ("how much AMPM for AM-S1?" → exact integer from the latest
inventory snapshot) without depending on ai_context.json being fresh.
"""

import os
import re
import json
import anthropic
import pandas as pd
import numpy as np
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any

from weekly_app.core import data_query

router = APIRouter(prefix="/api/ai", tags=["ai-chat"])

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

AI_CONTEXT_JSON = Path("data/processed/ai_context.json")
SALES_CSV       = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV = Path("data/ams_weekly_data/processed_ads/business_ads_joined.csv")
INVENTORY_CSV   = Path("data/processed/inventory_model_snapshot.csv")
MASTER_FILE     = Path("data/master/sku_master.xlsx")
INBOUND_CSV     = Path("data/processed/inbound_snapshot.csv")


# ── Per-model live dossier ────────────────────────────────────────────────────
# Cached lightly because read+pivot is ~50ms and we may hit it multiple times
# per session.  Cache key includes inventory mtime so refreshes invalidate.
_DOSSIER_CACHE: Dict[tuple, Dict[str, Any]] = {}


def _known_models() -> Dict[str, str]:
    """{model_upper → canonical_model} from sku_master.  Cached at module
    level via Python's import semantics; re-read happens only when the
    process restarts (master changes are infrequent)."""
    if not MASTER_FILE.exists():
        return {}
    try:
        m = pd.read_excel(MASTER_FILE)
        m.columns = m.columns.str.strip()
        if "Model" not in m.columns:
            return {}
        out = {}
        for v in m["Model"].dropna().astype(str):
            k = v.strip().upper()
            if k and k not in ("NAN", "NONE"):
                out[k] = v.strip()
        return out
    except Exception:
        return {}


def _extract_model_tokens(question: str) -> List[str]:
    """Pull model-like tokens out of the user's question and intersect with
    the master.  Catches "AM-S1", "K1", "JR1401", "AM-C13", "FS5602".

    We deliberately over-match with the regex and let the master
    intersection filter — avoids false positives like "ACOS", "ROAS"."""
    if not question:
        return []
    candidates = set(re.findall(r"\b[A-Z]{1,4}[A-Z0-9\-]{1,12}\b", question.upper()))
    # Also catch lowercase typed model names (operator may type "am-s1")
    candidates |= set(re.findall(r"\b[a-z]{1,4}[a-z0-9\-]{1,12}\b", question.lower()))
    candidates = {c.upper() for c in candidates}
    known = _known_models()
    hits = []
    for c in candidates:
        if c in known:
            hits.append(known[c])
    return hits[:5]  # cap — multi-model dump bloats prompt


def _build_model_dossier(model: str) -> Dict[str, Any]:
    """Live lookup across inventory + sales + ads + inbound snapshots for
    one model.  Returns a dict the LLM can quote directly — no further
    math required.  Per-channel inventory is the headline for accuracy
    on questions like "how much AMPM for AM-S1?"."""
    out: Dict[str, Any] = {"model": model}

    # 1) Inventory by channel — latest week, plus prior-week delta
    if INVENTORY_CSV.exists():
        try:
            inv = pd.read_csv(INVENTORY_CSV)
            inv["wn"] = pd.to_numeric(
                inv["week"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
            )
            mn = inv["model"].astype(str).str.strip().str.upper()
            sub = inv[mn == model.upper()].copy()
            if not sub.empty:
                latest_wn = int(sub["wn"].max())
                prev_wn   = latest_wn - 1
                latest = sub[sub["wn"] == latest_wn]
                prev   = sub[sub["wn"] == prev_wn]
                by_chan_latest = (
                    latest.groupby("channel")["inventory_units"].sum().astype(int).to_dict()
                )
                by_chan_prev = (
                    prev.groupby("channel")["inventory_units"].sum().astype(int).to_dict()
                )
                out["inventory"] = {
                    "latest_week": f"Week {latest_wn}",
                    "by_channel_latest": by_chan_latest,
                    "by_channel_prev":   by_chan_prev,
                    "total_latest":      int(latest["inventory_units"].sum()),
                    "total_prev":        int(prev["inventory_units"].sum()) if not prev.empty else None,
                    "brand": (latest["brand"].iloc[0] if "brand" in latest.columns and not latest.empty else None),
                    "asins": sorted({a for a in latest["asin"].astype(str).str.strip().unique() if a and a.lower() != "nan"})[:5],
                }
        except Exception as e:
            out["inventory_error"] = str(e)

    # 2) Inbound (Amazon-side SOH + in-transit) by ASIN — these are the
    # operator's truth for "what's already at Amazon" vs "in flight"
    if INBOUND_CSV.exists() and "inventory" in out and out["inventory"].get("asins"):
        try:
            inb = pd.read_csv(INBOUND_CSV)
            inb["asin"] = inb["asin"].astype(str).str.strip()
            asins = out["inventory"]["asins"]
            sub = inb[inb["asin"].isin(asins)]
            if not sub.empty:
                out["inventory"]["am_soh"]       = int(sub["am_soh"].sum())
                out["inventory"]["am_intransit"] = int(sub["am_intransit"].sum())
        except Exception:
            pass

    # 3) Sales — last 12 weeks, per channel + per week totals
    if SALES_CSV.exists():
        try:
            s = pd.read_csv(SALES_CSV)
            s["wn"] = pd.to_numeric(
                s["week"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
            )
            mn = s["model"].astype(str).str.strip().str.upper()
            sub = s[mn == model.upper()].copy()
            if not sub.empty:
                last12 = sorted(sub["wn"].dropna().unique())[-12:]
                sub = sub[sub["wn"].isin(last12)]
                weekly = (
                    sub.groupby("wn")
                       .agg(units=("units_sold", "sum"), gmv=("gross_sales", "sum"))
                       .round(2).reset_index()
                )
                weekly["week"] = weekly["wn"].astype(int).apply(lambda w: f"Week {w}")
                out["sales_last_12w"] = weekly[["week", "units", "gmv"]].to_dict("records")

                # Per-channel split, latest week only — answers "how much 1P?", "Amazon?"
                latest_w = int(sub["wn"].max())
                latest = sub[sub["wn"] == latest_w]
                out["sales_latest_by_channel"] = {
                    str(ch): {
                        "units": int(g["units_sold"].sum()),
                        "gmv":   round(float(g["gross_sales"].sum()), 2),
                    }
                    for ch, g in latest.groupby("channel")
                }
        except Exception as e:
            out["sales_error"] = str(e)

    # 4) Ads (AMS) — last 12 weeks per-week roll-up
    if AMS_CSV.exists():
        try:
            a = pd.read_csv(AMS_CSV)
            a["wn"] = pd.to_numeric(a["week"], errors="coerce")
            mn = a["Model"].astype(str).str.strip().str.upper() if "Model" in a.columns \
                else a.get("model", pd.Series([""])).astype(str).str.upper()
            sub = a[mn == model.upper()].copy()
            if not sub.empty:
                last12 = sorted(sub["wn"].dropna().unique())[-12:]
                sub = sub[sub["wn"].isin(last12)]
                cols = {
                    "spend":            "Spend" if "Spend" in sub.columns else "spend",
                    "attributed_sales": "attributed_sales",
                    "units":            "units" if "units" in sub.columns else None,
                    "gmv":              "gmv" if "gmv" in sub.columns else None,
                }
                grp = sub.groupby("wn")
                weekly = pd.DataFrame({
                    "wn":    sorted(sub["wn"].unique()),
                })
                weekly["spend"] = weekly["wn"].map(lambda w: float(grp.get_group(w)[cols["spend"]].sum())) if cols["spend"] in sub.columns else 0
                weekly["attributed_sales"] = weekly["wn"].map(lambda w: float(grp.get_group(w)[cols["attributed_sales"]].sum())) if cols["attributed_sales"] in sub.columns else 0
                if cols["gmv"]:
                    weekly["gmv"] = weekly["wn"].map(lambda w: float(grp.get_group(w)[cols["gmv"]].sum()))
                weekly["roas"]  = weekly.apply(
                    lambda r: round(r["attributed_sales"] / r["spend"], 2) if r["spend"] > 0 else None, axis=1
                )
                weekly["acos_pct"] = weekly.apply(
                    lambda r: round((r["spend"] / r["attributed_sales"]) * 100, 2) if r["attributed_sales"] > 0 else None, axis=1
                )
                if "gmv" in weekly.columns:
                    weekly["tacos_pct"] = weekly.apply(
                        lambda r: round((r["spend"] / r["gmv"]) * 100, 2) if r["gmv"] > 0 else None, axis=1
                    )
                weekly["week"] = weekly["wn"].astype(int).apply(lambda w: f"Week {w}")
                keep_cols = ["week", "spend", "attributed_sales", "roas", "acos_pct"]
                if "tacos_pct" in weekly.columns: keep_cols.append("tacos_pct")
                out["ads_last_12w"] = weekly[keep_cols].round(2).to_dict("records")
        except Exception as e:
            out["ads_error"] = str(e)

    return out


def _build_dossiers_for(question: str) -> List[Dict[str, Any]]:
    models = _extract_model_tokens(question)
    if not models:
        return []
    # Cache key: (inventory mtime, sales mtime, model)
    inv_m   = INVENTORY_CSV.stat().st_mtime if INVENTORY_CSV.exists() else 0
    sales_m = SALES_CSV.stat().st_mtime     if SALES_CSV.exists() else 0
    out = []
    for m in models:
        key = (int(inv_m), int(sales_m), m.upper())
        if key in _DOSSIER_CACHE:
            out.append(_DOSSIER_CACHE[key])
            continue
        d = _build_model_dossier(m)
        _DOSSIER_CACHE[key] = d
        out.append(d)
    return out


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

    # Always bundle inventory + alerts when a model is referenced — operator
    # expects sales + ads + inventory together in a single answer ("how's
    # AM-S1 doing?" should return all three dimensions, not just sales).
    if is_model or is_inventory or is_alert or is_overview:
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
def build_system_prompt(
    context: dict, week: str, brand: str, page: str,
    dossiers: list | None = None,
) -> str:
    ctx_str = json.dumps(context, indent=2, default=str)
    dossier_block = ""
    if dossiers:
        dossier_block = (
            "\n\nLIVE PER-MODEL DOSSIER (pulled fresh from the latest "
            "snapshots — these numbers are authoritative; prefer them "
            "over anything in the broader DATA block when they conflict):\n"
            + json.dumps(dossiers, indent=2, default=str)
        )

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
{dossier_block}

HOW TO ANSWER:
- Use ONLY the data above. Never invent numbers.
- WoW delta fields like gmv_wow_pct are pre-calculated — use them directly.
- Currency: ₹ with Cr (crore=10M), L (lakh=100K), K (thousand).

WHEN A SPECIFIC MODEL / SKU / ASIN IS MENTIONED (e.g. "how is AM-S1
doing?", "tell me about K1", "AM-C13 status"), the LIVE PER-MODEL
DOSSIER above carries the authoritative per-channel breakdown.  Use
those exact integers — do not round, do not approximate.  Structure
your answer in three short sections (do not give just one dimension):

  **Sales** — quote `sales_latest_by_channel` per-channel (Amazon, 1P
              Sales, B2B, etc.) AND the last-12w trend from
              `sales_last_12w`.  Note WoW change explicitly.
  **Ads (AMS)** — quote spend, ROAS, ACOS, attributed sales from
              `ads_last_12w` (latest week + 4w trend).
  **Inventory** — quote `inventory.by_channel_latest` EXACTLY.  When
              the operator asks for one channel (AMPM, B2B-AMPM, 1P,
              Amazon FBA, Pipeline, Open Order, YNT, Blinkit), give
              that exact integer.  Then add the WoW delta vs
              `by_channel_prev` so they can see the direction.  Also
              call out `am_soh` (already at Amazon FBA) and
              `am_intransit` (warehouse → Amazon in flight) if present.

Close with one operator action: reorder / bid up / bid down / pause / investigate.

WHEN A SPECIFIC CHANNEL IS NAMED FOR A MODEL — e.g. "AMPM for AM-S1",
"1P units of AA-22", "Amazon inventory K1" — answer with the exact
integer from `inventory.by_channel_latest[channel_name]` (or
`sales_latest_by_channel[channel_name]` for sales channels) in a
single direct sentence first, then the per-week trend.  Do NOT pad
with the full three-section breakdown unless the user also asked.

For "overall analysis" or "full picture": structure as:
  **Sales Overview** → **Channel Mix** → **Top Models** → **AMS Performance** → **Alerts & Risks**
For alerts: explain what they mean and what action to take.
Flag: ACOS > 60% = high risk. ROAS < 2 = low return. Stock < 14 days = reorder urgently.
Short questions: 3-5 sentences. Deep analysis: bold headers + bullets.
You have conversation history — reference earlier answers naturally."""


# ── Tool definitions for Anthropic tool-use ───────────────────────────────────
# Claude can call any of these to slice the canonical snapshots however the
# operator's question requires.  This gives "agent-like" combinatorial recall
# without an agent framework — Anthropic's native tool-use loop runs each
# request, deterministic and audit-friendly.
#
# Channel vocabulary the operator uses (so the tool descriptions teach Claude
# what to filter on):
#   Sales channels:    Amazon, 1p Sales, B2B, Blinkit Sales, D2C - <Brand>, Pharmaeasy
#   Inventory channels: 1P, AMPM, Amazon, B2B - AMPM, Pipeline, YNT, Blinkit
#   Inventory types:    Warehouse, Marketplace, In-Transit Inventory,
#                       Dispatch Partner

CHAT_TOOLS = [
    {
        "name": "list_brands",
        "description": "List every brand present in the sales snapshot. Use when the operator asks 'what brands do we have?' or to verify a brand name before filtering.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_models",
        "description": "List models (optionally filtered by brand). Use when the operator references a model you're not sure exists, or wants 'all models for Audio Array'.",
        "input_schema": {
            "type": "object",
            "properties": {"brand": {"type": "string", "description": "Optional brand filter"}},
        },
    },
    {
        "name": "list_weeks",
        "description": "List weeks available in a given data source so you know the valid range before asking for one. source must be one of: sales, inventory, ams, returns, inbound, margin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "enum": ["sales", "inventory", "ams", "returns", "inbound", "margin"]},
            },
            "required": ["source"],
        },
    },
    {
        "name": "list_channels",
        "description": "List the channel vocabulary used by a snapshot. source ∈ {sales, inventory}. Use when the operator says 'exclude X' and you need to confirm X exists.",
        "input_schema": {
            "type": "object",
            "properties": {"source": {"type": "string", "enum": ["sales", "inventory"]}},
            "required": ["source"],
        },
    },
    {
        "name": "get_sales",
        "description": "Get sales data. Supports filters: model, brand, asin, sku, channels_in (include only these channels), channels_not_in (exclude these channels), weeks (list of int week numbers). rollup controls aggregation: 'channel_week' (default), 'week' (across channels), 'channel' (across weeks), 'total', or 'detail' for per-row. Returns totals dict + rows list.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
                "sku":   {"type": "string"},
                "channels_in":     {"type": "array", "items": {"type": "string"}},
                "channels_not_in": {"type": "array", "items": {"type": "string"}},
                "weeks":           {"type": "array", "items": {"type": "integer"}},
                "rollup":          {"type": "string", "enum": ["channel_week","week","channel","total","detail"]},
            },
        },
    },
    {
        "name": "get_inventory",
        "description": "Get inventory snapshot data. Same filter semantics as get_sales plus types_in/types_not_in for the Type column (Warehouse, Marketplace, In-Transit Inventory, Dispatch Partner). latest_only=true (default) returns only the latest week present in the slice — set false to get history. rollup ∈ {channel (default, splits by channel × type), total, week, detail}. When the operator says 'AM-C1 inventory excluding Pipeline', pass model='AM-C1' and channels_not_in=['Pipeline'].",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
                "sku":   {"type": "string"},
                "channels_in":     {"type": "array", "items": {"type": "string"}},
                "channels_not_in": {"type": "array", "items": {"type": "string"}},
                "types_in":        {"type": "array", "items": {"type": "string"}},
                "types_not_in":    {"type": "array", "items": {"type": "string"}},
                "weeks":           {"type": "array", "items": {"type": "integer"}},
                "latest_only":     {"type": "boolean"},
                "rollup":          {"type": "string", "enum": ["channel","total","week","detail"]},
            },
        },
    },
    {
        "name": "get_ams",
        "description": "Get AMS / Amazon Ads metrics with derived ACOS, TACOS, ROAS, CPC. Filters: model, brand, asin, weeks. rollup ∈ {week (default), asin, total, detail}. Use for 'TACOS for AM-S1 last 4 weeks', 'ROAS for Audio Array W24'. AMS scope is Amazon + 1P only (ex-Fossil by operator rule).",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
                "weeks": {"type": "array", "items": {"type": "integer"}},
                "rollup": {"type": "string", "enum": ["week","asin","total","detail"]},
            },
        },
    },
    {
        "name": "get_returns",
        "description": "Get FBA returns aggregated per ASIN — returns_3p, sellable/unsellable units, sellable_pct, top_reason, last_return_at. Filters: model, brand, asin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
            },
        },
    },
    {
        "name": "get_margin",
        "description": "Get per-ASIN margin data (cost, fees, net contribution). Filters: model, brand, asin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
            },
        },
    },
    {
        "name": "get_inbound",
        "description": "Get FBA inbound + in-transit PO data per ASIN. Includes am_soh (already at Amazon) and am_intransit (warehouse → Amazon in flight). Filters: model, brand, asin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
            },
        },
    },
    {
        "name": "get_reviews",
        "description": "Get Helium-10 reviews / sales rank data per ASIN. Filters: model, brand, asin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "brand": {"type": "string"},
                "asin":  {"type": "string"},
            },
        },
    },
]


def _run_tool_use_loop(
    *,
    system_prompt: str,
    messages: list,
    max_iterations: int = 6,
) -> str:
    """Run Claude with tool-use enabled until it returns a plain text answer.

    Returns the final assistant text. Each tool call is dispatched to
    data_query and the result fed back as a tool_result block.
    """
    convo = list(messages)
    for _ in range(max_iterations):
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            system=system_prompt,
            tools=CHAT_TOOLS,
            messages=convo,
        )
        if resp.stop_reason != "tool_use":
            # Plain text terminal response — extract and return.
            text_parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
            return "\n".join(text_parts).strip()

        # Append the assistant message (with tool_use blocks) and execute each tool.
        convo.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for block in resp.content:
            if getattr(block, "type", None) != "tool_use":
                continue
            result = data_query.dispatch(block.name, dict(block.input or {}))
            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": block.id,
                "content":     json.dumps(result, default=str),
            })
        convo.append({"role": "user", "content": tool_results})

    return "I made too many lookups without converging. Please rephrase or narrow the question."


# ── Main endpoint ─────────────────────────────────────────────────────────────
@router.post("/ai-chat")
async def ai_chat(request: Request, body: ChatRequest):
    full_context    = load_context(body.week, body.brand)
    trimmed_context = trim_context_for_question(full_context, body.question)
    # Live per-model lookup whenever the question names a known model —
    # gives the LLM authoritative per-channel numbers that ai_context
    # doesn't carry at that granularity.
    dossiers        = _build_dossiers_for(body.question)
    system_prompt   = build_system_prompt(
        trimmed_context, body.week, body.brand, body.page, dossiers=dossiers,
    )

    messages = [{"role": m.role, "content": m.content} for m in body.history[-10:]]
    messages.append({"role": "user", "content": body.question})

    # Tool-use addendum — teaches Claude that it has live data access and
    # should prefer tool calls over the static DATA block when the operator
    # asks for a specific slice (channels excluded, week ranges, per-model
    # metrics).  The static block stays for high-level orientation.
    tool_prompt = (
        "\n\nLIVE QUERY TOOLS:\n"
        "You have data_query tools that read the canonical snapshots in real "
        "time.  PREFER tool calls over the static DATA block whenever the "
        "operator asks for a specific slice — model, channel exclusions, "
        "week ranges, per-ASIN metrics, returns, margin, inbound, reviews.  "
        "Chain tools as needed in one turn: e.g. for 'AM-C1 inventory excluding "
        "pipeline and TACOS last 4 weeks', call get_inventory(model='AM-C1', "
        "channels_not_in=['Pipeline']) AND get_ams(model='AM-C1', weeks=[21,22,23,24]) "
        "then compose the answer.  If a model/brand/week isn't found, call "
        "list_models / list_brands / list_weeks to recover.  Keep tool inputs "
        "concise; never call get_*(rollup='detail') unless the operator asked "
        "for per-row drilldown.  When you have the numbers, answer directly — "
        "do not narrate the tool calls."
    )
    system_prompt_with_tools = system_prompt + tool_prompt

    def stream_response():
        try:
            answer = _run_tool_use_loop(
                system_prompt=system_prompt_with_tools,
                messages=messages,
            )
            # Stream the final answer in modest chunks so the SPA's
            # progressive-render UX still feels live.
            CHUNK = 80
            for i in range(0, len(answer), CHUNK):
                yield f"data: {json.dumps({'text': answer[i:i+CHUNK]})}\n\n"
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
