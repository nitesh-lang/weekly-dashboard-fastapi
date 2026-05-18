"""
build_ai_context.py
====================
Pre-computes all dashboard aggregations, week-over-week deltas,
inventory x AMS joins, and anomaly alerts into a single JSON file.

Run this after every ETL:
    from weekly_app.etl.build_ai_context import build_ai_context
    build_ai_context()

Output: data/processed/ai_context.json
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

SALES_CSV     = Path("data/processed/weekly_sales_snapshot.csv")
AMS_CSV       = Path("data/ams_weekly_data/processed_ads/business_ads_joined.csv")
INVENTORY_CSV = Path("data/processed/inventory_model_snapshot.csv")
OUTPUT_JSON   = Path("data/processed/ai_context.json")

# All sum columns from business_ads_joined.csv
AMS_SUM_COLS  = ["Spend","Clicks","Impressions","attributed_sales","ams_orders","gmv","units","sessions"]
# All mean columns (ratios — averaging makes more sense than summing)
AMS_MEAN_COLS = ["buy_box_pct","conversion_pct","acos","roas","tacos","cac"]


# ── Helpers ──────────────────────────────────────────────────────────────────
def safe(val):
    """Convert numpy types to plain Python for JSON serialization."""
    if isinstance(val, (np.integer,)):  return int(val)
    if isinstance(val, (np.floating,)): return None if np.isnan(val) else round(float(val), 4)
    if isinstance(val, float):          return None if np.isnan(val) else round(val, 4)
    return val

def df_to_records(df):
    """Convert dataframe to JSON-safe list of dicts."""
    return [{k: safe(v) for k, v in row.items()} for row in df.to_dict(orient="records")]

def _week_to_int(w):
    """'Week 9' → 9, 19 → 19, anything else → 0 (sort first)."""
    try:
        digits = "".join(filter(str.isdigit, str(w)))
        return int(digits) if digits else 0
    except Exception:
        return 0

def sort_weeks(weeks):
    try:
        return sorted(weeks, key=_week_to_int)
    except Exception:
        return sorted(weeks)

def wow_delta(curr, prev):
    """Week-over-week % change."""
    if prev and prev != 0:
        return round((curr - prev) / abs(prev) * 100, 2)
    return None


# ── Sales aggregations ────────────────────────────────────────────────────────
def build_sales_context(brand_filter=None):
    if not SALES_CSV.exists():
        return {"error": "weekly_sales_snapshot.csv not found"}

    df = pd.read_csv(SALES_CSV, low_memory=False)
    if brand_filter and brand_filter not in ("All", ""):
        if "Brand" in df.columns:
            df = df[df["Brand"].str.lower() == brand_filter.lower()]

    all_weeks = sort_weeks(df["week"].dropna().unique().tolist()) if "week" in df.columns else []
    last4     = all_weeks[-4:] if len(all_weeks) >= 4 else all_weeks
    latest    = all_weeks[-1] if all_weeks else None
    prev      = all_weeks[-2] if len(all_weeks) >= 2 else None

    df4  = df[df["week"].isin(last4)]  if "week" in df.columns else df
    dfl  = df[df["week"] == latest]    if latest else pd.DataFrame()
    dfp  = df[df["week"] == prev]      if prev   else pd.DataFrame()

    def kpis(d):
        return {
            "units": int(d["units_sold"].sum()) if "units_sold" in d.columns else 0,
            "gmv":   safe(d["gmv"].sum())       if "gmv"        in d.columns else 0,
            "nlc":   safe(d["sales_nlc"].sum()) if "sales_nlc"  in d.columns else 0,
        }

    latest_kpis = kpis(dfl)
    prev_kpis   = kpis(dfp)

    ctx = {
        "all_weeks":    all_weeks,
        "last_4_weeks": last4,
        "latest_week":  latest,
        "prev_week":    prev,

        # KPIs with WoW deltas
        "latest_week_kpis": {
            **latest_kpis,
            "gmv_wow_pct":   wow_delta(latest_kpis["gmv"],   prev_kpis["gmv"]),
            "units_wow_pct": wow_delta(latest_kpis["units"], prev_kpis["units"]),
            "nlc_wow_pct":   wow_delta(latest_kpis["nlc"],   prev_kpis["nlc"]),
        },
        "last4_kpis": kpis(df4),
    }

    # ── Weekly trend ──
    if "week" in df.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df.columns}
        if agg:
            trend = df.groupby("week").agg(**agg).reset_index()
            trend = trend.sort_values("week", key=lambda s: s.map(_week_to_int)).reset_index(drop=True)
            # Add WoW delta columns
            for col in ["gmv", "units", "nlc"]:
                if col in trend.columns:
                    trend[f"{col}_wow_pct"] = trend[col].pct_change().mul(100).round(2)
            ctx["weekly_sales_trend"] = df_to_records(trend)

    # ── Channel breakdown (last 4 weeks) ──
    if "channel" in df4.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df4.columns}
        if agg:
            ch = df4.groupby("channel").agg(**agg).reset_index()
            total_gmv = ch["gmv"].sum() if "gmv" in ch.columns else 1
            ch["gmv_share_pct"] = (ch["gmv"] / total_gmv * 100).round(2)
            ctx["channel_breakdown"] = df_to_records(ch.sort_values("gmv", ascending=False))

    # ── Category breakdown ──
    cat_col = next((c for c in ["category_l0","Category","category"] if c in df4.columns), None)
    if cat_col:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv")] if v in df4.columns}
        if agg:
            cat = df4.groupby(cat_col).agg(**agg).reset_index()
            total = cat["gmv"].sum() if "gmv" in cat.columns else 1
            cat["gmv_share_pct"] = (cat["gmv"] / total * 100).round(2)
            ctx["category_breakdown"] = df_to_records(cat.sort_values("gmv", ascending=False))

    # ── Top models/SKUs with WoW delta ──
    model_col = next((c for c in ["Model","model"] if c in df.columns), None)
    sku_col   = next((c for c in ["FBA SKU","SKU","sku"] if c in df.columns), None)
    grp = [c for c in [model_col, sku_col, cat_col] if c]

    if grp and "gmv" in df.columns and "week" in df.columns:
        agg = {k: (v, "sum") for k, v in [("units","units_sold"),("gmv","gmv"),("nlc","sales_nlc")] if v in df.columns}

        latest_grp = dfl.groupby(grp).agg(**agg).reset_index() if not dfl.empty else pd.DataFrame()
        prev_grp   = dfp.groupby(grp).agg(**agg).reset_index() if not dfp.empty else pd.DataFrame()
        last4_grp  = df4.groupby(grp).agg(**agg).reset_index()

        if not latest_grp.empty and not prev_grp.empty:
            merged = latest_grp.merge(prev_grp, on=grp, suffixes=("","_prev"), how="left")
            for col in ["gmv","units"]:
                if col in merged.columns and f"{col}_prev" in merged.columns:
                    merged[f"{col}_wow_pct"] = merged.apply(
                        lambda r: wow_delta(r[col], r[f"{col}_prev"]), axis=1
                    )
            ctx["top_models_latest_week"] = df_to_records(merged.sort_values("gmv", ascending=False).head(25))

        ctx["top_models_last4"] = df_to_records(last4_grp.sort_values("gmv" if "gmv" in last4_grp.columns else grp[0], ascending=False).head(25))

        # Model × week trend (for "how is X trending?")
        if model_col:
            mwt = df.groupby([model_col, "week"]).agg(**agg).reset_index()
            mwt["_wnum"] = mwt["week"].map(_week_to_int)
            mwt = mwt.sort_values([model_col, "_wnum"]).drop(columns=["_wnum"]).reset_index(drop=True)
            for col in ["gmv", "units"]:
                if col in mwt.columns:
                    mwt[f"{col}_wow_pct"] = mwt.groupby(model_col)[col].pct_change().mul(100).round(2)
            ctx["model_weekly_sales"] = df_to_records(mwt)

    return ctx


# ── AMS aggregations ──────────────────────────────────────────────────────────
def build_ams_context(brand_filter=None):
    if not AMS_CSV.exists():
        return {"error": "business_ads_joined.csv not found"}

    df = pd.read_csv(AMS_CSV, low_memory=False)
    if brand_filter and brand_filter not in ("All", ""):
        if "brand" in df.columns:
            df = df[df["brand"].str.lower() == brand_filter.lower()]

    all_weeks = sort_weeks(df["week"].dropna().unique().tolist()) if "week" in df.columns else []
    last4     = all_weeks[-4:]
    latest    = all_weeks[-1] if all_weeks else None
    prev      = all_weeks[-2] if len(all_weeks) >= 2 else None

    df4 = df[df["week"].isin(last4)] if "week" in df.columns else df
    dfl = df[df["week"] == latest]   if latest else pd.DataFrame()
    dfp = df[df["week"] == prev]     if prev   else pd.DataFrame()

    def ams_kpis(d):
        def s(col): return round(float(d[col].sum()), 2) if col in d.columns else 0
        def m(col): return safe(d[col].replace([np.inf,-np.inf], np.nan).mean()) if col in d.columns else None
        return {
            "spend":            s("Spend"),
            "attributed_sales": s("attributed_sales"),
            "ams_orders":       s("ams_orders"),
            "gmv":              s("gmv"),
            "units":            s("units"),
            "clicks":           s("Clicks"),
            "impressions":      s("Impressions"),
            "sessions":         s("sessions"),
            "acos":             m("acos"),
            "roas":             m("roas"),
            "tacos":            m("tacos"),
            "buy_box_pct":      m("buy_box_pct"),
            "conversion_pct":   m("conversion_pct"),
            "cac":              m("cac"),
        }

    l_kpis = ams_kpis(dfl)
    p_kpis = ams_kpis(dfp)

    ctx = {
        "ams_last_4_weeks": last4,
        "ams_latest_week_kpis": {
            **l_kpis,
            "spend_wow_pct": wow_delta(l_kpis["spend"], p_kpis["spend"]),
            "gmv_wow_pct":   wow_delta(l_kpis["gmv"],   p_kpis["gmv"]),
        },
        "ams_last4_kpis": ams_kpis(df4),
    }

    # ── AMS weekly trend ──
    if "week" in df.columns:
        agg = {col: (col, "sum") for col in AMS_SUM_COLS if col in df.columns}
        agg.update({col: (col, "mean") for col in AMS_MEAN_COLS if col in df.columns})
        if agg:
            trend = df.groupby("week").agg(**agg).reset_index().sort_values("week")
            for col in ["Spend","gmv","attributed_sales"]:
                if col in trend.columns:
                    trend[f"{col}_wow_pct"] = trend[col].pct_change().mul(100).round(2)
            ctx["ams_weekly_trend"] = df_to_records(trend)

    # ── Per-model AMS performance (last 4 weeks) ──
    if "model" in df4.columns:
        grp = [c for c in ["model","asin","child_asin","brand","category_l0","category_l1","category_l2"] if c in df4.columns]
        agg = {col: (col, "sum") for col in AMS_SUM_COLS if col in df4.columns}
        agg.update({col: (col, "mean") for col in AMS_MEAN_COLS if col in df4.columns})
        mp = df4.groupby(grp).agg(**agg).reset_index()
        # Recalculate ratios from aggregated sums for accuracy
        if "Spend" in mp.columns and "attributed_sales" in mp.columns:
            mp["acos"] = (mp["Spend"] / mp["attributed_sales"].replace(0, np.nan)).round(4)
            mp["roas"] = (mp["attributed_sales"] / mp["Spend"].replace(0, np.nan)).round(2)
        if "Spend" in mp.columns and "gmv" in mp.columns:
            mp["tacos"] = (mp["Spend"] / mp["gmv"].replace(0, np.nan)).round(4)
        if "units" in mp.columns and "sessions" in mp.columns:
            mp["conversion_pct"] = (mp["units"] / mp["sessions"].replace(0, np.nan)).round(4)
        ctx["model_ams_performance"] = df_to_records(mp.sort_values("gmv" if "gmv" in mp.columns else "Spend", ascending=False).head(30))

    # ── Model × week AMS trend ──
    if "model" in df.columns and "week" in df.columns:
        agg = {col: (col, "sum") for col in AMS_SUM_COLS if col in df.columns}
        if agg:
            mwt = df.groupby(["model","week"]).agg(**agg).reset_index().sort_values(["model","week"])
            # Recalculate ratios per row
            if "Spend" in mwt.columns and "attributed_sales" in mwt.columns:
                mwt["acos"] = (mwt["Spend"] / mwt["attributed_sales"].replace(0, np.nan)).round(4)
                mwt["roas"] = (mwt["attributed_sales"] / mwt["Spend"].replace(0, np.nan)).round(2)
            if "Spend" in mwt.columns and "gmv" in mwt.columns:
                mwt["tacos"] = (mwt["Spend"] / mwt["gmv"].replace(0, np.nan)).round(4)
            if "units" in mwt.columns and "sessions" in mwt.columns:
                mwt["conversion_pct"] = (mwt["units"] / mwt["sessions"].replace(0, np.nan)).round(4)
            for col in ["Spend","gmv","attributed_sales"]:
                if col in mwt.columns:
                    mwt[f"{col}_wow_pct"] = mwt.groupby("model")[col].pct_change().mul(100).round(2)
            ctx["model_ams_weekly_trend"] = df_to_records(mwt)

    # ── ASIN level ──
    if "asin" in df4.columns:
        grp = [c for c in ["asin","child_asin","model","brand","category_l0","category_l1"] if c in df4.columns]
        agg = {col: (col, "sum") for col in AMS_SUM_COLS if col in df4.columns}
        if agg:
            ap = df4.groupby(grp).agg(**agg).reset_index()
            if "Spend" in ap.columns and "attributed_sales" in ap.columns:
                ap["acos"] = (ap["Spend"] / ap["attributed_sales"].replace(0, np.nan)).round(4)
                ap["roas"] = (ap["attributed_sales"] / ap["Spend"].replace(0, np.nan)).round(2)
            if "Spend" in ap.columns and "gmv" in ap.columns:
                ap["tacos"] = (ap["Spend"] / ap["gmv"].replace(0, np.nan)).round(4)
            if "units" in ap.columns and "sessions" in ap.columns:
                ap["conversion_pct"] = (ap["units"] / ap["sessions"].replace(0, np.nan)).round(4)
            ctx["asin_ams_performance"] = df_to_records(ap.sort_values("gmv" if "gmv" in ap.columns else "Spend", ascending=False).head(20))

    return ctx


# ── Inventory context ─────────────────────────────────────────────────────────
def build_inventory_context(brand_filter=None):
    if not INVENTORY_CSV.exists():
        return {}
    try:
        df = pd.read_csv(INVENTORY_CSV, low_memory=False)
        if brand_filter and brand_filter not in ("All","") and "Brand" in df.columns:
            df = df[df["Brand"].str.lower() == brand_filter.lower()]
        return {"inventory": df_to_records(df.head(60))}
    except Exception as e:
        return {"inventory_error": str(e)}


# ── Anomaly detection ─────────────────────────────────────────────────────────
def detect_anomalies(sales_ctx: dict, ams_ctx: dict, inventory_ctx: dict) -> list:
    alerts = []

    # ── Sales anomalies ──
    wow = sales_ctx.get("latest_week_kpis", {})
    gmv_wow = wow.get("gmv_wow_pct")
    if gmv_wow is not None and gmv_wow <= -20:
        alerts.append({
            "type": "sales_drop",
            "severity": "high" if gmv_wow <= -30 else "medium",
            "message": f"Overall GMV dropped {abs(gmv_wow):.1f}% week-over-week.",
            "metric": "gmv_wow_pct",
            "value": gmv_wow,
        })

    # Per-model GMV drops
    for row in sales_ctx.get("top_models_latest_week", []):
        model = row.get("Model") or row.get("model","?")
        gmv_d = row.get("gmv_wow_pct")
        gmv   = row.get("gmv", 0) or 0
        if gmv_d is not None and gmv_d <= -30 and gmv > 10000:
            alerts.append({
                "type":     "model_sales_drop",
                "severity": "high" if gmv_d <= -50 else "medium",
                "message":  f"Model {model} GMV dropped {abs(gmv_d):.1f}% WoW.",
                "model":    model,
                "value":    gmv_d,
            })

    # ── AMS anomalies ──
    for row in ams_ctx.get("model_ams_performance", []):
        model = row.get("model","?")
        acos  = row.get("acos")
        roas  = row.get("roas")
        spend = row.get("Spend") or row.get("spend", 0) or 0

        if acos is not None and acos > 0.6 and spend > 5000:
            alerts.append({
                "type":     "high_acos",
                "severity": "high" if acos > 1.0 else "medium",
                "message":  f"Model {model} has ACOS of {acos*100:.1f}% — high ad spend relative to attributed sales.",
                "model":    model,
                "value":    round(acos * 100, 1),
            })

        if roas is not None and roas < 2 and spend > 5000:
            alerts.append({
                "type":     "low_roas",
                "severity": "medium",
                "message":  f"Model {model} ROAS is {roas:.2f} — below 2x threshold.",
                "model":    model,
                "value":    roas,
            })

    # AMS spend spike
    ams_wow = ams_ctx.get("ams_latest_week_kpis", {})
    spend_wow = ams_wow.get("spend_wow_pct")
    if spend_wow is not None and spend_wow >= 30:
        alerts.append({
            "type":     "spend_spike",
            "severity": "medium",
            "message":  f"AMS spend increased {spend_wow:.1f}% WoW — verify if intentional.",
            "value":    spend_wow,
        })

    # ── Inventory anomalies ──
    for row in inventory_ctx.get("inventory", []):
        model = row.get("Model") or row.get("model","?")
        days  = row.get("days_of_stock") or row.get("days_cover") or row.get("dos")
        st    = row.get("sell_through_pct") or row.get("sell_through")

        if days is not None and isinstance(days, (int,float)) and days < 14:
            alerts.append({
                "type":     "low_stock",
                "severity": "high" if days < 7 else "medium",
                "message":  f"Model {model} has only {days:.0f} days of stock remaining.",
                "model":    model,
                "value":    days,
            })

        # Inventory × AMS: spending on nearly OOS product
        if days is not None and isinstance(days, (int,float)) and days < 14:
            for ams_row in ams_ctx.get("model_ams_performance", []):
                if str(ams_row.get("model","")).lower() == str(model).lower():
                    spend = ams_row.get("Spend") or ams_row.get("spend", 0) or 0
                    if spend > 3000:
                        alerts.append({
                            "type":     "spend_on_oos_risk",
                            "severity": "high",
                            "message":  f"Model {model} has ₹{spend:,.0f} AMS spend but only {days:.0f} days of stock — burning budget on near-OOS product.",
                            "model":    model,
                        })

    # ── Zero sales SKUs with AMS spend ──
    for row in ams_ctx.get("model_ams_performance", []):
        model = row.get("model","?")
        gmv   = row.get("gmv", 0) or 0
        spend = row.get("Spend") or row.get("spend", 0) or 0
        if gmv == 0 and spend > 1000:
            alerts.append({
                "type":     "zero_sales_with_spend",
                "severity": "high",
                "message":  f"Model {model} has ₹{spend:,.0f} AMS spend but ₹0 GMV in last 4 weeks.",
                "model":    model,
                "spend":    spend,
            })

    return alerts


# ── Main builder ──────────────────────────────────────────────────────────────
def build_ai_context():
    print("🤖 Building AI context JSON...")
    start = datetime.now()

    sales_ctx     = build_sales_context()
    ams_ctx       = build_ams_context()
    inventory_ctx = build_inventory_context()
    alerts        = detect_anomalies(sales_ctx, ams_ctx, inventory_ctx)

    context = {
        "generated_at": datetime.now().isoformat(),
        "alerts":        alerts,
        "alert_count":   len(alerts),
        "high_alerts":   sum(1 for a in alerts if a.get("severity") == "high"),
        **sales_ctx,
        **ams_ctx,
        **inventory_ctx,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(context, f, indent=2, default=str)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"✅ AI context built in {elapsed:.1f}s — {len(alerts)} alerts — saved to {OUTPUT_JSON}")
    return context


if __name__ == "__main__":
    build_ai_context()