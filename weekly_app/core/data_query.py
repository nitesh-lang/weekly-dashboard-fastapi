"""Tool-use query layer for the AI chat.

Pure-Python query functions over the canonical snapshots.  Each
function takes a small set of optional filters and returns a compact
dict with totals + per-row detail.  Claude calls these via Anthropic's
tool-use API to answer combinatorial questions like:

    "AM-C1 inventory excluding pipeline"
    "TACOS for AI-04 RED over the last 4 weeks"
    "Returns for Audio Array W22-W24 by reason"

Design constraints:
- Read-only, deterministic, reproducible.
- Reuses df_cache so repeated queries within a session don't reread CSVs.
- Each return payload caps at a few KB so token cost stays bounded.
- No state, no side effects, no agent loops.  Pure functions.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from weekly_app.core.df_cache import load_csv_cached, load_excel_cached

ROOT = Path(__file__).resolve().parents[2]
SALES_CSV         = ROOT / "data" / "processed" / "weekly_sales_snapshot.csv"
INVENTORY_CSV     = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
INVENTORY_AMS_CSV = ROOT / "data" / "processed" / "inventory_ams_snapshot.csv"
MARGIN_CSV        = ROOT / "data" / "processed" / "margin_snapshot.csv"
RETURNS_CSV       = ROOT / "data" / "processed" / "returns_snapshot.csv"
INBOUND_CSV       = ROOT / "data" / "processed" / "inbound_snapshot.csv"
REVIEWS_CSV       = ROOT / "data" / "processed" / "reviews_snapshot.csv"
BIZ_ADS_CSV       = ROOT / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
MASTER_XLSX       = ROOT / "data" / "master" / "sku_master.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wn(week_value) -> Optional[int]:
    """Extract integer week from 'Week 24', 24, '24', etc."""
    if week_value is None or (isinstance(week_value, float) and pd.isna(week_value)):
        return None
    m = re.search(r"\d+", str(week_value))
    return int(m.group()) if m else None


def _norm_str(s: Any) -> str:
    return "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s).strip()


def _ci_match(series: pd.Series, value: str) -> pd.Series:
    """Case-insensitive equality match."""
    return series.astype(str).str.strip().str.lower() == _norm_str(value).lower()


def _normalize_week_list(weeks: Optional[Sequence[Any]]) -> Optional[set[int]]:
    if not weeks:
        return None
    out: set[int] = set()
    for w in weeks:
        n = _wn(w)
        if n is not None:
            out.add(n)
    return out or None


def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".xlsx":
        return load_excel_cached(path).copy()
    return load_csv_cached(path).copy()


def _ensure_wn(df: pd.DataFrame, col: str = "week") -> pd.DataFrame:
    if col not in df.columns:
        df["wn"] = pd.NA
        return df
    df["wn"] = pd.to_numeric(
        df[col].astype(str).str.extract(r"(\d+)", expand=False),
        errors="coerce",
    ).astype("Int64")
    return df


def _filter(
    df: pd.DataFrame,
    *,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    asin: Optional[str] = None,
    sku: Optional[str] = None,
    channels_in: Optional[Sequence[str]] = None,
    channels_not_in: Optional[Sequence[str]] = None,
    types_in: Optional[Sequence[str]] = None,
    types_not_in: Optional[Sequence[str]] = None,
    weeks_in: Optional[set[int]] = None,
    week_col: str = "week",
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    if brand and "brand" in out.columns:
        out = out[_ci_match(out["brand"], brand)]
    if model and "model" in out.columns:
        out = out[_ci_match(out["model"], model)]
    if asin and "asin" in out.columns:
        out = out[_ci_match(out["asin"], asin)]
    if sku and "sku" in out.columns:
        out = out[_ci_match(out["sku"], sku)]
    if channels_in and "channel" in out.columns:
        lc = {c.strip().lower() for c in channels_in}
        out = out[out["channel"].astype(str).str.strip().str.lower().isin(lc)]
    if channels_not_in and "channel" in out.columns:
        lc = {c.strip().lower() for c in channels_not_in}
        out = out[~out["channel"].astype(str).str.strip().str.lower().isin(lc)]
    if types_in and "type" in out.columns:
        lc = {c.strip().lower() for c in types_in}
        out = out[out["type"].astype(str).str.strip().str.lower().isin(lc)]
    if types_not_in and "type" in out.columns:
        lc = {c.strip().lower() for c in types_not_in}
        out = out[~out["type"].astype(str).str.strip().str.lower().isin(lc)]
    if weeks_in is not None:
        if "wn" not in out.columns:
            out = _ensure_wn(out, week_col)
        out = out[out["wn"].isin(weeks_in)]
    return out


def _cap_rows(df: pd.DataFrame, n: int = 200) -> List[Dict[str, Any]]:
    """Convert df to records, capped to avoid bloating tool payloads."""
    if df.empty:
        return []
    return df.head(n).to_dict(orient="records")


# ---------------------------------------------------------------------------
# Catalog / discovery tools
# ---------------------------------------------------------------------------

def list_brands() -> Dict[str, Any]:
    df = _load(SALES_CSV)
    if df.empty or "brand" not in df.columns:
        return {"brands": []}
    brands = sorted({_norm_str(b) for b in df["brand"].dropna()} - {""})
    return {"brands": brands}


def list_models(brand: Optional[str] = None) -> Dict[str, Any]:
    df = _load(SALES_CSV)
    if df.empty:
        return {"models": []}
    if brand:
        df = df[_ci_match(df["brand"], brand)]
    models = sorted({_norm_str(m).upper() for m in df.get("model", pd.Series(dtype=str)).dropna()} - {""})
    return {"brand": brand or "ALL", "models": models}


def list_weeks(source: str = "sales") -> Dict[str, Any]:
    """source ∈ {'sales','inventory','ams','returns','inbound','margin'}"""
    path = {
        "sales": SALES_CSV, "inventory": INVENTORY_CSV, "ams": BIZ_ADS_CSV,
        "returns": RETURNS_CSV, "inbound": INBOUND_CSV, "margin": MARGIN_CSV,
    }.get(source, SALES_CSV)
    df = _load(path)
    if df.empty or "week" not in df.columns:
        return {"source": source, "weeks": [], "min": None, "max": None}
    df = _ensure_wn(df)
    weeks = sorted({int(w) for w in df["wn"].dropna().unique()})
    return {"source": source, "weeks": weeks, "min": weeks[0] if weeks else None,
            "max": weeks[-1] if weeks else None}


def list_channels(source: str = "sales") -> Dict[str, Any]:
    """Show channel vocabulary so Claude knows what to ask exclude/include."""
    path = SALES_CSV if source == "sales" else INVENTORY_CSV
    df = _load(path)
    if df.empty or "channel" not in df.columns:
        return {"source": source, "channels": []}
    channels = sorted({_norm_str(c) for c in df["channel"].dropna()} - {""})
    return {"source": source, "channels": channels}


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------

def get_sales(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
    sku: Optional[str] = None,
    channels_in: Optional[List[str]] = None,
    channels_not_in: Optional[List[str]] = None,
    weeks: Optional[List[int]] = None,
    rollup: str = "channel_week",
) -> Dict[str, Any]:
    """Sales slice.

    rollup ∈ {
        'channel_week' (default — one row per channel × week),
        'week'          (one row per week, totals across channels),
        'channel'       (one row per channel, totals across weeks),
        'total'         (single total row),
        'detail'        (per-row, no aggregation, capped at 200 rows),
    }
    """
    df = _ensure_wn(_load(SALES_CSV))
    df = _filter(
        df, brand=brand, model=model, asin=asin, sku=sku,
        channels_in=channels_in, channels_not_in=channels_not_in,
        weeks_in=_normalize_week_list(weeks),
    )
    if df.empty:
        return {"rows": [], "totals": {"units": 0, "gmv": 0.0}}

    num_cols = ["units_sold", "gross_sales", "sales_nlc"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    totals = {
        "units": int(df.get("units_sold", pd.Series(dtype=float)).sum()),
        "gmv":   round(float(df.get("gross_sales", pd.Series(dtype=float)).sum()), 2),
        "nlc":   round(float(df.get("sales_nlc", pd.Series(dtype=float)).sum()), 2),
        "rows_in_slice": int(len(df)),
    }

    if rollup == "detail":
        keep = [c for c in ["wn","brand","model","sku","asin","channel","units_sold","gross_sales","sales_nlc"] if c in df.columns]
        return {"totals": totals, "rows": _cap_rows(df[keep].sort_values("wn"), 200)}
    if rollup == "total":
        return {"totals": totals, "rows": []}
    if rollup == "week":
        g = df.groupby("wn", as_index=False).agg(units=("units_sold","sum"), gmv=("gross_sales","sum"))
    elif rollup == "channel":
        g = df.groupby("channel", as_index=False).agg(units=("units_sold","sum"), gmv=("gross_sales","sum"))
    else:  # channel_week
        g = df.groupby(["wn","channel"], as_index=False).agg(units=("units_sold","sum"), gmv=("gross_sales","sum"))
    g["gmv"] = g["gmv"].round(2)
    return {"totals": totals, "rows": _cap_rows(g, 200)}


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def get_inventory(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
    sku: Optional[str] = None,
    channels_in: Optional[List[str]] = None,
    channels_not_in: Optional[List[str]] = None,
    types_in: Optional[List[str]] = None,
    types_not_in: Optional[List[str]] = None,
    weeks: Optional[List[int]] = None,
    latest_only: bool = True,
    rollup: str = "channel",
) -> Dict[str, Any]:
    """Inventory slice.

    rollup ∈ {
        'channel'     (one row per channel × type),
        'total'       (single total row),
        'week'        (one row per week, totals across channels),
        'detail'      (per-row, no aggregation),
    }
    latest_only=True keeps only the most recent week present in the slice.
    """
    df = _ensure_wn(_load(INVENTORY_CSV))
    df = _filter(
        df, brand=brand, model=model, asin=asin, sku=sku,
        channels_in=channels_in, channels_not_in=channels_not_in,
        types_in=types_in, types_not_in=types_not_in,
        weeks_in=_normalize_week_list(weeks),
    )
    if df.empty:
        return {"rows": [], "totals": {"units": 0, "value": 0.0}}

    if latest_only and not weeks:
        mx = int(df["wn"].dropna().max())
        df = df[df["wn"] == mx]

    df["inventory_units"] = pd.to_numeric(df.get("inventory_units"), errors="coerce").fillna(0)
    df["inventory_value"] = pd.to_numeric(df.get("inventory_value"), errors="coerce").fillna(0)

    totals = {
        "units": int(df["inventory_units"].sum()),
        "value": round(float(df["inventory_value"].sum()), 2),
        "week":  int(df["wn"].max()) if "wn" in df.columns and df["wn"].notna().any() else None,
        "rows_in_slice": int(len(df)),
    }

    if rollup == "detail":
        keep = [c for c in ["wn","brand","model","sku","asin","channel","type","inventory_units","inventory_value","nlc","category_l0","category_l1"] if c in df.columns]
        return {"totals": totals, "rows": _cap_rows(df[keep].sort_values(["wn","channel"]), 200)}
    if rollup == "total":
        return {"totals": totals, "rows": []}
    if rollup == "week":
        g = df.groupby("wn", as_index=False).agg(units=("inventory_units","sum"), value=("inventory_value","sum"))
    else:  # channel
        g = df.groupby(["channel","type"], as_index=False, dropna=False).agg(units=("inventory_units","sum"), value=("inventory_value","sum"))
    g["value"] = g["value"].round(2)
    return {"totals": totals, "rows": _cap_rows(g, 200)}


# ---------------------------------------------------------------------------
# AMS / ads
# ---------------------------------------------------------------------------

def get_ams(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
    weeks: Optional[List[int]] = None,
    rollup: str = "week",
) -> Dict[str, Any]:
    """AMS / Amazon Ads slice with derived ACOS, TACOS, ROAS, CPC.

    rollup ∈ {'week','asin','total','detail'}
    """
    df = _ensure_wn(_load(BIZ_ADS_CSV))
    df = _filter(
        df, brand=brand, model=model, asin=asin,
        weeks_in=_normalize_week_list(weeks),
    )
    if df.empty:
        return {"rows": [], "totals": {}}

    for c in ("Spend","Clicks","Impressions","attributed_sales","ams_orders","gmv","sessions","units"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    def _derive(g: pd.DataFrame) -> pd.DataFrame:
        out = g.copy()
        out["acos"]   = (out["spend"] / out["attributed_sales"].replace(0, pd.NA)).round(4)
        out["tacos"]  = (out["spend"] / out["gmv"].replace(0, pd.NA)).round(4)
        out["roas"]   = (out["attributed_sales"] / out["spend"].replace(0, pd.NA)).round(2)
        out["cpc"]    = (out["spend"] / out["clicks"].replace(0, pd.NA)).round(2)
        for c in ("spend","attributed_sales","gmv"):
            if c in out.columns:
                out[c] = out[c].round(2)
        return out

    if rollup == "detail":
        keep = [c for c in ["wn","brand","model","asin","Spend","Clicks","Impressions","attributed_sales","ams_orders","gmv","sessions","units","acos","tacos","roas"] if c in df.columns]
        return {"rows": _cap_rows(df[keep].sort_values("wn"), 200), "totals": {}}

    if rollup == "asin":
        g = df.groupby(["asin","model"], as_index=False).agg(
            spend=("Spend","sum"), clicks=("Clicks","sum"),
            impressions=("Impressions","sum"),
            attributed_sales=("attributed_sales","sum"),
            ams_orders=("ams_orders","sum"),
            gmv=("gmv","sum"), sessions=("sessions","sum"),
            units=("units","sum"),
        )
    elif rollup == "total":
        g = df.agg(
            spend=("Spend","sum"), clicks=("Clicks","sum"),
            impressions=("Impressions","sum"),
            attributed_sales=("attributed_sales","sum"),
            ams_orders=("ams_orders","sum"),
            gmv=("gmv","sum"), sessions=("sessions","sum"),
            units=("units","sum"),
        ).to_frame().T
    else:  # week
        g = df.groupby("wn", as_index=False).agg(
            spend=("Spend","sum"), clicks=("Clicks","sum"),
            impressions=("Impressions","sum"),
            attributed_sales=("attributed_sales","sum"),
            ams_orders=("ams_orders","sum"),
            gmv=("gmv","sum"), sessions=("sessions","sum"),
            units=("units","sum"),
        )

    g = _derive(g)
    totals_row = g.sum(numeric_only=True).to_dict()
    totals = {
        "spend":            round(float(totals_row.get("spend", 0)), 2),
        "attributed_sales": round(float(totals_row.get("attributed_sales", 0)), 2),
        "gmv":              round(float(totals_row.get("gmv", 0)), 2),
        "sessions":         int(totals_row.get("sessions", 0)),
        "units":            int(totals_row.get("units", 0)),
        "weeks_in_slice":   int(g.shape[0]) if rollup == "week" else None,
    }
    if totals["spend"] > 0:
        if totals["attributed_sales"] > 0:
            totals["acos"]  = round(totals["spend"] / totals["attributed_sales"], 4)
            totals["roas"]  = round(totals["attributed_sales"] / totals["spend"], 2)
        if totals["gmv"] > 0:
            totals["tacos"] = round(totals["spend"] / totals["gmv"], 4)
    return {"rows": _cap_rows(g, 200), "totals": totals}


# ---------------------------------------------------------------------------
# Returns / Margin / Inbound / Reviews
# ---------------------------------------------------------------------------

def get_returns(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
) -> Dict[str, Any]:
    df = _load(RETURNS_CSV)
    df = _filter(df, brand=brand, model=model, asin=asin)
    if df.empty:
        return {"rows": [], "totals": {"returns": 0}}
    if "returns_3p" in df.columns:
        df["returns_3p"] = pd.to_numeric(df["returns_3p"], errors="coerce").fillna(0)
    totals = {
        "returns_3p":        int(df.get("returns_3p", pd.Series(dtype=float)).sum()),
        "sellable_units":    int(pd.to_numeric(df.get("sellable_units"), errors="coerce").fillna(0).sum()) if "sellable_units" in df.columns else None,
        "unsellable_units":  int(pd.to_numeric(df.get("unsellable_units"), errors="coerce").fillna(0).sum()) if "unsellable_units" in df.columns else None,
        "rows_in_slice":     int(len(df)),
    }
    keep = [c for c in ["brand","model","sku","asin","returns_3p","sellable_units","unsellable_units","sellable_pct","top_reason","last_return_at"] if c in df.columns]
    return {"totals": totals, "rows": _cap_rows(df[keep], 200)}


def get_margin(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
) -> Dict[str, Any]:
    df = _load(MARGIN_CSV)
    df = _filter(df, brand=brand, model=model, asin=asin)
    if df.empty:
        return {"rows": []}
    keep = [c for c in df.columns if c not in ("category_l0","category_l1","category_l2")] + [c for c in ("category_l0","category_l1","category_l2") if c in df.columns]
    return {"rows": _cap_rows(df[keep], 200)}


def get_inbound(
    model: Optional[str] = None,
    brand: Optional[str] = None,
    asin: Optional[str] = None,
) -> Dict[str, Any]:
    df = _load(INBOUND_CSV)
    df = _filter(df, brand=brand, model=model, asin=asin)
    if df.empty:
        return {"rows": [], "totals": {"soh": 0, "intransit": 0}}
    for c in df.columns:
        if c in ("am_soh","am_intransit"):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    totals = {
        "am_soh":       int(df.get("am_soh", pd.Series(dtype=float)).sum()),
        "am_intransit": int(df.get("am_intransit", pd.Series(dtype=float)).sum()),
        "rows_in_slice": int(len(df)),
    }
    return {"totals": totals, "rows": _cap_rows(df, 200)}


def get_reviews(
    asin: Optional[str] = None,
    brand: Optional[str] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    df = _load(REVIEWS_CSV)
    df = _filter(df, brand=brand, model=model, asin=asin)
    if df.empty:
        return {"rows": []}
    return {"rows": _cap_rows(df, 200)}


# ---------------------------------------------------------------------------
# Tool dispatcher
# ---------------------------------------------------------------------------

TOOL_REGISTRY = {
    "list_brands":    list_brands,
    "list_models":    list_models,
    "list_weeks":     list_weeks,
    "list_channels":  list_channels,
    "get_sales":      get_sales,
    "get_inventory":  get_inventory,
    "get_ams":        get_ams,
    "get_returns":    get_returns,
    "get_margin":     get_margin,
    "get_inbound":    get_inbound,
    "get_reviews":    get_reviews,
}


def dispatch(name: str, args: Dict[str, Any]) -> Any:
    """Execute a tool by name with kwargs. Defensive — never raises;
    returns an {error: ...} dict on failure so Claude can recover."""
    fn = TOOL_REGISTRY.get(name)
    if fn is None:
        return {"error": f"unknown tool {name!r}"}
    try:
        return fn(**(args or {}))
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
