"""Dashboard JSON API — per-brand.

Endpoint returns every section the two source dashboards render:
 · KPI header  (monthly_target, target_till, actual, achievement, pace)
 · Total units ordered
 · Day-wise performance table
 · MTD chart (labels / actual / target)
 · Week-wise rollup (structured — mirrors the Bootstrap table the source app produced)
 · Target-vs-Actual by ASIN
 · Target-vs-Actual by Category
 · Category donut (AA-only — Nexlev's services module doesn't have it; returns null there)
 · 3-month model trend
 · Validation summary
 · Monthwise ASIN chart
 · Upload / SP-API sync capability flags
"""
from __future__ import annotations

import gc
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .. import activity
from ..auth import require_user
from ..brands import BRANDS, get_brand, list_brands
from ..ledger_io import load_ledger

router = APIRouter()

ADMIN_UPLOAD_KEY = (os.getenv("ADMIN_UPLOAD_KEY") or "").strip() or None
SYNC_SALES_TOKEN = os.getenv("SYNC_SALES_TOKEN")


@router.get("/brands")
def brands():
    return {"brands": list_brands()}


def _month_bounds(m: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.to_datetime(m, format="%b %Y")
    end = (start + pd.offsets.MonthEnd(1)).normalize()
    return start, end


def _available_months(ledger: pd.DataFrame) -> list[str]:
    if ledger.empty:
        return []
    periods = sorted(ledger["date"].dt.to_period("M").dropna().unique())
    return [p.strftime("%b %Y") for p in periods]


def _json_default(o: Any) -> Any:
    """Fallback encoder for numpy scalars, pandas Timestamps, Series, and
    anything else pydantic v2's default serializer refuses."""
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        f = float(o)
        return f if math.isfinite(f) else None
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    if isinstance(o, pd.Series):
        return o.tolist()
    if isinstance(o, pd.DataFrame):
        return o.to_dict("records")
    try:
        if pd.isna(o):
            return None
    except (TypeError, ValueError):
        pass
    # Last-resort str() — surfaces as a string instead of crashing the endpoint.
    return str(o)


def _finite(o):
    """Recursively replace NaN/inf with None — a plan row with a 0 goal makes
    achievement% infinite, and one non-finite float 500s the whole dashboard
    ('Out of range float values are not JSON compliant', 08/09/26)."""
    if isinstance(o, dict):
        return {k: _finite(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_finite(v) for v in o]
    if isinstance(o, float) and (o != o or o in (float("inf"), float("-inf"))):
        return None
    return o


def _respond(ctx: dict) -> JSONResponse:
    """Serialize dashboard payload with numpy-aware fallback. Bypasses
    pydantic's response validation (which chokes on numpy.int64)."""
    body = json.dumps(_finite(ctx), default=_json_default, allow_nan=False)
    return JSONResponse(content=json.loads(body))


# ── Computed-payload cache ───────────────────────────────────────────────
# The dashboard recomputes ~2-3s of pandas per request while the underlying
# ledger changes only when a pull/upload lands. Cache the SERIALIZED body per
# (brand + query params), byte-bounded (512MB box discipline — see the
# AMS-Trend 502 incident), and dropped for a brand the moment its ledger is
# written (ledger_io._invalidate calls invalidate_payloads).
_PAYLOAD_CACHE: dict[tuple, tuple[float, str]] = {}
_PAYLOAD_TTL_SEC = 300
_PAYLOAD_MAX_BYTES = 6 * 1024 * 1024
_payload_bytes = 0


def invalidate_payloads(brand: str | None = None) -> None:
    global _payload_bytes
    if brand is None:
        _PAYLOAD_CACHE.clear()
        _payload_bytes = 0
        return
    for k in [k for k in _PAYLOAD_CACHE if k[0] == brand]:
        _payload_bytes -= len(_PAYLOAD_CACHE.pop(k)[1])


def _payload_get(key: tuple) -> str | None:
    hit = _PAYLOAD_CACHE.get(key)
    if hit and (time.time() - hit[0]) < _PAYLOAD_TTL_SEC:
        return hit[1]
    return None


# ── Weekly-data enrichments: variation families + AMS orders ─────────────
# Sanctioned cross-read of the weekly project's outputs (same monorepo):
# raw seller sales give child→parent→model (Amazon variation families, e.g.
# B0GYFK8LCY = SC-01 + SC-04 + SC-05), business_ads_joined gives per-week
# ad-attributed orders. Both mtime/path-cached, tiny.
_WREPO = Path(__file__).resolve().parents[3]
_AMS_CSV = _WREPO / "data" / "ams_weekly_data" / "processed_ads" / "business_ads_joined.csv"
_RAW_SALES = _WREPO / "data" / "raw" / "sales"
_W33_SUNDAY = pd.Timestamp("2026-08-09")   # weekly project's Sun-Sat anchor: W33
_enrich_cache: dict = {}


def _latest_week_dir():
    best, num = None, -1
    if _RAW_SALES.exists():
        for p in _RAW_SALES.iterdir():
            m = re.match(r"Week (\d+)$", p.name)
            if m and int(m.group(1)) > num:
                num, best = int(m.group(1)), p
    return best


def _families() -> tuple[dict, dict]:
    """({parent: 'M1 + M2 + M3'} for multi-model families, {child: parent})."""
    wk = _latest_week_dir()
    if wk is None:
        return {}, {}
    key = ("fam", str(wk))
    hit = _enrich_cache.get(key)
    if hit:
        return hit
    fam: dict[str, set] = {}
    c2p: dict[str, str] = {}
    for f in wk.glob("*/Seller Sales (SP-API).xlsx"):
        try:
            df = pd.read_excel(f)
        except Exception:
            continue
        if "(Parent) ASIN" not in df.columns:
            continue
        for _, r in df.iterrows():
            p = str(r.get("(Parent) ASIN") or "").strip().upper()
            c = str(r.get("(Child) ASIN") or "").strip().upper()
            mdl = str(r.get("Model") or "").strip()
            if p and c and p != "NAN":
                c2p[c] = p
            if p and mdl and mdl.lower() not in ("nan", "none", ""):
                fam.setdefault(p, set()).add(mdl)
    comp = {p: " + ".join(sorted(m)) for p, m in fam.items() if len(m) >= 2}
    for k in [k for k in _enrich_cache if k[0] == "fam"]:
        _enrich_cache.pop(k, None)
    _enrich_cache[key] = (comp, c2p)
    return comp, c2p


def _ams_orders_by_parent(tf, tt) -> dict:
    """{parent_asin: ad-attributed orders} for weeks overlapping [tf, tt]."""
    if not _AMS_CSV.exists():
        return {}
    key = ("ams", _AMS_CSV.stat().st_mtime, str(tf)[:10], str(tt)[:10])
    hit = _enrich_cache.get(key)
    if hit is not None:
        return hit
    df = pd.read_csv(_AMS_CSV, usecols=["asin", "child_asin", "week", "ams_orders"])
    wk = pd.to_numeric(df["week"], errors="coerce")
    sun = _W33_SUNDAY + pd.to_timedelta((wk - 33) * 7, unit="D")
    sat = sun + pd.Timedelta(days=6)
    df = df[(sun <= pd.Timestamp(tt)) & (sat >= pd.Timestamp(tf))]
    _, c2p = _families()
    child = df["child_asin"].fillna("").astype(str).str.strip().str.upper()
    asin = df["asin"].fillna("").astype(str).str.strip().str.upper()
    parent = child.map(c2p).fillna(asin.map(c2p)).fillna(asin)
    out = (pd.to_numeric(df["ams_orders"], errors="coerce").fillna(0)
             .groupby(parent).sum().to_dict())
    for k in [k for k in _enrich_cache if k[0] == "ams"][:-8]:
        _enrich_cache.pop(k, None)
    _enrich_cache[key] = out
    return out


def _payload_put(key: tuple, body: str) -> None:
    global _payload_bytes
    old = _PAYLOAD_CACHE.pop(key, None)
    if old:
        _payload_bytes -= len(old[1])
    _PAYLOAD_CACHE[key] = (time.time(), body)
    _payload_bytes += len(body)
    while _payload_bytes > _PAYLOAD_MAX_BYTES and _PAYLOAD_CACHE:
        k = min(_PAYLOAD_CACHE, key=lambda k: _PAYLOAD_CACHE[k][0])
        _payload_bytes -= len(_PAYLOAD_CACHE.pop(k)[1])


def _weekwise_rows(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return (
        df.assign(week=df["date"].dt.to_period("W").astype(str))
        .groupby("week", as_index=False)["net_sales"]
        .sum()
        .round(1)
        .to_dict("records")
    )


@router.get("/{brand_key}/dashboard")
def dashboard(
    brand_key: str,
    request: Request,
    selected_month: Optional[str] = Query(None, alias="month"),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    trend_month: Optional[str] = None,
    plan_scope: bool = Query(True, description="Restrict KPI + daywise + weekwise + MTD to ASINs present in the brand's planning file. ASIN + Category tables always include out-of-plan rows."),
    _user: str = Depends(require_user),
):
    brand = get_brand(brand_key)
    if brand is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "unknown_brand"}})

    _ck = (brand.key, selected_month, from_date, to_date, trend_month, plan_scope)
    _cached = _payload_get(_ck)
    if _cached is not None:
        return JSONResponse(content=json.loads(_cached))

    activity.log(
        _user.get("email") if isinstance(_user, dict) else str(_user),
        "data_fetch",
        page="dashboard",
        brand=brand.key,
        detail={"month": selected_month, "from": from_date, "to": to_date,
                "plan_scope": plan_scope},
    )

    svc = brand.load_services()

    ledger = load_ledger(brand.key)
    months = _available_months(ledger)

    today = pd.Timestamp.today().normalize()
    trend_months: list[str] = []
    for _m in months:
        _m_start = pd.to_datetime(_m, format="%b %Y")
        _m_end = (_m_start + pd.offsets.MonthEnd(0)).normalize()
        if today < _m_end:
            continue
        # Both service modules ship `get_planning_file_for_date` — reuse it.
        try:
            plan_path = svc.get_planning_file_for_date(_m_start)
        except Exception:
            plan_path = None
        if plan_path and os.path.exists(str(plan_path)):
            trend_months.append(_m)

    f = t = None
    ledger_filtered = ledger

    if from_date and to_date:
        f = pd.to_datetime(from_date, format="%Y-%m-%d", errors="coerce")
        t = pd.to_datetime(to_date, format="%Y-%m-%d", errors="coerce")
        if pd.notna(f) and pd.notna(t):
            ledger_filtered = svc.filter_by_date_range(ledger, f, t)
    elif selected_month:
        f, t = _month_bounds(selected_month)
        ledger_filtered = svc.filter_by_date_range(ledger, f, t)

    ctx: dict[str, Any] = {
        "brand": {"key": brand.key, "label": brand.label},
        "upload_enabled": bool(ADMIN_UPLOAD_KEY),
        "sync_enabled": bool(SYNC_SALES_TOKEN),
        "sp_api_enabled": all(
            os.getenv(acc.refresh_token_env)
            and os.getenv(acc.lwa_client_id_env)
            and os.getenv(acc.lwa_client_secret_env)
            for acc in brand.sp_accounts
        ),
        "months": months,
        "trend_months": trend_months,
        "selected_month": selected_month or "",
        "from_date": from_date or "",
        "to_date": to_date or "",
        "trend_month": trend_month or "",
        "plan_scope": bool(plan_scope),
    }

    # Plan-scope filter — restrict EVERY view (KPIs, day-wise, week-wise, MTD,
    # ASIN table, Category, Donut, Monthwise) to ASINs actually present in the
    # brand's plan for the period in view. Cross-brand rows from shared seller
    # accounts (Cambium Retail / Viomi selling other brands' SKUs) are dropped
    # entirely.
    plan_asins: set[str] = set()
    plan_scope_active = False
    if plan_scope:
        plan_ref = f if f is not None else (ledger["date"].dropna().max() if not ledger.empty else None)
        if plan_ref is not None and not pd.isna(plan_ref):
            try:
                plan_df = svc.load_planning_main(plan_ref)
                if not plan_df.empty and "asin" in plan_df.columns:
                    plan_asins = set(plan_df["asin"].astype(str).str.strip())
                    # Family-aware plan scope (operator 09/09): a planned ASIN
                    # covers its WHOLE variation family — parent and children —
                    # because the parent-grain ledger reports siblings under
                    # one roof and would otherwise fall out of plan scope.
                    try:
                        fam_comp, c2p = _families()
                        parents_of_plan = {c2p.get(a.upper(), a.upper()) for a in plan_asins}
                        children_of_plan = {c for c, p in c2p.items() if p in parents_of_plan}
                        plan_asins = plan_asins | parents_of_plan | children_of_plan
                    except Exception:
                        pass
            except Exception:
                plan_asins = set()
        plan_scope_active = bool(plan_asins)
    ctx["plan_scope_active"] = plan_scope_active
    ctx["plan_asin_count"] = len(plan_asins)

    # Apply the plan-scope to the ledger THAT FEEDS ASIN + Category tables as
    # well. Everything the operator sees on the dashboard now respects the
    # planning file — no more No-Plan surprises anywhere.
    if plan_scope_active:
        ledger = ledger[ledger["ASIN"].astype(str).isin(plan_asins)]

    if ledger_filtered.empty:
        ctx.update(
            {
                "monthly_target": 0,
                "target_till": 0,
                "actual": 0,
                "achievement": 0,
                "pace": 0,
                "total_units_ordered": 0,
                "daywise": [],
                "chart": {"labels": [], "actual": [], "target": []},
                "weekwise": [],
                "asin_rows": [],
                "cat_rows": [],
                "category_donut": _try_donut(svc, ledger, f, t, plan_asins),
                "monthwise_chart": _try_monthwise(svc, ledger, plan_asins),
                "model_trend": None,
                "validation": svc.validation_summary(ledger, f, t),
            }
        )
        return _respond(ctx)

    ref_date = t if t is not None else (f if f is not None else ledger_filtered["date"].dropna().max())
    if pd.isna(ref_date):
        ref_date = pd.Timestamp.today()

    # KPI / day-wise / MTD chart / week-wise math runs on the plan-scoped
    # ledger so achievement % isn't inflated by cross-brand ASINs sold on
    # shared seller accounts.
    if plan_scope_active:
        kpi_ledger = ledger_filtered[ledger_filtered["ASIN"].astype(str).isin(plan_asins)]
    else:
        kpi_ledger = ledger_filtered

    ctx.update(svc.calculate_kpis(kpi_ledger, ref_date))
    ctx["daywise"] = svc.day_wise_performance(kpi_ledger, ref_date)
    ctx["chart"] = svc.mtd_chart(kpi_ledger, ref_date)
    ctx["weekwise"] = _weekwise_rows(kpi_ledger)
    ctx["validation"] = svc.validation_summary(ledger, f, t)

    # Units ordered is a plain sum, so it is valid over ANY period — including
    # "All months", where no single planning-file target exists. It used to be
    # derived from `asin_rows` inside the target-vs-actual guard below, which
    # meant it silently read 0 whenever that guard didn't run (All months, no
    # date range), and undercounted whenever asin_rows was capped at 500.
    # Summing kpi_ledger here keeps it consistent with `actual` and the daywise
    # sparkline, which are built from the same rows.
    ctx["total_units_ordered"] = (
        int(kpi_ledger["units"].fillna(0).sum())
        if not kpi_ledger.empty and "units" in kpi_ledger.columns
        else 0
    )

    if (pd.notna(f) and pd.notna(t)) or selected_month:
        tf, tt = f, t
        if f is not None and t is not None and (f.month != t.month or f.year != t.year):
            tf = ref_date.replace(day=1)
            tt = ref_date
        asin_fn = getattr(svc, "asin_target_vs_actual_json", svc.asin_target_vs_actual)
        cat_fn = getattr(svc, "category_target_vs_actual_json", svc.category_target_vs_actual)
        asin_rows = asin_fn(ledger, tf, tt)
        # Some ported services return HTML strings — coerce to empty list so
        # the frontend consumer stays uniform.
        if not isinstance(asin_rows, list):
            asin_rows = []
        cat_rows = cat_fn(ledger, tf, tt)
        if not isinstance(cat_rows, list):
            cat_rows = []
        # sku_master overlay — planning files sometimes carry a garbage
        # category (empty / "0" / "nan"). Fall back to the canonical
        # sku_master.category_l2 for those rows so the UI never surfaces "0".
        from ..sku_master import get as _sku_get
        _junk = {"", "0", "0.0", "nan", "none", "—", "-"}
        # sku_master is hand-maintained too and carries its own word-order
        # variants ("Dynamic Microphone" alongside "Microphone Dynamic"), so
        # run the fallback through the brand's alias map. Without this the ASIN
        # table can label a product with a spelling the category table no
        # longer has.
        _alias_fn = getattr(svc, "category_alias_map", None)
        _alias = {}
        if _alias_fn is not None:
            try:
                _alias = _alias_fn(tf) or {}
            except Exception:
                _alias = {}
        # Variation families + AMS orders from the weekly project's outputs.
        try:
            _fam, _ = _families()
            _ams = _ams_orders_by_parent(tf, tt)
        except Exception:
            _fam, _ams = {}, {}
        for r in asin_rows:
            cat = str(r.get("category", "")).strip()
            if cat.lower() in _junk:
                m = _sku_get(r.get("asin", ""))
                if m.get("category_l2"):
                    _c = str(m["category_l2"]).strip()
                    r["category"] = _alias.get(_c, _c)
            if not str(r.get("model_no") or "").strip():
                m = _sku_get(r.get("asin", ""))
                if m.get("model"):
                    r["model_no"] = m["model"]
            _a = str(r.get("asin", "")).strip().upper()
            r["ams_orders"] = int(round(_ams.get(_a, 0)))
            # A parent that roofs multiple models shows its family so a
            # "241 vs 132" (family vs one model) can never confuse again.
            if _a in _fam:
                _mn = str(r.get("model_no") or "").strip()
                r["model_no"] = _fam[_a] if not _mn or _mn in _fam[_a].split(" + ") else _mn
        # Payload trim — cap at top 500 by actual so a 700-model brand
        # doesn't overflow JSON. Sparklines are KEPT (~1-2 KB each) because
        # plan-scope filtering already trims the row count 5-15x.
        asin_rows.sort(key=lambda r: -(r.get("actual") or 0))
        if len(asin_rows) > 500:
            asin_rows = asin_rows[:500]
        ctx["asin_rows"] = asin_rows
        ctx["cat_rows"] = cat_rows
        ctx["category_donut"] = _try_donut(svc, ledger, tf, tt, plan_asins)
        ctx["monthwise_chart"] = _try_monthwise(svc, ledger, plan_asins)
    else:
        ctx["asin_rows"] = []
        ctx["cat_rows"] = []
        ctx["category_donut"] = _try_donut(svc, ledger, f, t, plan_asins)
        ctx["monthwise_chart"] = _try_monthwise(svc, ledger, plan_asins)

    if trend_month:
        try:
            trend_ref = pd.to_datetime(trend_month, format="%b %Y") + pd.offsets.MonthEnd(1)
        except Exception:
            trend_ref = None
    else:
        trend_ref = None

    if trend_ref is None or pd.isna(trend_ref):
        # Default to the CURRENT calendar month end so the trend view is
        # {current month + prior 2} — matches how operators think about it.
        trend_ref = (pd.Timestamp.today() + pd.offsets.MonthEnd(0)).normalize()
    if pd.isna(trend_ref):
        trend_ref = ref_date

    ctx["trend_month"] = trend_month or pd.to_datetime(trend_ref).strftime("%b %Y")
    ctx["model_trend"] = svc.model_trend_3months(ledger, trend_ref)

    for k in ("monthly_target", "target_till", "actual", "achievement", "pace"):
        ctx.setdefault(k, 0)

    # Free heavyweight DataFrame references before serialization + return.
    ledger = ledger_filtered = None  # noqa
    kpi_ledger = None  # noqa
    gc.collect()

    body = json.dumps(_finite(ctx), default=_json_default, allow_nan=False)
    _payload_put(_ck, body)
    return JSONResponse(content=json.loads(body))


def _try_donut(svc, ledger, f, t, plan_asins: set[str] | None = None):
    fn = getattr(svc, "category_donut_data", None)
    if fn is None:
        return None
    try:
        if plan_asins:
            ledger = ledger[ledger["ASIN"].astype(str).isin(plan_asins)]
        return fn(ledger, f, t)
    except Exception:
        return None


def _try_monthwise(svc, ledger, plan_asins: set[str] | None = None):
    fn = getattr(svc, "monthwise_asin_chart_data", None)
    if fn is None:
        return {"labels": [], "asins": [], "data": []}
    try:
        # Restrict to planned ASINs so out-of-plan revenue never surfaces
        # even in the "top ASIN" chart.
        if plan_asins:
            ledger = ledger[ledger["ASIN"].astype(str).isin(plan_asins)]
        return fn(ledger)
    except Exception:
        return {"labels": [], "asins": [], "data": []}
