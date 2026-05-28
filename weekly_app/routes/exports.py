from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
import pandas as pd
from pathlib import Path
from io import BytesIO

router = APIRouter(prefix="/export", tags=["Exports"])

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
INV_FILE = Path("data/processed/inventory_model_snapshot.csv")
MASTER_FILE = Path("data/master/sku_master.xlsx")


# ==================================================
# PARAM SAFETY (ROOT FIX)
# ==================================================
def clean_param(x):
    if x in [None, "", "None", "null"]:
        return None
    return x


# ==================================================
# HYGIENE
# ==================================================
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "sku_status" in df.columns:
        df["sku_status"] = df["sku_status"].astype(str).str.strip().str.upper()

    for c in ["units_sold", "inventory_units"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype(int)

    for c in ["gross_sales", "gmv", "inventory_value", "nlc", "sales_nlc"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).round(2)

    return df

from io import BytesIO
def csv_response(df: pd.DataFrame, filename: str):
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def apply_filters(df, week=None, brand=None, view="mapped"):
    week = clean_param(week)
    brand = clean_param(brand)

    if week:
        df = df[df["week"] == week]
    if brand:
        df = df[df["brand"] == brand]
    if view == "mapped" and "sku_status" in df.columns:
        df = df[df["sku_status"] == "MAPPED"]

    return df


# ==================================================
# INVENTORY SOURCE
# data/processed/inventory_model_snapshot.csv only carries model-level
# rollups: [week, brand, model, inventory_units, inventory_value] — no
# sku, channel, category_l0, type, nlc. So all the SKU+channel-level
# exports were crashing on the missing columns.
# Use the inventory dashboard's load_all_inventory() instead — it reads
# the raw weekly xlsx files and produces a SKU+channel+category-rich
# frame with everything those exports need.
# ==================================================
def _load_rich_inventory(with_sku_status: bool = False) -> pd.DataFrame:
    """Returns SKU/channel/category-rich inventory.

    If with_sku_status=True, joins against the SKU master to stamp each
    row with sku_status = MAPPED/UNMAPPED so view-filters work.

    Falls back to the lossy model-level CSV if the rich loader fails
    (degraded mode — many endpoints will still produce empty results).
    """
    df = pd.DataFrame()
    try:
        from weekly_app.routes.inventory_dashboard import load_all_inventory
        df = load_all_inventory()
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        if INV_FILE.exists():
            df = normalize(pd.read_csv(INV_FILE))

    if df is None or df.empty:
        return pd.DataFrame()

    if with_sku_status and "sku" in df.columns:
        try:
            from weekly_app.routes.dashboard import _load_excel_cached, SKU_MASTER
            master = _load_excel_cached(SKU_MASTER)
            master.columns = master.columns.str.strip()
            master = master.rename(columns={"FBA SKU": "sku"})
            mapped_skus = (
                set(master["sku"].astype(str).str.strip())
                if "sku" in master.columns else set()
            )
            df = df.copy()
            df["sku_status"] = df["sku"].astype(str).str.strip().apply(
                lambda s: "MAPPED" if s in mapped_skus else "UNMAPPED"
            )
        except Exception:
            pass

    return df


def _norm_channel(s: pd.Series) -> pd.Series:
    """Channel values come from heterogeneous sources (sales snapshot vs
    raw inventory xlsx); title-case both sides so groupby/merge align.
    'Amazon' / '1P Sales' / 'D2C - Audio Array'.
    """
    return s.astype(str).str.strip().str.title()


# ==================================================
# CHANNEL SUMMARY (MATCH DASHBOARD)
# ==================================================
@router.get("/channel-summary")
def export_channel_summary(
    week: str = Query(None),
    brand: str = Query(None),
):
    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    sales = apply_filters(sales, week, brand, "mapped")
    inv = apply_filters(inv, week, brand, "mapped")

    if "channel" in sales.columns:
        sales["channel"] = _norm_channel(sales["channel"])
    if "channel" in inv.columns:
        inv["channel"] = _norm_channel(inv["channel"])

    sales_agg_kwargs = {
        "units_sold": ("units_sold", "sum"),
        "gmv": ("gross_sales", "sum"),
    }
    if "sales_nlc" in sales.columns:
        sales_agg_kwargs["sales_nlc"] = ("sales_nlc", "sum")

    s = sales.groupby("channel", as_index=False).agg(**sales_agg_kwargs)

    if "channel" in inv.columns and not inv.empty:
        i = inv.groupby("channel", as_index=False).agg(
            inventory_units=("inventory_units", "sum"),
            inventory_value=("inventory_value", "sum"),
        )
    else:
        i = pd.DataFrame(columns=["channel", "inventory_units", "inventory_value"])

    out = s.merge(i, on="channel", how="outer").fillna(0)
    out["sell_through_pct"] = (
        out["units_sold"] / out["inventory_units"].replace(0, pd.NA)
    ).fillna(0).round(1)

    total_gmv = out["gmv"].sum() or 1
    out["sales_contribution_pct"] = (out["gmv"] / total_gmv * 100).round(2)

    return csv_response(out, "channel_summary.csv")


# ==================================================
# CATEGORY SUMMARY
# ==================================================
@router.get("/category-summary")
def export_category_summary(
    week: str = Query(None),
    brand: str = Query(None),
):
    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    sales = apply_filters(sales, week, brand, "mapped")
    inv = apply_filters(inv, week, brand, "mapped")

    sales_agg_kwargs = {
        "units_sold": ("units_sold", "sum"),
        "gmv": ("gross_sales", "sum"),
    }
    if "sales_nlc" in sales.columns:
        sales_agg_kwargs["sales_nlc"] = ("sales_nlc", "sum")

    s = sales.groupby("category_l0", as_index=False).agg(**sales_agg_kwargs)

    if "category_l0" in inv.columns and not inv.empty:
        i = inv.groupby("category_l0", as_index=False).agg(
            inventory_units=("inventory_units", "sum"),
            inventory_value=("inventory_value", "sum"),
        )
    else:
        i = pd.DataFrame(columns=["category_l0", "inventory_units", "inventory_value"])

    out = s.merge(i, on="category_l0", how="outer").fillna(0)
    out["sell_through_pct"] = (
        out["units_sold"] / out["inventory_units"].replace(0, pd.NA)
    ).fillna(0).round(1)

    total_gmv = out["gmv"].sum() or 1
    out["sales_contribution_pct"] = (out["gmv"] / total_gmv * 100).round(2)

    return csv_response(out, "category_summary.csv")


# ==================================================
# INVENTORY SNAPSHOT
# ==================================================
@router.get("/inventory")
def export_inventory(
    week: str = Query(None),
    brand: str = Query(None),
    view: str = Query("mapped"),
):
    df = normalize(pd.read_csv(INV_FILE))
    df = apply_filters(df, week, brand, view)
    return csv_response(df, "inventory_snapshot.csv")


# ==================================================
# STOCKOUT (SKU + CHANNEL)
# ==================================================
@router.get("/stockout")
def export_stockout(
    week: str = Query(None),
    brand: str = Query(None),
    view: str = Query("mapped"),
):
    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    sales = apply_filters(sales, week, brand, view)
    inv = apply_filters(inv, week, brand, view)

    if "channel" in sales.columns:
        sales["channel"] = _norm_channel(sales["channel"])
    if "channel" in inv.columns:
        inv["channel"] = _norm_channel(inv["channel"])

    s = sales.groupby(["sku", "channel"], as_index=False).agg(
        units_sold=("units_sold", "sum")
    )

    if {"sku", "channel"}.issubset(inv.columns) and not inv.empty:
        i = inv.groupby(["sku", "channel"], as_index=False).agg(
            inventory_units=("inventory_units", "sum")
        )
    else:
        i = pd.DataFrame(columns=["sku", "channel", "inventory_units"])

    df = s.merge(i, on=["sku", "channel"], how="outer").fillna(0)
    df["oversold"] = df["units_sold"] - df["inventory_units"]

    df = df[df["oversold"] > 0]

    return csv_response(df, "stockout.csv")


# ==================================================
# DEAD STOCK (SKU + CHANNEL)
# ==================================================
@router.get("/deadstock")
def export_deadstock(
    week: str = Query(None),
    brand: str = Query(None),
    view: str = Query("mapped"),
):
    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    sales = apply_filters(sales, week, brand, view)
    inv = apply_filters(inv, week, brand, view)

    if "channel" in sales.columns:
        sales["channel"] = _norm_channel(sales["channel"])
    if "channel" in inv.columns:
        inv["channel"] = _norm_channel(inv["channel"])

    if not {"sku", "channel", "inventory_units"}.issubset(inv.columns) or inv.empty:
        return csv_response(
            pd.DataFrame(columns=["sku", "channel", "inventory_units"]),
            "deadstock.csv",
        )

    sold_pairs = (
        set(zip(sales["sku"].astype(str), sales["channel"].astype(str)))
        if {"sku", "channel"}.issubset(sales.columns) else set()
    )

    inv = inv.copy()
    inv["_pair"] = list(zip(inv["sku"].astype(str), inv["channel"].astype(str)))
    dead = inv[(inv["inventory_units"] > 0) & (~inv["_pair"].isin(sold_pairs))].drop(columns=["_pair"])

    return csv_response(dead, "deadstock.csv")


# ==================================================
# RECONCILIATION EXPORT (⭐ NEW – SINGLE SOURCE ⭐)
# ==================================================
@router.get("/reconciliation")
def export_reconciliation(
    week: str = Query(None),
    brand: str = Query(None),
    channel: str = Query(None),
    view: str = Query("mapped"),
):
    week = clean_param(week)
    brand = clean_param(brand)
    channel = clean_param(channel)

    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    if week:
        sales = sales[sales["week"] == week]
        if "week" in inv.columns:
            inv = inv[inv["week"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        if "brand" in inv.columns:
            inv = inv[inv["brand"] == brand]

    if channel:
        sales = sales[sales["channel"] == channel]
        if "channel" in inv.columns:
            inv = inv[inv["channel"] == channel]

    if view == "mapped":
        if "sku_status" in sales.columns:
            sales = sales[sales["sku_status"] == "MAPPED"]
        if "sku_status" in inv.columns:
            inv = inv[inv["sku_status"] == "MAPPED"]

    if "channel" in sales.columns:
        sales["channel"] = _norm_channel(sales["channel"])
    if "channel" in inv.columns:
        inv["channel"] = _norm_channel(inv["channel"])

    sales_g = sales.groupby(
        ["week", "brand", "channel", "sku", "sku_status"],
        as_index=False,
    ).agg(
        units_sold=("units_sold", "sum"),
        gross_sales=("gross_sales", "sum"),
    )

    inv_keys = ["week", "brand", "channel", "sku"]
    if {*inv_keys, "inventory_units"}.issubset(inv.columns) and not inv.empty:
        inv_g = inv.groupby(inv_keys, as_index=False).agg(
            inventory_units=("inventory_units", "sum"),
            inventory_value=("inventory_value", "sum"),
            nlc=("nlc", "max") if "nlc" in inv.columns else ("inventory_units", "max"),
        )
    else:
        inv_g = pd.DataFrame(columns=inv_keys + ["inventory_units", "inventory_value", "nlc"])

    out = sales_g.merge(inv_g, on=inv_keys, how="outer")
    # Rows present only on the inv side have no sku_status — flag them
    if "sku_status" in out.columns:
        out["sku_status"] = out["sku_status"].fillna("INV-ONLY")
    out = out.fillna(0)

    out["sell_through_gap"] = out["inventory_units"] - out["units_sold"]
    out["stockout_flag"] = out["units_sold"] > out["inventory_units"]
    out["dead_stock_flag"] = (
        (out["inventory_units"] > 0) & (out["units_sold"] == 0)
    )

    return csv_response(out, "reconciliation.csv")


# ==================================================
# UNMAPPED (ALWAYS BOTH)
# ==================================================
@router.get("/unmapped")
def export_unmapped(
    week: str = Query(None),
    brand: str = Query(None),
):
    week = clean_param(week)
    brand = clean_param(brand)

    sales = normalize(pd.read_csv(SALES_FILE))
    inv = _load_rich_inventory(with_sku_status=True)

    if "sku_status" in sales.columns:
        sales = sales[sales["sku_status"] == "UNMAPPED"]
    if "sku_status" in inv.columns:
        inv = inv[inv["sku_status"] == "UNMAPPED"]

    if week:
        sales = sales[sales["week"] == week]
        if "week" in inv.columns:
            inv = inv[inv["week"] == week]
    if brand:
        sales = sales[sales["brand"] == brand]
        if "brand" in inv.columns:
            inv = inv[inv["brand"] == brand]

    out = pd.concat([sales, inv], ignore_index=True) if not inv.empty else sales
    return csv_response(out, "unmapped.csv")

# ==================================================
# DASHBOARD SKU EXPORT — UI PARITY
# Rebuilds the wide SKU breakdown shown on /dashboard:
#   sku, model_no, category_l0,
#   total_units, total_sales, total_nlc, sales_contribution_pct,
#   amazon_am_*, amazon_1p_*, amazon_total_*
# Reuses dashboard.py loaders/helpers so logic stays in one place.
# ==================================================
@router.get("/dashboard-sku")
def export_dashboard_sku(
    week: str = Query(None),
    weeks: list[str] = Query(default=[]),
    brand: str = Query(None),
    view: str = Query("mapped"),
):
    from weekly_app.routes.dashboard import (
        SALES_FILE as _DSH_SALES_FILE,
        SKU_MASTER as _DSH_MASTER,
        is_amazon, round_df,
        _load_csv_cached, _load_excel_cached,
    )

    week = clean_param(week)
    brand = clean_param(brand)

    def _fix_week(w):
        w = str(w).strip()
        if w.startswith("Week") and " " not in w:
            w = w.replace("Week", "Week ")
        return w

    if weeks:
        active_weeks = [_fix_week(w) for w in weeks if str(w).strip()]
    elif week:
        active_weeks = [_fix_week(week)]
    else:
        active_weeks = []

    full_sales = _load_csv_cached(_DSH_SALES_FILE)
    full_sales.columns = full_sales.columns.str.strip().str.lower()
    full_sales["week"] = full_sales["week"].astype(str).str.strip()
    full_sales["sku"] = full_sales["sku"].astype(str)
    full_sales["channel"] = full_sales["channel"].astype(str)

    sales = full_sales.copy()
    if active_weeks:
        sales = sales[sales["week"].isin(active_weeks)]
    if brand and "brand" in sales.columns:
        sales = sales[sales["brand"].str.lower() == brand.lower()]
    if view == "mapped" and "sku_status" in sales.columns:
        sales = sales[sales["sku_status"] == "MAPPED"]

    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        if c not in sales.columns:
            sales[c] = 0
        sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0)

    total_gmv = float(sales["gross_sales"].sum())

    sku_totals = sales.groupby("sku", as_index=False).agg(
        total_units=("units_sold", "sum"),
        total_sales=("gross_sales", "sum"),
        total_nlc=("sales_nlc", "sum"),
    )
    if total_gmv > 0:
        sku_totals["sales_contribution_pct"] = (
            sku_totals["total_sales"] / total_gmv * 100
        ).round(2)
    else:
        sku_totals["sales_contribution_pct"] = 0.0

    amazon = sales[sales["channel"].apply(is_amazon)].copy()
    if amazon.empty:
        amazon = sales.iloc[0:0].copy()

    if not amazon.empty:
        ch_lower = amazon["channel"].str.lower()
        is_am_mask = ch_lower.str.contains("amazon") & ~ch_lower.str.contains("1p")
        is_1p_mask = ch_lower.str.contains("1p")

        amazon["amazon_am_units"] = amazon["units_sold"].where(is_am_mask, 0)
        amazon["amazon_am_sales"] = amazon["gross_sales"].where(is_am_mask, 0)
        amazon["amazon_am_nlc"]   = amazon["sales_nlc"].where(is_am_mask, 0)
        amazon["amazon_1p_units"] = amazon["units_sold"].where(is_1p_mask, 0)
        amazon["amazon_1p_sales"] = amazon["gross_sales"].where(is_1p_mask, 0)
        amazon["amazon_1p_nlc"]   = amazon["sales_nlc"].where(is_1p_mask, 0)

        amazon_split = amazon.groupby("sku", as_index=False).agg(
            amazon_am_units=("amazon_am_units", "sum"),
            amazon_am_sales=("amazon_am_sales", "sum"),
            amazon_am_nlc=("amazon_am_nlc", "sum"),
            amazon_1p_units=("amazon_1p_units", "sum"),
            amazon_1p_sales=("amazon_1p_sales", "sum"),
            amazon_1p_nlc=("amazon_1p_nlc", "sum"),
        )
    else:
        amazon_split = pd.DataFrame(columns=[
            "sku",
            "amazon_am_units", "amazon_am_sales", "amazon_am_nlc",
            "amazon_1p_units", "amazon_1p_sales", "amazon_1p_nlc",
        ])

    amazon_split["amazon_total_units"] = (
        amazon_split.get("amazon_am_units", 0) + amazon_split.get("amazon_1p_units", 0)
    )
    amazon_split["amazon_total_sales"] = (
        amazon_split.get("amazon_am_sales", 0) + amazon_split.get("amazon_1p_sales", 0)
    )
    amazon_split["amazon_total_nlc"] = (
        amazon_split.get("amazon_am_nlc", 0) + amazon_split.get("amazon_1p_nlc", 0)
    )

    master = _load_excel_cached(_DSH_MASTER)
    master.columns = master.columns.str.strip()
    master = master.rename(columns={
        "FBA SKU": "sku",
        "Model No.": "model_no",
        "Model": "model",
        "ASIN":  "asin",
    })
    if "model_no" not in master.columns and "model" in master.columns:
        master["model_no"] = master["model"]
    master["sku"] = master["sku"].astype(str)
    if "asin" in master.columns:
        master["asin"] = master["asin"].astype(str)
        master = master[["sku", "model_no", "asin", "category_l0"]]
    else:
        master["asin"] = ""
        master = master[["sku", "model_no", "asin", "category_l0"]]

    sku = (
        sku_totals
        .merge(amazon_split, on="sku", how="left")
        .merge(master, on="sku", how="left")
    )

    for col in ["model_no", "asin", "category_l0"]:
        if col in sku.columns:
            sku[col] = sku[col].astype(str).replace("nan", "")

    sku = round_df(sku)
    sku = sku.sort_values("total_sales", ascending=False)

    cols = [
        "sku", "model_no", "asin", "category_l0",
        "total_units", "total_sales", "total_nlc", "sales_contribution_pct",
        "amazon_am_units", "amazon_am_sales", "amazon_am_nlc",
        "amazon_1p_units", "amazon_1p_sales", "amazon_1p_nlc",
        "amazon_total_units", "amazon_total_sales", "amazon_total_nlc",
    ]
    for c in cols:
        if c not in sku.columns:
            sku[c] = 0
    sku = sku[cols].fillna(0)

    if not sku.empty:
        total_row = {
            "sku": "TOTAL",
            "model_no": "",
            "asin": "",
            "category_l0": "",
            "total_units": int(sku["total_units"].sum()),
            "total_sales": float(sku["total_sales"].sum()),
            "total_nlc": float(sku["total_nlc"].sum()),
            "sales_contribution_pct": 100.0,
            "amazon_am_units": int(sku["amazon_am_units"].sum()),
            "amazon_am_sales": float(sku["amazon_am_sales"].sum()),
            "amazon_am_nlc": float(sku["amazon_am_nlc"].sum()),
            "amazon_1p_units": int(sku["amazon_1p_units"].sum()),
            "amazon_1p_sales": float(sku["amazon_1p_sales"].sum()),
            "amazon_1p_nlc": float(sku["amazon_1p_nlc"].sum()),
            "amazon_total_units": int(sku["amazon_total_units"].sum()),
            "amazon_total_sales": float(sku["amazon_total_sales"].sum()),
            "amazon_total_nlc": float(sku["amazon_total_nlc"].sum()),
        }
        sku = pd.concat([sku, pd.DataFrame([total_row])], ignore_index=True)

    return csv_response(sku, "dashboard_sku.csv")


# ==================================================
# SALES TREND SKU — FULL EXPORT (UI PARITY)
# Mirrors /sales-trend (sales_trend_sku.html) columns:
#   model, brand, category_l0/l1/l2,
#   per-week sales / units / sales_pct,
#   last_4w_units, avg_4w, inventory_units, trend,
#   plus Grand Total row.
# Reuses sales_trend.py loaders + trend() helper.
# ==================================================
@router.get("/sales-trend-sku")
def export_sales_trend_sku(
    brand: str = Query("All"),                       # legacy single-brand
    brands: list[str] = Query(default=[]),           # multi-brand checkboxes
    sel_weeks: list[str] = Query(default=[]),
):
    from weekly_app.routes.sales_trend import (
        load_sales as _st_load_sales,
        load_inventory as _st_load_inventory,
        trend as _st_trend,
    )
    from weekly_app.core.df_cache import load_excel_cached

    sales = _st_load_sales()

    eff_brands_lower = [b.strip().lower() for b in (brands or []) if b and b.strip().lower() != "all"]
    if not eff_brands_lower and brand and brand != "All":
        eff_brands_lower = [brand.strip().lower()]

    base = sales
    if eff_brands_lower:
        base = sales[sales["brand"].isin(eff_brands_lower)]

    weeks_df = (
        base[["week", "week_num"]]
        .dropna()
        .drop_duplicates()
        .sort_values("week_num")
    )
    if sel_weeks:
        weeks_df = weeks_df[weeks_df["week"].isin(sel_weeks)]
    else:
        weeks_df = weeks_df.tail(4)

    weeks = weeks_df["week"].tolist()

    if weeks_df.empty:
        return csv_response(pd.DataFrame(), "sales_trend_sku_full.csv")

    latest_week = weeks_df["week_num"].iloc[-1]
    inventory = _st_load_inventory(latest_week)

    base_in_weeks = base[base["week"].isin(weeks)] if weeks else base

    # SKU → primary ASIN lookup from master (operator's source of truth)
    sku_to_asin: dict = {}
    try:
        m = load_excel_cached(MASTER_FILE)
        m.columns = m.columns.str.strip()
        sku_col  = next((c for c in ["FBA SKU", "SKU", "sku"] if c in m.columns), None)
        asin_col = next((c for c in ["ASIN", "Asin", "asin"] if c in m.columns), None)
        if sku_col and asin_col:
            for _, mr in m.iterrows():
                k = str(mr[sku_col]).strip()
                v = str(mr[asin_col]).strip()
                if k and k.lower() not in ("nan", "none") and v and v.lower() not in ("nan", "none"):
                    sku_to_asin[k] = v
    except Exception:
        pass

    # Grain shifted from (model) to (sku, model) so multi-SKU/multi-ASIN
    # models break out into individual rows — caller can see per-SKU and
    # per-ASIN performance side by side instead of a model-only rollup.
    data = {}
    for _, r in base_in_weeks.iterrows():
        model = r["model"]
        sku   = str(r.get("sku", "") or "").strip()
        week  = r["week"]
        key   = (sku, model)
        data.setdefault(key, {
            "sku":   sku,
            "asin":  sku_to_asin.get(sku, ""),
            "model": model,
            "brand": str(r.get("brand", "") or ""),
            "category_l0": str(r.get("category_l0", "") or "").replace("nan", ""),
            "category_l1": str(r.get("category_l1", "") or "").replace("nan", ""),
            "category_l2": str(r.get("category_l2", "") or "").replace("nan", ""),
            "weeks": {},
        })
        data[key]["weeks"].setdefault(week, {"units": 0, "sales": 0})
        data[key]["weeks"][week]["units"] += r["units"]
        data[key]["weeks"][week]["sales"] += r["sales"]

    total_sales = {
        w: sum(v["weeks"].get(w, {}).get("sales", 0) for v in data.values()) or 1
        for w in weeks
    }

    rows = []
    for key, v in data.items():
        units_seq = [v["weeks"].get(w, {}).get("units", 0) for w in weeks]
        row = {
            "sku":   v["sku"],
            "model": v["model"],
            "asin":  v["asin"],
            "brand": v.get("brand"),
            "category_l0": v["category_l0"],
            "category_l1": v["category_l1"],
            "category_l2": v["category_l2"],
            "last_4w_units": sum(units_seq),
            "avg_4w": round(sum(units_seq) / max(len(units_seq), 1), 2),
            "trend": _st_trend(units_seq),
            "inventory_units": inventory.get(v["model"], 0),
        }
        for w in weeks:
            s = v["weeks"].get(w, {}).get("sales", 0)
            u = v["weeks"].get(w, {}).get("units", 0)
            row[f"{w}_units"] = u
            row[f"{w}_sales"] = round(s, 2)
            row[f"{w}_sales_pct"] = round((s / total_sales[w]) * 100, 2)
        rows.append(row)

    if rows:
        grand = {
            "sku":   "Grand Total",
            "model": "",
            "asin":  "",
            "brand": "",
            "category_l0": "",
            "category_l1": "",
            "category_l2": "",
            "inventory_units": "",
            "trend": "",
        }
        for w in weeks:
            grand[f"{w}_units"] = sum(r[f"{w}_units"] for r in rows)
            grand[f"{w}_sales"] = round(sum(r[f"{w}_sales"] for r in rows), 2)
            grand[f"{w}_sales_pct"] = 0.0
        grand["last_4w_units"] = sum(r["last_4w_units"] for r in rows)
        grand["avg_4w"] = 0.0
        rows.append(grand)

    base_cols = ["sku", "model", "asin", "brand",
                 "category_l0", "category_l1", "category_l2"]
    week_cols  = [f"{w}_sales"     for w in weeks]
    week_cols += [f"{w}_units"     for w in weeks]
    week_cols += [f"{w}_sales_pct" for w in weeks]
    summary_cols = ["last_4w_units", "avg_4w", "inventory_units", "trend"]
    cols = base_cols + week_cols + summary_cols

    out = pd.DataFrame(rows)
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols]
    return csv_response(out, "sales_trend_sku_full.csv")


# ==================================================
# AMAZON + 1P TREND — FULL EXPORT (UI PARITY)
# Mirrors /amazon-sales-trend (sales_trend_amazon.html):
#   model, brand, category_l0/l1/l2,
#   per-week sales / units / sales_pct / sessions / conversion,
#   last_4w_units, avg_4w_units,
#   last_4w_sessions, avg_4w_sessions,
#   last_4w_conversion, avg_4w_conversion,
#   inventory_units, trend,
#   plus Grand Total row.
# Reuses build_amazon_sales_trend so the CSV stays 1:1 with the page.
# ==================================================
@router.get("/amazon-trend")
def export_amazon_trend(
    brand: str = Query("All"),                       # legacy single-brand back-compat
    brands: list[str] = Query(default=[]),           # multi-brand checkboxes
    sel_weeks: list[str] = Query(default=[]),
):
    from weekly_app.routes.AM_sales_trend import (
        load_sales as _ams_load_sales,
        load_business as _ams_load_business,
        build_amazon_sales_trend,
    )

    sales = _ams_load_sales()
    sales = sales[
        sales["channel"].astype(str).str.strip().str.lower()
        .isin(["amazon", "1p sales"])
    ]

    # Resolve effective brand list: multi takes precedence over legacy single.
    eff_brands_lower = [b.strip().lower() for b in brands if b and b.strip().lower() != "all"]
    if not eff_brands_lower and brand and brand != "All":
        eff_brands_lower = [brand.strip().lower()]
    if eff_brands_lower:
        sales = sales[
            sales["brand"].astype(str).str.strip().str.lower().isin(eff_brands_lower)
        ]

    business = _ams_load_business()

    rows, weeks = build_amazon_sales_trend(sales, business, sel_weeks or None)

    base_cols = ["model", "brand", "category_l0", "category_l1", "category_l2"]
    week_cols  = [f"{w}_sales"      for w in weeks]
    week_cols += [f"{w}_units"      for w in weeks]
    week_cols += [f"{w}_sales_pct"  for w in weeks]
    week_cols += [f"{w}_sessions"   for w in weeks]
    week_cols += [f"{w}_conversion" for w in weeks]
    summary_cols = [
        "last_4w_units", "avg_4w_units",
        "last_4w_sessions", "avg_4w_sessions",
        "last_4w_conversion", "avg_4w_conversion",
        "inventory_units", "trend",
    ]
    cols = base_cols + week_cols + summary_cols

    out = pd.DataFrame(rows)
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols]
    return csv_response(out, "amazon_trend_full.csv")


# ==================================================
# INVENTORY DASHBOARD — FULL EXPORT (UI parity)
# Mirrors what /inventory-dashboard renders: same columns, same
# groupby aggregation, same default-to-latest-week behavior.
# Accepts both `week` (legacy single) and `weeks` / `sel_weeks`
# (forwarded by page_chrome.js across modules).
# ==================================================
@router.get("/inventory-full")
def export_inventory_full(
    week: str = Query(None),
    weeks: list[str] = Query(default=[]),
    sel_weeks: list[str] = Query(default=[]),
    brand: str = Query(None),
    brands: list[str] = Query(default=[]),
):
    try:
        from weekly_app.routes.inventory_dashboard import (
            load_all_inventory, extract_week_num,
        )
        df = load_all_inventory()

        if df is None or df.empty:
            return csv_response(pd.DataFrame(), "inventory_full.csv")

        # Resolve active week(s) — same precedence as the dashboard route
        active_weeks = [w for w in list(weeks) + list(sel_weeks) if str(w).strip()]
        if not active_weeks and week:
            active_weeks = [week]
        if not active_weeks:
            mw = df["week_num"].max()
            df = df[df["week_num"] == mw]
        else:
            df = df[df["week"].isin(active_weeks)]

        # Resolve brand(s)
        active_brands = [b.strip() for b in (brands or []) if b and b.strip()]
        if not active_brands and brand:
            active_brands = [brand.strip()]
        if active_brands and "brand" in df.columns:
            df = df[df["brand"].isin(active_brands)]

        # Mirror the UI's aggregation EXACTLY — same groupby keys, same aggs.
        if not df.empty:
            df[["category_l0", "category_l1", "category_l2"]] = (
                df[["category_l0", "category_l1", "category_l2"]].fillna("")
            )
            df = df.groupby(
                ["week", "brand", "model", "sku",
                 "category_l0", "category_l1", "category_l2",
                 "channel", "type"],
                as_index=False,
            ).agg({
                "inventory_units": "sum",
                "inventory_value": "sum",
                "nlc": "mean",
            })

        cols = [c for c in [
            "week", "brand", "model", "sku",
            "category_l0", "category_l1", "category_l2",
            "channel", "type",
            "inventory_units", "nlc", "inventory_value",
        ] if c in df.columns]
        df = df[cols].fillna("")
    except Exception as _e:
        print(f"⚠ /export/inventory-full fell back to model-level CSV: {_e}")
        df = normalize(pd.read_csv(INV_FILE))
        if week:
            df = df[df["week"] == week]
        if brand:
            df = df[df["brand"] == brand]
    return csv_response(df, "inventory_full.csv")


# ==================================================
# CATEGORY SALES — FULL EXPORT (UI PARITY)
# Mirrors /category-sales (category_sales.html) — Category, Units Sold,
# GMV (₹), % of GMV, plus Grand Total row at the bottom.
# Accepts both `weeks` and `sel_weeks` (the page sends `weeks`).
# ==================================================
@router.get("/category-full")
def export_category_full(
    brand: str = Query(None),                       # legacy single-brand
    brands: list[str] = Query(default=[]),          # multi-brand checkboxes
    sel_weeks: list[str] = Query(default=[]),
    weeks: list[str] = Query(default=[]),
    level: str = Query("l0"),
    value: str = Query(None),
):
    df = pd.read_csv(SALES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    from weekly_app.routes.category_sales import norm as _norm
    eff_brands = [_norm(b) for b in (brands or []) if b and b.strip()]
    if not eff_brands and brand:
        eff_brands = [_norm(brand)]
    if eff_brands:
        df = df[df["brand"].astype(str).apply(_norm).isin(eff_brands)]

    active_weeks = list(weeks) + [w for w in sel_weeks if w not in weeks]
    if active_weeks:
        df = df[df["week"].isin(active_weeks)]

    group_col = {"l0": "category_l0", "l1": "category_l1", "l2": "category_l2"}.get(level, "category_l0")
    if value:
        parent = {"l1": "category_l0", "l2": "category_l1"}.get(level)
        if parent:
            df = df[df[parent].astype(str).str.strip() == value.strip()]

    out = (
        df.groupby(group_col, as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
        .sort_values("gross_sales", ascending=False)
    )
    total = out["gross_sales"].sum() or 1
    out["gmv_pct"] = (out["gross_sales"] / total * 100).round(1)

    if not out.empty:
        grand = pd.DataFrame([{
            group_col: "Grand Total",
            "units_sold": int(out["units_sold"].sum()),
            "gross_sales": float(out["gross_sales"].sum()),
            "gmv_pct": 100.0,
        }])
        out = pd.concat([out, grand], ignore_index=True)

    return csv_response(out, "category_sales_full.csv")
