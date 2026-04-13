from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
import pandas as pd
from pathlib import Path
from io import BytesIO

router = APIRouter(prefix="/export", tags=["Exports"])

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
INV_FILE = Path("data/processed/inventory_model_snapshot.csv")


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

    for c in ["gross_sales", "gmv", "inventory_value", "nlc"]:
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
# CHANNEL SUMMARY (MATCH DASHBOARD)
# ==================================================
@router.get("/channel-summary")
def export_channel_summary(
    week: str = Query(None),
    brand: str = Query(None),
):
    sales = normalize(pd.read_csv(SALES_FILE))
    inv = normalize(pd.read_csv(INV_FILE))

    sales = apply_filters(sales, week, brand, "mapped")
    inv = apply_filters(inv, week, brand, "mapped")

    s = sales.groupby("channel", as_index=False).agg(
        units_sold=("units_sold", "sum"),
        gmv=("gross_sales", "sum"),
    )

    i = inv.groupby("channel", as_index=False).agg(
        inventory_units=("inventory_units", "sum"),
        inventory_value=("inventory_value", "sum"),
    )

    out = s.merge(i, on="channel", how="outer").fillna(0)
    out["sell_through_pct"] = (
        out["units_sold"] / out["inventory_units"].replace(0, pd.NA)
    ).fillna(0).round(1)

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
    inv = normalize(pd.read_csv(INV_FILE))

    sales = apply_filters(sales, week, brand, "mapped")
    inv = apply_filters(inv, week, brand, "mapped")

    s = sales.groupby("category_l0", as_index=False).agg(
        units_sold=("units_sold", "sum"),
        gmv=("gross_sales", "sum"),
    )

    i = inv.groupby("category_l0", as_index=False).agg(
        inventory_units=("inventory_units", "sum"),
        inventory_value=("inventory_value", "sum"),
    )

    out = s.merge(i, on="category_l0", how="outer").fillna(0)
    out["sell_through_pct"] = (
        out["units_sold"] / out["inventory_units"].replace(0, pd.NA)
    ).fillna(0).round(1)

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
    inv = normalize(pd.read_csv(INV_FILE))

    sales = apply_filters(sales, week, brand, view)
    inv = apply_filters(inv, week, brand, view)

    s = sales.groupby(["sku", "channel"], as_index=False).agg(
        units_sold=("units_sold", "sum")
    )

    i = inv.groupby(["sku", "channel"], as_index=False).agg(
        inventory_units=("inventory_units", "sum")
    )

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
    inv = normalize(pd.read_csv(INV_FILE))

    sales = apply_filters(sales, week, brand, view)
    inv = apply_filters(inv, week, brand, view)

    sold_pairs = set(zip(sales["sku"], sales["channel"]))

    dead = inv[
        inv.apply(
            lambda r: (r["sku"], r["channel"]) not in sold_pairs
            and r["inventory_units"] > 0,
            axis=1,
        )
    ]

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
    inv = normalize(pd.read_csv(INV_FILE))

    if week:
        sales = sales[sales["week"] == week]
        inv = inv[inv["week"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        inv = inv[inv["brand"] == brand]

    if channel:
        sales = sales[sales["channel"] == channel]
        inv = inv[inv["channel"] == channel]

    if view == "mapped":
        sales = sales[sales["sku_status"] == "MAPPED"]
        inv = inv[inv["sku_status"] == "MAPPED"]

    sales_g = sales.groupby(
        ["week", "brand", "channel", "sku", "sku_status"],
        as_index=False,
    ).agg(
        units_sold=("units_sold", "sum"),
        gross_sales=("gross_sales", "sum"),
    )

    inv_g = inv.groupby(
        ["week", "brand", "channel", "sku", "sku_status"],
        as_index=False,
    ).agg(
        inventory_units=("inventory_units", "sum"),
        inventory_value=("inventory_value", "sum"),
        nlc=("nlc", "max"),
    )

    out = (
        sales_g.merge(
            inv_g,
            on=["week", "brand", "channel", "sku", "sku_status"],
            how="outer",
        )
        .fillna(0)
    )

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
    inv = normalize(pd.read_csv(INV_FILE))

    sales = sales[sales["sku_status"] == "UNMAPPED"]
    inv = inv[inv["sku_status"] == "UNMAPPED"]

    if week:
        sales = sales[sales["week"] == week]
        inv = inv[inv["week"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        inv = inv[inv["brand"] == brand]

    out = pd.concat([sales, inv], ignore_index=True)
    return csv_response(out, "unmapped.csv")

# ==================================================
# DASHBOARD SKU EXPORT (NEW)
# ==================================================
@router.get("/dashboard-sku")
def export_dashboard_sku(
    week: str = Query(None),
    brand: str = Query(None),
):
    df = normalize(pd.read_csv(SALES_FILE))
    df = apply_filters(df, week, brand, "mapped")
    return csv_response(df, "dashboard_sku.csv")


# ==================================================
# SALES TREND SKU — FULL EXPORT
# ==================================================
@router.get("/sales-trend-sku")
def export_sales_trend_sku(
    brand: str = Query("All"),
    sel_weeks: list[str] = Query(default=[]),
):
    import re as _re
    from io import StringIO as _SIO

    df = pd.read_csv(SALES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    df.rename(columns={"units_sold": "units", "gross_sales": "sales"}, inplace=True)
    df["brand_norm"] = df["brand"].astype(str).str.strip().str.lower()

    if brand and brand != "All":
        df = df[df["brand_norm"] == brand.strip().lower()]

    def wnum(w):
        m = _re.search(r"\d+", str(w))
        return int(m.group()) if m else 0

    all_weeks = sorted(df["week"].dropna().unique(), key=wnum)
    weeks = [w for w in all_weeks if w in sel_weeks] if sel_weeks else all_weeks[-4:]

    data = {}
    for _, r in df.iterrows():
        model = str(r.get("model", "") or "")
        week = r["week"]
        data.setdefault(model, {"model": model, "brand": str(r.get("brand", "")),
            "category_l0": str(r.get("category_l0", "") or "").replace("nan", ""),
            "category_l1": str(r.get("category_l1", "") or "").replace("nan", ""),
            "category_l2": str(r.get("category_l2", "") or "").replace("nan", ""), "weeks": {}})
        data[model]["weeks"].setdefault(week, {"units": 0, "sales": 0})
        data[model]["weeks"][week]["units"] += float(r.get("units", 0) or 0)
        data[model]["weeks"][week]["sales"] += float(r.get("sales", 0) or 0)

    rows = []
    for model, v in data.items():
        row = {"model": model, "brand": v["brand"],
               "category_l0": v["category_l0"], "category_l1": v["category_l1"],
               "category_l2": v["category_l2"]}
        for w in weeks:
            row[f"{w}_units"] = v["weeks"].get(w, {}).get("units", 0)
            row[f"{w}_sales"] = round(v["weeks"].get(w, {}).get("sales", 0), 2)
        rows.append(row)

    out = pd.DataFrame(rows)
    return csv_response(out, "sales_trend_sku_full.csv")


# ==================================================
# AMAZON + 1P TREND — FULL EXPORT
# ==================================================
@router.get("/amazon-trend")
def export_amazon_trend(
    brand: str = Query("All"),
    sel_weeks: list[str] = Query(default=[]),
):
    df = pd.read_csv(SALES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    if brand and brand != "All":
        df = df[df["brand"].astype(str).str.strip().str.lower() == brand.strip().lower()]
    amazon_channels = ["Amazon", "1p Sales", "1p sales", "amazon"]
    df = df[df["channel"].astype(str).str.strip().isin(["Amazon", "1p Sales"])]
    if sel_weeks:
        df = df[df["week"].isin(sel_weeks)]
    return csv_response(df, "amazon_trend_full.csv")


# ==================================================
# INVENTORY DASHBOARD — FULL EXPORT (via raw files)
# ==================================================
@router.get("/inventory-full")
def export_inventory_full(
    week: str = Query(None),
    brand: str = Query(None),
):
    import sys as _sys
    _sys.path.insert(0, ".")
    try:
        from weekly_app.routes.inventory_dashboard import load_all_inventory
        df = load_all_inventory()
        if week:
            df = df[df["week"] == week]
        if brand:
            df = df[df["brand"] == brand]
        cols = [c for c in ["week","brand","model","sku","category_l0","category_l1",
                             "category_l2","channel","type","inventory_units","nlc","inventory_value"]
                if c in df.columns]
        df = df[cols].fillna("")
    except Exception:
        df = normalize(pd.read_csv(INV_FILE))
        if week:
            df = df[df["week"] == week]
        if brand:
            df = df[df["brand"] == brand]
    return csv_response(df, "inventory_full.csv")


# ==================================================
# CATEGORY SALES — FULL EXPORT
# ==================================================
@router.get("/category-full")
def export_category_full(
    brand: str = Query(None),
    sel_weeks: list[str] = Query(default=[]),
    level: str = Query("l0"),
    value: str = Query(None),
):
    df = pd.read_csv(SALES_FILE)
    df.columns = [c.strip().lower() for c in df.columns]
    if brand:
        from weekly_app.routes.category_sales import norm as _norm
        df = df[df["brand"].astype(str).apply(_norm) == _norm(brand)]
    if sel_weeks:
        df = df[df["week"].isin(sel_weeks)]
    group_col = {"l0": "category_l0", "l1": "category_l1", "l2": "category_l2"}.get(level, "category_l0")
    if value:
        parent = {"l1": "category_l0", "l2": "category_l1"}.get(level)
        if parent:
            df = df[df[parent].astype(str).str.strip() == value.strip()]
    out = df.groupby(group_col, as_index=False).agg(
        units_sold=("units_sold", "sum"),
        gross_sales=("gross_sales", "sum"),
    ).sort_values("gross_sales", ascending=False)
    total = out["gross_sales"].sum() or 1
    out["gmv_pct"] = (out["gross_sales"] / total * 100).round(1)
    return csv_response(out, "category_sales_full.csv")
