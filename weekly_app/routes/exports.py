from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
import pandas as pd
from pathlib import Path
from io import StringIO

router = APIRouter(prefix="/export", tags=["Exports"])

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
INV_FILE = Path("data/processed/weekly_inventory_snapshot.csv")


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


def csv_response(df: pd.DataFrame, filename: str):
    buffer = StringIO()
    df.to_csv(buffer, index=False)
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
        df = df[df["week_start"] == week]
    if brand:
        df = df[df["brand"] == brand]
    if view == "mapped":
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
        sales = sales[sales["week_start"] == week]
        inv = inv[inv["week_start"] == week]

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
        ["week_start", "brand", "channel", "sku", "sku_status"],
        as_index=False,
    ).agg(
        units_sold=("units_sold", "sum"),
        gross_sales=("gross_sales", "sum"),
    )

    inv_g = inv.groupby(
        ["week_start", "brand", "channel", "sku", "sku_status"],
        as_index=False,
    ).agg(
        inventory_units=("inventory_units", "sum"),
        inventory_value=("inventory_value", "sum"),
        nlc=("nlc", "max"),
    )

    out = (
        sales_g.merge(
            inv_g,
            on=["week_start", "brand", "channel", "sku", "sku_status"],
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
        sales = sales[sales["week_start"] == week]
        inv = inv[inv["week_start"] == week]

    if brand:
        sales = sales[sales["brand"] == brand]
        inv = inv[inv["brand"] == brand]

    out = pd.concat([sales, inv], ignore_index=True)
    return csv_response(out, "unmapped.csv")
