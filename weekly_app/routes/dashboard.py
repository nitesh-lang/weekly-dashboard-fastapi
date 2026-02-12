from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import pandas as pd
from pathlib import Path
import urllib.parse
from typing import Dict, Any

from weekly_app.etl.sales_auto_etl import run_sales_auto_etl

# =====================================================
# ROUTER + TEMPLATE INIT
# =====================================================
router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

# =====================================================
# FILE PATHS
# =====================================================
SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
SKU_MASTER = Path("data/master/sku_master.xlsx")

# 🔥 ADDITIVE — AMS WEEKLY FACT (READ ONLY)
AMS_FILE = Path(
    "data/ams_weekly_data/ams_weekly_fact/ams_weekly_fact_with_category.csv"
)

print("✅ DASHBOARD.PY LOADED — SAFE CONTRIBUTION VERSION (LOCKED CORE)")

# =====================================================
# ------------------ HELPERS (LOCKED) -----------------
# =====================================================

def round_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            if c.endswith("_pct"):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(2)
            else:
                df[c] = (
                    pd.to_numeric(df[c], errors="coerce")
                    .fillna(0)
                    .round(0)
                    .astype(int)
                )
    return df


def norm(x: Any) -> str:
    if pd.isna(x):
        return ""
    x = urllib.parse.unquote_plus(str(x))
    x = x.replace("&", "and")
    return " ".join(x.lower().strip().split())


def is_amazon(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) or ("1p" in ch)


def is_amazon_1p(ch: str) -> bool:
    return "1p" in str(ch).lower()


def is_amazon_am(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) and ("1p" not in ch)


# =====================================================
# DASHBOARD ROUTE
# =====================================================
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    week: str = None,
    brand: str = None,
    view: str = "mapped",
):

    # =================================================
    # WEEK NORMALIZATION (LOCKED)
    # =================================================
    if week and week.startswith("Week") and " " not in week:
        week = week.replace("Week", "Week ")

    selected = {
        "week": week,
        "brand": brand,
        "view": view
    }

    # =================================================
    # AUTO SELECT LATEST WEEK (LOCKED)
    # =================================================
    if not week and SALES_FILE.exists():
        try:
            all_weeks = (
                pd.read_csv(SALES_FILE)["week"]
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )

            def _wk(w):
                try:
                    return int(w.replace("Week", "").strip())
                except Exception:
                    return -1

            all_weeks = [w for w in all_weeks if _wk(w) >= 0]
            all_weeks.sort(key=_wk)

            if all_weeks:
                week = all_weeks[-1]
                selected["week"] = week
        except Exception:
            pass

    # =================================================
    # AUTO ETL (LOCKED)
    # =================================================
    try:
        run_sales_auto_etl()
    except Exception:
        pass

    # =================================================
    # LOAD SKU MASTER (LOCKED)
    # =================================================
    master = pd.read_excel(SKU_MASTER)
    master.columns = master.columns.str.strip()
    print(master.columns)
    master = master.rename(
        columns={
            "FBA SKU": "sku",
            "Model No.": "model_no",
            "Model": "model",
        }
    )

    if "model_no" not in master.columns and "model" in master.columns:
        master["model_no"] = master["model"]

    master["sku"] = master["sku"].astype(str)
    master = master[["sku", "model_no", "category_l0"]]

    # =================================================
    # SAFE SALES LOAD (LOCKED)
    # =================================================
    if not SALES_FILE.exists():
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "kpis": {"units": 0, "gmv": 0, "inventory_value": 0, "sell_through": 0},
                "sku_rows": [],
                "channel_summary": [],
                "category_summary": [],
                "ams_pivot": [],
                "weeks": [],
                "brands": [],
                "selected": selected,
            },
        )

    sales = pd.read_csv(SALES_FILE)
    sales.columns = sales.columns.str.strip().str.lower()
    sales["week"] = sales["week"].astype(str).str.strip()
    sales["sku"] = sales["sku"].astype(str)
    sales["channel"] = sales["channel"].astype(str)

    if "category_l0" in sales.columns:
        sales["category_l0_norm"] = sales["category_l0"].apply(norm)

    if week:
        sales = sales[sales["week"] == week]

        # ✅ STEP 2 — BRAND FILTER (CORRECT PLACE)
    if brand and "brand" in sales.columns:
      sales = sales[sales["brand"].str.lower() == brand.lower()]


    if view == "mapped" and "sku_status" in sales.columns:
        sales = sales[sales["sku_status"] == "MAPPED"]

    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0)

    total_gmv = float(sales["gross_sales"].sum())

    # =================================================
    # SKU TOTALS (LOCKED)
    # =================================================
    sku_totals = (
    sales.groupby("sku", as_index=False)
    .agg(
        total_units=("units_sold", "sum"),
        total_sales=("gross_sales", "sum"),
        total_nlc=("sales_nlc", "sum"),
    )
)

    if total_gmv > 0:
        sku_totals["sales_contribution_pct"] = (
            sku_totals["total_sales"] / total_gmv * 100
        ).round(2)
    else:
        sku_totals["sales_contribution_pct"] = 0.0

    # =================================================
    # AMAZON SPLIT (LOCKED)
    # =================================================
    amazon = sales[sales["channel"].apply(is_amazon)].copy()
    if amazon.empty:
        amazon = sales.iloc[0:0].copy()

    amazon["amazon_am_units"] = amazon.apply(
        lambda r: r["units_sold"] if is_amazon_am(r["channel"]) else 0, axis=1
    )
    amazon["amazon_am_sales"] = amazon.apply(
        lambda r: r["gross_sales"] if is_amazon_am(r["channel"]) else 0, axis=1
    )
    amazon["amazon_am_nlc"] = amazon.apply(
        lambda r: r["sales_nlc"] if is_amazon_am(r["channel"]) else 0, axis=1
    )

    amazon["amazon_1p_units"] = amazon.apply(
        lambda r: r["units_sold"] if is_amazon_1p(r["channel"]) else 0, axis=1
    )
    amazon["amazon_1p_sales"] = amazon.apply(
        lambda r: r["gross_sales"] if is_amazon_1p(r["channel"]) else 0, axis=1
    )
    amazon["amazon_1p_nlc"] = amazon.apply(
        lambda r: r["sales_nlc"] if is_amazon_1p(r["channel"]) else 0, axis=1
    )

    amazon_split = (
        amazon.groupby("sku", as_index=False)
        .agg(
            amazon_am_units=("amazon_am_units", "sum"),
            amazon_am_sales=("amazon_am_sales", "sum"),
            amazon_am_nlc=("amazon_am_nlc", "sum"),
            amazon_1p_units=("amazon_1p_units", "sum"),
            amazon_1p_sales=("amazon_1p_sales", "sum"),
            amazon_1p_nlc=("amazon_1p_nlc", "sum"),
        )
    )

    amazon_split["amazon_total_units"] = (
        amazon_split["amazon_am_units"] +
        amazon_split["amazon_1p_units"]
    )
    amazon_split["amazon_total_sales"] = (
        amazon_split["amazon_am_sales"] +
        amazon_split["amazon_1p_sales"]
    )
    amazon_split["amazon_total_nlc"] = (
        amazon_split["amazon_am_nlc"] +
        amazon_split["amazon_1p_nlc"]
    )

    sku = (
    sku_totals
    .merge(amazon_split, on="sku", how="left")
    .merge(master[["sku", "model_no", "category_l0"]], on="sku", how="left")
)

    sku = round_df(sku)

    kpis = {
        "units": int(sales["units_sold"].sum()),
        "gmv": int(total_gmv),
        "inventory_value": 0,
        "sell_through": 0,
    }

    # =================================================
    # CHANNEL SUMMARY (LOCKED)
    # =================================================
    channel_summary_df = (
        sales.groupby("channel", as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gmv=("gross_sales", "sum"),
            sales_nlc=("sales_nlc", "sum"),
        )
        .sort_values("gmv", ascending=False)
    )

    if total_gmv > 0:
        channel_summary_df["sales_contribution_pct"] = (
            channel_summary_df["gmv"] / total_gmv * 100
        ).round(2)
    else:
        channel_summary_df["sales_contribution_pct"] = 0.0

    channel_summary = round_df(channel_summary_df).to_dict("records")

    # =================================================
    # CATEGORY SUMMARY (LOCKED)
    # =================================================
    category_summary_df = (
        sales.groupby(["category_l0", "category_l0_norm"], as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
        .sort_values("gross_sales", ascending=False)
    )

    if total_gmv > 0:
        category_summary_df["sales_contribution_pct"] = (
            category_summary_df["gross_sales"] / total_gmv * 100
        ).round(2)
    else:
        category_summary_df["sales_contribution_pct"] = 0.0

    category_summary = round_df(category_summary_df).to_dict("records")

    # =================================================
    # 🔥 AMS PIVOT (ADD-ONLY)
    # =================================================
    ams_pivot = []

    if AMS_FILE.exists() and week:
        try:
            ams = pd.read_csv(AMS_FILE)
            ams["week"] = ams["week"].astype(str)

            ams = ams[ams["week"] == week.replace("Week", "").strip()]

            ams_pivot_df = (
                ams.groupby("week", as_index=False)
                .agg(
                    ad_spend=("Spend", "sum"),
                    attributed_sales=("attributed_sales", "sum"),
                    sessions=("sessions", "sum"),
                )
            )

            if not ams_pivot_df.empty:
                ams_pivot_df["acos"] = (
                    ams_pivot_df["ad_spend"] /
                    ams_pivot_df["attributed_sales"]
                ).replace([pd.NA, float("inf")], 0).round(3)

                ams_pivot = ams_pivot_df.to_dict("records")
        except Exception:
            ams_pivot = []

    # =================================================
    # FILTER METADATA
    # =================================================
    full_sales = pd.read_csv(SALES_FILE)
    sales.columns = sales.columns.str.strip().str.lower()
    weeks = sorted(full_sales["week"].astype(str).unique())
    brands = (
        sorted(full_sales["brand"].dropna().unique())
        if "brand" in full_sales.columns else []
    )

    # =================================================
    # TEMPLATE RESPONSE (LOCKED KEYS + AMS)
    # =================================================
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "kpis": kpis,
            "sku_rows": sku.to_dict("records"),
            "channel_summary": channel_summary,
            "category_summary": category_summary,
            "ams_pivot": ams_pivot,  # 🔥 NEW
            "weeks": weeks,
            "brands": brands,
            "selected": selected,
        },
    )
