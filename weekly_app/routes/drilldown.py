from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import StreamingResponse
from io import BytesIO
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path
import urllib.parse

# =====================================================
# ROUTER INIT
# =====================================================
router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

# =====================================================
# FILE PATHS
# =====================================================
SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")
SKU_MASTER = Path("data/master/sku_master.xlsx")

print("🔥 DRILLDOWN.PY LOADED — SALES + CATEGORY + AMAZON AM/1P SAFE 🔥")

# =====================================================
# ---------------- HELPERS (UNCHANGED) ----------------
# =====================================================
def norm(x):
    if pd.isna(x):
        return ""
    x = urllib.parse.unquote_plus(str(x))
    x = x.replace("&", "and")
    return " ".join(x.lower().strip().split())


def round_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rounds numeric columns ONLY.
    This function is intentionally aggressive
    to match dashboard behavior.
    """
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = (
                pd.to_numeric(df[c], errors="coerce")
                .fillna(0)
                .round(0)
                .astype(int)
            )
    return df

def csv_response(df: pd.DataFrame, filename: str):
    output = BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        },
    )



def is_amazon(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) or ("1p" in ch)


def is_amazon_1p(ch: str) -> bool:
    return "1p" in str(ch).lower()


def is_amazon_am(ch: str) -> bool:
    ch = str(ch).lower()
    return ("amazon" in ch) and ("1p" not in ch)


# =====================================================
# ---------------- LOADERS (SAFE) ---------------------
# =====================================================
def load_base_sales(week: str | None):
    """
    Loads base sales snapshot.
    Handles missing file / missing columns safely.
    """
    if not SALES_FILE.exists():
        return pd.DataFrame()

    sales = pd.read_csv(SALES_FILE)

    # ---------------- BASIC SANITIZATION ----------------
    sales["week"] = sales["week"].astype(str).str.strip()
    sales["sku"] = sales["sku"].astype(str)
    sales["channel"] = sales["channel"].astype(str)

    # ---------------- CATEGORY NORMALIZATION -------------
    for c in ["category_l0", "category_l1", "category_l2"]:
        if c in sales.columns:
            sales[c] = sales[c].apply(norm)

    # ---------------- WEEK FILTER ------------------------
    if week not in (None, "", "None"):
        sales = sales[sales["week"] == week]

    # ---------------- NUMERIC SANITIZATION ---------------
    for c in ["units_sold", "gross_sales", "sales_nlc"]:
        sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0)

    return sales


def load_master():
    """
    Loads SKU master safely.
    SKU is NOT dropped.
    MODEL is used as primary grouping key later.
    """
    if not SKU_MASTER.exists():
        return pd.DataFrame(columns=["sku", "model_no"])

    m = pd.read_excel(SKU_MASTER)
    m.columns = m.columns.str.strip()

    # Original renames
    m = m.rename(
        columns={
            "FBA SKU": "sku",
            "Model No.": "model_no",
            "Model": "model",  # source of truth
        }
    )

    # ---------------- SAFE MODEL ALIAS (CRITICAL FIX) ----------------
    if "model_no" not in m.columns and "model" in m.columns:
        m["model_no"] = m["model"]
    # ----------------------------------------------------------------

    m["sku"] = m["sku"].astype(str)

    return m[["sku", "model_no", "Brand"]]


# =====================================================
# ---------------- DRILLDOWN ROUTE -------------------
# =====================================================
@router.get("/drilldown", response_class=HTMLResponse)
def drilldown(
    request: Request,
    type: str,
    week: str | None = None,
    brand: str | None = None,   # ← ADD THIS
    channel: str | None = None,
    level: str | None = None,
    value: str | None = None,
    export: str | None = None,   # ✅ ADD THIS LINE
):
    """
    Universal drilldown:
    - Sales → SKU × Channel
    - Category → SKU × Category

    MODEL uniqueness is additive and does NOT
    remove SKU visibility.
    """

    # ---------------- BASIC NORMALIZATION ----------------
    type = type.lower().strip()
    channel = channel.strip() if channel else None

    sales = load_base_sales(week)
    master = load_master()
    # 🔥 SINGLE SOURCE OF TRUTH
    base = sales.merge(master, on="sku", how="left")
    base["Brand"] = base["Brand"].astype(str).str.strip()
    if brand and "Brand" in base.columns:
        base = base[base["Brand"].astype(str).str.strip().str.lower() == brand.strip().lower()
                    ]
    available_brands = sorted(
    master["Brand"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

    
    # =====================================================
    # EMPTY SAFE RENDER
    # =====================================================
    if base.empty:
        return templates.TemplateResponse(
            "drilldown_sales.html",
            {
                "request": request,
                "week": week,
                "channel": channel,
                "brand": brand,   # ✅ ADD THIS
                "available_brands": available_brands,  # ← ADD THIS
                "channel_summary": [],
                "sku_channel_rows": [],
            },
        )

    # =====================================================
    # CHANNEL SUMMARY (UNCHANGED CORE LOGIC)
    # =====================================================
    channel_summary = (
        base.groupby("channel", as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gmv=("gross_sales", "sum"),
            sales_nlc=("sales_nlc", "sum"),
        )
        .sort_values("gmv", ascending=False)
    )

    channel_summary = round_df(channel_summary).to_dict("records")

    # =====================================================
    # SALES DRILLDOWN
    # =====================================================
    if type == "sales":

        # =================================================
        # ALL CHANNELS VIEW (SKU + MODEL SAFE)
        # =================================================
        if channel is None:
            sku_base = base.copy()

            # ---------------- AMAZON AM ----------------
            sku_base["amazon_am_units"] = sku_base.apply(
                lambda r: r["units_sold"] if is_amazon_am(r["channel"]) else 0, axis=1
            )
            sku_base["amazon_am_sales"] = sku_base.apply(
                lambda r: r["gross_sales"] if is_amazon_am(r["channel"]) else 0, axis=1
            )
            sku_base["amazon_am_nlc"] = sku_base.apply(
                lambda r: r["sales_nlc"] if is_amazon_am(r["channel"]) else 0, axis=1
            )

            # ---------------- AMAZON 1P ----------------
            sku_base["amazon_1p_units"] = sku_base.apply(
                lambda r: r["units_sold"] if is_amazon_1p(r["channel"]) else 0, axis=1
            )
            sku_base["amazon_1p_sales"] = sku_base.apply(
                lambda r: r["gross_sales"] if is_amazon_1p(r["channel"]) else 0, axis=1
            )
            sku_base["amazon_1p_nlc"] = sku_base.apply(
                lambda r: r["sales_nlc"] if is_amazon_1p(r["channel"]) else 0, axis=1
            )

            # ---------------- OTHER CHANNELS ----------------
            other_channels = sorted(
    c for c in base["channel"].unique()
    if not is_amazon(c)
)

            for ch in other_channels:
                k = ch.lower().replace(" ", "_")
                sku_base[f"{k}_units"] = sku_base.apply(
                    lambda r: r["units_sold"] if r["channel"] == ch else 0, axis=1
                )
                sku_base[f"{k}_sales"] = sku_base.apply(
                    lambda r: r["gross_sales"] if r["channel"] == ch else 0, axis=1
                )
                sku_base[f"{k}_nlc"] = sku_base.apply(
                    lambda r: r["sales_nlc"] if r["channel"] == ch else 0, axis=1
                )

            # ---------------- AGGREGATION ----------------
            agg = {
                "sku": lambda x: ", ".join(sorted(set(x))),
                "amazon_am_units": "sum",
                "amazon_am_sales": "sum",
                "amazon_am_nlc": "sum",
                "amazon_1p_units": "sum",
                "amazon_1p_sales": "sum",
                "amazon_1p_nlc": "sum",
            }

            for ch in other_channels:
                k = ch.lower().replace(" ", "_")
                agg[f"{k}_units"] = "sum"
                agg[f"{k}_sales"] = "sum"
                agg[f"{k}_nlc"] = "sum"

            sku = (
    sku_base
    .groupby(["sku", "model_no", "category_l0"], as_index=False)
    .agg(agg)
)
              
            total_gmv = base["gross_sales"].sum()
            channel_summary_df = pd.DataFrame(channel_summary)
            if total_gmv > 0:
                channel_summary_df["sales_contribution_pct"] = (channel_summary_df["gmv"] / total_gmv * 100).round(2)
            else:
                channel_summary_df["sales_contribution_pct"] = 0.0
                channel_summary = channel_summary_df.to_dict("records")
                
                    

            # ---------------- AMAZON TOTAL ----------------
            sku["amazon_total_units"] = sku["amazon_am_units"] + sku["amazon_1p_units"]
            sku["amazon_total_sales"] = sku["amazon_am_sales"] + sku["amazon_1p_sales"]
            sku["amazon_total_nlc"] = sku["amazon_am_nlc"] + sku["amazon_1p_nlc"]

            # ---------------- GRAND TOTAL ----------------
            non_amazon_units = [
                c for c in sku.columns if c.endswith("_units") and not c.startswith("amazon_")
            ]
            non_amazon_sales = [
                c for c in sku.columns if c.endswith("_sales") and not c.startswith("amazon_")
            ]
            non_amazon_nlc = [
                c for c in sku.columns if c.endswith("_nlc") and not c.startswith("amazon_")
            ]

            sku["total_units"] = sku["amazon_total_units"] + sku[non_amazon_units].sum(axis=1)
            sku["total_sales"] = sku["amazon_total_sales"] + sku[non_amazon_sales].sum(axis=1)
            sku["total_nlc"] = sku["amazon_total_nlc"] + sku[non_amazon_nlc].sum(axis=1)

            sku = round_df(sku)

            if export == "csv":
             return csv_response(sku, f"sales_drilldown_{week}.csv")

            return templates.TemplateResponse(
                "drilldown_sales.html",
                {
                    "request": request,
                    "week": week,
                    "channel": "ALL",
                     "brand": brand,   # ✅ ADD
                     "available_brands": available_brands,  # ← ADD THIS
                    "channel_summary": channel_summary,
                    "sku_channel_rows": sku.to_dict("records"),
                },
            )

        # =================================================
        # AMAZON ONLY (WITH CONTRIBUTION %)
        # =================================================
        if is_amazon(channel):
            if "1p" in channel.lower():
                amazon = base[base["channel"].str.contains("1p", case=False, na=False)]
            else:
             amazon = base[base["channel"].str.contains("amazon", case=False, na=False)]   
            total_channel_sales = float(amazon["gross_sales"].sum())

            sku = (
    amazon
    .groupby(["sku", "model_no", "category_l0"], as_index=False)
    .agg(
        units_sold=("units_sold", "sum"),
        gmv=("gross_sales", "sum"),
        sales_nlc=("sales_nlc", "sum"),
    )
)

            if total_channel_sales > 0:
                sku["channel_contribution_pct"] = (
                    sku["gmv"] / total_channel_sales * 100
                ).round(2)
            else:
                sku["channel_contribution_pct"] = 0.0

            sku = round_df(sku)
            if export == "csv":
             return csv_response(sku, f"sales_drilldown_{week}.csv")

            return templates.TemplateResponse(
                "drilldown_sales.html",
                {
                    "request": request,
                    "week": week,
                    "channel": "Amazon",
                    "brand": brand,   # ✅ ADD
                    "available_brands": available_brands,  # ← ADD THIS
                    "channel_summary": channel_summary,
                    "sku_channel_rows": sku.to_dict("records"),
                },
            )

        # =================================================
        # SINGLE NON-AMAZON CHANNEL
        # =================================================
        other = base[base["channel"] == channel]
        total_channel_sales = float(other["gross_sales"].sum())

        sku = (
    other
    .groupby(["sku", "model_no", "category_l0"], as_index=False)
    .agg(
        units_sold=("units_sold", "sum"),
        gmv=("gross_sales", "sum"),
        sales_nlc=("sales_nlc", "sum"),
    )
)

        if total_channel_sales > 0:
            sku["channel_contribution_pct"] = (
                sku["gmv"] / total_channel_sales * 100
            ).round(2)
        else:
            sku["channel_contribution_pct"] = 0.0

        sku = round_df(sku)

        if export == "csv":
         return csv_response(sku, f"sales_drilldown_{week}.csv")

        return templates.TemplateResponse(
            "drilldown_sales.html",
            {
                "request": request,
                "week": week,
                "channel": channel,
                 "brand": brand,   # ✅ ADD
                 "available_brands": available_brands,  # ← ADD THIS
                "channel_summary": channel_summary,
                "sku_channel_rows": sku.to_dict("records"),
            },
        )

    # =====================================================
    # CATEGORY DRILLDOWN (MODEL SAFE)
    # =====================================================
    if type == "category":
        col = f"category_{level}"

        filtered = base[base[col] == norm(value)]

        sku = (
            filtered.merge(master, on="sku", how="left")
            .groupby(["model_no", col], as_index=False)
            .agg(
                skus=("sku", lambda x: ", ".join(sorted(set(x)))),
                units_sold=("units_sold", "sum"),
                gmv=("gross_sales", "sum"),
                sales_nlc=("sales_nlc", "sum"),
            )
        )

        sku = round_df(sku)

        return templates.TemplateResponse(
            "drilldown_sales.html",
            {
                "request": request,
                "week": week,
                "channel": f"Category: {value}",
                "brand": brand,   # ✅ ADD
                "available_brands": available_brands,  # ← ADD THIS
                "channel_summary": channel_summary,
                "sku_channel_rows": sku.to_dict("records"),
            },
        )

    # =====================================================
    # FALLBACK
    # =====================================================
    return HTMLResponse("Invalid drilldown type", status_code=400)
