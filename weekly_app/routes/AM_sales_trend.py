# ============================================================
# AMAZON + 1P SALES TREND (WITH SESSIONS + CONVERSION)
# ============================================================

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import re

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "processed"
AMS_DATA_DIR = BASE_DIR / "data" / "ams_weekly_data" / "processed_ads"

# ============================================================
# NORMALIZATION
# ============================================================

def norm(x):
    return str(x).strip()

def norm_model(x):
    return str(x).strip().upper()

def extract_week(v):
    m = re.search(r"\d+", str(v))
    return int(m.group()) if m else None

# ============================================================
# FILE FINDER
# ============================================================

def find_file(base, stems):
    for f in base.iterdir():
        if not f.is_file():
            continue
        name = f.name.lower().replace(" ", "_")
        for s in stems:
            if s in name:
                return f
    raise FileNotFoundError(stems)

# ============================================================
# LOAD SALES SNAPSHOT
# ============================================================

def load_sales():

    f = find_file(DATA_DIR, ["weekly_sales_snapshot", "weekly_sales"])
    df = pd.read_csv(f)
    df.columns = [c.strip().lower() for c in df.columns]

    df = df.rename(columns={
        "units_sold": "units",
        "gross_sales": "sales"
    })

    for c in ["week", "model", "brand", "units", "sales", "channel"]:
        if c not in df.columns:
            df[c] = 0

    df["brand"] = df["brand"].astype(str).str.lower().str.strip()
    df["model"] = df["model"].apply(norm_model)
    df["channel"] = df["channel"].astype(str).str.lower().str.strip()

    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0)
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)

    df["week_num"] = df["week"].apply(extract_week)

    return df


# ============================================================
# LOAD BUSINESSJOINED (SESSIONS + CONVERSION)
# ============================================================

def load_business():

    f = find_file(AMS_DATA_DIR, ["business_ads_joined"])
    df = pd.read_csv(f)
    df.columns = [c.strip().lower() for c in df.columns]

    for c in ["model", "week", "sessions", "conversion_pct"]:
        if c not in df.columns:
            df[c] = 0

    df["model"] = df["model"].apply(norm_model)
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)
    df["conversion_pct"] = pd.to_numeric(
        df["conversion_pct"], errors="coerce"
    ).fillna(0)

    df["week_num"] = df["week"].apply(extract_week)

    return df


# ============================================================
# INVENTORY SNAPSHOT
# ============================================================

def load_inventory(latest_week):

    try:
        f = find_file(DATA_DIR, ["inventory_model_snapshot"])
    except FileNotFoundError:
        return {}

    df = pd.read_csv(f)
    df.columns = [c.strip().lower() for c in df.columns]

    df["model"] = df["model"].apply(norm_model)
    df["inventory_units"] = pd.to_numeric(
        df["inventory_units"], errors="coerce"
    ).fillna(0)

    df["week_num"] = df["week"].apply(extract_week)

    df = df[df["week_num"] == latest_week]

    return df.set_index("model")["inventory_units"].to_dict()


# ============================================================
# TREND LOGIC
# ============================================================

def trend(seq):

    if len(seq) < 3:
        return "FLAT"

    a, b, c = seq[-3:]

    if a < b < c:
        return "UP"
    if a > b > c:
        return "DOWN"

    return "FLAT"


# ============================================================
# CORE BUILDER
# ============================================================

def build_amazon_sales_trend(sales_df, business_df):

    # -------- LAST 4 WEEKS ----------
    weeks_df = (
        sales_df[["week", "week_num"]]
        .dropna()
        .drop_duplicates()
        .sort_values("week_num")
        .tail(4)
    )

    weeks = weeks_df["week"].tolist()
    latest_week = weeks_df["week_num"].iloc[-1]

    inventory_map = load_inventory(latest_week)

    # -------- MERGE SALES + BUSINESS ----------
    merged = sales_df.merge(
    business_df[["model", "week_num", "sessions", "conversion_pct"]],
    on=["model", "week_num"],
    how="left"
)

    merged["sessions"] = merged["sessions"].fillna(0)
    merged["conversion_pct"] = merged["conversion_pct"].fillna(0)

    # -------- AGGREGATE MODEL LEVEL ----------
    data = {}

    for _, r in merged.iterrows():

        model = r["model"]
        week = r["week"]

        data.setdefault(model, {
            "brand": r.get("brand"),
            "model": model,
            "category_l0": r.get("category_l0"),
            "category_l1": r.get("category_l1"),
            "category_l2": r.get("category_l2"),
            "weeks": {}
        })

        data[model]["weeks"].setdefault(
            week,
            {
                "units": 0,
                "sales": 0,
                "sessions": 0,
                "conversion": 0
            }
        )

        data[model]["weeks"][week]["units"] += r["units"]
        data[model]["weeks"][week]["sales"] += r["sales"]
        data[model]["weeks"][week]["sessions"] += r["sessions"]
        data[model]["weeks"][week]["conversion"] += r["conversion_pct"]

    # -------- TOTAL SALES FOR % ----------
    total_sales = {
        w: sum(
            v["weeks"].get(w, {}).get("sales", 0)
            for v in data.values()
        ) or 1
        for w in weeks
    }

    rows = []

    # =====================================================
    # BUILD FINAL ROWS
    # =====================================================

    for model, v in data.items():

        units_seq = [
            v["weeks"].get(w, {}).get("units", 0)
            for w in weeks
        ]

        sessions_seq = [
            v["weeks"].get(w, {}).get("sessions", 0)
            for w in weeks
        ]

        conversion_seq = [
            v["weeks"].get(w, {}).get("conversion", 0)
            for w in weeks
        ]

        row = {
            "model": model,
            "brand": v.get("brand"),
            "category_l0": v.get("category_l0"),
            "category_l1": v.get("category_l1"),
            "category_l2": v.get("category_l2"),

            "last_4w_units": sum(units_seq),
            "avg_4w_units": round(sum(units_seq) / max(len(units_seq), 1), 2),

            "last_4w_sessions": sum(sessions_seq),
            "avg_4w_sessions": round(
                sum(sessions_seq) / max(len(sessions_seq), 1), 2
            ),

            "last_4w_conversion": round(sum(conversion_seq), 2),
            "avg_4w_conversion": round(
                sum(conversion_seq) / max(len(conversion_seq), 1), 2
            ),

            "trend": trend(units_seq),
            "inventory_units": inventory_map.get(model, 0)
        }

        # -------- DYNAMIC WEEK FIELDS ----------
        for w in weeks:

            week_data = v["weeks"].get(w, {})

            row[f"{w}_units"] = week_data.get("units", 0)
            row[f"{w}_sales"] = round(week_data.get("sales", 0), 2)
            row[f"{w}_sales_pct"] = round(
                (week_data.get("sales", 0) / total_sales[w]) * 100,
                2
            )

            row[f"{w}_sessions"] = week_data.get("sessions", 0)
            row[f"{w}_conversion"] = round(
                week_data.get("conversion", 0),
                2
            )

        rows.append(row)

    return rows, weeks


# ============================================================
# ROUTE – AMAZON + 1P ONLY
# ============================================================

@router.get("/amazon-sales-trend", response_class=HTMLResponse)
def amazon_sales_trend(request: Request, brand: str = "All"):

    sales = load_sales()

    # -------- FILTER AMAZON + 1P ----------
    sales = sales[sales["channel"].isin(["amazon", "1p sales"])]

    if brand and brand != "All":
        sales = sales[sales["brand"] == brand.lower().strip()]

    business = load_business()

    rows, weeks = build_amazon_sales_trend(sales, business)

    brands = sorted(sales["brand"].dropna().unique())

    return templates.TemplateResponse(
        "sales_trend_amazon.html",
        {
            "request": request,
            "rows": rows,
            "weeks": weeks,
            "brands": brands,
            "selected_brand": brand,
            "page_title": "Amazon + 1P Sales Trend"
        }
    )