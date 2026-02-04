from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path
import urllib.parse
import re

router = APIRouter()
templates = Jinja2Templates(directory="weekly_app/templates")

SALES_FILE = Path("data/processed/weekly_sales_snapshot.csv")

print("✅ CATEGORY_SALES.PY LOADED")


# -------------------------------------------------
# NORMALIZER (STRICT + SAFE)
# -------------------------------------------------
def norm(x):
    if pd.isna(x):
        return ""
    x = urllib.parse.unquote_plus(str(x))
    x = x.replace("&", "and")
    return " ".join(x.lower().strip().split())


def extract_week(v):
    """
    Converts:
      'Week 4' -> 4
      '4'      -> 4
      None     -> None
    """
    if pd.isna(v):
        return None
    m = re.search(r"(\d+)", str(v))
    return int(m.group(1)) if m else None


# -------------------------------------------------
# CATEGORY SALES VIEW (L0 → L1 → L2)
# -------------------------------------------------
@router.get("/category-sales", response_class=HTMLResponse)
def category_sales(
    request: Request,
    level: str = "l0",            # l0 | l1 | l2
    value: str | None = None,
    week: str | None = None,
    brand: str | None = None,
):
    # ---------------- LOAD FILE ----------------
    if not SALES_FILE.exists():
        return HTMLResponse("Sales file not found", status_code=500)

    df = pd.read_csv(SALES_FILE)

    # ---------------- NORMALIZE TEXT ----------------
    for c in ["category_l0", "category_l1", "category_l2", "brand"]:
        if c in df.columns:
            df[c] = df[c].apply(norm)

    # ---------------- WEEK NORMALIZATION ----------------
    df["week_num"] = df["week"].apply(extract_week)

    # Drop invalid weeks
    df = df[df["week_num"].notna()]

    # ---------------- BUILD AVAILABLE WEEKS (CRITICAL FIX) ----------------
    available_weeks = (
        df[["week", "week_num"]]
        .drop_duplicates()
        .sort_values("week_num")
        ["week"]
        .tolist()
    )

    latest_week_num = df["week_num"].max()

    # ---------------- WEEK FILTER ----------------
    if week not in (None, "", "None"):
        selected_week = extract_week(week)
        if selected_week is not None:
            df = df[df["week_num"] == selected_week]
            week = selected_week
    else:
        df = df[df["week_num"] == latest_week_num]
        week = latest_week_num

    # ---------------- BRAND FILTER ----------------
    if brand not in (None, "", "None"):
        brand = norm(brand)
        df = df[df["brand"] == brand]

    # ---------------- PARENT FILTER ----------------
    if value:
        value = norm(value)
        if level == "l1":
            df = df[df["category_l0"] == value]
        elif level == "l2":
            df = df[df["category_l1"] == value]

    # ---------------- GROUP COLUMN ----------------
    group_col = f"category_{level}"
    if group_col not in df.columns:
        return HTMLResponse(f"Invalid category level: {level}", status_code=400)

    # ---------------- AGGREGATION ----------------
    summary = (
        df.groupby(group_col, as_index=False)
        .agg(
            units_sold=("units_sold", "sum"),
            gross_sales=("gross_sales", "sum"),
        )
        .sort_values("gross_sales", ascending=False)
    )

    # ---------------- RENDER ----------------
    return templates.TemplateResponse(
        "category_sales.html",
        {
            "request": request,
            "rows": summary.to_dict("records"),
            "weeks": available_weeks,           # ✅ THIS FIXES WEEK 5 VISIBILITY
            "level": level,
            "value": value,
            "week": f"Week {week}" if week else None,
            "selected_brand": brand,
        },
    )
