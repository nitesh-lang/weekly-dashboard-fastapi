
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from pathlib import Path
import urllib.parse

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


# -------------------------------------------------
# CATEGORY SALES VIEW (L0 → L1 → L2)
# -------------------------------------------------
@router.get("/category-sales", response_class=HTMLResponse)
def category_sales(
    request: Request,
    level: str = "l0",            # l0 | l1 | l2
    value: str | None = None,     # parent value
    week: str | None = None,
    brand: str | None = None,     # 🔧 ADDED
):
    if not SALES_FILE.exists():
        return HTMLResponse("Sales file not found", status_code=500)

    df = pd.read_csv(SALES_FILE)

    # ---------------- NORMALIZE ----------------
    for c in ["category_l0", "category_l1", "category_l2", "brand"]:
        if c in df.columns:
            df[c] = df[c].apply(norm)

    # ---------------- WEEK FILTER ----------------
    if week not in (None, "", "None"):
        df["week"] = df["week"].astype(str)
        df = df[df["week"] == str(week)]

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
            "level": level,
            "value": value,
            "week": week,
            "selected_brand": brand,   # 🔧 PASSED TO UI
        },
    )
