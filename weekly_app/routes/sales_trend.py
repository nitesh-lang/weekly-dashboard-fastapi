
# ============================================================
# SALES TREND – LAST 4 WEEKS (WITH BRAND FILTER)
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

# ============================================================
# NORMALIZATION
# ============================================================

def norm(x):
    return str(x).strip()

def norm_model(x):
    return str(x).strip().upper()

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

    for c in ["week", "model", "brand", "units", "sales"]:
        if c not in df.columns:
            df[c] = 0

    df["brand"] = df["brand"].astype(str).str.strip().str.lower()
    df["model"] = df["model"].apply(norm_model)
    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0).astype(int)
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)

    df["week_num"] = df["week"].astype(str).apply(
        lambda x: int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else None
    )

    return df

# ============================================================
# INVENTORY SNAPSHOT (LATEST WEEK)
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

    df["week_num"] = df["week"].astype(str).apply(
        lambda x: int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else None
    )

    df = df[df["week_num"] == latest_week]
    return df.set_index("model")["inventory_units"].to_dict()

# ============================================================
# TREND LOGIC
# ============================================================

def trend(units):
    if len(units) < 3:
        return "FLAT"
    a, b, c = units[-3:]
    if a < b < c:
        return "UP"
    if a > b > c:
        return "DOWN"
    return "FLAT"

# ============================================================
# ROUTE
# ============================================================

@router.get("/sales-trend", response_class=HTMLResponse)
def sales_trend(request: Request, brand: str = "All"):

    sales = load_sales()

    base = sales
    if brand and brand != "All":
        base = sales[sales["brand"] == brand.strip().lower()]

    weeks_df = (
        base[["week", "week_num"]]
        .dropna()
        .drop_duplicates()
        .sort_values("week_num")
        .tail(4)
    )

    weeks = weeks_df["week"].tolist()
    latest_week = weeks_df["week_num"].iloc[-1]

    inventory = load_inventory(latest_week)

    data = {}

    # FIX: iterate over base (brand-filtered), NOT full sales
    for _, r in base.iterrows():
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

        data[model]["weeks"].setdefault(week, {"units": 0, "sales": 0})
        data[model]["weeks"][week]["units"] += r["units"]
        data[model]["weeks"][week]["sales"] += r["sales"]

    total_sales = {
        w: sum(v["weeks"].get(w, {}).get("sales", 0) for v in data.values()) or 1
        for w in weeks
    }

    rows = []

    for model, v in data.items():
        units_seq = [v["weeks"].get(w, {}).get("units", 0) for w in weeks]

        row = {
            "model": model,
            "brand": v.get("brand"),
            "category_l0": v["category_l0"],
            "category_l1": v["category_l1"],
            "category_l2": v["category_l2"],
            "last_4w_units": sum(units_seq),
            "avg_4w": round(sum(units_seq) / max(len(units_seq), 1), 2),
            "trend": trend(units_seq),
            "inventory_units": inventory.get(model, 0)
        }

        for w in weeks:
            s = v["weeks"].get(w, {}).get("sales", 0)
            u = v["weeks"].get(w, {}).get("units", 0)
            row[f"{w}_units"] = u
            row[f"{w}_sales"] = round(s, 2)
            row[f"{w}_sales_pct"] = round((s / total_sales[w]) * 100, 2)

        rows.append(row)
    # ================= GRAND TOTAL =================
    grand = {
        "model": "Grand Total",
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

        
    rows.append(grand)   # ✅ HERE (after loop ends)

    brands = sorted(load_sales()["brand"].dropna().unique())

    return templates.TemplateResponse(
        "sales_trend_sku.html",
        {
            "request": request,
            "rows": rows,
            "weeks": weeks,
            "brands": brands,
            "selected_brand": brand
        }
    )
