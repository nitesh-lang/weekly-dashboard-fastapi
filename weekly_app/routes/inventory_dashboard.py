# ============================================================
# INVENTORY DASHBOARD MODULE
# ============================================================

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import re

router = APIRouter()
from jinja2 import Environment, FileSystemLoader
_env = Environment(loader=FileSystemLoader("weekly_app/templates"), cache_size=0)

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_INV_DIR = BASE_DIR / "data" / "raw" / "inventory"

# ============================================================
# HELPERS
# ============================================================

def extract_week(val):
    if pd.isna(val):
        return None
    m = re.search(r"\d+", str(val))
    return f"Week {int(m.group())}" if m else None

def extract_week_num(val):
    if pd.isna(val):
        return None
    m = re.search(r"\d+", str(val))
    return int(m.group()) if m else None

def extract_brand(path: Path):
    p = str(path).lower()
    if "nexlev" in p: return "Nexlev"
    if "white" in p or "mulberry" in p: return "White Mulberry"
    if "audio" in p: return "Audio Array"
    if "tonor" in p: return "Tonor"
    return "Unknown"

# ============================================================
# LOADER
# ============================================================

def load_all_inventory():

    frames = []

    for file in RAW_INV_DIR.rglob("*.xlsx"):
        try:
            df = pd.read_excel(file)
        except:
            continue

        df.columns = [c.strip().lower() for c in df.columns]

        # REQUIRE MODEL
        if "model" not in df.columns:
            continue

        # SCHEMA FIX (important)
        if "qty" not in df.columns:
            if "inventory_units" in df.columns:
                df["qty"] = df["inventory_units"]
            else:
                continue

        df["brand"] = extract_brand(file)

        if "week" in df.columns:
            df["week"] = df["week"].apply(extract_week)
        else:
            df["week"] = extract_week(file.parents[1].name)

        df = df.dropna(subset=["week"])

        for col in ["sku","category_l0","category_l1","category_l2","channel","type"]:
            if col not in df.columns:
                df[col] = ""

        df["model"] = df["model"].astype(str).str.strip()
        df["sku"] = df["sku"].astype(str).str.strip()
        df["channel"] = df["channel"].astype(str).str.title()
        df["type"] = df["type"].astype(str).str.title()

        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
        df["nlc"] = pd.to_numeric(df.get("nlc",0), errors="coerce").fillna(0)

        df["inventory_units"] = df["qty"]
        df["inventory_value"] = df["qty"] * df["nlc"]

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    data["week_num"] = data["week"].apply(extract_week_num)

    return data

# ============================================================
# DASHBOARD ROUTE
# ============================================================

@router.get("/inventory-dashboard", response_class=HTMLResponse)
def inventory_dashboard(request: Request, week: str|None=Query(None), brand: str|None=Query(None)):

    df = load_all_inventory()

    if df.empty:
        return HTMLResponse(_env.get_template("inventory_dashboard.html").render(
            request=request, rows=[], latest_week="NA",
            kpis={}, aging=[], channel_summary=[],
            available_weeks=[], available_brands=[]))

    available_weeks = sorted(df["week"].dropna().unique(), key=extract_week_num)
    available_brands = sorted(df["brand"].dropna().unique())

    # WEEK FILTER
    if week:
        df = df[df["week"]==week]
        active_week = week
    else:
        mw = df["week_num"].max()
        df = df[df["week_num"]==mw]
        active_week = df["week"].iloc[0]

    # BRAND FILTER
    if brand:
        df = df[df["brand"]==brand]

    # ========================================================
    # AGGREGATION FIX
    # ========================================================
    # BRAND FILTER
    if brand:
     df = df[df["brand"]==brand]

# >>> ADD THIS BLOCK <<<
    df[["category_l0","category_l1","category_l2"]] = \
    df[["category_l0","category_l1","category_l2"]].fillna("")
    df_agg = df.groupby(
    ["week","brand","model","sku",
     "category_l0","category_l1","category_l2",
     "channel","type"],
    as_index=False
).agg({
    "inventory_units":"sum",
    "inventory_value":"sum",
    "nlc":"mean",
    "channel":"first",
    "type":"first"
})

    rows = df_agg.to_dict(orient="records")



    # KPIs
    total_units = df["inventory_units"].sum()
    total_value = df["inventory_value"].sum()

    in_transit = df[df["type"].str.contains("transit",case=False,na=False)]["inventory_units"].sum()
    unsellable = df[df["type"].str.contains("unsellable",case=False,na=False)]["inventory_units"].sum()

    kpis={
        "total_units":int(total_units),
        "total_value":float(total_value),
        "in_transit_pct":round((in_transit/total_units)*100,2) if total_units else 0,
        "unsellable_pct":round((unsellable/total_units)*100,2) if total_units else 0
    }

    aging=[
        {"bucket":"0–30 days","units":int(total_units-in_transit-unsellable)},
        {"bucket":"31–60 days","units":int(in_transit)},
        {"bucket":"60+ days","units":int(unsellable)}
    ]

    # CHANNEL SUMMARY (use original df)
    cs = df.groupby(["channel","type"],as_index=False).agg({
        "inventory_units":"sum",
        "inventory_value":"sum"
    })

    channel_summary=[
        {"channel":r["channel"],"location":r["type"],
         "units":r["inventory_units"],"value":r["inventory_value"]}
        for _,r in cs.iterrows()
    ]

    return HTMLResponse(_env.get_template("inventory_dashboard.html").render(
        request=request,
        rows=rows,
        latest_week=active_week,
        kpis=kpis,
        aging=aging,
        channel_summary=channel_summary,
        available_weeks=available_weeks,
        available_brands=available_brands,
    ))
