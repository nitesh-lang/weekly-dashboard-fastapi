import pandas as pd
from pathlib import Path

# ==================================================
# STEP 4: BUSINESS + ADS + SKU MASTER (FINAL)
#
# BASE TABLE      : ads_weekly_aggregated.csv (SP + SD ONLY)
# BUSINESS GMV    : ams_weekly_fact.csv
# BRAND/CATEGORY  : sku_master.xlsx
#
# GUARANTEES:
# - SB COMPLETELY REMOVED
# - ADS IS BASE (NO DUPLICATION)
# - BRAND ALWAYS PRESENT
# - SAFE METRICS (NO CRASH)
# ==================================================

print("🚀 STEP 4 – BUSINESS + ADS + CATEGORY (FINAL)")

# --------------------------------------------------
# PATH CONFIG
# --------------------------------------------------
BASE_PATH = Path(__file__).resolve().parents[2] / "data"
AMS_DIR = BASE_PATH / "ams_weekly_data"

ADS_FILE = AMS_DIR / "processed_ads" / "ads_weekly_aggregated.csv"
BIZ_FILE = AMS_DIR / "ams_weekly_fact" / "ams_weekly_fact.csv"
SKU_FILE = BASE_PATH / "master" / "sku_master.xlsx"

OUT_DIR = AMS_DIR / "processed_ads"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "business_ads_joined.csv"

# --------------------------------------------------
# LOAD FILES
# --------------------------------------------------
ads = pd.read_csv(ADS_FILE)
biz = pd.read_csv(BIZ_FILE)
sku = pd.read_excel(SKU_FILE)

ads.columns = ads.columns.str.strip()
biz.columns = biz.columns.str.strip()
sku.columns = sku.columns.str.strip()

# --------------------------------------------------
# REMOVE SB COMPLETELY
# --------------------------------------------------
if "ad_type" in ads.columns:
    ads = ads[ads["ad_type"].isin(["SP", "SD", "SP_SD"])].copy()

# --------------------------------------------------
# NORMALIZE ADS
# --------------------------------------------------
ads.rename(columns={
    "spend": "Spend",
    "cost": "Spend",
    "Impression": "Impressions",
    "impression": "Impressions",
    "attributed_": "attributed_sales",
    "ams_order": "ams_orders",
}, inplace=True)

ads["asin"] = ads["asin"].astype(str).str.strip()
ads["week"] = pd.to_numeric(ads["week"], errors="coerce")

for c in ["Spend", "Clicks", "Impressions", "attributed_sales", "ams_orders"]:
    if c not in ads.columns:
        ads[c] = 0
    ads[c] = pd.to_numeric(ads[c], errors="coerce").fillna(0)

# 🔒 HARD DEDUPE — ONE ROW PER ASIN+WEEK
ads = ads.groupby(
    ["asin", "week"], as_index=False
).agg({
    "Spend": "sum",
    "Clicks": "sum",
    "Impressions": "sum",
    "attributed_sales": "sum",
    "ams_orders": "sum",
})

# --------------------------------------------------
# NORMALIZE BUSINESS FACT
# --------------------------------------------------
asin_col = next(
    (c for c in ["asin", "ASIN", "(Parent) ASIN", "parent_asin"] if c in biz.columns),
    None
)
if not asin_col:
    raise RuntimeError("❌ ASIN column missing in AMS fact")

biz["asin"] = biz[asin_col].astype(str).str.strip()
biz["week"] = pd.to_numeric(biz["week"], errors="coerce")

biz["gmv"] = pd.to_numeric(biz.get("ordered_product_sales", 0), errors="coerce").fillna(0)
biz["sessions"] = pd.to_numeric(biz.get("sessions", 0), errors="coerce").fillna(0)
biz["units"] = pd.to_numeric(biz.get("units_ordered", 0), errors="coerce").fillna(0)
biz["buy_box_pct"] = pd.to_numeric(biz.get("buy_box_pct", 0), errors="coerce").fillna(0)

biz = biz[[
    "asin", "week", "gmv", "sessions", "units", "buy_box_pct"
]].drop_duplicates(["asin", "week"])

# --------------------------------------------------
# JOIN 1: ADS ← BUSINESS (ADS BASE)
# --------------------------------------------------
final = ads.merge(
    biz,
    on=["asin", "week"],
    how="left"
)

for c in ["gmv", "sessions", "units", "buy_box_pct"]:
    final[c] = pd.to_numeric(final[c], errors="coerce").fillna(0)

# --------------------------------------------------
# LOAD BUSINESS REPORTS (PARENT → ONE CHILD)
# --------------------------------------------------
latest_week = int(final["week"].max())
maps = []

for brand_dir in AMS_DIR.iterdir():
    if not brand_dir.is_dir():
        continue
    if brand_dir.name in ["processed_ads", "ams_weekly_fact"]:
        continue

    rpt = brand_dir / f"business_report_week{latest_week}.xlsx"
    if not rpt.exists():
        continue

    df = pd.read_excel(rpt)
    df.columns = df.columns.str.strip()

    if not {"(Parent) ASIN", "(Child) ASIN", "Model"}.issubset(df.columns):
        continue

    df = df.rename(columns={
        "(Parent) ASIN": "asin",
        "(Child) ASIN": "child_asin",
        "Model": "model",
    })

    df["asin"] = df["asin"].astype(str).str.strip()
    df["child_asin"] = df["child_asin"].astype(str).str.strip()
    df["model"] = df["model"].astype(str).str.upper().str.strip()

    # 🔒 ONE CHILD PER PARENT
    df = df.drop_duplicates(subset=["asin"])

    maps.append(df[["asin", "child_asin", "model"]])

map_df = (
    pd.concat(maps, ignore_index=True)
    if maps else
    pd.DataFrame(columns=["asin", "child_asin", "model"])
)

final = final.merge(map_df, on="asin", how="left")

# --------------------------------------------------
# SKU MASTER = BRAND + CATEGORY (SOURCE OF TRUTH)
# --------------------------------------------------
sku = sku.rename(columns={"ASIN": "child_asin", "Brand": "brand"})
sku["child_asin"] = sku["child_asin"].astype(str).str.strip()
sku = sku.drop_duplicates(subset=["child_asin"])

final = final.merge(
    sku[["child_asin", "brand", "category_l0", "category_l1", "category_l2"]],
    on="child_asin",
    how="left"
)

# 🛡️ BRAND GUARANTEE
final["brand"] = final["brand"].fillna("UNKNOWN")

# TOTAL AMAZON SALES (WEEK LEVEL)
total_amazon_sales = (
    final.groupby("week")["gmv"].transform("sum").replace(0, pd.NA)
)

# --------------------------------------------------
# DERIVED METRICS (VECTOR SAFE)
# --------------------------------------------------
final["conversion_pct"] = final["units"] / final["sessions"].replace(0, pd.NA)
final["roas"] = final["gmv"] / final["Spend"].replace(0, pd.NA)
final["contribution_to_sales_pct"] = (final["gmv"] / total_amazon_sales)
final["acos"] = final["Spend"] / final["attributed_sales"].replace(0, pd.NA)
final["tacos"] = final["Spend"] / final["gmv"].replace(0, pd.NA)
final["cac"] = final["Spend"] / final["ams_orders"].replace(0, pd.NA)

# --------------------------------------------------
# FINAL COLUMN ORDER
# --------------------------------------------------
FINAL_COLS = [
    "brand", "model", "asin", "child_asin", "week",
    "Spend", "Clicks", "Impressions", "attributed_sales", "ams_orders",
    "gmv", "sessions", "units", "buy_box_pct",
    "conversion_pct", "acos", "roas", "tacos", "cac",
    "category_l0", "category_l1", "category_l2",
]

final = final[[c for c in FINAL_COLS if c in final.columns]]

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
final.to_csv(OUT_FILE, index=False)

print("✅ STEP 4 COMPLETE")
print("📁 Output:", OUT_FILE)
print("📊 Rows:", len(final))
print("💰 Spend total:", round(final['Spend'].sum(), 2))
print("🏷️ Brand populated:", final["brand"].ne("UNKNOWN").sum())
