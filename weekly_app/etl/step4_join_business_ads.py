import pandas as pd
from pathlib import Path
import re

# ==================================================
# STEP 4: JOIN BUSINESS + ADS + SKU MASTER (MODEL MASTER)
# ==================================================

# --------------------------------------------------
# PATH CONFIG
# --------------------------------------------------
BASE_PATH = Path(__file__).resolve().parents[2] / "data" / "ams_weekly_data"

ADS_AGG_FILE = BASE_PATH / "processed_ads" / "ads_weekly_aggregated.csv"
BIZ_FACT_FILE = BASE_PATH / "ams_weekly_fact" / "ams_weekly_fact.csv"

SKU_MASTER_FILE = BASE_PATH.parents[1] / "master" / "sku_master.xlsx"

OUTPUT_DIR = BASE_PATH / "processed_ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUTPUT_DIR / "business_ads_joined.csv"

# --------------------------------------------------
# VALIDATIONS
# --------------------------------------------------
if not ADS_AGG_FILE.exists():
    raise RuntimeError(f"❌ Missing ads aggregate CSV: {ADS_AGG_FILE}")

if not BIZ_FACT_FILE.exists():
    raise RuntimeError(f"❌ Missing business fact CSV: {BIZ_FACT_FILE}")

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
ads_df = pd.read_csv(ADS_AGG_FILE)
biz_df = pd.read_csv(BIZ_FACT_FILE)

ads_df.columns = ads_df.columns.str.strip()
biz_df.columns = biz_df.columns.str.strip()

# --------------------------------------------------
# NORMALIZE GMV (BUSINESS SOURCE OF TRUTH)
# --------------------------------------------------
if "GMV" in biz_df.columns and "gmv" not in biz_df.columns:
    biz_df = biz_df.rename(columns={"GMV": "gmv"})

gmv_cols = [
    c for c in biz_df.columns
    if re.search(r"ordered.*product.*sales", c, re.I)
]
if gmv_cols:
    biz_df["gmv"] = biz_df[gmv_cols[0]]

# --------------------------------------------------
# REQUIRED COLUMNS
# --------------------------------------------------
for col in ["asin", "week", "Model"]:
    if col not in biz_df.columns:
        raise RuntimeError(f"❌ ams_weekly_fact.csv missing: {col}")

for col in ["asin", "week", "Model"]:
    if col not in ads_df.columns:
        raise RuntimeError(f"❌ ads_weekly_aggregated.csv missing: {col}")

# --------------------------------------------------
# NORMALIZE TYPES
# --------------------------------------------------
for df in (ads_df, biz_df):
    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df["asin"] = df["asin"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip().str.upper()

# --------------------------------------------------
# ADS METRICS
# --------------------------------------------------
ADS_NUM_COLS = [
    "Spend",
    "Clicks",
    "Impressions",
    "attributed_sales",
    "ams_orders"
]

for c in ADS_NUM_COLS:
    if c not in ads_df.columns:
        ads_df[c] = 0
    ads_df[c] = pd.to_numeric(ads_df[c], errors="coerce").fillna(0)

# --------------------------------------------------
# BUSINESS METRICS
# --------------------------------------------------
BIZ_NUM_COLS = [
    "gmv",
    "sessions",
    "units",
    "buy_box_pct"
]

for c in BIZ_NUM_COLS:
    if c not in biz_df.columns:
        biz_df[c] = None
    biz_df[c] = pd.to_numeric(biz_df[c], errors="coerce")

# --------------------------------------------------
# MAIN JOIN (BUSINESS ← ADS) — MODEL SAFE
# --------------------------------------------------
final_df = pd.merge(
    biz_df,
    ads_df,
    on=["asin", "Model", "week"],
    how="left",
    suffixes=("", "_ads")
)

for c in ADS_NUM_COLS:
    final_df[c] = pd.to_numeric(final_df[c], errors="coerce").fillna(0)

# --------------------------------------------------
# SKU MASTER JOIN (MODEL = SOURCE OF TRUTH)
# --------------------------------------------------
if SKU_MASTER_FILE.exists():
    sku = pd.read_excel(SKU_MASTER_FILE)
    sku.columns = sku.columns.str.strip()

    rename_map = {
        "Model": "Model",
        "Brand": "brand",
        "Category L0": "category_l0",
        "Category L1": "category_l1",
        "Category L2": "category_l2",
    }
    sku = sku.rename(columns={k: v for k, v in rename_map.items() if k in sku.columns})

    sku["Model"] = sku["Model"].astype(str).str.strip().str.upper()

    final_df = final_df.merge(
        sku[["Model", "brand", "category_l0", "category_l1", "category_l2"]],
        on="Model",
        how="left"
    )

# --------------------------------------------------
# DERIVED METRICS (FINAL, CLEAN)
# --------------------------------------------------
final_df["conversion_pct"] = final_df.apply(
    lambda x: x["units"] / x["sessions"]
    if x["sessions"] and x["sessions"] > 0 else None,
    axis=1
)

final_df["roas"] = final_df.apply(
    lambda x: x["attributed_sales"] / x["Spend"]
    if x["Spend"] > 0 else None,
    axis=1
)

final_df["acos"] = final_df.apply(
    lambda x: x["Spend"] / x["attributed_sales"]
    if x["attributed_sales"] > 0 else None,
    axis=1
)

final_df["tacos"] = final_df.apply(
    lambda x: x["Spend"] / x["gmv"]
    if x["gmv"] and x["gmv"] > 0 else None,
    axis=1
)

final_df["cac"] = final_df.apply(
    lambda x: x["Spend"] / x["ams_orders"]
    if x["ams_orders"] > 0 else None,
    axis=1
)

final_df["attributed_sales_pct"] = final_df.apply(
    lambda x: x["attributed_sales"] / x["gmv"]
    if x["gmv"] and x["gmv"] > 0 else None,
    axis=1
)

final_df["organic_sales_pct"] = final_df["attributed_sales_pct"].apply(
    lambda x: 1 - x if x is not None else None
)

# --------------------------------------------------
# UI SAFETY (NO FAKE FALLBACKS)
# --------------------------------------------------
for c in ["brand", "category_l0", "category_l1", "category_l2"]:
    if c not in final_df.columns:
        final_df[c] = None

# --------------------------------------------------
# ORDERING (UI STABLE)
# --------------------------------------------------
KEY_ORDER = [
    "brand", "Model", "asin", "week",
    "Spend", "Clicks", "Impressions",
    "attributed_sales", "ams_orders",
    "gmv", "sessions", "units",
    "buy_box_pct", "conversion_pct",
    "acos", "roas", "tacos", "cac",
    "attributed_sales_pct", "organic_sales_pct",
    "category_l0", "category_l1", "category_l2"
]

final_df = final_df[
    [c for c in KEY_ORDER if c in final_df.columns] +
    [c for c in final_df.columns if c not in KEY_ORDER]
]

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
final_df.to_csv(OUT_FILE, index=False)

print("✅ STEP 4 COMPLETE — MODEL IS MASTER, CATEGORIES FIXED")
print("📁 Output:", OUT_FILE)
print("📊 Rows:", len(final_df))
