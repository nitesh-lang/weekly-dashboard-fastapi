import pandas as pd
from pathlib import Path

print("🚀 STEP 5 – CATEGORY MAPPING (UI + PIPELINE COMPATIBLE)")

# ==================================================
# CONFIG
# ==================================================
BASE_PATH = Path(__file__).resolve().parents[2] / "data"

AMS_DIR = BASE_PATH / "ams_weekly_data" / "ams_weekly_fact"
AMS_FILE = AMS_DIR / "ams_weekly_fact.csv"

BRANDS_BASE_DIR = BASE_PATH / "ams_weekly_data"
SKU_MASTER_FILE = BASE_PATH / "master" / "sku_master.xlsx"

OUTPUT_FILE = AMS_DIR / "ams_weekly_fact_with_category.csv"

# ==================================================
# VALIDATION
# ==================================================
if not AMS_FILE.exists():
    raise FileNotFoundError(f"❌ AMS weekly fact not found: {AMS_FILE}")

if not SKU_MASTER_FILE.exists():
    print(f"⚠️ SKU master not found, categories will be null: {SKU_MASTER_FILE}")
    sku_df = pd.DataFrame(
        columns=["model", "category_l0", "category_l1", "category_l2"]
    )

# ==================================================
# LOAD AMS WEEKLY FACT
# ==================================================
ams_df = pd.read_csv(AMS_FILE)
ams_df.columns = ams_df.columns.str.strip().str.lower()

print("📥 AMS rows:", len(ams_df))

# --------------------------------------------------
# ENSURE BASE COLUMNS
# --------------------------------------------------
for col in ["week", "asin", "ad_channel", "model"]:
    if col not in ams_df.columns:
        ams_df[col] = None

# --------------------------------------------------
# NORMALIZE WEEK (UI SAFE)
# --------------------------------------------------
ams_df["week"] = (
    ams_df["week"]
    .astype(str)
    .str.replace("Week", "", regex=False)
    .str.replace("W", "", regex=False)
    .str.strip()
)
ams_df["week"] = pd.to_numeric(ams_df["week"], errors="coerce")

print("📅 Weeks detected:", sorted(ams_df["week"].dropna().unique()))

# ==================================================
# SPLIT SB vs NON-SB
# ==================================================
sb_df = ams_df[ams_df["ad_channel"] == "SB"].copy()
non_sb_df = ams_df[ams_df["ad_channel"] != "SB"].copy()

print("🟡 SB rows:", len(sb_df))
print("🟢 Non-SB rows:", len(non_sb_df))

# ==================================================
# PICK LATEST WEEK
# ==================================================
latest_week = int(ams_df["week"].max())

biz_files = list(
    BRANDS_BASE_DIR.glob(f"*/business_report_week{latest_week}.xlsx")
)

if not biz_files:
    raise RuntimeError(f"❌ No business reports found for week {latest_week}")

print(f"📊 Found {len(biz_files)} business reports for week {latest_week}")

# ==================================================
# LOAD BUSINESS REPORT (ASIN → MODEL)
# ==================================================
biz_dfs = []

for f in biz_files:
    df = pd.read_excel(f)
    df["brand"] = f.parent.name
    biz_dfs.append(df)

biz_df = pd.concat(biz_dfs, ignore_index=True)
biz_df.columns = biz_df.columns.str.strip()

biz_df = biz_df.rename(columns={
    "(Child) ASIN": "asin",
    "Model": "model"
})

for col in ["asin", "model"]:
    if col not in biz_df.columns:
        raise RuntimeError(f"❌ Missing column in business report: {col}")

biz_df = biz_df[["asin", "model"]].dropna()
biz_df["asin"] = biz_df["asin"].astype(str).str.strip()
biz_df["model"] = biz_df["model"].astype(str).str.strip().str.upper()
biz_df = biz_df.drop_duplicates()

print("📘 Business ASIN→Model rows:", len(biz_df))

# ==================================================
# LOAD SKU MASTER (MODEL → CATEGORY)
# ==================================================
sku_df = pd.read_excel(SKU_MASTER_FILE)
sku_df.columns = sku_df.columns.str.strip()

sku_df = sku_df.rename(columns={
    "Model": "model",
    "Category L0": "category_l0",
    "Category L1": "category_l1",
    "Category L2": "category_l2"
})

for col in ["model", "category_l0", "category_l1", "category_l2"]:
    if col not in sku_df.columns:
        sku_df[col] = None

sku_df["model"] = sku_df["model"].astype(str).str.strip().str.upper()
sku_df = sku_df.drop_duplicates(subset=["model"])

print("📘 SKU master rows:", len(sku_df))

# ==================================================
# NORMALIZE AMS KEYS
# ==================================================
non_sb_df["asin"] = non_sb_df["asin"].astype(str).str.strip()
non_sb_df["model"] = non_sb_df["model"].astype(str).str.strip().str.upper()

# ==================================================
# JOIN 1: AMS → BUSINESS (ASIN → MODEL)
# ==================================================
non_sb_df = pd.merge(
    non_sb_df,
    biz_df,
    on="asin",
    how="left",
    suffixes=("", "_biz")
)

# Prefer business model when present
non_sb_df["model"] = non_sb_df["model_biz"].fillna(non_sb_df["model"])
non_sb_df.drop(columns=["model_biz"], inplace=True)

# ==================================================
# JOIN 2: MODEL → CATEGORY (SKU MASTER)
# ==================================================
non_sb_df = pd.merge(
    non_sb_df,
    sku_df,
    on="model",
    how="left"
)

# ==================================================
# SB SAFE CATEGORY ASSIGNMENT
# ==================================================
sb_df["model"] = "SB"
sb_df["category_l0"] = "Sponsored Brands"
sb_df["category_l1"] = "SB Campaigns"

if "campaign_name" not in sb_df.columns:
    sb_df["campaign_name"] = None

sb_df["category_l2"] = sb_df["campaign_name"].fillna("SB")

# ==================================================
# RECOMBINE
# ==================================================
final_df = non_sb_df.copy()

# ==================================================
# UI NORMALIZATION
# ==================================================
final_df["Model"] = final_df.get("model")

required_cols = [
    "category_l0",
    "category_l1",
    "category_l2",
    "Model",
    "week"
]

for col in required_cols:
    if col not in final_df.columns:
        final_df[col] = None

# ==================================================
# UNITS SAFETY
# ==================================================
if "units" not in final_df.columns:
    if "units_ordered" in final_df.columns:
        final_df["units"] = final_df["units_ordered"]
    else:
        final_df["units"] = 0

# ==================================================
# FINAL CHECKS
# ==================================================
print("🔎 FINAL CHECKS")
print("Rows:", len(final_df))
print("Weeks:", final_df["week"].min(), "→", final_df["week"].max())
print("SB rows:", (final_df["ad_channel"] == "SB").sum())
print("Categories filled:",
      final_df["category_l0"].notna().sum())

# ==================================================
# SAVE
# ==================================================
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final_df.to_csv(OUTPUT_FILE, index=False)

# ==================================================
# ALSO WRITE PROCESSED ADS FILE (AMS TREND SOURCE)
# ==================================================
PROCESSED_ADS_DIR = BASE_PATH / "ams_weekly_data" / "processed_ads"
PROCESSED_ADS_FILE = PROCESSED_ADS_DIR / "business_ads_joined.csv"

PROCESSED_ADS_DIR.mkdir(parents=True, exist_ok=True)
final_df.to_csv(PROCESSED_ADS_FILE, index=False)

print("📁 Processed ADS Output:", PROCESSED_ADS_FILE)
print("✅ STEP 5 COMPLETE – CATEGORY MAPPING")
print("📁 Output:", OUTPUT_FILE)
print("📊 Rows:", len(final_df))
