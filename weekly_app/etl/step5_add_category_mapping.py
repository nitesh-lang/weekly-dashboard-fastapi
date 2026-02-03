import pandas as pd
from pathlib import Path

print("🚀 STEP 5 – CATEGORY FINAL (SAFE, NON-DESTRUCTIVE)")

# ==================================================
# CONFIG
# ==================================================
BASE_PATH = Path(__file__).resolve().parents[2] / "data"

AMS_DATA_DIR = BASE_PATH / "ams_weekly_data"
AMS_FACT_DIR = AMS_DATA_DIR / "ams_weekly_fact"

AMS_FACT_FILE = AMS_FACT_DIR / "ams_weekly_fact.csv"
STEP4_FILE = AMS_DATA_DIR / "processed_ads" / "business_ads_joined.csv"
SKU_MASTER_FILE = BASE_PATH / "master" / "sku_master.xlsx"

OUTPUT_AMS_FILE = AMS_FACT_DIR / "ams_weekly_fact_with_category.csv"

# ==================================================
# VALIDATION
# ==================================================
if not AMS_FACT_FILE.exists():
    raise FileNotFoundError(f"❌ Missing AMS weekly fact: {AMS_FACT_FILE}")

if not STEP4_FILE.exists():
    raise FileNotFoundError(f"❌ Missing STEP-4 output: {STEP4_FILE}")

if not SKU_MASTER_FILE.exists():
    raise FileNotFoundError(f"❌ Missing SKU master: {SKU_MASTER_FILE}")

# ==================================================
# LOAD DATA
# ==================================================
ams_df = pd.read_csv(AMS_FACT_FILE)
step4_df = pd.read_csv(STEP4_FILE)

ams_df.columns = ams_df.columns.str.strip().str.lower()
step4_df.columns = step4_df.columns.str.strip().str.lower()

print("📥 AMS rows:", len(ams_df))
print("📥 STEP-4 rows:", len(step4_df))

# ==================================================
# ENSURE BASE COLUMNS
# ==================================================
for col in ["week", "asin", "ad_channel", "model"]:
    if col not in ams_df.columns:
        ams_df[col] = None

# ==================================================
# NORMALIZE WEEK
# ==================================================
ams_df["week"] = (
    ams_df["week"]
    .astype(str)
    .str.replace("week", "", case=False)
    .str.replace("w", "", case=False)
    .str.strip()
)
ams_df["week"] = pd.to_numeric(ams_df["week"], errors="coerce")

print("📅 Weeks:", sorted(ams_df["week"].dropna().unique()))

# ==================================================
# NORMALIZE ASIN / MODEL
# ==================================================
ams_df["asin"] = ams_df["asin"].astype(str).str.strip()
ams_df["model"] = ams_df["model"].astype(str).str.strip().str.upper()

step4_df["asin"] = step4_df["asin"].astype(str).str.strip()
step4_df["model"] = step4_df.get("model", step4_df.get("Model", "")).astype(str).str.strip().str.upper()

# ==================================================
# SPLIT SB vs NON-SB
# ==================================================
sb_df = ams_df[ams_df["ad_channel"] == "SB"].copy()
non_sb_df = ams_df[ams_df["ad_channel"] != "SB"].copy()

print("🟡 SB rows:", len(sb_df))
print("🟢 Non-SB rows:", len(non_sb_df))

# ==================================================
# STEP-4 CATEGORY SOURCE (ASIN LEVEL – SOURCE OF TRUTH)
# ==================================================
cat_cols = ["asin", "brand", "category_l0", "category_l1", "category_l2"]

missing_cols = [c for c in cat_cols if c not in step4_df.columns]
if missing_cols:
    raise RuntimeError(f"❌ STEP-4 missing category columns: {missing_cols}")

category_df = (
    step4_df[cat_cols]
    .dropna(subset=["asin"])
    .drop_duplicates(subset=["asin"])
)

print("📦 Category rows from STEP-4:", len(category_df))

# ==================================================
# APPLY CATEGORY TO NON-SB (ASIN JOIN)
# ==================================================
non_sb_df = non_sb_df.merge(
    category_df,
    on="asin",
    how="left"
)

print(
    "✅ Non-SB category filled:",
    non_sb_df["category_l0"].notna().sum(),
    "/",
    len(non_sb_df)
)

# ==================================================
# SB HANDLING (EXPLICIT, SAFE)
# ==================================================
sb_df["model"] = "SB"
sb_df["category_l0"] = "Sponsored Brands"
sb_df["category_l1"] = "SB Campaigns"

if "campaign_name" in sb_df.columns:
    sb_df["category_l2"] = sb_df["campaign_name"].fillna("SB")
else:
    sb_df["category_l2"] = "SB"

# ==================================================
# RECOMBINE
# ==================================================
final_df = pd.concat([non_sb_df, sb_df], ignore_index=True)

# ==================================================
# UI NORMALIZATION
# ==================================================
final_df["Model"] = final_df["model"]

for col in ["category_l0", "category_l1", "category_l2", "Model"]:
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
print("Categories filled:", final_df["category_l0"].notna().sum())

# ==================================================
# SAVE OUTPUT
# ==================================================
AMS_FACT_DIR.mkdir(parents=True, exist_ok=True)
final_df.to_csv(OUTPUT_AMS_FILE, index=False)

print("📁 AMS Fact Output:", OUTPUT_AMS_FILE)
print("✅ STEP 5 COMPLETE — CATEGORY PRESERVED (STEP-4 SOURCE)")
print("📊 Rows:", len(final_df))
