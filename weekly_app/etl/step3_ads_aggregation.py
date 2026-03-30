import pandas as pd
from pathlib import Path
import re

# ==================================================
# STEP 3 : ADS AGGREGATION (SP + SD ONLY)
#
# OUTPUT (SOURCE OF TRUTH FOR STEP 4):
# data/ams_weekly_data/processed_ads/ads_weekly_aggregated.csv
# ==================================================

print("🚀 STEP 3 – ADS AGGREGATION (SP + SD ONLY)")

# --------------------------------------------------
# PATH CONFIG
# --------------------------------------------------
BASE_PATH = Path(__file__).resolve().parents[2] / "data"
AMS_DATA_DIR = BASE_PATH / "ams_weekly_data"
OUTPUT_DIR = AMS_DATA_DIR / "processed_ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SKU_MASTER_FILE = BASE_PATH / "master" / "sku_master.xlsx"

# --------------------------------------------------
# WEEK EXTRACTOR
# --------------------------------------------------
def extract_week(filename: str):
    m = re.search(r"week\s*(\d+)", filename.lower())
    return int(m.group(1)) if m else None

# --------------------------------------------------
# LOAD SKU MASTER (ASIN → MODEL)
# --------------------------------------------------
sku_df = None
if SKU_MASTER_FILE.exists():
    sku_df = pd.read_excel(SKU_MASTER_FILE)
    sku_df.columns = sku_df.columns.str.strip()

    REQUIRED = {"ASIN", "Model"}
    if not REQUIRED.issubset(sku_df.columns):
        raise RuntimeError("❌ SKU master missing ASIN / Model")

    sku_df = sku_df.rename(columns={"ASIN": "asin"})
    sku_df["asin"] = sku_df["asin"].astype(str).str.strip()
    sku_df["Model"] = sku_df["Model"].astype(str).str.upper().str.strip()

    sku_df = sku_df[["asin", "Model"]].drop_duplicates()
else:
    raise RuntimeError("❌ SKU master not found")

# --------------------------------------------------
# AGGREGATION (SP + SD ONLY)
# --------------------------------------------------
rows = []

for brand_dir in AMS_DATA_DIR.iterdir():
    if not brand_dir.is_dir():
        continue
    if brand_dir.name in ["processed_ads", "ams_weekly_fact"]:
        continue

    brand = brand_dir.name.replace("_", " ").strip()


    for ads_file in brand_dir.glob("ads_report_week*.xlsx"):
        week = extract_week(ads_file.name)
        if not week:
            continue

        asin_frames = []

        # ===============================
        # SP + SD (ASIN LEVEL)
        # ===============================
        for sheet in ["SP", "SD"]:
            try:
                df = pd.read_excel(ads_file, sheet_name=sheet)
                df.columns = df.columns.str.strip()

                

                if "Advertised ASIN" not in df.columns:
                    continue

                asin_frames.append(df)
            except Exception:
                continue

        if not asin_frames:
            continue

        ads_df = pd.concat(asin_frames, ignore_index=True)

        agg = (
            ads_df
            .groupby("Advertised ASIN", as_index=False)
            .agg({
                "Spend": "sum",
                "Clicks": "sum",
                "Impressions": "sum",
                "14 Day Total Sales (₹)": "sum",
                "14 Day Total Units (#)": "sum",
            })
        )

        agg = agg.rename(columns={
            "Advertised ASIN": "asin",
            "14 Day Total Sales (₹)": "attributed_sales",
            "14 Day Total Units (#)": "ams_orders",
        })

        agg["asin"] = agg["asin"].astype(str).str.strip()

        # MODEL JOIN (SAFE)
        agg = agg.merge(sku_df, on="asin", how="left")

        agg["brand"] = brand
        agg["week"] = week
        agg["ad_type"] = "SP_SD"

        rows.append(agg)

# --------------------------------------------------
# FINALIZE
# --------------------------------------------------
if not rows:
    raise RuntimeError("❌ No SP / SD ads data found")

final_ads = pd.concat(rows, ignore_index=True)
# --------------------------------------------------
# SAFETY CHECK — ENSURE NO DUPLICATE ASIN + WEEK + BRAND
# --------------------------------------------------
NUM_COLS = ["Spend", "Clicks", "Impressions", "attributed_sales", "ams_orders"]
final_ads = final_ads.groupby(
    ["asin", "week", "brand", "Model", "ad_type"], as_index=False
)[NUM_COLS].sum()

# --------------------------------------------------
# SAFETY NORMALIZATION
# --------------------------------------------------
NUM_COLS = ["Spend", "Clicks", "Impressions", "attributed_sales", "ams_orders"]
for c in NUM_COLS:
    if c not in final_ads.columns:
        final_ads[c] = 0
    final_ads[c] = pd.to_numeric(final_ads[c], errors="coerce").fillna(0)

final_ads["week"] = pd.to_numeric(final_ads["week"], errors="coerce")
final_ads["Model"] = final_ads["Model"].astype(str).str.upper().str.strip()

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
out_file = OUTPUT_DIR / "ads_weekly_aggregated.csv"
final_ads.to_csv(out_file, index=False)

print("✅ STEP 3 ADS AGGREGATION COMPLETE (SP + SD ONLY)")
print("📁 Output:", out_file)
print("📊 Rows:", len(final_ads))
print("📦 Spend total:", final_ads["Spend"].sum())
print("📦 Attributed sales total:", final_ads["attributed_sales"].sum())
