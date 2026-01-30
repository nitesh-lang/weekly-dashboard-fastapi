import pandas as pd
from pathlib import Path
import re

# ==================================================
# STEP 3 : ADS AGGREGATION (MODEL + AMS TREND READY)
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[2] / "data" / "ams_weekly_data"
ADS_ROOT = BASE_DIR
OUTPUT_DIR = BASE_DIR / "processed_ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SKU_MASTER_FILE = BASE_DIR.parents[1] / "master" / "sku_master.xlsx"

# --------------------------------------------------
# WEEK EXTRACTOR
# --------------------------------------------------
def extract_week(fname: str):
    for part in fname.split("_"):
        if part.lower().startswith("week"):
            m = re.search(r"\d+", part)
            if m:
                return int(m.group())
    return None

# --------------------------------------------------
# LOAD SKU MASTER (MODEL = SOURCE OF TRUTH)
# --------------------------------------------------
sku_df = None
if SKU_MASTER_FILE.exists():
    sku_df = pd.read_excel(SKU_MASTER_FILE)
    sku_df.columns = sku_df.columns.str.strip()

    rename_map = {
        "ASIN": "asin",
        "Model": "Model",
    }
    sku_df = sku_df.rename(
        columns={k: v for k, v in rename_map.items() if k in sku_df.columns}
    )

    if "asin" in sku_df.columns:
        sku_df["asin"] = sku_df["asin"].astype(str).str.strip()
    else:
        raise RuntimeError("❌ SKU master missing ASIN column")

    if "Model" in sku_df.columns:
        sku_df["Model"] = sku_df["Model"].astype(str).str.strip().str.upper()
    else:
        raise RuntimeError("❌ SKU master missing Model column")

    sku_df = sku_df[["asin", "Model"]].drop_duplicates()

# --------------------------------------------------
# AGGREGATION
# --------------------------------------------------
all_rows = []

for brand_dir in ADS_ROOT.iterdir():
    if not brand_dir.is_dir():
        continue
    if brand_dir.name in ["processed_ads", "ams_weekly_fact"]:
        continue

    brand = brand_dir.name

    for ads_file in brand_dir.glob("ads_report_week*.xlsx"):
        WEEK = extract_week(ads_file.name)
        if WEEK is None:
            continue

        # ===============================
        # SP + SD (ASIN LEVEL)
        # ===============================
        frames = []
        for sheet in ["SP", "SD"]:
            try:
                df = pd.read_excel(ads_file, sheet_name=sheet)
                df.columns = df.columns.str.strip()
                frames.append(df)
            except Exception:
                pass

        if frames:
            ads_df = pd.concat(frames, ignore_index=True)

            if "Advertised ASIN" not in ads_df.columns:
                continue

            asin_week = (
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

            asin_week.rename(columns={
                "Advertised ASIN": "asin",
                "14 Day Total Sales (₹)": "attributed_sales",
                "14 Day Total Units (#)": "ams_orders",
            }, inplace=True)

            asin_week["asin"] = asin_week["asin"].astype(str).str.strip()

            # 🔗 MODEL JOIN (CRITICAL)
            if sku_df is not None:
                asin_week = asin_week.merge(
                    sku_df,
                    on="asin",
                    how="left"
                )
            else:
                asin_week["Model"] = None

            asin_week["brand"] = brand
            asin_week["week"] = WEEK
            asin_week["ad_type"] = "SP_SD"

            all_rows.append(asin_week)

        # ===============================
        # SB (NO ASIN → NO MODEL)
        # ===============================
        try:
            sb_df = pd.read_excel(ads_file, sheet_name="SB")
            sb_df.columns = sb_df.columns.str.strip()

            if "Campaign Name" not in sb_df.columns:
                raise ValueError

            sb_week = (
                sb_df
                .groupby("Campaign Name", as_index=False)
                .agg({
                    "Spend": "sum",
                    "Clicks": "sum",
                    "Impressions": "sum",
                    "14 Day Total Sales (₹)": "sum",
                    "14 Day Total Units (#)": "sum",
                })
            )

            sb_week.rename(columns={
                "Campaign Name": "campaign_name",
                "14 Day Total Sales (₹)": "attributed_sales",
                "14 Day Total Units (#)": "ams_orders",
            }, inplace=True)

            sb_week["asin"] = None
            sb_week["Model"] = None
            sb_week["brand"] = brand
            sb_week["week"] = WEEK
            sb_week["ad_type"] = "SB"

            all_rows.append(sb_week)

        except Exception:
            pass

# --------------------------------------------------
# FINALIZE
# --------------------------------------------------
if not all_rows:
    raise RuntimeError("❌ No ads data aggregated")

final_ads = pd.concat(all_rows, ignore_index=True)

# SAFETY NORMALIZATION
final_ads["week"] = pd.to_numeric(final_ads["week"], errors="coerce")
final_ads["Model"] = final_ads["Model"].astype(str).str.strip().str.upper()

out_file = OUTPUT_DIR / "ads_weekly_aggregated.csv"
final_ads.to_csv(out_file, index=False)

print("✅ STEP 3 ADS AGGREGATION COMPLETE (MODEL READY)")
print(f"📁 Output: {out_file}")
print(f"📊 Rows: {len(final_ads)}")
print("📦 Model populated:", final_ads["Model"].notna().sum())
