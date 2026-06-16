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
# AD TYPE WHITELIST
# Legacy code dropped SB entirely ("REMOVE SB COMPLETELY") because
# SB campaigns weren't attributed to ASINs.  We now ingest SB via
# weekly_app/etl/sb_ingest.py which writes per-ASIN SB rows (L1+L2+L3
# attribution), so SB is admitted here alongside SP/SD.
# --------------------------------------------------
if "ad_type" in ads.columns:
    ads = ads[ads["ad_type"].isin(["SP", "SD", "SP_SD", "SB"])].copy()

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

# Preserve the source-folder brand so synthetic ASINs (e.g. __SB__) that
# can't be matched to master keep their original tag instead of falling
# to "UNKNOWN" on the UI.  Real ASINs get re-tagged via sku_master below.
biz["brand_src"] = (
    biz.get("brand", "")
    .astype(str).str.strip()
    .str.replace("_", " ", regex=False)
)

biz = (
    biz[["asin", "week", "gmv", "sessions", "units", "buy_box_pct", "brand_src"]]
    .groupby(["asin", "week"], as_index=False)
    .agg({
        "gmv": "sum",
        "sessions": "sum",
        "units": "sum",
        "buy_box_pct": "mean",
        "brand_src": "first",
    })
)

# --------------------------------------------------
# JOIN 1: BUSINESS ← ADS (BUSINESS BASE)
final = biz.merge(
    ads,
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
# SKU MASTER = BRAND + MODEL + CATEGORY (SOURCE OF TRUTH)
# Model is also sourced from master so ASINs without a Model entry in
# business_report (e.g. all Fossil ASINs — Fossil has no per-week
# business_report) still resolve.  Without this, ~₹3.24 Cr of GMV
# across W21-W24 was bucketing into AMS Trend's model=NaN row.
# --------------------------------------------------
sku = sku.rename(columns={"ASIN": "child_asin", "Brand": "brand", "Model": "model_master"})
sku["child_asin"] = sku["child_asin"].astype(str).str.strip()
sku["brand"] = sku["brand"].astype(str).str.strip()
if "model_master" in sku.columns:
    sku["model_master"] = sku["model_master"].astype(str).str.upper().str.strip().replace(
        {"NAN": "", "NONE": "", "-": ""}
    )
sku = sku.drop_duplicates(subset=["child_asin"])

merge_cols = ["child_asin", "brand", "category_l0", "category_l1", "category_l2"]
if "model_master" in sku.columns:
    merge_cols.insert(2, "model_master")

final = final.merge(sku[merge_cols], on="child_asin", how="left")

# 🛡️ MODEL FALLBACK — prefer business_report's Model, fall back to master.
# When neither has it, leave blank so the AMS Trend route can collapse
# the row into the brand row instead of a phantom "(no model)" bucket.
if "model_master" in final.columns:
    final["model"] = final["model"].astype(str).str.upper().str.strip().replace(
        {"NAN": "", "NONE": "", "-": ""}
    )
    final["model"] = final["model"].where(final["model"].ne(""), final["model_master"])
    final = final.drop(columns=["model_master"])

# 🛡️ BRAND GUARANTEE — prefer master, fall back to source-folder brand
# from biz, then UNKNOWN.  This keeps __SB__ placeholder rows attached
# to their actual brand on the UI instead of all collapsing to UNKNOWN.
if "brand_src" in final.columns:
    final["brand"] = final["brand"].fillna(final["brand_src"])
    final = final.drop(columns=["brand_src"])
final["brand"] = final["brand"].replace("", pd.NA).fillna("UNKNOWN")

# TOTAL AMAZON SALES (WEEK LEVEL)
total_amazon_sales = (
    final.groupby("week")["gmv"].transform("sum").replace(0, pd.NA)
)

# --------------------------------------------------
# DERIVED METRICS (VECTOR SAFE)
# --------------------------------------------------
final["conversion_pct"] = final["units"] / final["sessions"].replace(0, pd.NA)
# ROAS = Attributed Sales / Ad Spend (industry standard).
# Was previously gmv/Spend which is just 1/TACOS — meaningless as ROAS.
final["roas"] = final["attributed_sales"] / final["Spend"].replace(0, pd.NA)
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
# STRICT FILTER — drop rows whose ASIN is not in sku_master at all.
# Untagged ASINs (e.g. 341 unmatched CRPL ASINs that aren't yet
# assigned to a Brand/Model in master) get a brand via the
# folder-name fallback (`brand_src` → Audio Array etc.) but no
# Model assignment, so they bucket into AMS Trend's "model=NaN"
# row and inflate it by ₹78L+ for W24, breaking cross-route
# consistency vs Sales Trend / Amazon+1P (which only emit rows
# for ASINs admitted by the sku_master ASIN→Brand join).
#
# Gate on the actual admission criterion: ASIN in sku_master.
# Rows whose ASIN IS in master but model is empty in
# business_report are KEPT — they still attribute to a brand
# and reconcile at the brand level.  Off-master ASINs still
# surface in raw audit files (_audit/*.csv) for backfill.
# --------------------------------------------------
master_asins = set(sku["child_asin"].astype(str).str.strip().str.upper())
before = len(final)
final["_asin_n"] = final["child_asin"].astype(str).str.strip().str.upper()
filt = final["_asin_n"].isin(master_asins)
dropped = final[~filt].copy()
final = final[filt].drop(columns=["_asin_n"])
n_dropped = before - len(final)
print(f"🧹 Strict filter: dropped {n_dropped} off-master-ASIN rows "
      f"(was Rs {dropped['gmv'].sum():,.0f} GMV / {int(dropped['units'].sum())} units / "
      f"Rs {dropped['Spend'].sum():,.0f} ad spend)")

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
final.to_csv(OUT_FILE, index=False)

print("✅ STEP 4 COMPLETE")
print("📁 Output:", OUT_FILE)
print("📊 Rows:", len(final))
print("💰 Spend total:", round(final['Spend'].sum(), 2))
print("🏷️ Brand populated:", final["brand"].ne("UNKNOWN").sum())
