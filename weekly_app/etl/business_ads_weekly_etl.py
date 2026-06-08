import pandas as pd
from pathlib import Path
import re
import sys

# ============================================================
# CONFIG (DO NOT HARD-CODE PER BRAND)
# ============================================================
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
BASE_PATH = DATA_DIR / "ams_weekly_data"

OUTPUT_DIR = BASE_PATH / "ams_weekly_fact"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# SAFETY CHECK
# ============================================================
if not BASE_PATH.exists():
    raise RuntimeError(f"❌ BASE PATH NOT FOUND: {BASE_PATH}")

# ============================================================
# AUTO-DETECT BRAND FOLDERS
# ============================================================
BRAND_FOLDERS = [
    p for p in BASE_PATH.iterdir()
    if p.is_dir() and p.name not in ["ams_weekly_fact", "ads_report", "business_report"]

]

if not BRAND_FOLDERS:
    raise RuntimeError("❌ No brand folders found under ams_weekly_data")

print("🏷 Brands detected:", [b.name for b in BRAND_FOLDERS])

# ============================================================
# WEEK DETECTION (FROM BUSINESS REPORTS)
# ============================================================
def detect_weeks(brand_dir: Path):
    weeks = set()
    for f in brand_dir.glob("business_report_week*.xlsx"):
        m = re.search(r"week(\d+)", f.name.lower())
        if m:
            weeks.add(int(m.group(1)))
    return sorted(weeks)

# ============================================================
# MAIN COLLECTOR
# ============================================================
all_data = []

# ============================================================
# LOOP: BRAND → WEEK
# ============================================================
for brand_dir in BRAND_FOLDERS:
    brand = brand_dir.name
    print(f"\n🏷 Processing Brand: {brand}")

    weeks = detect_weeks(brand_dir)

    if not weeks:
        print(f"⚠ No weeks found for {brand}")
        continue

    # Keep last 4 weeks
    weeks = sorted(weeks)[-4:]
    print(f"🗓 Weeks selected: {weeks}")

    for WEEK in weeks:
        print(f"▶ Week {WEEK}")

        business_file = brand_dir / f"business_report_week{WEEK}.xlsx"
        # Deterministic ads-file pick — skip _api intermediates and .bak
        # variants.  Path.glob() order is non-deterministic across OSes;
        # on the Linux runner the _api.xlsx (lowercase 'asin' column) got
        # picked instead of the canonical operator-exported file (which
        # has 'Advertised ASIN'), crashing the groupby.
        _candidates = [
            p for p in brand_dir.glob(f"ads_report_week{WEEK}*.xlsx")
            if "_api" not in p.name and ".bak" not in p.name
        ]
        # Prefer exact-name match first
        exact = brand_dir / f"ads_report_week{WEEK}.xlsx"
        ads_file = exact if exact in _candidates else (_candidates[0] if _candidates else None)


        if not business_file.exists():
            print(f"⚠ Missing business file: {business_file.name}")
            continue

        # Tonor / Fossil / any brand without its own ads account land here:
        # business_report exists but ads_report does not.  Don't skip — emit
        # the biz rows with zeroed ads.  step4 picks up the real spend later
        # via ads_weekly_aggregated.csv (which is brand-agnostic, keyed on
        # asin+week — Tonor ASINs there come from the AudioArray account).
        ads_missing = ads_file is None
        if ads_missing:
            print(f"⚠ Missing ads file: ads_report_week{WEEK}.xlsx — emitting biz-only rows")

        # ====================================================
        # READ BUSINESS REPORT
        # ====================================================
        biz_df = pd.read_excel(business_file)
        biz_df.columns = biz_df.columns.str.strip()

        biz_df["gmv"] = biz_df.get("gmv", 0)
        biz_df = biz_df.rename(columns={
            "(Child) ASIN": "asin",
            "Sessions - Total": "sessions",
            "Featured Offer Percentage": "buy_box_pct",
            "Unit Session Percentage": "conversion_pct",
            "Units Ordered": "units",
            "Ordered Product Sales": "gmv"
        })

        biz_df["week"] = WEEK
        biz_df["brand"] = brand

        for col in ["sessions", "units", "gmv"]:
            if col in biz_df.columns:
                biz_df[col] = pd.to_numeric(
                    biz_df[col], errors="coerce"
                ).fillna(0)

        # ====================================================
        # DEDUPE BIZ ROWS PER (ASIN, WEEK, BRAND)
        # Amazon's business_report exports multiple rows per SKU
        # (segmented views). Without this, the downstream merge
        # multiplies ad spend N× for each duplicate ASIN row.
        # Totals: sum; rates (*_pct, *_percentage): mean.
        # ====================================================
        _key_cols = ["asin", "week", "brand"]
        _agg = {}
        for _c in biz_df.columns:
            if _c in _key_cols:
                continue
            if pd.api.types.is_numeric_dtype(biz_df[_c]):
                _agg[_c] = "mean" if ("pct" in _c.lower() or "percentage" in _c.lower()) else "sum"
            else:
                _agg[_c] = "first"
        biz_df = biz_df.groupby(_key_cols, as_index=False).agg(_agg)

        # ====================================================
        # READ ADS (SP + SD)
        # Skip when no local ads_file — biz rows still get emitted with
        # zeroed ad metrics; step4 will join real spend from
        # ads_weekly_aggregated.csv (cross-account, by asin+week).
        # ====================================================
        if ads_missing:
            ads_asin = pd.DataFrame(columns=[
                "asin", "week", "brand", "ad_channel",
                "Spend", "Clicks", "Impressions",
                "attributed_sales", "ams_orders",
            ])
        else:
            # Defensive read: a brand × week may legitimately have no SP
            # or SD data (e.g., when the Reports API returns empty for
            # that segment).  Treat a missing sheet as an empty frame so
            # the workflow doesn't crash on the first blank brand.
            _sheets = pd.ExcelFile(ads_file).sheet_names
            sp_df = pd.read_excel(ads_file, sheet_name="SP") if "SP" in _sheets else pd.DataFrame()
            sd_df = pd.read_excel(ads_file, sheet_name="SD") if "SD" in _sheets else pd.DataFrame()

            ads_df = pd.concat([sp_df, sd_df], ignore_index=True)
            ads_df.columns = ads_df.columns.str.strip()

            ads_asin = (
                ads_df
                .groupby("Advertised ASIN", as_index=False)
                .agg({
                    "Spend": "sum",
                    "Clicks": "sum",
                    "Impressions": "sum",
                    "14 Day Total Sales (₹)": "sum",
                    "14 Day Total Units (#)": "sum"
                })
            )

            ads_asin = ads_asin.rename(columns={
                "Advertised ASIN": "asin",
                "14 Day Total Sales (₹)": "attributed_sales",
                "14 Day Total Units (#)": "ams_orders"
            })

            ads_asin["week"] = WEEK
            ads_asin["brand"] = brand
            ads_asin["ad_channel"] = "SP_SD"

            for col in [
                "Spend", "Clicks", "Impressions",
                "attributed_sales", "ams_orders"
            ]:
                ads_asin[col] = pd.to_numeric(
                    ads_asin[col], errors="coerce"
                ).fillna(0)

        # ====================================================
        # JOIN BUSINESS + ADS
        # ====================================================
        final_df = pd.merge(
            biz_df,
            ads_asin,
            on=["asin", "week", "brand"],
            how="left"
        )

        for col in [
            "Spend", "Clicks", "Impressions",
            "attributed_sales", "ams_orders"
        ]:
            final_df[col] = final_df[col].fillna(0)

        # ====================================================
        # DERIVED METRICS (UNCHANGED LOGIC)
        # ====================================================
        final_df["acos"] = final_df.apply(
            lambda x: x["Spend"] / x["attributed_sales"]
            if x["attributed_sales"] > 0 else None,
            axis=1
        )

        final_df["roas"] = final_df.apply(
            lambda x: x["attributed_sales"] / x["Spend"]
            if x["Spend"] > 0 else None,
            axis=1
        )

        final_df["tacos"] = final_df.apply(
            lambda x: x["Spend"] / x["gmv"]
            if x["gmv"] > 0 else None,
            axis=1
        )

        final_df["cac"] = final_df.apply(
            lambda x: x["Spend"] / x["ams_orders"]
            if x["ams_orders"] > 0 else None,
            axis=1
        )

        final_df["attributed_sales_pct"] = final_df.apply(
            lambda x: x["attributed_sales"] / x["gmv"]
            if x["gmv"] > 0 else None,
            axis=1
        )

        final_df["organic_sales_pct"] = final_df[
            "attributed_sales_pct"
        ].apply(lambda x: 1 - x if x is not None else None)

        # ====================================================
        # READ SB ADS — skipped entirely when no local ads_file
        # ====================================================
        if ads_missing:
            sb_df = pd.DataFrame(columns=final_df.columns)
        else:
            sb_df = pd.read_excel(ads_file, sheet_name="SB")
            sb_df.columns = sb_df.columns.str.strip()

            sb_df = sb_df.rename(columns={
                "Campaign Name": "campaign_name",
                "Portfolio name": "portfolio_name",
                "14 Day Total Sales (₹)": "attributed_sales",
                "14 Day Total Units (#)": "ams_orders"
            })

            sb_df["week"] = WEEK
            sb_df["brand"] = brand
            sb_df["asin"] = "__SB__"
            sb_df["ad_channel"] = "SB"

            for col in [
                "Spend", "Clicks", "Impressions",
                "attributed_sales", "ams_orders"
            ]:
                if col in sb_df.columns:
                    sb_df[col] = pd.to_numeric(
                        sb_df[col], errors="coerce"
                    ).fillna(0)

            # ALIGN SCHEMA
            for col in final_df.columns:
                if col not in sb_df.columns:
                    sb_df[col] = None

            sb_df = sb_df[final_df.columns]

        # ====================================================
        # COMBINE WEEK DATA
        # ====================================================
        combined_week_df = pd.concat([df for df in [final_df, sb_df] if not df.empty], ignore_index=True)
        combined_week_df["brand"] = brand
        combined_week_df["week"] = WEEK


        all_data.append(combined_week_df)

# ============================================================
# FINAL OUTPUT
# ============================================================
if not all_data:
    raise RuntimeError("❌ No AMS data generated")

final_df = pd.concat(all_data, ignore_index=True)

output_file = OUTPUT_DIR / "ams_weekly_fact.csv"
final_df.to_csv(output_file, index=False)

print("\n✅ AMS WEEKLY FACT GENERATED (ALL BRANDS)")
print(f"📁 Output: {output_file}")
print(f"📊 Rows: {len(final_df)}")
