"""
For weeks where the operator hasn't exported the SB campaign report,
read the API-pulled SB ad-level data and apply the SAME L0/L1/L2/L3
attribution + even-split distribution as sb_ingest.py.

Source:    data/ams_weekly_data/<Brand>/ads_report_week<N>_api.xlsx  (sheet=SB)
Target:    data/ams_weekly_data/<Brand>/ads_report_week<N>.xlsx       (sheet=SB)

Skips any (brand, week) where the canonical file already has an SB sheet
written by sb_ingest (operator-Excel sourced), so the latest week's
verified data isn't overwritten.
"""
from __future__ import annotations
import re
import shutil
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from weekly_app.etl.sb_ingest import (
    build_master_lookups, build_active_set,
    load_campaign_asin_map, load_synonyms,
    attribute_campaign, OUT_COLS, FOLDER_TO_BRAND,
)

AMS_ROOT = ROOT / "data" / "ams_weekly_data"


def aggregate_api_sb(sheet_df: pd.DataFrame) -> pd.DataFrame:
    """Group the API per-ad rows to per-(Portfolio, Campaign) and rename
    columns to match what attribute_campaign + writer expect."""
    if sheet_df.empty or "campaignName" not in sheet_df.columns:
        return pd.DataFrame()
    df = sheet_df.copy()
    if "Portfolio name" not in df.columns:
        df["Portfolio name"] = ""
    df = df.rename(columns={
        "campaignName":     "Campaign Name",
        "attributed_sales": "14 Day Total Sales (₹)",
        "ams_orders":       "14 Day Total Units (#)",
    })
    nums = ["Spend", "Clicks", "Impressions",
            "14 Day Total Sales (₹)", "14 Day Total Units (#)"]
    for c in nums:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return (df.groupby(["Portfolio name", "Campaign Name"], as_index=False)
              [nums].sum())


def run_for_week(brand_dir: Path, week: int,
                 asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
                 cat_to_brand_asins, cats_sorted_vals, synonyms,
                 campaign_asin_map, active) -> dict:
    api_file   = brand_dir / f"ads_report_week{week}_api.xlsx"
    canon_file = brand_dir / f"ads_report_week{week}.xlsx"
    out = {"brand": brand_dir.name, "week": week, "status": "skip", "note": ""}
    if not api_file.exists():
        out["note"] = "no api file"; return out
    if not canon_file.exists():
        out["note"] = "no canonical file"; return out

    # Skip if canonical already has SB sheet (don't overwrite the verified one)
    try:
        existing_sheets = pd.ExcelFile(canon_file).sheet_names
    except Exception as e:
        out["note"] = f"cant open canonical: {e}"; return out
    if "SB" in existing_sheets:
        out["note"] = "SB already present (operator-exported)"
        out["status"] = "skip"
        return out

    # Read API SB sheet
    api_x = pd.ExcelFile(api_file)
    if "SB" not in api_x.sheet_names:
        out["note"] = "no SB sheet in api file"; return out
    api_sb = pd.read_excel(api_file, sheet_name="SB")
    agg = aggregate_api_sb(api_sb)
    if agg.empty:
        out["note"] = "empty after aggregation"; return out

    file_brand = FOLDER_TO_BRAND.get(brand_dir.name, brand_dir.name)
    rows_by_brand_folder: dict[str, list[dict]] = {}
    unmapped = 0
    mapped   = 0
    layer_count: dict[str, int] = {}

    for _, r in agg.iterrows():
        campaign = str(r["Campaign Name"]).strip()
        spend    = float(r["Spend"])
        if spend <= 0:
            continue
        layer, asins, brand = attribute_campaign(
            campaign, file_brand,
            asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
            cat_to_brand_asins, cats_sorted_vals, synonyms, active,
            campaign_asin_map=campaign_asin_map,
        )
        base_layer = ("L0_ads_api"  if layer.startswith("L0_") else
                      "L3_category" if layer.startswith("L3_") else
                      "L4_synonym"  if layer.startswith("L4_") else layer)
        layer_count[base_layer] = layer_count.get(base_layer, 0) + 1
        if not asins:
            unmapped += 1
            continue
        mapped += 1
        n = len(asins)
        template = {
            "Portfolio name":  str(r["Portfolio name"]),
            "Campaign Name":   campaign,
            "Impressions":             float(r["Impressions"]) / n,
            "Clicks":                  float(r["Clicks"]) / n,
            "Spend":                   spend / n,
            "14 Day Total Sales (₹)":  float(r["14 Day Total Sales (₹)"]) / n,
            "14 Day Total Units (#)":  float(r["14 Day Total Units (#)"]) / n,
            "Brand":                   brand,
        }
        folder_for_brand = brand.replace(" ", "_") if brand else brand_dir.name
        for asin in asins:
            rows_by_brand_folder.setdefault(folder_for_brand, []).append(
                {**template, "Advertised ASIN": asin}
            )

    # Each API file lives under ONE folder, but L0 may reassign to other
    # brand folders (e.g. Tonor campaigns inside Audio_Array).  Write SB
    # sheet to the canonical of the brand-folder this run produced rows for.
    # Common case: rows go back into the same brand_dir.
    canonical_target_rows = rows_by_brand_folder.get(brand_dir.name, [])
    if not canonical_target_rows:
        out["status"] = "no_rows_for_self"
        out["note"]   = f"all rows reattributed to other brands: {list(rows_by_brand_folder)}"
        # Still write empty SB sheet so step3 doesn't try to import old data
        df_sb = pd.DataFrame(columns=OUT_COLS)
    else:
        df_sb = pd.DataFrame(canonical_target_rows)
        df_sb = (df_sb.groupby(["Portfolio name", "Campaign Name",
                                "Advertised ASIN", "Brand"], as_index=False)
                      [["Impressions", "Clicks", "Spend",
                        "14 Day Total Sales (₹)", "14 Day Total Units (#)"]].sum())
        df_sb = df_sb[OUT_COLS]

    # Read existing sheets, add SB
    sheets_out = {}
    for s in existing_sheets:
        try:
            sheets_out[s] = pd.read_excel(canon_file, sheet_name=s)
        except Exception:
            pass
    sheets_out["SB"] = df_sb

    with pd.ExcelWriter(canon_file, engine="openpyxl", mode="w") as w:
        for name, sdf in sheets_out.items():
            sdf.to_excel(w, sheet_name=name, index=False)

    out["status"]   = "wrote_sb"
    out["sb_rows"]  = len(df_sb)
    out["spend"]    = float(df_sb["Spend"].sum()) if not df_sb.empty else 0.0
    out["mapped"]   = mapped
    out["unmapped"] = unmapped
    out["layers"]   = layer_count
    return out


def main():
    # Load master lookups once
    (asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
     cat_to_brand_asins, cats_sorted_vals) = build_master_lookups()
    campaign_asin_map = load_campaign_asin_map()
    synonyms = load_synonyms()
    print(f"Master ASINs={len(asin_to_meta)}  L0 entries={len(campaign_asin_map)}  "
          f"Synonyms enabled={len(synonyms)}")

    # Per-week active sets cached
    active_by_week: dict[int, set] = {}

    n_wrote = n_skip = 0
    grand_spend = 0.0
    for brand_dir in sorted(AMS_ROOT.iterdir()):
        if not brand_dir.is_dir() or brand_dir.name in ("processed_ads", "ams_weekly_fact"):
            continue
        for api_file in sorted(brand_dir.glob("ads_report_week*_api.xlsx")):
            m = re.search(r"week(\d+)_api", api_file.name)
            if not m: continue
            week = int(m.group(1))
            if week not in active_by_week:
                active_by_week[week] = build_active_set(week)
            res = run_for_week(
                brand_dir, week,
                asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
                cat_to_brand_asins, cats_sorted_vals, synonyms,
                campaign_asin_map, active_by_week[week],
            )
            if res["status"] == "wrote_sb":
                n_wrote += 1
                grand_spend += res["spend"]
                print(f"  ✅ {brand_dir.name:<16} W{week:<2}  rows={res['sb_rows']:>3}  "
                      f"spend=₹{res['spend']:>9,.0f}  mapped={res['mapped']}/{res['mapped']+res['unmapped']}  "
                      f"layers={res['layers']}")
            else:
                n_skip += 1
                print(f"  ⊘ {brand_dir.name:<16} W{week:<2}  {res['status']}  {res['note']}")
    print(f"\n✅ wrote SB for {n_wrote} (brand,week), skipped {n_skip}  ·  total SB spend=₹{grand_spend:,.0f}")


if __name__ == "__main__":
    main()
