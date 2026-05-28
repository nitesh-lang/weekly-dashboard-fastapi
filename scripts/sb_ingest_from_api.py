"""
SB ingest from API data — sales-weighted distribution.

For each (brand, week) where the operator hasn't exported the SB campaign
report, this script reads the API-pulled SB data and distributes per-ad
spend to advertised ASINs.

Distribution methodology (per ad group):
  1. SALES-WEIGHTED  — when the SB Purchased-Product report has new-to-
                       brand sales for one or more purchasedAsins in this
                       ad group, distribute spend by sales weight.
                       (NTB sales correlate strongly with total sales and
                       are the only ASIN-level metric Amazon exposes for SB.)
  2. EVEN-SPLIT (L0) — when no NTB sales were attributed, fall back to an
                       even split across the campaign's known creative
                       ASINs (from sb_campaign_asins.json).
  3. EVEN-SPLIT (L1-L4) — if the L0 map has no entry for this campaign,
                       fall back to the heuristic attribution layers
                       (ASIN-in-name, model-in-name, category-in-name).

Source:  data/ams_weekly_data/<Brand>/ads_report_week<N>_api.xlsx
         (sheet 'SB' = per-ad cost; sheet 'SB_PURCH' = per-(adGroup, ASIN) NTB sales)
Target:  data/ams_weekly_data/<Brand>/ads_report_week<N>.xlsx (sheet 'SB')

Skips weeks where the canonical file already has an SB sheet written by
the operator-Excel flow — that's the verified ground truth.

Run-end summary breaks spend down by methodology so the operator can see
attribution quality at a glance.
"""
from __future__ import annotations
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from weekly_app.etl.sb_ingest import (
    build_master_lookups, build_active_set,
    load_campaign_asin_map, load_synonyms,
    attribute_campaign, OUT_COLS, FOLDER_TO_BRAND, SB_FILENAME,
)

AMS_ROOT = ROOT / "data" / "ams_weekly_data"


def has_operator_sb_export(brand_dir: Path, week: int) -> bool:
    """Returns True iff there's an operator-exported SB campaign report
    for this (brand, week) — that's the authoritative source so we skip
    API attribution there."""
    wk_dir = brand_dir / f"Week {week}"
    if not wk_dir.exists():
        return False
    return any(p.name == SB_FILENAME and not p.name.startswith("~")
               for p in wk_dir.rglob(SB_FILENAME))


def aggregate_to_adgroup(sb_df: pd.DataFrame) -> pd.DataFrame:
    """API SB sheet: per-(campaign × adGroup × ad) rows.  Sum to adGroup
    level so we can join with SB_PURCH (which is per-(adGroup × ASIN))."""
    if sb_df.empty:
        return pd.DataFrame()
    for c in ("Spend", "Impressions", "Clicks", "ams_orders",
              "attributed_sales", "units_sold"):
        if c not in sb_df.columns:
            sb_df[c] = 0
        sb_df[c] = pd.to_numeric(sb_df[c], errors="coerce").fillna(0)
    keys = ["campaignName", "campaignId", "adGroupName", "adGroupId"]
    return (sb_df.groupby(keys, as_index=False)
                  .agg({"Spend":"sum","Impressions":"sum","Clicks":"sum",
                        "ams_orders":"sum","attributed_sales":"sum",
                        "units_sold":"sum"}))


def build_adgroup_sales_map(purch_df: pd.DataFrame) -> dict:
    """{adGroupId: {asin: ntb_sales}}  — only ASINs with positive NTB sales."""
    if purch_df.empty or "asin" not in purch_df.columns:
        return {}
    sales_col = next((c for c in ("purch_sales", "newToBrandSales14d") if c in purch_df.columns), None)
    if not sales_col:
        return {}
    purch_df[sales_col] = pd.to_numeric(purch_df[sales_col], errors="coerce").fillna(0)
    grp = (purch_df.groupby(["adGroupId", "asin"], as_index=False)[sales_col].sum())
    out: dict[str, dict] = {}
    for _, r in grp.iterrows():
        v = float(r[sales_col])
        if v <= 0:
            continue
        out.setdefault(r["adGroupId"], {})[r["asin"]] = v
    return out


def run_for_week(brand_dir, week,
                 asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
                 cat_to_brand_asins, cats_sorted_vals, synonyms,
                 campaign_asin_map, active):
    api_file   = brand_dir / f"ads_report_week{week}_api.xlsx"
    canon_file = brand_dir / f"ads_report_week{week}.xlsx"
    out = {"brand": brand_dir.name, "week": week, "status": "skip", "note": ""}
    if not api_file.exists():
        out["note"] = "no api file"; return out
    if not canon_file.exists():
        out["note"] = "no canonical file"; return out
    try:
        existing_sheets = pd.ExcelFile(canon_file).sheet_names
    except Exception as e:
        out["note"] = f"cant open canonical: {e}"; return out
    # Only skip when there's an OPERATOR-EXPORTED SB file for this week —
    # that's ground truth.  Otherwise overwrite the API-derived SB sheet
    # so methodology updates take effect on re-run.
    if has_operator_sb_export(brand_dir, week):
        out["status"] = "skip"; out["note"] = "operator-exported SB report exists; preserving it"
        return out

    api_x = pd.ExcelFile(api_file)
    if "SB" not in api_x.sheet_names:
        out["note"] = "no SB sheet in api file"; return out
    sb_ads   = pd.read_excel(api_file, sheet_name="SB")
    sb_purch = (pd.read_excel(api_file, sheet_name="SB_PURCH")
                if "SB_PURCH" in api_x.sheet_names else pd.DataFrame())

    adgroups = aggregate_to_adgroup(sb_ads)
    if adgroups.empty:
        out["note"] = "no SB rows in api file"; return out
    adgroup_sales = build_adgroup_sales_map(sb_purch)

    file_brand = FOLDER_TO_BRAND.get(brand_dir.name, brand_dir.name)
    rows_by_brand_folder: dict[str, list[dict]] = {}
    methodology_spend = defaultdict(float)
    methodology_count = defaultdict(int)

    for _, ag in adgroups.iterrows():
        ag_spend = float(ag["Spend"])
        if ag_spend <= 0:
            continue
        ag_id    = ag["adGroupId"]
        campaign = str(ag["campaignName"]).strip()

        # Aggregate metrics for this adGroup
        ag_imp   = float(ag["Impressions"])
        ag_clk   = float(ag["Clicks"])
        ag_ord   = float(ag["ams_orders"])
        ag_sales = float(ag["attributed_sales"])
        ag_units = float(ag["units_sold"])

        sales_by_asin = adgroup_sales.get(ag_id, {})

        # Method 1 — Sales-weighted (only when this adGroup has NTB sales)
        if sales_by_asin:
            total_sales = sum(sales_by_asin.values())
            # Resolve brand for these ASINs via master (fall back to file brand)
            first_asin = next(iter(sales_by_asin))
            brand = (asin_to_meta.get(first_asin, {}).get("brand") or file_brand)
            folder_for_brand = brand.replace(" ", "_") if brand else brand_dir.name

            for asin, sales_w in sales_by_asin.items():
                w = sales_w / total_sales
                row = {
                    "Portfolio name":  "",
                    "Campaign Name":   campaign,
                    "Advertised ASIN": asin,
                    "Impressions":              ag_imp   * w,
                    "Clicks":                   ag_clk   * w,
                    "Spend":                    ag_spend * w,
                    "14 Day Total Sales (₹)":   ag_sales * w,
                    "14 Day Total Units (#)":   ag_units * w,
                    "Brand":                    brand,
                }
                rows_by_brand_folder.setdefault(folder_for_brand, []).append(row)
            methodology_spend["sales_weighted"] += ag_spend
            methodology_count["sales_weighted"] += 1
            continue

        # Method 2 — Even split via L0/L1/L2/L3/L4 (no NTB sales for this adGroup)
        layer, asins, brand = attribute_campaign(
            campaign, file_brand,
            asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
            cat_to_brand_asins, cats_sorted_vals, synonyms, active,
            campaign_asin_map=campaign_asin_map,
        )
        if not asins:
            methodology_spend["unmapped"] += ag_spend
            methodology_count["unmapped"] += 1
            continue
        n = len(asins)
        folder_for_brand = brand.replace(" ", "_") if brand else brand_dir.name
        for asin in asins:
            row = {
                "Portfolio name":  "",
                "Campaign Name":   campaign,
                "Advertised ASIN": asin,
                "Impressions":              ag_imp   / n,
                "Clicks":                   ag_clk   / n,
                "Spend":                    ag_spend / n,
                "14 Day Total Sales (₹)":   ag_sales / n,
                "14 Day Total Units (#)":   ag_units / n,
                "Brand":                    brand,
            }
            rows_by_brand_folder.setdefault(folder_for_brand, []).append(row)

        # Bucket by L-layer for the methodology summary
        base_layer = ("L0_even_split"  if layer.startswith("L0_") else
                      "L1_even_split"  if layer.startswith("L1_") else
                      "L2_even_split"  if layer.startswith("L2_") else
                      "L3_even_split"  if layer.startswith("L3_") else
                      "L4_even_split"  if layer.startswith("L4_") else
                      "unmapped")
        methodology_spend[base_layer] += ag_spend
        methodology_count[base_layer] += 1

    # Write SB sheet to canonical file
    target_rows = rows_by_brand_folder.get(brand_dir.name, [])
    if not target_rows:
        df_sb = pd.DataFrame(columns=OUT_COLS)
    else:
        df_sb = pd.DataFrame(target_rows)
        df_sb = (df_sb.groupby(["Portfolio name", "Campaign Name",
                                "Advertised ASIN", "Brand"], as_index=False)
                       [["Impressions","Clicks","Spend",
                         "14 Day Total Sales (₹)","14 Day Total Units (#)"]].sum())
        # Impressions / Clicks / Units are physical event counts — round to
        # integers so the UI doesn't display "417.23 impressions".
        # Spend and Sales stay float (rupee amounts can have paise).
        for c in ("Impressions", "Clicks", "14 Day Total Units (#)"):
            df_sb[c] = df_sb[c].round().astype(int)
        df_sb = df_sb[OUT_COLS]

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

    out["status"]              = "wrote_sb"
    out["sb_rows"]             = len(df_sb)
    out["spend"]               = float(df_sb["Spend"].sum()) if not df_sb.empty else 0.0
    out["methodology_spend"]   = dict(methodology_spend)
    out["methodology_count"]   = dict(methodology_count)
    return out


def main():
    (asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
     cat_to_brand_asins, cats_sorted_vals) = build_master_lookups()
    campaign_asin_map = load_campaign_asin_map()
    synonyms = load_synonyms()
    print(f"Master ASINs={len(asin_to_meta):,}  L0 entries={len(campaign_asin_map)}  "
          f"Synonyms enabled={len(synonyms)}")

    active_by_week: dict[int, set] = {}
    totals = defaultdict(float)
    counts = defaultdict(int)
    n_wrote = n_skip = 0

    for brand_dir in sorted(AMS_ROOT.iterdir()):
        if not brand_dir.is_dir() or brand_dir.name in ("processed_ads", "ams_weekly_fact"):
            continue
        for api_file in sorted(brand_dir.glob("ads_report_week*_api.xlsx")):
            m = re.search(r"week(\d+)_api", api_file.name)
            if not m:
                continue
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
                m_spend = res.get("methodology_spend", {})
                m_count = res.get("methodology_count", {})
                sw  = m_spend.get("sales_weighted", 0)
                tot = sum(m_spend.values()) or 1
                sw_pct = sw / tot * 100
                print(f"  ✅ {brand_dir.name:<16} W{week:<2}  rows={res['sb_rows']:>3}  "
                      f"spend=₹{res['spend']:>9,.0f}  "
                      f"sales-wt={sw_pct:>5.1f}%  "
                      f"{ {k:int(v) for k,v in m_count.items()} }")
                for k, v in m_spend.items():
                    totals[k] += v
                for k, v in m_count.items():
                    counts[k] += v
            else:
                n_skip += 1
                print(f"  ⊘ {brand_dir.name:<16} W{week:<2}  {res['status']}  {res['note']}")

    print()
    print(f"━━━ Methodology summary across {n_wrote} (brand × week) runs ━━━")
    grand = sum(totals.values()) or 1
    for k in ("sales_weighted", "L0_even_split", "L1_even_split", "L2_even_split",
              "L3_even_split", "L4_even_split", "unmapped"):
        if k not in totals:
            continue
        spend = totals[k]
        pct = spend / grand * 100
        print(f"  {k:<18}  {counts[k]:>5} adGroups  ₹{spend:>11,.0f}  ({pct:>5.1f}%)")
    print(f"\n  TOTAL SB spend distributed: ₹{grand:,.0f}")
    print(f"\n  Skipped (operator-exported or out-of-window): {n_skip}")


if __name__ == "__main__":
    main()
