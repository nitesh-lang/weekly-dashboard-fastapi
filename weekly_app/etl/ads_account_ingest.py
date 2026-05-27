"""
Ads-account ingestion — new entry point that replaces the operator's
old per-brand ads_report_weekN.xlsx uploads.

The operator now drops the raw Seller-Central Sponsored Products +
Sponsored Display reports straight from each ad account, organised as:

    data/ams_weekly_data/<Brand>/Week N/                     ← single-account brand
        Sponsored_Products_Advertised_product_report.xlsx
        Sponsored_Display_Advertised_product_report.xlsx
        Sponsored_Brands_Campaign_report.xlsx             (ignored — SB later)

    data/ams_weekly_data/<Brand>/Week N/<Account>/          ← multi-account brand
        Sponsored_Products_*.xlsx
        Sponsored_Display_*.xlsx
        Sponsored_Brands_*.xlsx                           (ignored)

Some accounts serve multiple brands (Audio Array + Tonor share one
ad account; both their ASINs appear in the same Sponsored_Products
file).  We resolve brand PER ROW via master ASIN lookup — folder name
is just an organisational hint, never trusted for brand attribution.

Output (one file per brand, matches the legacy schema so the rest of
the pipeline keeps working unchanged):

    data/ams_weekly_data/<Brand>/ads_report_weekN.xlsx

Columns:
    Portfolio name | Campaign Name | Advertised ASIN |
    Impressions | Clicks | Spend |
    14 Day Total Sales (₹) | 14 Day Total Units (#) | Brand

For weeks where the new Week-N subfolder is missing, this ETL is a
no-op for that week — the existing ads_report_weekN.xlsx stays in
place as a fallback so historical weeks keep working.

SB is intentionally NOT ingested here — operator will tackle that
separately later.
"""
from __future__ import annotations
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).resolve().parent.parent.parent
MASTER     = ROOT / "data" / "master" / "sku_master.xlsx"
AMS_ROOT   = ROOT / "data" / "ams_weekly_data"

SP_FILENAME = "Sponsored_Products_Advertised_product_report.xlsx"
SD_FILENAME = "Sponsored_Display_Advertised_product_report.xlsx"

# Map source filename → ad-type label so we can split the output into
# the two sheets ("SP", "SD") that step3_ads_aggregation expects.
AD_TYPE_BY_FILENAME = {
    SP_FILENAME: "SP",
    SD_FILENAME: "SD",
}

# Output columns mirror the legacy ads_report_weekN.xlsx so step3
# ads_aggregation can consume the file unchanged.
OUTPUT_COLS = [
    "Portfolio name", "Campaign Name", "Advertised ASIN",
    "Impressions", "Clicks", "Spend",
    "14 Day Total Sales (₹)", "14 Day Total Units (#)",
    "Brand",
]

# Campaign-name fallback when ASIN isn't in master (rare).
BRAND_KEYWORDS = {
    "audio array":    "Audio Array",
    "tonor":          "Tonor",
    "nexlev":         "Nexlev",
    "white mulberry": "White Mulberry",
    "fossil":         "Fossil",
}


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Master ASIN → Brand lookup ──────────────────────────────────────────
def build_asin_brand_map() -> dict[str, str]:
    if not MASTER.exists():
        return {}
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()
    out: dict[str, str] = {}
    for _, r in m.iterrows():
        brand = _norm(r.get("Brand"))
        if not brand:
            continue
        primary = _norm(r.get("ASIN"))
        if primary:
            out[primary] = brand
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x and x not in out:
                    out[x] = brand
    return out


def brand_from_campaign(campaign: str) -> str:
    s = (campaign or "").lower()
    for kw, brand in BRAND_KEYWORDS.items():
        if kw in s:
            return brand
    return ""


def resolve_brand(asin: str, campaign: str, asin_map: dict[str, str]) -> str:
    if asin and asin in asin_map:
        return asin_map[asin]
    fb = brand_from_campaign(campaign)
    if fb:
        return fb
    return "(unmapped)"


# ── Discover Week-N source files across all brand folders ──────────────
def find_week_sources(week_num: int) -> list[Path]:
    """Returns the list of SP + SD .xlsx files for the given week
    across every brand sub-tree.  Recursive so multi-account layouts
    (e.g. White_Mulberry/Week N/CRPL + WM) all get picked up."""
    found: list[Path] = []
    folder_name = f"Week {week_num}"
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        wk = brand_dir / folder_name
        if not wk.exists():
            continue
        # Recursive — handles both flat (files directly in Week N/) and
        # nested (Week N/<Account>/files…) layouts.
        for p in wk.rglob(SP_FILENAME):
            if not p.name.startswith("~"):
                found.append(p)
        for p in wk.rglob(SD_FILENAME):
            if not p.name.startswith("~"):
                found.append(p)
    return found


# ── Read one SP / SD file → aggregated rows ────────────────────────────
def read_and_aggregate(path: Path, asin_map: dict[str, str]) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()

    # Coerce required columns (fill missing with safe defaults)
    keep_str = ["Portfolio name", "Campaign Name", "Advertised ASIN"]
    keep_num = ["Impressions", "Clicks", "Spend",
                "14 Day Total Sales (₹)", "14 Day Total Units (#)"]
    for c in keep_str:
        if c not in df.columns:
            df[c] = ""
    for c in keep_num:
        if c not in df.columns:
            df[c] = 0

    df["Portfolio name"]  = df["Portfolio name"].map(_norm)
    df["Campaign Name"]   = df["Campaign Name"].map(_norm)
    df["Advertised ASIN"] = df["Advertised ASIN"].map(_norm)
    for c in keep_num:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Per-row brand resolution via master ASIN (fallback to campaign keyword)
    df["Brand"] = df.apply(
        lambda r: resolve_brand(r["Advertised ASIN"], r["Campaign Name"], asin_map),
        axis=1,
    )

    # Aggregate (Portfolio, Campaign, ASIN, Brand) → sum numerics
    agg = (df.groupby(
                ["Portfolio name", "Campaign Name", "Advertised ASIN", "Brand"],
                as_index=False)
             [keep_num].sum())
    return agg


# ── Main ETL entry point ────────────────────────────────────────────────
def run_for_week(week_num: int) -> dict:
    """Returns a summary dict — caller can print or log."""
    sources = find_week_sources(week_num)
    if not sources:
        return {"week": week_num, "sources": 0, "skipped": "no Week-N subfolder"}

    asin_map = build_asin_brand_map()
    # Keep SP and SD as separate frames — output writes one sheet each
    # so step3_ads_aggregation finds the "SP" / "SD" sheet names it
    # already expects.
    frames_by_ad_type: dict[str, list[pd.DataFrame]] = {"SP": [], "SD": []}
    for p in sources:
        ad_type = AD_TYPE_BY_FILENAME.get(p.name)
        if not ad_type:
            continue
        try:
            agg = read_and_aggregate(p, asin_map)
            frames_by_ad_type[ad_type].append(agg)
            print(f"   📥 {p.relative_to(AMS_ROOT)}  →  {len(agg):,} rows  [{ad_type}]")
        except Exception as e:
            print(f"   ⚠ {p.relative_to(AMS_ROOT)} failed: {e}")

    if not any(frames_by_ad_type.values()):
        return {"week": week_num, "sources": len(sources), "skipped": "all reads failed"}

    keep_num = ["Impressions", "Clicks", "Spend",
                "14 Day Total Sales (₹)", "14 Day Total Units (#)"]

    # Per ad-type: re-aggregate across multiple source files (e.g. WM has
    # CRPL/SP + WM/SP → both feed the "SP" sheet) and split per brand.
    final_by_ad_type: dict[str, pd.DataFrame] = {}
    for ad_type, frames in frames_by_ad_type.items():
        if not frames:
            final_by_ad_type[ad_type] = pd.DataFrame(columns=OUTPUT_COLS)
            continue
        combined = pd.concat(frames, ignore_index=True)
        final = (combined.groupby(
                    ["Portfolio name", "Campaign Name", "Advertised ASIN", "Brand"],
                    as_index=False)
                      [keep_num].sum())
        final_by_ad_type[ad_type] = final[OUTPUT_COLS]

    all_brands = (
        set(final_by_ad_type["SP"]["Brand"]) |
        set(final_by_ad_type["SD"]["Brand"])
    )

    summary = {"week": week_num, "sources": len(sources),
               "rows": sum(len(f) for f in final_by_ad_type.values()),
               "per_brand": {}, "skipped": None}

    for brand in sorted(all_brands):
        sp_sub = final_by_ad_type["SP"]
        sd_sub = final_by_ad_type["SD"]
        sp_sub = sp_sub[sp_sub["Brand"] == brand].copy()
        sd_sub = sd_sub[sd_sub["Brand"] == brand].copy()

        if brand == "(unmapped)":
            out = AMS_ROOT / f"ads_report_week{week_num}_unmapped.xlsx"
        else:
            folder = AMS_ROOT / brand.replace(" ", "_")
            folder.mkdir(parents=True, exist_ok=True)
            out = folder / f"ads_report_week{week_num}.xlsx"

        # Backup existing file before overwriting
        if out.exists():
            bak = out.with_suffix(out.suffix + ".bak")
            if not bak.exists():
                shutil.copy2(out, bak)

        # Write 2-sheet workbook: "SP" + "SD".  Sheet names match what
        # step3_ads_aggregation reads with sheet_name="SP" / "SD".
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            sp_sub.to_excel(w, sheet_name="SP", index=False)
            sd_sub.to_excel(w, sheet_name="SD", index=False)

        total_rows  = len(sp_sub) + len(sd_sub)
        total_spend = float(sp_sub["Spend"].sum() + sd_sub["Spend"].sum())
        total_sales = float(sp_sub["14 Day Total Sales (₹)"].sum()
                          + sd_sub["14 Day Total Sales (₹)"].sum())
        summary["per_brand"][brand] = {
            "rows":   total_rows,
            "sp_rows": len(sp_sub),
            "sd_rows": len(sd_sub),
            "spend":  total_spend,
            "sales":  total_sales,
            "file":   str(out.relative_to(ROOT)),
        }
        print(f"   ✅ {brand:<18}  SP={len(sp_sub):>4}  SD={len(sd_sub):>4}  "
              f"→ {out.relative_to(ROOT)}")

    return summary


def main() -> None:
    # Discover every Week N subfolder that exists anywhere under AMS_ROOT.
    weeks: set[int] = set()
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        for wk_dir in brand_dir.iterdir():
            if wk_dir.is_dir() and wk_dir.name.startswith("Week "):
                try:
                    weeks.add(int(wk_dir.name.replace("Week", "").strip()))
                except Exception:
                    pass

    if not weeks:
        print("⚠ No Week-N subfolders found under data/ams_weekly_data/<Brand>/")
        print("   Drop new ads files at e.g. data/ams_weekly_data/Audio_Array/Week 21/")
        print("   (or .../Week 21/<Account>/ for multi-account brands).")
        return

    for wk in sorted(weeks):
        print(f"\n━━━ Week {wk} ━━━")
        summary = run_for_week(wk)
        if summary.get("skipped"):
            print(f"   ⏭  {summary['skipped']}")
            continue
        print()
        print(f"   Total rows aggregated: {summary['rows']:,}")
        for brand, info in summary["per_brand"].items():
            print(f"     {brand:<18}  rows={info['rows']:>4}  "
                  f"spend=₹{info['spend']:>11,.0f}  sales=₹{info['sales']:>11,.0f}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import sys, traceback
        traceback.print_exc()
        sys.exit(1)
