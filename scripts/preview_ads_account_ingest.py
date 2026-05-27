"""
PREVIEW (read-only) for the new ads-account ingestion.

Walks one account folder (e.g. `Audio Array_Tonor/`), reads the three
Seller-Central exports inside:

    Sponsored_Products_Advertised_product_report.xlsx
    Sponsored_Display_Advertised_product_report.xlsx
    Sponsored_Brands_Campaign_report.xlsx

…and shows what the production ETL will emit.

Brand resolution rule (per operator):
  1. Look up Advertised ASIN in sku_master.ASIN  → use master's Brand
  2. If ASIN is in master's Variation ASINs list → use that row's Brand
  3. Fallback: scan Campaign Name for brand keywords
     (audio array / tonor / nexlev / white mulberry)
  4. Else: "(unmapped)"

Output schema mirrors the legacy ads_report_weekN.xlsx (so step3
ads aggregation can consume it unchanged):

  Portfolio name | Campaign Name | Advertised ASIN |
  Impressions | Clicks | Spend |
  14 Day Total Sales (₹) | 14 Day Total Units (#) | Brand

SP + SD are concatenated into one stream (operator-confirmed they
share an account view).  SB is processed separately at campaign
level (no Advertised ASIN column in SB report).

Usage:
    python scripts/preview_ads_account_ingest.py \
        "C:/Users/Admin/Downloads/Test/Audio Array_Tonor"
"""
from __future__ import annotations
import sys
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"

# Brand keywords for the campaign-name fallback (lower-case match)
BRAND_KEYWORDS = {
    "audio array":    "Audio Array",
    "tonor":          "Tonor",
    "nexlev":         "Nexlev",
    "white mulberry": "White Mulberry",
    "fossil":         "Fossil",
}


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Master ASIN → brand map (incl. variations) ──────────────────────────
def build_asin_brand_map() -> dict[str, str]:
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


# ── Per-file readers + aggregators ──────────────────────────────────────
def aggregate_sp_sd(df: pd.DataFrame, source_label: str, asin_map: dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    keep = ["Portfolio name", "Campaign Name", "Advertised ASIN",
            "Impressions", "Clicks", "Spend",
            "14 Day Total Sales (₹)", "14 Day Total Units (#)"]
    for c in keep:
        if c not in df.columns:
            df[c] = 0 if c not in ("Portfolio name", "Campaign Name", "Advertised ASIN") else ""
    df = df[keep]

    # Brand resolution per row
    df["Advertised ASIN"] = df["Advertised ASIN"].map(_norm)
    df["Brand"] = df.apply(
        lambda r: resolve_brand(r["Advertised ASIN"], _norm(r["Campaign Name"]), asin_map),
        axis=1,
    )

    # Coerce numerics
    for c in ("Impressions", "Clicks", "Spend",
              "14 Day Total Sales (₹)", "14 Day Total Units (#)"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Aggregate to (Portfolio, Campaign, ASIN, Brand) — same grain as the
    # legacy ads_report_weekN.xlsx.  Daily rows roll up to a single line.
    agg = (df.groupby(
                ["Portfolio name", "Campaign Name", "Advertised ASIN", "Brand"],
                as_index=False)
             .agg({"Impressions":          "sum",
                   "Clicks":               "sum",
                   "Spend":                "sum",
                   "14 Day Total Sales (₹)":  "sum",
                   "14 Day Total Units (#)":  "sum"}))
    agg["_source"] = source_label
    return agg


def aggregate_sb(df: pd.DataFrame, asin_map: dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    keep = ["Portfolio name", "Campaign Name",
            "Impressions", "Clicks", "Spend",
            "14 Day Total Sales (₹)", "14 Day Total Units (#)"]
    for c in keep:
        if c not in df.columns:
            df[c] = 0 if c not in ("Portfolio name", "Campaign Name") else ""
    df = df[keep]

    df["Advertised ASIN"] = ""   # SB is campaign-level, no ASIN
    df["Brand"] = df["Campaign Name"].map(brand_from_campaign).replace("", "(unmapped)")

    for c in ("Impressions", "Clicks", "Spend",
              "14 Day Total Sales (₹)", "14 Day Total Units (#)"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return (df.groupby(["Portfolio name", "Campaign Name", "Brand"], as_index=False)
              .agg({"Impressions":          "sum",
                    "Clicks":               "sum",
                    "Spend":                "sum",
                    "14 Day Total Sales (₹)":  "sum",
                    "14 Day Total Units (#)":  "sum"}))


# ── Main ────────────────────────────────────────────────────────────────
def main(account_dir: Path) -> None:
    if not account_dir.exists():
        print(f"⚠ {account_dir} not found")
        return

    print(f"📥 Account folder: {account_dir.name}")
    asin_map = build_asin_brand_map()
    print(f"   Master ASIN→Brand map: {len(asin_map):,} entries\n")

    sp_path = account_dir / "Sponsored_Products_Advertised_product_report.xlsx"
    sd_path = account_dir / "Sponsored_Display_Advertised_product_report.xlsx"
    sb_path = account_dir / "Sponsored_Brands_Campaign_report.xlsx"

    sp_sd_frames = []
    for label, path in [("SP", sp_path), ("SD", sd_path)]:
        if path.exists():
            df = pd.read_excel(path)
            print(f"   {label}: {len(df):,} raw rows in {path.name}")
            agg = aggregate_sp_sd(df, label, asin_map)
            print(f"        → {len(agg):,} aggregated rows")
            sp_sd_frames.append(agg)
        else:
            print(f"   {label}: file not found")

    combined = (pd.concat(sp_sd_frames, ignore_index=True)
                if sp_sd_frames else pd.DataFrame())

    if combined.empty:
        print("   (no SP/SD data)")
    else:
        print("\n   Brand split (SP + SD):")
        split = (combined.groupby("Brand", as_index=False)
                          .agg(rows=("Advertised ASIN", "count"),
                               spend=("Spend", "sum"),
                               sales=("14 Day Total Sales (₹)", "sum")))
        for _, r in split.iterrows():
            print(f"     {r['Brand']:<18}  rows={int(r['rows']):>4}  "
                  f"spend=₹{r['spend']:>11,.0f}  sales=₹{r['sales']:>11,.0f}")

    if sb_path.exists():
        sb = pd.read_excel(sb_path)
        print(f"\n   SB: {len(sb):,} raw rows in {sb_path.name}")
        sb_agg = aggregate_sb(sb, asin_map)
        print(f"        → {len(sb_agg):,} aggregated rows")
        print("\n   Brand split (SB, by campaign name keyword):")
        for _, r in sb_agg.groupby("Brand", as_index=False).agg(
                rows=("Campaign Name", "count"),
                spend=("Spend", "sum"),
                sales=("14 Day Total Sales (₹)", "sum")).iterrows():
            print(f"     {r['Brand']:<18}  rows={int(r['rows']):>4}  "
                  f"spend=₹{r['spend']:>11,.0f}  sales=₹{r['sales']:>11,.0f}")
    else:
        print("\n   SB: file not found")

    # Sanity: list any (unmapped) SP/SD rows
    if not combined.empty:
        orphans = combined[combined["Brand"] == "(unmapped)"]
        if not orphans.empty:
            print(f"\n   ⚠ {len(orphans)} unmapped SP/SD rows — ASINs:")
            for asin in sorted(orphans["Advertised ASIN"].dropna().unique())[:10]:
                print(f"     {asin}")


if __name__ == "__main__":
    target = (Path(sys.argv[1]) if len(sys.argv) > 1
              else Path(r"C:/Users/Admin/Downloads/Test/Audio Array_Tonor"))
    main(target)
