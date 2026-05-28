"""
Sponsored Brands (SB) ingestion — produces ASIN-level attribution
rows for SB campaigns using a three-layer rule:

  L1 — ASIN appears in campaign name           → that ASIN (active only)
  L2 — Model appears in campaign name          → active ASINs of that
                                                   model (even split)
  L3 — Category appears in campaign name       → active ASINs of that
                                                   brand × category
                                                   (even split)
  L4 — Synonym map (data/master/sb_synonyms.json) DISABLED by default.
        Flip _enabled=true in the JSON when operator confirms each
        entry's mapping.  Until then unmapped campaigns are written
        to a side report.

"Active ASIN" = had sessions > 0 OR units_ordered > 0 in this week's
business_report (with a 4-week fallback so paused-this-week-but-
selling-recently ASINs still get attribution).

Output:
  • An "SB" sheet ADDED to each brand's ads_report_weekN.xlsx,
    matching the SP/SD column schema so step3_ads_aggregation picks
    it up as a third ad type.
  • Unmapped campaigns written to
    data/processed/sb_unmapped_weekN.csv — operator visibility for
    the 5-or-so catch-all campaigns that can't be attributed yet.
"""
from __future__ import annotations
import json
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent.parent
MASTER   = ROOT / "data" / "master" / "sku_master.xlsx"
AMS_ROOT = ROOT / "data" / "ams_weekly_data"
PROCESSED_DIR = ROOT / "data" / "processed"
SYNONYMS_FILE = ROOT / "data" / "master" / "sb_synonyms.json"

SB_FILENAME = "Sponsored_Brands_Campaign_report.xlsx"
ASIN_RE     = re.compile(r"\bB0[A-Z0-9]{8}\b")
ACTIVE_WINDOW_WEEKS = 4   # current week + 3 prior

# Layer 0 — campaign → ASIN map sourced from Amazon Ads API
# (built by weekly_app/etl/sb_ads_api_pull.py).  Most authoritative
# layer: bypasses all heuristic layers below when a hit is found.
CAMPAIGN_ASINS_FILE = ROOT / "data" / "processed" / "sb_campaign_asins.json"

# Output column shape mirrors the SP/SD sheets so step3 can aggregate
# SB through the same code path.
OUT_COLS = [
    "Portfolio name", "Campaign Name", "Advertised ASIN",
    "Impressions", "Clicks", "Spend",
    "14 Day Total Sales (₹)", "14 Day Total Units (#)",
    "Brand",
]

FOLDER_TO_BRAND = {
    "Audio_Array":    "Audio Array",
    "Nexlev":         "Nexlev",
    "Tonor":          "Tonor",
    "White_Mulberry": "White Mulberry",
    "Fossil":         "Fossil",
}


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ─────────────────────────────────────────────────────────────────────
# Master + active lookups
# ─────────────────────────────────────────────────────────────────────
def build_master_lookups():
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()

    asin_to_meta: dict[str, dict] = {}
    rows_by_brand: dict[str, list[dict]] = {}
    for _, r in m.iterrows():
        brand = _norm(r.get("Brand"))
        if not brand:
            continue
        rec = {
            "brand": brand,
            "model": _norm(r.get("Model")),
            "cat0":  _norm(r.get("category_l0")),
            "cat1":  _norm(r.get("category_l1")),
            "cat2":  _norm(r.get("category_l2")),
        }
        primary = _norm(r.get("ASIN"))
        if primary:
            asin_to_meta[primary] = rec
            rows_by_brand.setdefault(brand, []).append({"asin": primary, **rec})
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x and x not in asin_to_meta:
                    asin_to_meta[x] = rec
                    rows_by_brand.setdefault(brand, []).append({"asin": x, **rec})

    model_to_asins: dict[str, list[str]] = {}
    for asin, rec in asin_to_meta.items():
        if rec["model"]:
            model_to_asins.setdefault(rec["model"].lower(), []).append(asin)

    models_sorted = sorted(
        {rec["model"] for rec in asin_to_meta.values() if rec["model"]},
        key=lambda s: (-len(s), s),
    )

    cat_to_brand_asins: dict[str, dict[str, list[str]]] = {}
    cat_values = set()
    for asin, rec in asin_to_meta.items():
        for val in (rec["cat0"], rec["cat1"], rec["cat2"]):
            if val:
                cat_to_brand_asins.setdefault(val.lower(), {}).setdefault(rec["brand"], []).append(asin)
                cat_values.add(val)
    cats_sorted_vals = sorted(cat_values, key=lambda s: (-len(s), s))
    return asin_to_meta, rows_by_brand, model_to_asins, models_sorted, cat_to_brand_asins, cats_sorted_vals


def build_active_set(week_num: int) -> set[str]:
    active: set[str] = set()
    weeks_to_try = [week_num] + [week_num - i for i in range(1, ACTIVE_WINDOW_WEEKS)]
    for wk in weeks_to_try:
        wk_active: set[str] = set()
        for brand_dir in AMS_ROOT.iterdir():
            if not brand_dir.is_dir():
                continue
            biz = brand_dir / f"business_report_week{wk}.xlsx"
            if not biz.exists():
                continue
            try:
                df = pd.read_excel(biz)
            except Exception:
                continue
            df.columns = df.columns.str.strip()
            asin_col = next((c for c in ("(Child) ASIN", "asin", "ASIN") if c in df.columns), None)
            if not asin_col:
                continue
            df[asin_col] = df[asin_col].map(_norm)
            sessions = pd.to_numeric(df.get("Sessions - Total", 0), errors="coerce").fillna(0)
            units = pd.to_numeric(df.get("units_ordered", 0), errors="coerce").fillna(0)
            mask = (sessions > 0) | (units > 0)
            wk_active.update(df.loc[mask, asin_col].dropna())
        # If current week has activity, that's our set; else accumulate fallback
        if wk == week_num and wk_active:
            return wk_active
        active.update(wk_active)
    return active


def load_campaign_asin_map() -> dict[str, dict]:
    """Reads data/processed/sb_campaign_asins.json (produced by
    sb_ads_api_pull.py).  Returns {campaign_name_lower: {brand, asins}}.
    Missing file → empty dict (Layer 0 simply doesn't fire)."""
    if not CAMPAIGN_ASINS_FILE.exists():
        return {}
    try:
        with open(CAMPAIGN_ASINS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for name, rec in data.items():
        if isinstance(rec, dict) and rec.get("asins"):
            out[name.strip().lower()] = rec
    return out


def load_synonyms() -> dict:
    """Loads sb_synonyms.json.  Each entry is only ACTIVE if it carries
    `_enabled: true` — operator must explicitly opt in per keyword."""
    if not SYNONYMS_FILE.exists():
        return {}
    try:
        with open(SYNONYMS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for k, v in data.items():
        if k.startswith("_"):
            continue
        if isinstance(v, dict) and v.get("_enabled") is True:
            out[k.lower()] = v
    return out


def find_sb_files(week_num: int) -> list[Path]:
    found = []
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        wk = brand_dir / f"Week {week_num}"
        if not wk.exists():
            continue
        for p in wk.rglob(SB_FILENAME):
            if not p.name.startswith("~"):
                found.append(p)
    return found


def resolve_synonym(entry: dict, rows_by_brand, active: set) -> list[str]:
    brand = entry.get("brand")
    if not brand:
        return []
    candidates = rows_by_brand.get(brand, [])
    matched: list[str] = []
    if "category_l2" in entry:
        wanted = entry["category_l2"]; wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wlo = {w.lower() for w in wanted}
        for row in candidates:
            if row["cat2"].lower() in wlo:
                matched.append(row["asin"])
    elif "category_l1" in entry:
        wanted = entry["category_l1"]; wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wlo = {w.lower() for w in wanted}
        for row in candidates:
            if row["cat1"].lower() in wlo:
                matched.append(row["asin"])
    elif "category_l1_contains" in entry:
        wanted = entry["category_l1_contains"]; wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wlo = [w.lower() for w in wanted]
        for row in candidates:
            cl = row["cat1"].lower()
            if any(w in cl for w in wlo):
                matched.append(row["asin"])
    elif "model_regex" in entry:
        try:
            pat = re.compile(entry["model_regex"], re.IGNORECASE)
        except Exception:
            return []
        for row in candidates:
            if row["model"] and pat.search(row["model"]):
                matched.append(row["asin"])

    seen, uniq = set(), []
    for a in matched:
        if a in seen: continue
        seen.add(a); uniq.append(a)
    return [a for a in uniq if a in active]


# ─────────────────────────────────────────────────────────────────────
# Per-campaign attribution
# ─────────────────────────────────────────────────────────────────────
def attribute_campaign(
    campaign: str, file_brand: str,
    asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
    cat_to_brand_asins, cats_sorted_vals, synonyms, active,
    campaign_asin_map: dict | None = None,
) -> tuple[str, list[str], str]:
    """Returns (layer, [asin, …], brand-or-empty).  Caller distributes
    metrics evenly across the returned ASINs."""
    if not campaign:
        return ("unmapped", [], file_brand)
    cam_upper = campaign.upper()

    # L0 — Amazon Ads API authoritative lookup
    if campaign_asin_map:
        hit = campaign_asin_map.get(campaign.strip().lower())
        if hit and hit.get("asins"):
            asins = hit["asins"]
            active_hits = [a for a in asins if a in active] or asins
            brand = hit.get("brand") or (asin_to_meta[active_hits[0]]["brand"]
                                          if active_hits[0] in asin_to_meta else file_brand)
            return ("L0_ads_api", active_hits, brand)

    # L1 — ASIN in name
    matches = ASIN_RE.findall(cam_upper)
    in_master = [a for a in matches if a in asin_to_meta]
    if in_master:
        active_hits = [a for a in in_master if a in active] or in_master
        brand = asin_to_meta[active_hits[0]]["brand"]
        return ("L1_asin", active_hits, brand)

    # L2 — Model in name
    for model in models_sorted:
        if not model:
            continue
        pat = r"(?<![A-Z0-9])" + re.escape(model.upper()) + r"(?![A-Z0-9])"
        if re.search(pat, cam_upper):
            asins = model_to_asins.get(model.lower(), [])
            used = [a for a in asins if a in active]
            if used:
                return ("L2_model", used, asin_to_meta[used[0]]["brand"])

    # L3 — Category in name
    for cat in cats_sorted_vals:
        pat = r"(?<![A-Z0-9])" + re.escape(cat.upper()) + r"(?![A-Z0-9])"
        if re.search(pat, cam_upper):
            brand_map = cat_to_brand_asins.get(cat.lower(), {})
            asins = brand_map.get(file_brand, [])
            if not asins:
                for b, lst in brand_map.items():
                    asins.extend(lst)
            used = [a for a in asins if a in active]
            if used:
                return ("L3_category:" + cat, used, asin_to_meta[used[0]]["brand"])

    # L4 — synonym map (only enabled entries)
    if synonyms:
        cam_low = campaign.lower()
        for kw, entry in synonyms.items():
            pat = r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])"
            if re.search(pat, cam_low):
                asins = resolve_synonym(entry, rows_by_brand, active)
                if asins:
                    return ("L4_synonym:" + kw, asins, entry.get("brand", file_brand))

    return ("unmapped", [], file_brand)


# ─────────────────────────────────────────────────────────────────────
# Aggregate SB raw → per-campaign rows
# ─────────────────────────────────────────────────────────────────────
NUMERIC_COLS = ["Impressions", "Clicks", "Spend",
                "14 Day Total Sales (₹)", "14 Day Total Units (#)"]


def read_sb_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()
    if "Campaign Name" not in df.columns:
        return pd.DataFrame(columns=OUT_COLS)
    if "Portfolio name" not in df.columns:
        df["Portfolio name"] = ""
    for c in NUMERIC_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    # Aggregate daily rows to one row per campaign
    agg = (df.groupby(["Portfolio name", "Campaign Name"], as_index=False)
             [NUMERIC_COLS].sum())
    return agg


# ─────────────────────────────────────────────────────────────────────
# Write SB sheet back to per-brand ads_report_weekN.xlsx
# ─────────────────────────────────────────────────────────────────────
def write_sb_sheets(per_brand_rows: dict[str, pd.DataFrame], week_num: int) -> None:
    """Adds an "SB" sheet to each brand's ads_report_weekN.xlsx,
    preserving the existing SP and SD sheets.  Backs up before write."""
    for brand_folder, df_sb in per_brand_rows.items():
        out = AMS_ROOT / brand_folder / f"ads_report_week{week_num}.xlsx"
        if not out.exists():
            print(f"   ⚠ {brand_folder}: no existing ads_report — skipping (run ads_account_ingest first)")
            continue

        # Backup once per file
        bak = out.with_suffix(out.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(out, bak)

        # Read existing SP/SD then write all three in one go
        try:
            xl = pd.ExcelFile(out)
        except Exception as e:
            print(f"   ⚠ {brand_folder}: failed to open {out.name}: {e}")
            continue

        sheets_to_write = {}
        for s in xl.sheet_names:
            if s.upper() == "SB":
                continue  # we'll overwrite SB
            sheets_to_write[s] = pd.read_excel(out, sheet_name=s)
        sheets_to_write["SB"] = df_sb[OUT_COLS] if not df_sb.empty else pd.DataFrame(columns=OUT_COLS)

        with pd.ExcelWriter(out, engine="openpyxl") as w:
            for sheet, df in sheets_to_write.items():
                df.to_excel(w, sheet_name=sheet, index=False)


# ─────────────────────────────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────────────────────────────
def run_for_week(week_num: int) -> dict:
    sb_files = find_sb_files(week_num)
    if not sb_files:
        return {"week": week_num, "skipped": "no SB files found"}

    (asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
     cat_to_brand_asins, cats_sorted_vals) = build_master_lookups()
    synonyms          = load_synonyms()
    campaign_asin_map = load_campaign_asin_map()
    active            = build_active_set(week_num)

    print(f"   Master ASINs={len(asin_to_meta):,}  Active in W{week_num} (±{ACTIVE_WINDOW_WEEKS-1}w)={len(active):,}  "
          f"L0 API map entries={len(campaign_asin_map)}  Synonyms enabled={len(synonyms)}")

    # Per-brand row buckets
    per_brand_rows: dict[str, list[dict]] = {}
    unmapped_rows: list[dict] = []
    layer_counts = {"L0_ads_api": 0, "L1_asin": 0, "L2_model": 0, "L3_category": 0,
                    "L4_synonym": 0, "unmapped": 0}
    layer_spend  = {k: 0.0 for k in layer_counts}

    for p in sb_files:
        df = read_sb_file(p)
        if df.empty:
            continue
        rel = p.relative_to(AMS_ROOT)
        folder = rel.parts[0]
        file_brand = FOLDER_TO_BRAND.get(folder, folder)

        for _, r in df.iterrows():
            campaign = _norm(r["Campaign Name"])
            spend    = float(r["Spend"])
            if spend <= 0:
                continue   # skip paused campaigns

            layer, asins, brand = attribute_campaign(
                campaign, file_brand,
                asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
                cat_to_brand_asins, cats_sorted_vals, synonyms, active,
                campaign_asin_map=campaign_asin_map,
            )
            base = ("L0_ads_api"  if layer.startswith("L0_") else
                    "L3_category" if layer.startswith("L3_") else
                    "L4_synonym"  if layer.startswith("L4_") else layer)
            layer_counts[base] += 1
            layer_spend[base]  += spend

            if not asins:
                unmapped_rows.append({
                    "week":       week_num,
                    "brand":      brand or file_brand,
                    "campaign":   campaign,
                    "spend":      spend,
                    "sales":      float(r["14 Day Total Sales (₹)"]),
                    "impressions":float(r["Impressions"]),
                    "clicks":     float(r["Clicks"]),
                    "units":      float(r["14 Day Total Units (#)"]),
                    "reason":     "no L1/L2/L3 match",
                })
                continue

            # Distribute the campaign's metrics evenly across the ASINs.
            n = len(asins)
            row_template = {
                "Portfolio name":  _norm(r["Portfolio name"]),
                "Campaign Name":   campaign,
                "Impressions":          float(r["Impressions"]) / n,
                "Clicks":               float(r["Clicks"]) / n,
                "Spend":                float(r["Spend"]) / n,
                "14 Day Total Sales (₹)":  float(r["14 Day Total Sales (₹)"]) / n,
                "14 Day Total Units (#)":  float(r["14 Day Total Units (#)"]) / n,
                "Brand":            brand,
            }
            folder_for_brand = brand.replace(" ", "_") if brand else folder
            for asin in asins:
                per_brand_rows.setdefault(folder_for_brand, []).append(
                    {**row_template, "Advertised ASIN": asin}
                )

    # Materialise per-brand DataFrames
    per_brand_dfs: dict[str, pd.DataFrame] = {}
    for k, rows in per_brand_rows.items():
        df_sb = pd.DataFrame(rows)
        # Final per-(Portfolio, Campaign, ASIN, Brand) aggregation in case
        # the same ASIN got hit by multiple synonyms or duplicate files
        df_sb = (df_sb.groupby(
                    ["Portfolio name", "Campaign Name", "Advertised ASIN", "Brand"],
                    as_index=False)
                      [NUMERIC_COLS].sum())
        # Impressions / Clicks / Units are physical event counts — round to
        # integers so they don't look like "79157.55" in the export.  Spend
        # and Sales stay float (rupee amounts can carry paise).
        for c in ("Impressions", "Clicks", "14 Day Total Units (#)"):
            if c in df_sb.columns:
                df_sb[c] = df_sb[c].round().astype(int)
        per_brand_dfs[k] = df_sb

    # Write to disk
    print()
    write_sb_sheets(per_brand_dfs, week_num)
    for k, df in per_brand_dfs.items():
        spend = float(df["Spend"].sum())
        print(f"   ✅ {k:<16}  SB rows={len(df):>3}  spend=₹{spend:>9,.0f}  "
              f"→ data/ams_weekly_data/{k}/ads_report_week{week_num}.xlsx [sheet=SB]")

    # Unmapped log
    if unmapped_rows:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        out = PROCESSED_DIR / f"sb_unmapped_week{week_num}.csv"
        pd.DataFrame(unmapped_rows).to_csv(out, index=False)
        u_spend = sum(r["spend"] for r in unmapped_rows)
        print(f"\n   ⚠ Unmapped SB rows: {len(unmapped_rows)}  spend=₹{u_spend:,.0f}  →  {out.relative_to(ROOT)}")

    # Console summary
    total_spend = sum(layer_spend.values())
    print()
    print(f"   {'Layer':<14} {'Camps':>5}  {'Spend':>11}  {'% of SB':>7}")
    for k in ("L0_ads_api", "L1_asin", "L2_model", "L3_category", "L4_synonym", "unmapped"):
        pct = (layer_spend[k] / total_spend * 100) if total_spend else 0
        print(f"   {k:<14} {layer_counts[k]:>5}  ₹{layer_spend[k]:>10,.0f}  {pct:>6.1f}%")

    return {"week": week_num, "layer_counts": layer_counts,
            "layer_spend": layer_spend, "unmapped_n": len(unmapped_rows)}


def main() -> None:
    if not AMS_ROOT.exists():
        print(f"⚠ {AMS_ROOT} not found")
        return
    weeks: set[int] = set()
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        for wk in brand_dir.iterdir():
            if wk.is_dir() and wk.name.startswith("Week "):
                try:
                    weeks.add(int(wk.name.replace("Week", "").strip()))
                except Exception:
                    pass
    if not weeks:
        print("⚠ No Week-N subfolders found — drop SB files at data/ams_weekly_data/<Brand>/Week N/")
        return

    for wk in sorted(weeks):
        print(f"\n━━━ Week {wk} ━━━")
        run_for_week(wk)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import sys, traceback
        traceback.print_exc()
        sys.exit(1)
