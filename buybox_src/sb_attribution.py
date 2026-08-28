"""
SB campaign-level L0–L4 attribution — ported from the weekly project's
weekly_app/etl/sb_ingest.py so monthly numbers reconcile to weekly
when rolled up.

Source weekly file ref:
  nitesh-lang/weekly-dashboard-fastapi @ weekly_app/etl/sb_ingest.py:252
  (attribute_campaign function + supporting lookups)

Attribution cascade (first match wins, applied per CAMPAIGN):
  L0  Amazon Ads API authoritative map  (sb_campaign_asins.json)
  L1  ASIN in campaign name             (regex match against master ASINs)
  L2  Model in campaign name            (longest model first; word-boundary)
  L3  Category in campaign name         (matched to master category_l0/l1/l2)
  L4  Synonym map                       (optional; skipped if file absent)
  unmapped                              (spend stays unattributed)

Each layer returns a list of ASINs; the caller even-splits the campaign's
metrics across that list — same convention as the weekly project, so when
4 weeks of weekly attribution roll into a month they tie to monthly
attribution.

Inputs expected on disk (paths relative to project root):
  data/master/sku_master.xlsx               - copied from weekly G drive
  data/processed/sb_campaign_asins.json     - produced by ads_api_pull;
                                              latest weekly cron run is fine
  data/<Brand>/<MonthName>/business_report.{xlsx,csv}
                                            - operator-dropped monthly
                                              business reports; used to
                                              compute the "active ASIN" set
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT          = Path(__file__).resolve().parent
MASTER        = ROOT / "data" / "master" / "sku_master.xlsx"
CAMPAIGN_ASINS_FILE = ROOT / "data" / "processed" / "sb_campaign_asins.json"
DATA_DIR      = ROOT / "data"

# Month-number → folder name used in this project's data/<Brand>/<Month>/
MONTH_FOLDERS = {
    1: "Jan",  2: "Feb",  3: "Mar",  4: "Apr",  5: "May",  6: "Jun",
    7: "Jul",  8: "Aug",  9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Rolling activity window for the "active ASIN" set.  Weekly uses 4
# weeks; monthly equivalent ≈ 3 months (current + 2 prior) — captures
# any ASIN seen in the latest quarter of sessions / units data.
ACTIVE_WINDOW_MONTHS = 3

ASIN_RE = re.compile(r"\bB0[A-Z0-9]{8}\b")


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ──────────────────────────────────────────────────────────────────────
# Master + active lookups
# ──────────────────────────────────────────────────────────────────────
def build_master_lookups():
    """Returns the 6-tuple needed by attribute_campaign():
        (asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
         cat_to_brand_asins, cats_sorted_vals)
    """
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
            "sku":   _norm(r.get("FBA SKU")),
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

    # Sort longest-first so 'TVCT1ML' wins over 'TV' on overlapping prefixes.
    models_sorted = sorted(
        {rec["model"] for rec in asin_to_meta.values() if rec["model"]},
        key=lambda s: (-len(s), s),
    )

    cat_to_brand_asins: dict[str, dict[str, list[str]]] = {}
    cat_values: set[str] = set()
    for asin, rec in asin_to_meta.items():
        for val in (rec["cat0"], rec["cat1"], rec["cat2"]):
            if val:
                cat_to_brand_asins.setdefault(val.lower(), {}).setdefault(rec["brand"], []).append(asin)
                cat_values.add(val)
    cats_sorted_vals = sorted(cat_values, key=lambda s: (-len(s), s))
    return (asin_to_meta, rows_by_brand, model_to_asins,
            models_sorted, cat_to_brand_asins, cats_sorted_vals)


def build_active_set_monthly(target_year: int, target_month: int) -> set[str]:
    """Returns the set of ASINs seen with sessions>0 or units>0 in any
    of the last ACTIVE_WINDOW_MONTHS calendar months (current + 2 prior).

    Reads data/<Brand>/<MonthName>/business_report.{xlsx,csv}.  Works
    with both file formats — the monthly project has a mix.
    """
    months_to_try: list[str] = []
    y, m = target_year, target_month
    for _ in range(ACTIVE_WINDOW_MONTHS):
        months_to_try.append(MONTH_FOLDERS[m])
        m -= 1
        if m == 0:
            m, y = 12, y - 1

    active: set[str] = set()
    for brand_dir in DATA_DIR.iterdir():
        if not brand_dir.is_dir():
            continue
        for mname in months_to_try:
            mdir = brand_dir / mname
            if not mdir.exists():
                continue
            biz = mdir / "business_report.xlsx"
            if not biz.exists():
                biz = mdir / "business_report.csv"
            if not biz.exists():
                continue
            try:
                df = (pd.read_excel(biz) if biz.suffix == ".xlsx"
                      else pd.read_csv(biz))
            except Exception:
                continue
            df.columns = df.columns.str.strip()
            asin_col = next(
                (c for c in ("(Child) ASIN", "asin", "ASIN", "child_asin")
                 if c in df.columns),
                None,
            )
            if not asin_col:
                continue
            df[asin_col] = df[asin_col].map(_norm)
            sessions = pd.to_numeric(df.get("Sessions - Total", 0), errors="coerce").fillna(0)
            units_col = next(
                (c for c in ("units_ordered", "Units Ordered", "Units Sold")
                 if c in df.columns),
                None,
            )
            units = (pd.to_numeric(df[units_col], errors="coerce").fillna(0)
                     if units_col else pd.Series([0] * len(df)))
            mask = (sessions > 0) | (units > 0)
            active.update(df.loc[mask, asin_col].dropna())
    return active


def load_campaign_asin_map() -> dict[str, dict]:
    """Reads data/processed/sb_campaign_asins.json (produced by
    ads_api_pull.py sb).  Returns {campaign_name_lower: {brand, asins}}.
    Missing file → empty dict (Layer 0 simply doesn't fire)."""
    if not CAMPAIGN_ASINS_FILE.exists():
        return {}
    try:
        with open(CAMPAIGN_ASINS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for name, entry in data.items():
        key = (name or "").strip().lower()
        if key:
            out[key] = entry
    return out


# ──────────────────────────────────────────────────────────────────────
# Core L0–L4 cascade — VERBATIM port from sb_ingest.py:252-316
# ──────────────────────────────────────────────────────────────────────
def attribute_campaign(
    campaign: str,
    file_brand: str,
    asin_to_meta,
    rows_by_brand,
    model_to_asins,
    models_sorted,
    cat_to_brand_asins,
    cats_sorted_vals,
    active: set,
    campaign_asin_map: dict | None = None,
) -> tuple[str, list[str], str]:
    """Returns (layer, [asin, ...], brand).  Caller distributes metrics
    evenly across the returned ASIN list — spend / N.
    """
    if not campaign:
        return ("unmapped", [], file_brand)
    cam_upper = campaign.upper()

    # L0 — Amazon Ads API authoritative lookup
    if campaign_asin_map:
        hit = campaign_asin_map.get(campaign.strip().lower())
        if hit and hit.get("asins"):
            asins = hit["asins"]
            active_hits = [a for a in asins if a in active] or asins
            brand = hit.get("brand") or (
                asin_to_meta[active_hits[0]]["brand"]
                if active_hits[0] in asin_to_meta else file_brand
            )
            return ("L0_ads_api", active_hits, brand)

    # L1 — ASIN literal in campaign name
    matches = ASIN_RE.findall(cam_upper)
    in_master = [a for a in matches if a in asin_to_meta]
    if in_master:
        active_hits = [a for a in in_master if a in active] or in_master
        brand = asin_to_meta[active_hits[0]]["brand"]
        return ("L1_asin", active_hits, brand)

    # L2 — Model token in campaign name (longest first; word-bounded)
    for model in models_sorted:
        if not model:
            continue
        pat = r"(?<![A-Z0-9])" + re.escape(model.upper()) + r"(?![A-Z0-9])"
        if re.search(pat, cam_upper):
            asins = model_to_asins.get(model.lower(), [])
            used = [a for a in asins if a in active]
            if used:
                return ("L2_model", used, asin_to_meta[used[0]]["brand"])

    # L3 — Category token in campaign name
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

    # L4 — synonym map (optional file; skipped if absent)
    # Port of weekly's synonym-pack support.  Not wired in monthly until
    # operator drops data/processed/sb_synonyms.json.
    # (Skipped here — adds noise if not configured.)

    return ("unmapped", [], file_brand)


# ──────────────────────────────────────────────────────────────────────
# Top-level helper called from monthly_buybox_pull.attribute_sb()
# ──────────────────────────────────────────────────────────────────────
def attribute_sb_l0_l4(
    sb_ad_rows: list[dict],
    target_year: int,
    target_month: int,
) -> list[dict]:
    """Campaign-level L0–L4 attribution with even-split distribution.

    Input:  sb_ad_rows = per-(date × campaign × adGroup × adId) rows
                          from sbAds Reports API job.
    Output: per-(campaign × asin) attributed rows (date dropped during
            campaign rollup, matching the weekly's grain).

    Each row tagged with `method` = L0_ads_api | L1_asin | L2_model |
    L3_category | unmapped.
    """
    if not sb_ad_rows:
        return []

    ad_df = pd.DataFrame(sb_ad_rows)
    if ad_df.empty:
        return []

    # Normalise numeric cols
    metric_cols = ["cost", "impressions", "clicks", "sales", "unitsSold", "purchases"]
    for c in metric_cols:
        if c not in ad_df.columns:
            ad_df[c] = 0
        ad_df[c] = pd.to_numeric(ad_df[c], errors="coerce").fillna(0)

    # Aggregate to CAMPAIGN level (matches weekly's grain).  Drop date
    # and adGroup/adId since the cascade decides ASINs per campaign.
    for c in ("campaignName", "campaignId"):
        if c not in ad_df.columns:
            ad_df[c] = ""
    cam = (ad_df.groupby(["campaignId", "campaignName"], as_index=False)
                 .agg({c: "sum" for c in metric_cols}))

    # Skip paused campaigns (zero spend) — matches sb_ingest line 412.
    cam = cam[cam["cost"] > 0]

    # Load lookups once
    (asin_to_meta, rows_by_brand, model_to_asins,
     models_sorted, cat_to_brand_asins, cats_sorted_vals) = build_master_lookups()
    campaign_asin_map = load_campaign_asin_map()
    active = build_active_set_monthly(target_year, target_month)

    out: list[dict] = []
    for _, r in cam.iterrows():
        layer, asins, brand = attribute_campaign(
            campaign=str(r["campaignName"]),
            file_brand="",  # API source has no folder hint
            asin_to_meta=asin_to_meta,
            rows_by_brand=rows_by_brand,
            model_to_asins=model_to_asins,
            models_sorted=models_sorted,
            cat_to_brand_asins=cat_to_brand_asins,
            cats_sorted_vals=cats_sorted_vals,
            active=active,
            campaign_asin_map=campaign_asin_map,
        )
        if not asins:
            # Unmapped — emit a single row tagging the campaign so the
            # operator can see what wasn't attributed.
            out.append({
                "campaignId":       r["campaignId"],
                "campaignName":     r["campaignName"],
                "brand":            "(unmapped)",
                "asin":             "",
                "spend":            float(r["cost"]),
                "impressions":      int(r["impressions"]),
                "clicks":           int(r["clicks"]),
                "attributed_sales": float(r["sales"]),
                "units_sold":       int(r["unitsSold"]),
                "ams_orders":       int(r["purchases"]),
                "method":           "unmapped",
            })
            continue
        n = len(asins)
        for asin in asins:
            # Pull canonical SKU + Model + Brand + categories from master
            # via ASIN.  Master is the source of truth — overrides the
            # cascade's `brand` (which came from the L0 map, profile-
            # derived).  This is what re-tags Tonor ASINs running inside
            # the AudioArray ad account from brand=Audio Array (map) to
            # brand=Tonor (master).  Mirrors weekly step4's behaviour.
            meta = asin_to_meta.get(asin, {})
            row_brand = meta.get("brand") or brand
            out.append({
                "campaignId":       r["campaignId"],
                "campaignName":     r["campaignName"],
                "brand":            row_brand,
                "asin":             asin,
                "sku":              meta.get("sku", ""),
                "model":            meta.get("model", ""),
                "category_l0":      meta.get("cat0", ""),
                "category_l1":      meta.get("cat1", ""),
                "category_l2":      meta.get("cat2", ""),
                "spend":            round(float(r["cost"])         / n, 4),
                "impressions":      int(round(float(r["impressions"]) / n)),
                "clicks":           int(round(float(r["clicks"])      / n)),
                "attributed_sales": round(float(r["sales"])        / n, 2),
                "units_sold":       int(round(float(r["unitsSold"])   / n)),
                "ams_orders":       int(round(float(r["purchases"])   / n)),
                "method":           layer,
            })
    return out


# ──────────────────────────────────────────────────────────────────────
# Reusable enrichment helper — for SP / SD outputs (NOT just SB)
# ──────────────────────────────────────────────────────────────────────
def enrich_with_master(df: pd.DataFrame, asin_col: str) -> pd.DataFrame:
    """Override SKU / Model / Brand / category_l0/l1/l2 with master's
    canonical values via ASIN lookup.  Mirrors the weekly's
    step4_join_business_ads master-by-ASIN re-tag.

    Rows whose ASIN isn't in master are left untouched.  Empty ASIN
    rows are also left untouched.
    """
    if asin_col not in df.columns or df.empty:
        return df
    asin_to_meta, *_ = build_master_lookups()
    if not asin_to_meta:
        return df

    a = df[asin_col].astype(str).str.strip()
    mask = a.isin(asin_to_meta)
    if not mask.any():
        return df

    df.loc[mask, "sku"]         = a[mask].map(lambda k: asin_to_meta[k]["sku"])
    df.loc[mask, "model"]       = a[mask].map(lambda k: asin_to_meta[k]["model"])
    df.loc[mask, "brand"]       = a[mask].map(lambda k: asin_to_meta[k]["brand"])
    df.loc[mask, "category_l0"] = a[mask].map(lambda k: asin_to_meta[k]["cat0"])
    df.loc[mask, "category_l1"] = a[mask].map(lambda k: asin_to_meta[k]["cat1"])
    df.loc[mask, "category_l2"] = a[mask].map(lambda k: asin_to_meta[k]["cat2"])
    return df
