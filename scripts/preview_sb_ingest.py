"""
PREVIEW for Sponsored Brands ingestion (read-only).

Tries to attribute each SB campaign to ASIN(s) via three layers:

  Layer 1: ASIN regex match  — campaign name contains a B0XXXXXXXX
                                token that resolves in master.
  Layer 2: Model name match  — campaign name contains a model code
                                that appears in master.Model.  Once
                                we know the Model, we resolve to that
                                Model's ASIN(s) from master.
  Layer 3: Unmapped          — needs a rule; spend shown so we can
                                decide if it's material.

Walks every Sponsored_Brands_Campaign_report.xlsx under
data/ams_weekly_data/<Brand>/Week N/.

Read-only — no files written.
"""
from __future__ import annotations
import re
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent
MASTER   = ROOT / "data" / "master" / "sku_master.xlsx"
AMS_ROOT = ROOT / "data" / "ams_weekly_data"

SB_FILENAME = "Sponsored_Brands_Campaign_report.xlsx"

# ASIN pattern: B0 followed by 8 alphanumeric chars.  Also catch the
# 10-char form some legacy ASINs use (any leading letter+digits).
ASIN_RE = re.compile(r"\bB0[A-Z0-9]{8}\b")


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Build master lookups ────────────────────────────────────────────────
def build_master_lookups():
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()

    asin_to_brand_model: dict[str, tuple[str, str]] = {}
    for _, r in m.iterrows():
        brand = _norm(r.get("Brand"))
        model = _norm(r.get("Model"))
        if not brand:
            continue
        primary = _norm(r.get("ASIN"))
        if primary:
            asin_to_brand_model[primary] = (brand, model)
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x and x not in asin_to_brand_model:
                    asin_to_brand_model[x] = (brand, model)

    # Model → list of ASINs (so an Layer-2 match can fan out to all
    # ASINs that share the model)
    model_to_asins: dict[str, list[str]] = {}
    for asin, (_, model) in asin_to_brand_model.items():
        if model:
            model_to_asins.setdefault(model.lower(), []).append(asin)

    # Sorted model list — longest first so when we substring-match
    # against a campaign name we hit "AM-W47 Wireless" before "AM-W47"
    models_sorted = sorted(
        {m for m in (_norm(model) for _, model in asin_to_brand_model.values()) if m},
        key=lambda s: (-len(s), s),
    )
    return asin_to_brand_model, model_to_asins, models_sorted


# ── Find every SB file ──────────────────────────────────────────────────
def find_sb_files() -> list[Path]:
    found: list[Path] = []
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        for wk_dir in brand_dir.iterdir():
            if not (wk_dir.is_dir() and wk_dir.name.startswith("Week ")):
                continue
            for p in wk_dir.rglob(SB_FILENAME):
                if not p.name.startswith("~"):
                    found.append(p)
    return found


# ── Layer attribution per row ───────────────────────────────────────────
def attribute_row(
    campaign: str,
    asin_to_brand_model: dict,
    model_to_asins: dict,
    models_sorted: list[str],
) -> tuple[str, list[str], str]:
    """Returns (layer, [asins], brand-or-empty)."""
    if not campaign:
        return ("unmapped", [], "")

    # Layer 1: ASIN regex
    matches = ASIN_RE.findall(campaign.upper())
    valid = [a for a in matches if a in asin_to_brand_model]
    if valid:
        brand = asin_to_brand_model[valid[0]][0]
        return ("L1_asin", valid, brand)

    # Layer 2: Model substring match (longest first to avoid partials)
    cam_upper = campaign.upper()
    for model in models_sorted:
        if not model:
            continue
        # Word-boundary-ish: require the model to appear as a token
        m_up = model.upper()
        pattern = r"(?<![A-Z0-9])" + re.escape(m_up) + r"(?![A-Z0-9])"
        if re.search(pattern, cam_upper):
            asins = model_to_asins.get(model.lower(), [])
            if asins:
                brand = asin_to_brand_model[asins[0]][0]
                return ("L2_model", asins, brand)

    return ("unmapped", [], "")


# ── Main ────────────────────────────────────────────────────────────────
def main() -> None:
    sb_files = find_sb_files()
    if not sb_files:
        print(f"⚠ No {SB_FILENAME} files found under data/ams_weekly_data/<Brand>/Week N/")
        return

    asin_to_brand_model, model_to_asins, models_sorted = build_master_lookups()
    print(f"Master: {len(asin_to_brand_model):,} ASINs · {len(models_sorted):,} unique Models\n")

    # Aggregate per-file then global
    per_layer_global: dict[str, dict] = {"L1_asin": {"campaigns": 0, "spend": 0.0, "sales": 0.0},
                                          "L2_model": {"campaigns": 0, "spend": 0.0, "sales": 0.0},
                                          "unmapped": {"campaigns": 0, "spend": 0.0, "sales": 0.0}}
    unmapped_samples: list[tuple[str, str, float]] = []   # (brand-folder, campaign, spend)

    for p in sb_files:
        try:
            df = pd.read_excel(p)
        except Exception as e:
            print(f"⚠ {p.relative_to(AMS_ROOT)} failed to read: {e}")
            continue
        df.columns = df.columns.str.strip()

        # SB exports are daily — aggregate per campaign first
        if "Campaign Name" not in df.columns:
            continue
        for c in ("Spend", "14 Day Total Sales (₹)", "Impressions", "Clicks"):
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        agg = (df.groupby("Campaign Name", as_index=False)
                 .agg({"Spend":                "sum",
                       "14 Day Total Sales (₹)": "sum",
                       "Impressions":          "sum",
                       "Clicks":               "sum"}))

        # Brand folder of this file (informational, not used for attribution)
        brand_folder = p.relative_to(AMS_ROOT).parts[0]

        l_count = {"L1_asin": 0, "L2_model": 0, "unmapped": 0}
        l_spend = {"L1_asin": 0.0, "L2_model": 0.0, "unmapped": 0.0}
        for _, r in agg.iterrows():
            layer, asins, brand = attribute_row(
                _norm(r["Campaign Name"]),
                asin_to_brand_model, model_to_asins, models_sorted,
            )
            l_count[layer] += 1
            l_spend[layer] += float(r["Spend"])
            per_layer_global[layer]["campaigns"] += 1
            per_layer_global[layer]["spend"] += float(r["Spend"])
            per_layer_global[layer]["sales"] += float(r["14 Day Total Sales (₹)"])
            if layer == "unmapped" and float(r["Spend"]) > 0:
                unmapped_samples.append((brand_folder, _norm(r["Campaign Name"]), float(r["Spend"])))

        print(f"📥 {p.relative_to(AMS_ROOT)}  ({len(agg)} campaigns)")
        for layer in ("L1_asin", "L2_model", "unmapped"):
            if l_count[layer]:
                print(f"   {layer:<10}  {l_count[layer]:>3} campaigns  ₹{l_spend[layer]:>9,.0f}")
        print()

    # Global summary
    print("=" * 60)
    total_camp = sum(d["campaigns"] for d in per_layer_global.values())
    total_spend = sum(d["spend"] for d in per_layer_global.values())
    total_sales = sum(d["sales"] for d in per_layer_global.values())
    print(f"GLOBAL  total campaigns={total_camp}  total spend=₹{total_spend:,.0f}  total sales=₹{total_sales:,.0f}\n")
    for layer in ("L1_asin", "L2_model", "unmapped"):
        d = per_layer_global[layer]
        pct = (d["spend"] / total_spend * 100) if total_spend else 0
        print(f"  {layer:<10}  {d['campaigns']:>3} campaigns  "
              f"spend=₹{d['spend']:>9,.0f} ({pct:5.1f}%)  "
              f"sales=₹{d['sales']:>9,.0f}")

    if unmapped_samples:
        print()
        print("─" * 60)
        print(f"Top unmapped campaigns by spend (need rule):")
        for bd, c, sp in sorted(unmapped_samples, key=lambda x: -x[2])[:25]:
            print(f"  ₹{sp:>9,.0f}   [{bd:<14}]  {c}")


if __name__ == "__main__":
    main()
