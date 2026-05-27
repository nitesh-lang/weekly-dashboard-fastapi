"""
PREVIEW for SB attribution with the synonym map (read-only).

Layers tried in order:
  L1 — ASIN in campaign name
  L2 — Model in campaign name (active ASINs only)
  L3 — Category match against master.category_l0/l1/l2
  L4 — Synonym map (data/master/sb_synonyms.json)
        operator-editable.  Maps a campaign keyword to a predicate
        that returns a list of master ASINs.
  unmapped — everything else
"""
from __future__ import annotations
import json
import re
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent
MASTER   = ROOT / "data" / "master" / "sku_master.xlsx"
AMS_ROOT = ROOT / "data" / "ams_weekly_data"
SYNONYMS_FILE = ROOT / "data" / "master" / "sb_synonyms.json"

SB_FILENAME = "Sponsored_Brands_Campaign_report.xlsx"
ASIN_RE     = re.compile(r"\bB0[A-Z0-9]{8}\b")
ACTIVE_WINDOW_WEEKS = 4

FOLDER_TO_BRAND = {
    "Audio_Array": "Audio Array", "Nexlev": "Nexlev",
    "Tonor": "Tonor", "White_Mulberry": "White Mulberry", "Fossil": "Fossil",
}


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


def load_synonyms() -> dict:
    if not SYNONYMS_FILE.exists():
        return {}
    with open(SYNONYMS_FILE, encoding="utf-8") as f:
        data = json.load(f)
    # Skip metadata keys (those starting with "_")
    return {k.lower(): v for k, v in data.items() if not k.startswith("_")}


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
            "cat0": _norm(r.get("category_l0")),
            "cat1": _norm(r.get("category_l1")),
            "cat2": _norm(r.get("category_l2")),
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
        for level, val in (("cat2", rec["cat2"]), ("cat1", rec["cat1"]), ("cat0", rec["cat0"])):
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
        if wk == week_num and wk_active:
            return wk_active
        active.update(wk_active)
    return active


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


# ── Resolve a synonym entry to a list of ASINs ─────────────────────────
def resolve_synonym(entry: dict, rows_by_brand: dict, active: set) -> list[str]:
    brand = entry.get("brand")
    if not brand:
        return []
    candidates = rows_by_brand.get(brand, [])
    matched: list[str] = []
    if "category_l2" in entry:
        wanted = entry["category_l2"]
        wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wanted_lower = {w.lower() for w in wanted}
        for row in candidates:
            if row["cat2"].lower() in wanted_lower:
                matched.append(row["asin"])
    elif "category_l1" in entry:
        wanted = entry["category_l1"]
        wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wanted_lower = {w.lower() for w in wanted}
        for row in candidates:
            if row["cat1"].lower() in wanted_lower:
                matched.append(row["asin"])
    elif "category_l1_contains" in entry:
        wanted = entry["category_l1_contains"]
        wanted = [wanted] if isinstance(wanted, str) else list(wanted)
        wanted_lower = [w.lower() for w in wanted]
        for row in candidates:
            cat1_low = row["cat1"].lower()
            if any(w in cat1_low for w in wanted_lower):
                matched.append(row["asin"])
    elif "model_regex" in entry:
        try:
            pat = re.compile(entry["model_regex"], re.IGNORECASE)
        except Exception:
            return []
        for row in candidates:
            if row["model"] and pat.search(row["model"]):
                matched.append(row["asin"])

    # Deduplicate + filter to active
    seen = set()
    uniq = []
    for a in matched:
        if a in seen:
            continue
        seen.add(a)
        uniq.append(a)
    return [a for a in uniq if a in active]


def attribute(
    campaign: str, spend: float, file_brand: str,
    asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
    cat_to_brand_asins, cats_sorted_vals, synonyms, active,
):
    if not campaign or spend <= 0:
        return ("zero_spend", [], "")
    cam_upper = campaign.upper()

    # L1
    matches = ASIN_RE.findall(cam_upper)
    in_master = [a for a in matches if a in asin_to_meta]
    if in_master:
        active_hits = [a for a in in_master if a in active] or in_master
        share = spend / len(active_hits)
        brand = asin_to_meta[active_hits[0]]["brand"]
        return ("L1_asin", [(a, share) for a in active_hits], brand)

    # L2
    for model in models_sorted:
        if not model:
            continue
        pat = r"(?<![A-Z0-9])" + re.escape(model.upper()) + r"(?![A-Z0-9])"
        if re.search(pat, cam_upper):
            asins = model_to_asins.get(model.lower(), [])
            used = [a for a in asins if a in active]
            if used:
                share = spend / len(used)
                brand = asin_to_meta[used[0]]["brand"]
                return ("L2_model", [(a, share) for a in used], brand)

    # L3
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
                share = spend / len(used)
                brand = asin_to_meta[used[0]]["brand"]
                return ("L3_category:" + cat, [(a, share) for a in used], brand)

    # L4 — synonym map
    cam_low = campaign.lower()
    for kw, entry in synonyms.items():
        # Word-boundary match on the synonym keyword
        pat = r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])"
        if re.search(pat, cam_low):
            asins = resolve_synonym(entry, rows_by_brand, active)
            if asins:
                share = spend / len(asins)
                brand = entry.get("brand", file_brand)
                return ("L4_synonym:" + kw, [(a, share) for a in asins], brand)

    return ("unmapped", [], file_brand)


def main(week: int = 21) -> None:
    sb_files = find_sb_files(week)
    if not sb_files:
        print(f"⚠ No SB files for Week {week}")
        return

    (asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
     cat_to_brand_asins, cats_sorted_vals) = build_master_lookups()
    synonyms = load_synonyms()
    active = build_active_set(week)
    print(f"Master ASINs={len(asin_to_meta):,}  Models={len(models_sorted):,}  "
          f"Categories={len(cats_sorted_vals):,}  Synonyms={len(synonyms)}")
    print(f"Active ASINs (W{week} or last {ACTIVE_WINDOW_WEEKS-1}): {len(active):,}\n")

    summary: dict[str, list] = {k: [0, 0.0, 0] for k in
                ("L1_asin", "L2_model", "L3_category", "L4_synonym", "unmapped", "zero_spend")}
    detail: list[dict] = []
    unmapped_lines: list[tuple] = []

    for p in sb_files:
        try:
            df = pd.read_excel(p)
        except Exception:
            continue
        df.columns = df.columns.str.strip()
        if "Campaign Name" not in df.columns:
            continue
        for c in ("Spend", "14 Day Total Sales (₹)"):
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        agg = (df.groupby("Campaign Name", as_index=False)
                 .agg(Spend=("Spend", "sum"),
                      sales=("14 Day Total Sales (₹)", "sum")))

        rel = p.relative_to(AMS_ROOT)
        folder_brand = FOLDER_TO_BRAND.get(rel.parts[0], rel.parts[0])

        for _, r in agg.iterrows():
            layer, alloc, brand = attribute(
                _norm(r["Campaign Name"]), float(r["Spend"]), folder_brand,
                asin_to_meta, rows_by_brand, model_to_asins, models_sorted,
                cat_to_brand_asins, cats_sorted_vals, synonyms, active,
            )
            base = ("L3_category" if layer.startswith("L3_") else
                    "L4_synonym" if layer.startswith("L4_") else layer)
            summary[base][0] += 1
            summary[base][1] += float(r["Spend"])
            summary[base][2] += len(alloc)
            if (layer.startswith("L3_") or layer.startswith("L4_")) and alloc:
                detail.append({"layer": layer, "brand": brand,
                                "campaign": _norm(r["Campaign Name"])[:65],
                                "spend": float(r["Spend"]),
                                "n_asins": len(alloc)})
            if layer == "unmapped" and float(r["Spend"]) > 0:
                unmapped_lines.append((folder_brand, _norm(r["Campaign Name"]), float(r["Spend"])))

    total = sum(d[1] for d in summary.values())
    print("=" * 70)
    print(f"Week {week}  ·  total SB spend: ₹{total:,.0f}\n")
    print(f"{'Layer':<14}  {'Camps':>5}  {'Spend':>11}  {'%':>6}  {'ASIN rows':>9}")
    for layer in ("L1_asin", "L2_model", "L3_category", "L4_synonym", "unmapped", "zero_spend"):
        d = summary[layer]
        pct = (d[1] / total * 100) if total else 0
        print(f"  {layer:<12}  {d[0]:>5}  ₹{d[1]:>10,.0f}  {pct:>5.1f}%  {d[2]:>9}")

    if detail:
        print()
        print("─" * 70)
        print("L3 / L4 attributions:")
        for e in sorted(detail, key=lambda x: -x["spend"]):
            label = e["layer"].split(":", 1)[-1] if ":" in e["layer"] else e["layer"]
            print(f"  ₹{e['spend']:>8,.0f}  [{e['brand']:<14}]  {e['layer']:<22}  "
                  f"({e['n_asins']} ASINs, ₹{e['spend']/e['n_asins']:,.0f} each)")
            print(f"             campaign: {e['campaign']}")

    if unmapped_lines:
        print()
        print("─" * 70)
        print("Still unmapped after L1+L2+L3+L4:")
        for bd, c, sp in sorted(unmapped_lines, key=lambda x: -x[2]):
            print(f"  ₹{sp:>8,.0f}  [{bd:<14}]  {c}")


if __name__ == "__main__":
    import sys
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 21)
