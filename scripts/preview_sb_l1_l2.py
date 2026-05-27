"""
PREVIEW for SB attribution layers 1 + 2 (read-only).

Layer 1 — ASIN found in campaign name:
    Token-scan the campaign name for B0XXXXXXXX.  If the ASIN is in
    master and is currently ACTIVE (sessions > 0 OR units > 0 in this
    week's business_report — or the last 4 weeks as a fallback), the
    entire campaign's spend lands on that ASIN.  If multiple ASINs
    are matched, split spend evenly across the active ones.

Layer 2 — Model found in campaign name:
    Substring-scan the campaign name for a master Model code
    (longest first to avoid partial matches).  Distribute the
    campaign's spend EVENLY across all ACTIVE ASINs that share that
    Model in master.  If no active ASIN exists for the model, the
    campaign falls through to "unmapped" (so we don't waste budget
    pointing at a dormant variant).

Output: per-campaign attribution preview — what each campaign gets
mapped to, and the spend split per ASIN.

No files written.
"""
from __future__ import annotations
import re
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent
MASTER   = ROOT / "data" / "master" / "sku_master.xlsx"
AMS_ROOT = ROOT / "data" / "ams_weekly_data"

SB_FILENAME = "Sponsored_Brands_Campaign_report.xlsx"
ASIN_RE     = re.compile(r"\bB0[A-Z0-9]{8}\b")
ACTIVE_WINDOW_WEEKS = 4   # used for fallback when current week is dry


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Master lookups ─────────────────────────────────────────────────────
def build_master_lookups():
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()

    asin_to_meta: dict[str, dict] = {}
    for _, r in m.iterrows():
        brand = _norm(r.get("Brand"))
        model = _norm(r.get("Model"))
        if not brand:
            continue
        primary = _norm(r.get("ASIN"))
        rec = {"brand": brand, "model": model}
        if primary:
            asin_to_meta[primary] = rec
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x and x not in asin_to_meta:
                    asin_to_meta[x] = rec

    # Model → list of ASINs (master-canonical mapping)
    model_to_asins: dict[str, list[str]] = {}
    for asin, rec in asin_to_meta.items():
        if rec["model"]:
            model_to_asins.setdefault(rec["model"].lower(), []).append(asin)

    # Longest models first for substring matching
    models_sorted = sorted(
        {rec["model"] for rec in asin_to_meta.values() if rec["model"]},
        key=lambda s: (-len(s), s),
    )
    return asin_to_meta, model_to_asins, models_sorted


# ── Activity lookup: ASIN → "active this week" (sessions or units > 0)
# ── Falls back to ACTIVE_WINDOW_WEEKS if the current week is dry. ──
def build_active_set(week_num: int) -> set[str]:
    """Returns set of ASINs considered active for this week."""
    active: set[str] = set()
    weeks_to_try = [week_num] + [week_num - i for i in range(1, ACTIVE_WINDOW_WEEKS)]
    for wk in weeks_to_try:
        wk_active: set[str] = set()
        for brand_dir in AMS_ROOT.iterdir():
            if not brand_dir.is_dir():
                continue
            biz_path = brand_dir / f"business_report_week{wk}.xlsx"
            if not biz_path.exists():
                continue
            try:
                df = pd.read_excel(biz_path)
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


# ── Find SB files for a specific week ──────────────────────────────────
def find_sb_files(week_num: int) -> list[Path]:
    found: list[Path] = []
    folder_name = f"Week {week_num}"
    for brand_dir in AMS_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        wk = brand_dir / folder_name
        if not wk.exists():
            continue
        for p in wk.rglob(SB_FILENAME):
            if not p.name.startswith("~"):
                found.append(p)
    return found


# ── Per-campaign attribution ───────────────────────────────────────────
def attribute_campaign(
    campaign: str,
    spend: float,
    asin_to_meta: dict,
    model_to_asins: dict,
    models_sorted: list[str],
    active: set[str],
) -> tuple[str, list[tuple[str, float]], str]:
    """Returns (layer, [(asin, spend_share), …], brand-or-empty)."""
    if not campaign or spend <= 0:
        return ("zero_spend", [], "")

    # ── Layer 1: ASIN match ──
    matches = ASIN_RE.findall(campaign.upper())
    in_master = [a for a in matches if a in asin_to_meta]
    if in_master:
        active_hits = [a for a in in_master if a in active]
        used = active_hits or in_master   # if none active, fall back to all matched (rare)
        share = spend / len(used)
        brand = asin_to_meta[used[0]]["brand"]
        return ("L1_asin", [(a, share) for a in used], brand)

    # ── Layer 2: Model match (longest first to avoid partials) ──
    cam_upper = campaign.upper()
    for model in models_sorted:
        if not model:
            continue
        pattern = r"(?<![A-Z0-9])" + re.escape(model.upper()) + r"(?![A-Z0-9])"
        if re.search(pattern, cam_upper):
            asins = model_to_asins.get(model.lower(), [])
            active_hits = [a for a in asins if a in active]
            used = active_hits
            if not used:
                # No active ASINs for this model — campaign falls through
                # to unmapped per operator rule.
                continue
            share = spend / len(used)
            brand = asin_to_meta[used[0]]["brand"]
            return ("L2_model", [(a, share) for a in used], brand)

    return ("unmapped", [], "")


# ── Main ────────────────────────────────────────────────────────────────
def main(week: int = 21) -> None:
    sb_files = find_sb_files(week)
    if not sb_files:
        print(f"⚠ No SB files for Week {week}")
        return

    asin_to_meta, model_to_asins, models_sorted = build_master_lookups()
    active = build_active_set(week)
    print(f"Master: {len(asin_to_meta):,} ASINs · {len(models_sorted):,} unique Models")
    print(f"Active ASINs (sessions/units > 0 in W{week} or prior {ACTIVE_WINDOW_WEEKS-1} wks): {len(active):,}")
    print()

    summary = {"L1_asin": {"camp": 0, "spend": 0.0, "rows_out": 0},
               "L2_model": {"camp": 0, "spend": 0.0, "rows_out": 0},
               "unmapped": {"camp": 0, "spend": 0.0, "rows_out": 0},
               "zero_spend": {"camp": 0, "spend": 0.0, "rows_out": 0}}
    examples: list[dict] = []

    for p in sb_files:
        try:
            df = pd.read_excel(p)
        except Exception:
            continue
        df.columns = df.columns.str.strip()
        if "Campaign Name" not in df.columns:
            continue
        for c in ("Spend", "14 Day Total Sales (₹)", "Impressions", "Clicks"):
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        agg = (df.groupby("Campaign Name", as_index=False)
                 .agg(Spend=("Spend", "sum"),
                      sales=("14 Day Total Sales (₹)", "sum")))

        for _, r in agg.iterrows():
            layer, alloc, brand = attribute_campaign(
                _norm(r["Campaign Name"]), float(r["Spend"]),
                asin_to_meta, model_to_asins, models_sorted, active,
            )
            summary[layer]["camp"] += 1
            summary[layer]["spend"] += float(r["Spend"])
            summary[layer]["rows_out"] += len(alloc)
            if layer in ("L1_asin", "L2_model"):
                examples.append({
                    "layer": layer, "brand": brand,
                    "campaign": _norm(r["Campaign Name"])[:60],
                    "spend": float(r["Spend"]), "asins": [a for a, _ in alloc],
                    "share_each": (float(r["Spend"]) / len(alloc)) if alloc else 0,
                })

    # ── Summary ───────────────────────────────────────────────────────
    print("=" * 70)
    total_spend = sum(d["spend"] for d in summary.values())
    print(f"Week {week}  ·  total SB spend: ₹{total_spend:,.0f}\n")
    print(f"{'Layer':<12}  {'Camps':>5}  {'Spend':>11}  {'Spend%':>6}  {'ASIN rows':>9}")
    for layer in ("L1_asin", "L2_model", "unmapped", "zero_spend"):
        d = summary[layer]
        pct = (d["spend"] / total_spend * 100) if total_spend else 0
        print(f"  {layer:<10}  {d['camp']:>5}  ₹{d['spend']:>10,.0f}  {pct:>5.1f}%  {d['rows_out']:>9}")

    print()
    print("─" * 70)
    print(f"Top {min(20, len(examples))} attributed campaigns (L1 + L2):")
    for e in sorted(examples, key=lambda x: -x["spend"])[:20]:
        asins = ", ".join(e["asins"])
        if len(e["asins"]) > 1:
            note = f"({len(e['asins'])} ASINs · ₹{e['share_each']:,.0f} each)"
        else:
            note = ""
        print(f"  ₹{e['spend']:>8,.0f}  [{e['layer']:<8}|{e['brand']:<14}]  "
              f"{e['campaign']!r:<62}  → {asins} {note}")


if __name__ == "__main__":
    import sys
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 21)
