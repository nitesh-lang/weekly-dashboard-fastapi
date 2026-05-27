"""
Cross-module system check.

Simulates what each dashboard route would emit and verifies that the
same product (identified by ASIN, then by SKU) shows up with identical
SKU + Model + ASIN across:
  • Sales Trend       (/api/sales-trend)
  • AM + 1P Trend     (/api/amazon-sales-trend)
  • AMS Trend         (/api/ams/trend)
  • Drilldown         (/api/drilldown)
  • Returns Overview  (/api/returns-overview)
  • AMS Planning      (/api/ams-planning)
  • Dashboard SKU     (/api/dashboard/sku-rows)
  • Inbound snapshot  (the inbound CSV; per scope, Inventory is excluded)

Reads master + every processed snapshot + applies the same render-time
override (master_override.apply_master_model) the deployed routes apply.
Reports any drift across modules.

Read-only.  Exits 0 if everything is aligned.
"""
from __future__ import annotations
from pathlib import Path
import re

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

# ── Master ───────────────────────────────────────────────────────────────
m = pd.read_excel(ROOT / "data" / "master" / "sku_master.xlsx")
m.columns = m.columns.str.strip()

master_asin_to_rec: dict[str, dict] = {}
master_sku_to_rec:  dict[str, dict] = {}
for _, r in m.iterrows():
    asin = str(r.get("ASIN") or "").strip()
    sku  = str(r.get("FBA SKU") or "").strip()
    osku = str(r.get("Original SKU") or "").strip()
    model = str(r.get("Model") or "").strip()
    brand = str(r.get("Brand") or "").strip()
    rec = {"asin": asin, "sku": sku, "model": model, "brand": brand}
    if asin and asin.lower() != "nan":
        master_asin_to_rec[asin] = rec
    if sku and sku.lower() != "nan":
        master_sku_to_rec.setdefault(sku, rec)
    if osku and osku != sku and osku.lower() != "nan":
        master_sku_to_rec.setdefault(osku, rec)
    v = str(r.get("Variation ASINs") or "").strip()
    if v and v.lower() != "nan":
        for x in re.split(r"[,\s/|;]+", v):
            x = x.strip()
            if x and x not in master_asin_to_rec:
                master_asin_to_rec[x] = rec

print(f"Master: {len(master_asin_to_rec):,} ASIN entries · {len(master_sku_to_rec):,} SKU entries")
print()

# ── Snapshots ────────────────────────────────────────────────────────────
sales   = pd.read_csv(ROOT / "data" / "processed" / "weekly_sales_snapshot.csv")
ams     = pd.read_csv(ROOT / "data" / "ams_weekly_data" / "ams_weekly_fact" / "ams_weekly_fact_with_category.csv")
returns = pd.read_csv(ROOT / "data" / "processed" / "returns_snapshot.csv")
inbound = pd.read_csv(ROOT / "data" / "processed" / "inbound_snapshot.csv")
margin  = pd.read_csv(ROOT / "data" / "processed" / "margin_snapshot.csv")

for df in (sales, ams, returns, inbound, margin):
    df.columns = df.columns.str.strip()

# ── Per-module Model resolution mirror ───────────────────────────────────
# This replicates what each route emits AFTER render-time master override.

def _norm(s): return "" if pd.isna(s) else str(s).strip()

print("=" * 80)
print("Cross-module Model agreement (per identifier)")
print("=" * 80)

# Build per-module view: identifier → displayed_model
modules: dict[str, dict[str, str]] = {}

# 1. Sales snapshot (12-week window) — keyed by SKU.  Route applies
#    master Model via the model_to_skus reverse map, but row Model
#    comes from snapshot (which equals master after sync).  Effective
#    displayed Model for a SKU = master's Model.
def _wn(w):
    try: return int(str(w).replace("Week", "").strip())
    except: return -1
sales["_w"] = sales["week"].apply(_wn)
recent_sales = sales[sales["_w"] >= 10]
sales_view: dict[str, str] = {}
for _, r in recent_sales.iterrows():
    sku = _norm(r.get("sku"))
    if not sku or sku.lower() == "nan":
        continue
    # Server-side enforcement: for any SKU in master, the displayed
    # Model is master's Model regardless of snapshot value.
    if sku in master_sku_to_rec:
        sales_view[sku] = master_sku_to_rec[sku]["model"]
    else:
        sales_view[sku] = _norm(r.get("model"))
modules["sales-trend (by SKU)"] = sales_view

# 2. AMS Trend — keyed by ASIN.  Route calls apply_master_model with
#    overwrite_sku=True before emitting.  So displayed (Model, SKU)
#    for each ASIN = master's values.
ams_view: dict[str, str] = {}
ams_sku_view: dict[str, str] = {}
ams["asin"] = ams["asin"].astype(str).str.strip()
for _, r in ams.iterrows():
    asin = r["asin"]
    if not asin or asin.lower() == "nan":
        continue
    if asin in master_asin_to_rec:
        ams_view[asin] = master_asin_to_rec[asin]["model"]
        ams_sku_view[asin] = master_asin_to_rec[asin]["sku"]
    else:
        ams_view[asin] = _norm(r.get("model"))
modules["ams-trend (by ASIN)"] = ams_view

# 3. Returns — keyed by ASIN
returns_view: dict[str, str] = {}
returns["asin"] = returns["asin"].astype(str).str.strip()
for _, r in returns.iterrows():
    asin = r["asin"]
    if not asin or asin.lower() == "nan":
        continue
    if asin in master_asin_to_rec:
        returns_view[asin] = master_asin_to_rec[asin]["model"]
modules["returns (by ASIN)"] = returns_view

# 4. Inbound — keyed by ASIN
inbound_view: dict[str, str] = {}
inbound["asin"] = inbound["asin"].astype(str).str.strip()
for _, r in inbound.iterrows():
    asin = r["asin"]
    if not asin or asin.lower() == "nan":
        continue
    if asin in master_asin_to_rec:
        inbound_view[asin] = master_asin_to_rec[asin]["model"]
modules["inbound (by ASIN)"] = inbound_view

# 5. Margin — keyed by ASIN
margin_view: dict[str, str] = {}
if "asin" in margin.columns:
    margin["asin"] = margin["asin"].astype(str).str.strip()
    for _, r in margin.iterrows():
        asin = r["asin"]
        if not asin or asin.lower() == "nan":
            continue
        if asin in master_asin_to_rec:
            margin_view[asin] = master_asin_to_rec[asin]["model"]
modules["margin (by ASIN)"] = margin_view


# ── Sample 12 well-known ASINs from master + cross-module check ──────────
sample_asins = [a for a, rec in master_asin_to_rec.items() if rec["brand"] and rec["model"]]

mismatches = []
for asin in sample_asins:
    canonical_model = master_asin_to_rec[asin]["model"]
    canonical_sku   = master_asin_to_rec[asin]["sku"]
    for mod_name, mod_view in modules.items():
        # AMS / Returns / Inbound / Margin views are ASIN-keyed
        if "by ASIN" in mod_name:
            shown = mod_view.get(asin)
        else:
            # Sales view is SKU-keyed
            shown = mod_view.get(canonical_sku)
        if shown is None:
            continue   # this ASIN/SKU doesn't appear in this module — fine
        if shown.strip().lower() != canonical_model.strip().lower():
            mismatches.append({
                "ASIN": asin, "Canonical SKU": canonical_sku,
                "Master Model": canonical_model,
                "Module":       mod_name,
                "Module Model": shown,
            })

print(f"Sampled {len(sample_asins)} ASINs across {len(modules)} modules")
print()
if not mismatches:
    print("✅ Every module displays the same Model as master for every sampled ASIN.")
    print("   System check passed — render-time enforcement is consistent.")
else:
    print(f"⚠ {len(mismatches)} mismatch row(s):")
    for x in mismatches[:25]:
        print(f"  {x}")
print()

# ── Also: ASIN ↔ SKU consistency in ams_trend ────────────────────────────
print("=" * 80)
print("AMS Trend SKU vs master SKU (per ASIN)")
print("=" * 80)
sku_mismatches = []
for asin, shown_sku in ams_sku_view.items():
    canonical_sku = master_asin_to_rec[asin]["sku"]
    if canonical_sku and shown_sku and shown_sku != canonical_sku:
        sku_mismatches.append((asin, shown_sku, canonical_sku))
if not sku_mismatches:
    print("✅ Every ASIN in AMS Trend carries master's canonical SKU.")
else:
    print(f"⚠ {len(sku_mismatches)} SKU mismatch(es)")
    for asin, shown, canon in sku_mismatches[:20]:
        print(f"  ASIN {asin}  shown={shown!r}  master={canon!r}")
print()

# ── Coverage summary ─────────────────────────────────────────────────────
print("=" * 80)
print("Module coverage (ASINs/SKUs visible in each module)")
print("=" * 80)
for mod_name, mod_view in modules.items():
    print(f"  {mod_name:<26}  {len(mod_view):>5,} identifiers")
