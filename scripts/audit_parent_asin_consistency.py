"""
Parent-ASIN consistency audit across channels (Week 21).

For every (Parent) ASIN that appears in any non-Fossil brand's Week 21
amazon_sales.xlsx, this script collects:

  • from amazon_sales:  SKU / Model / (Child) ASIN / units / sales
  • from other_channels: SKU / Model / ASIN / qty / sale
  • from sku_master:     canonical FBA SKU / Original SKU / Model / Brand

And flags any disagreement on:

  1. Master mismatch — parent ASIN is in master, but the SKU or Model in
     the raw file disagrees with master's canonical row.
  2. Cross-channel mismatch — the SKU or Model assigned to the parent
     ASIN differs between amazon_sales and other_channels.
  3. Parent ≠ Child ASIN inside amazon_sales — when parent and child are
     different, master should know both as either ASIN or as a Variation
     ASIN (catches new variants that were never added to master).
  4. Orphan Parent ASIN — not present in master at all.

Fossil intentionally excluded (operator request — no AMS / 1P scope).
Output: data/processed/week21_parent_asin_consistency.xlsx
"""
from __future__ import annotations
from pathlib import Path
import re

import pandas as pd

ROOT   = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"
WEEK21 = ROOT / "data" / "raw" / "sales" / "Week 21"
OUT    = ROOT / "data" / "processed" / "week21_parent_asin_consistency.xlsx"

BRAND_DIRS = ["Audio_Array", "Nexlev", "Tonor", "White_Mulberry"]   # NO Fossil

def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()

def _df(rows, cols):
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


# ── Master lookups ───────────────────────────────────────────────────────
print("📥 Loading master…")
m = pd.read_excel(MASTER)
m.columns = m.columns.str.strip()
for c in ("FBA SKU", "Original SKU", "ASIN", "Brand", "Model", "Variation ASINs"):
    if c in m.columns:
        m[c] = m[c].map(_norm)

# {asin → master row dict}.  Variation ASINs from master are *also*
# indexed here so we can recognise child ASINs that belong to a known parent.
master_by_asin: dict[str, dict] = {}
for _, r in m.iterrows():
    primary = r.get("ASIN", "")
    rec = {
        "sku":   r.get("FBA SKU", ""),
        "model": r.get("Model", ""),
        "brand": r.get("Brand", ""),
        "primary_asin": primary,
    }
    if primary:
        master_by_asin[primary] = rec
    vstr = r.get("Variation ASINs", "")
    if vstr:
        for v in re.split(r"[,\s/|;]+", vstr):
            v = v.strip()
            if v and v not in master_by_asin:
                master_by_asin[v] = rec   # inherit parent's mapping
print(f"   master ASINs (incl. variations): {len(master_by_asin):,}\n")


# ── Load raw amazon + other for all 4 brands ─────────────────────────────
print("📥 Loading Week 21 raw sales files…")
amzn_frames, other_frames = [], []
for bdir in BRAND_DIRS:
    a = WEEK21 / bdir / "amazon_sales.xlsx"
    if a.exists():
        df = pd.read_excel(a)
        df.columns = df.columns.str.strip()
        df["__brand_folder"] = bdir.replace("_", " ")
        amzn_frames.append(df)
    o = WEEK21 / bdir / "other_channels.xlsx"
    if o.exists():
        df = pd.read_excel(o)
        df.columns = df.columns.str.strip()
        df["__brand_folder"] = bdir.replace("_", " ")
        other_frames.append(df)

amzn  = pd.concat(amzn_frames,  ignore_index=True) if amzn_frames  else pd.DataFrame()
other = pd.concat(other_frames, ignore_index=True) if other_frames else pd.DataFrame()
print(f"   amazon_sales rows:    {len(amzn):,}")
print(f"   other_channels rows:  {len(other):,}\n")

# Normalise key columns
for df in (amzn, other):
    for c in ("SKU", "Model", "ASIN", "(Parent) ASIN", "(Child) ASIN"):
        if c in df.columns:
            df[c] = df[c].map(_norm)


# ── Build (parent_asin → {sku, model}) for each source ───────────────────
def _pivot_amzn(df: pd.DataFrame) -> dict:
    bag = {}
    for _, r in df.iterrows():
        p = r.get("(Parent) ASIN", "")
        c = r.get("(Child) ASIN", "")
        if not p:
            continue
        key = p
        sku  = r.get("SKU", "")
        mdl  = r.get("Model", "")
        rec  = bag.setdefault(key, {"skus": set(), "models": set(),
                                    "children": set(), "brand_folder": r.get("__brand_folder", "")})
        if sku: rec["skus"].add(sku)
        if mdl: rec["models"].add(mdl)
        if c:   rec["children"].add(c)
    return bag

def _pivot_other(df: pd.DataFrame) -> dict:
    bag = {}
    for _, r in df.iterrows():
        a = r.get("ASIN", "")
        if not a:
            continue
        sku = r.get("SKU", "")
        mdl = r.get("Model", "")
        rec = bag.setdefault(a, {"skus": set(), "models": set(),
                                 "brand_folder": r.get("__brand_folder", "")})
        if sku: rec["skus"].add(sku)
        if mdl: rec["models"].add(mdl)
    return bag

amzn_by_pasin  = _pivot_amzn(amzn)
other_by_asin  = _pivot_other(other)
print(f"   distinct Parent ASINs in amzn: {len(amzn_by_pasin):,}")
print(f"   distinct ASINs in other:       {len(other_by_asin):,}\n")


# ── Audits ───────────────────────────────────────────────────────────────
master_mismatch = []
cross_channel   = []
parent_child    = []
orphan_parents  = []

for pasin, rec in amzn_by_pasin.items():
    amzn_skus   = rec["skus"]
    amzn_models = rec["models"]
    amzn_childs = rec["children"]
    brand_folder= rec["brand_folder"]

    # 4 — orphan parent (not in master at all, even as a variation)
    if pasin not in master_by_asin:
        # If at least one child is mapped, that's a separate finding.
        orphan_parents.append({
            "Parent ASIN":   pasin,
            "Brand folder":  brand_folder,
            "Amzn SKUs":     ", ".join(sorted(amzn_skus)) if amzn_skus else "—",
            "Amzn Models":   ", ".join(sorted(amzn_models)) if amzn_models else "—",
            "Amzn Children": ", ".join(sorted(amzn_childs)) if amzn_childs else "—",
            "Children in master?": ", ".join(
                [c for c in sorted(amzn_childs) if c in master_by_asin]
            ) or "—",
        })
        continue

    # 1 — master mismatch
    mrec = master_by_asin[pasin]
    m_sku, m_model = mrec["sku"], mrec["model"]
    sku_clash   = amzn_skus   and m_sku   and m_sku   not in amzn_skus
    model_clash = amzn_models and m_model and m_model.lower() not in {x.lower() for x in amzn_models}
    if sku_clash or model_clash:
        master_mismatch.append({
            "Parent ASIN":   pasin,
            "Brand folder":  brand_folder,
            "Amzn SKU(s)":   ", ".join(sorted(amzn_skus)),
            "Master SKU":    m_sku,
            "Amzn Model(s)": ", ".join(sorted(amzn_models)),
            "Master Model":  m_model,
            "Mismatch on":   ", ".join(filter(None, [
                "SKU"   if sku_clash   else "",
                "Model" if model_clash else "",
            ])),
        })

    # 3 — parent ≠ child & child not in master
    unknown_children = [c for c in amzn_childs if c != pasin and c not in master_by_asin]
    if unknown_children:
        parent_child.append({
            "Parent ASIN":      pasin,
            "Brand folder":     brand_folder,
            "Master SKU":       m_sku,
            "Master Model":     m_model,
            "Unknown children": ", ".join(unknown_children),
        })

    # 2 — cross-channel comparison (only when present in other_channels too)
    if pasin in other_by_asin:
        orec        = other_by_asin[pasin]
        other_skus  = orec["skus"]
        other_models= orec["models"]
        sku_diff    = (amzn_skus   and other_skus   and amzn_skus.isdisjoint(other_skus))
        model_diff  = (amzn_models and other_models and
                       {x.lower() for x in amzn_models}.isdisjoint({x.lower() for x in other_models}))
        if sku_diff or model_diff:
            cross_channel.append({
                "Parent ASIN":     pasin,
                "Brand folder":    brand_folder,
                "Amzn SKU(s)":     ", ".join(sorted(amzn_skus)),
                "Other SKU(s)":    ", ".join(sorted(other_skus)),
                "Amzn Model(s)":   ", ".join(sorted(amzn_models)),
                "Other Model(s)":  ", ".join(sorted(other_models)),
                "Mismatch on":     ", ".join(filter(None, [
                    "SKU"   if sku_diff   else "",
                    "Model" if model_diff else "",
                ])),
            })


# Also scan other_channels for ASINs that ARE in master but disagree on SKU/Model
master_mismatch_other = []
for asin, rec in other_by_asin.items():
    if asin not in master_by_asin:
        continue
    mrec   = master_by_asin[asin]
    m_sku, m_model = mrec["sku"], mrec["model"]
    sku_clash   = rec["skus"]   and m_sku   and m_sku   not in rec["skus"]
    model_clash = rec["models"] and m_model and m_model.lower() not in {x.lower() for x in rec["models"]}
    if sku_clash or model_clash:
        master_mismatch_other.append({
            "ASIN":             asin,
            "Brand folder":     rec["brand_folder"],
            "Other SKU(s)":     ", ".join(sorted(rec["skus"])),
            "Master SKU":       m_sku,
            "Other Model(s)":   ", ".join(sorted(rec["models"])),
            "Master Model":     m_model,
            "Mismatch on":      ", ".join(filter(None, [
                "SKU"   if sku_clash   else "",
                "Model" if model_clash else "",
            ])),
        })


# ── Summary ──────────────────────────────────────────────────────────────
summary = [
    {"Category": "Orphan Parent ASIN (not in master)",       "Count": len(orphan_parents),
     "Action": "Add to master, OR fix the SKU/ASIN in raw file"},
    {"Category": "Amzn vs Master mismatch (SKU/Model)",      "Count": len(master_mismatch),
     "Action": "Reconcile master OR re-export amazon_sales"},
    {"Category": "Other vs Master mismatch (SKU/Model)",     "Count": len(master_mismatch_other),
     "Action": "Reconcile master OR re-export other_channels"},
    {"Category": "Amzn vs Other cross-channel mismatch",     "Count": len(cross_channel),
     "Action": "Pick one canonical identifier per Parent ASIN"},
    {"Category": "Unknown child ASINs (variant gap)",        "Count": len(parent_child),
     "Action": "Add the child ASIN to master Variation ASINs"},
]

print("─" * 65)
for s in summary:
    flag = "⚠ " if s["Count"] > 0 else "✓ "
    print(f"  {flag}{s['Category']:<48}  {s['Count']:>4}")
print("─" * 65)

# ── Excel ────────────────────────────────────────────────────────────────
OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    _df(summary, ["Category", "Count", "Action"]).to_excel(w, sheet_name="Summary", index=False)
    _df(orphan_parents,
        ["Parent ASIN", "Brand folder", "Amzn SKUs", "Amzn Models",
         "Amzn Children", "Children in master?"]
       ).to_excel(w, sheet_name="1 Orphan parents", index=False)
    _df(master_mismatch,
        ["Parent ASIN", "Brand folder", "Mismatch on",
         "Amzn SKU(s)", "Master SKU", "Amzn Model(s)", "Master Model"]
       ).to_excel(w, sheet_name="2 Amzn vs Master", index=False)
    _df(master_mismatch_other,
        ["ASIN", "Brand folder", "Mismatch on",
         "Other SKU(s)", "Master SKU", "Other Model(s)", "Master Model"]
       ).to_excel(w, sheet_name="3 Other vs Master", index=False)
    _df(cross_channel,
        ["Parent ASIN", "Brand folder", "Mismatch on",
         "Amzn SKU(s)", "Other SKU(s)", "Amzn Model(s)", "Other Model(s)"]
       ).to_excel(w, sheet_name="4 Amzn vs Other", index=False)
    _df(parent_child,
        ["Parent ASIN", "Brand folder", "Master SKU", "Master Model",
         "Unknown children"]
       ).to_excel(w, sheet_name="5 Unknown children", index=False)

print(f"\n📊 Report: {OUT.relative_to(ROOT)}")
