"""
Generate a fix-list for the raw Week 21 sales files.

Master is the source of truth.  For every (Parent) ASIN in amazon_sales
and every ASIN in other_channels, look it up in sku_master.  Where the
raw file's SKU or Model disagrees with master, emit a row in the fix
list showing exactly what should be edited in the raw file.

Output: data/processed/week21_raw_sales_fix_list.xlsx
"""
from __future__ import annotations
from pathlib import Path
import re

import pandas as pd

ROOT   = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"
WEEK21 = ROOT / "data" / "raw" / "sales" / "Week 21"
OUT    = ROOT / "data" / "processed" / "week21_raw_sales_fix_list.xlsx"

BRAND_DIRS = ["Audio_Array", "Nexlev", "Tonor", "White_Mulberry"]   # no Fossil

def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Master: build ASIN → canonical (sku, model, brand) ───────────────────
m = pd.read_excel(MASTER)
m.columns = m.columns.str.strip()
for c in ("FBA SKU", "ASIN", "Brand", "Model", "Variation ASINs"):
    if c in m.columns:
        m[c] = m[c].map(_norm)

master_by_asin: dict[str, dict] = {}
for _, r in m.iterrows():
    primary = r.get("ASIN", "")
    rec = {
        "sku":          r.get("FBA SKU", ""),
        "model":        r.get("Model", ""),
        "brand":        r.get("Brand", ""),
        "primary_asin": primary,
    }
    if primary:
        master_by_asin[primary] = rec
    vstr = r.get("Variation ASINs", "")
    if vstr:
        for v in re.split(r"[,\s/|;]+", vstr):
            v = v.strip()
            if v and v not in master_by_asin:
                master_by_asin[v] = rec


# ── Walk every row of amazon_sales (per brand) ───────────────────────────
amzn_fixes  = []
amzn_orphan = []
for bdir in BRAND_DIRS:
    p = WEEK21 / bdir / "amazon_sales.xlsx"
    if not p.exists():
        continue
    df = pd.read_excel(p)
    df.columns = df.columns.str.strip()
    for idx, r in df.iterrows():
        p_asin = _norm(r.get("(Parent) ASIN"))
        c_asin = _norm(r.get("(Child) ASIN"))
        raw_sku   = _norm(r.get("SKU"))
        raw_model = _norm(r.get("Model"))

        if not p_asin and not c_asin:
            continue

        # Prefer parent ASIN for the lookup; fall back to child if parent missing
        lookup_asin = p_asin or c_asin
        mrec = master_by_asin.get(lookup_asin) or (master_by_asin.get(c_asin) if c_asin else None)

        if mrec is None:
            amzn_orphan.append({
                "Brand folder":    bdir.replace("_", " "),
                "Excel row #":     int(idx) + 2,
                "Parent ASIN":     p_asin,
                "Child ASIN":      c_asin,
                "Raw SKU":         raw_sku,
                "Raw Model":       raw_model,
                "Action":          "Add this Parent/Child ASIN to master, OR fix the ASIN in raw if wrong",
            })
            continue

        sku_wrong   = bool(raw_sku)   and raw_sku   != mrec["sku"]
        model_wrong = bool(raw_model) and raw_model.lower() != mrec["model"].lower()
        if not (sku_wrong or model_wrong):
            continue

        amzn_fixes.append({
            "Brand folder":   bdir.replace("_", " "),
            "Excel row #":    int(idx) + 2,                # +2 = 1-based row + header
            "Parent ASIN":    p_asin,
            "Child ASIN":     c_asin,
            "Wrong on":       ", ".join(filter(None, ["SKU" if sku_wrong else "", "Model" if model_wrong else ""])),
            "Raw SKU":        raw_sku,
            "→ Should be":    mrec["sku"]   if sku_wrong   else "",
            "Raw Model":      raw_model,
            "→ Should be ":   mrec["model"] if model_wrong else "",
            "Master Brand":   mrec["brand"],
        })


# ── Walk every row of other_channels (per brand) ─────────────────────────
other_fixes  = []
other_orphan = []
for bdir in BRAND_DIRS:
    p = WEEK21 / bdir / "other_channels.xlsx"
    if not p.exists():
        continue
    df = pd.read_excel(p)
    df.columns = df.columns.str.strip()
    for idx, r in df.iterrows():
        asin      = _norm(r.get("ASIN"))
        raw_sku   = _norm(r.get("SKU"))
        raw_model = _norm(r.get("Model"))

        if not asin:
            continue

        mrec = master_by_asin.get(asin)
        if mrec is None:
            other_orphan.append({
                "Brand folder":  bdir.replace("_", " "),
                "Excel row #":   int(idx) + 2,
                "ASIN":          asin,
                "Raw SKU":       raw_sku,
                "Raw Model":     raw_model,
                "Action":        "Add this ASIN to master, OR fix the ASIN in raw if wrong",
            })
            continue

        sku_wrong   = bool(raw_sku)   and raw_sku   != mrec["sku"]
        model_wrong = bool(raw_model) and raw_model.lower() != mrec["model"].lower()
        if not (sku_wrong or model_wrong):
            continue

        other_fixes.append({
            "Brand folder":   bdir.replace("_", " "),
            "Excel row #":    int(idx) + 2,
            "ASIN":           asin,
            "Wrong on":       ", ".join(filter(None, ["SKU" if sku_wrong else "", "Model" if model_wrong else ""])),
            "Raw SKU":        raw_sku,
            "→ Should be":    mrec["sku"]   if sku_wrong   else "",
            "Raw Model":      raw_model,
            "→ Should be ":   mrec["model"] if model_wrong else "",
            "Master Brand":   mrec["brand"],
        })


# ── Summary + Excel ──────────────────────────────────────────────────────
summary = [
    {"File":   "amazon_sales (4 brands)",
     "Mismatches": len(amzn_fixes),
     "Orphan ASINs (need master add)": len(amzn_orphan)},
    {"File":   "other_channels (4 brands)",
     "Mismatches": len(other_fixes),
     "Orphan ASINs (need master add)": len(other_orphan)},
]
print(pd.DataFrame(summary).to_string(index=False))
print()

OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    pd.DataFrame(summary).to_excel(w, sheet_name="Summary", index=False)
    (pd.DataFrame(amzn_fixes) if amzn_fixes else pd.DataFrame(
        columns=["Brand folder", "Excel row #", "Parent ASIN", "Child ASIN",
                 "Wrong on", "Raw SKU", "→ Should be", "Raw Model", "→ Should be ", "Master Brand"]
    )).to_excel(w, sheet_name="1 amazon_sales fixes", index=False)
    (pd.DataFrame(other_fixes) if other_fixes else pd.DataFrame(
        columns=["Brand folder", "Excel row #", "ASIN", "Wrong on",
                 "Raw SKU", "→ Should be", "Raw Model", "→ Should be ", "Master Brand"]
    )).to_excel(w, sheet_name="2 other_channels fixes", index=False)
    (pd.DataFrame(amzn_orphan) if amzn_orphan else pd.DataFrame(
        columns=["Brand folder", "Excel row #", "Parent ASIN", "Child ASIN",
                 "Raw SKU", "Raw Model", "Action"]
    )).to_excel(w, sheet_name="3 amazon_sales orphans", index=False)
    (pd.DataFrame(other_orphan) if other_orphan else pd.DataFrame(
        columns=["Brand folder", "Excel row #", "ASIN", "Raw SKU", "Raw Model", "Action"]
    )).to_excel(w, sheet_name="4 other_channels orphans", index=False)

print(f"📊 Fix list: {OUT.relative_to(ROOT)}")
