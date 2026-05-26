"""
Investigate the 22 models that dropped to ₹0 in Week 21 sales.

For each suspect (brand + model), pulls every identifier the master
knows (SKU, Original SKU, ASIN, Variation ASINs) and then searches
the Week 21 raw sales files (amazon_sales.xlsx + other_channels.xlsx)
to figure out which of these three cases applies:

  CASE A — Truly dropped:  not present under any identifier.
                           Stock-out / discontinued / catalog removal.

  CASE B — Present in raw under a DIFFERENT identifier (master
                           rename or a SKU swap happened mid-week and
                           the join broke).

  CASE C — Present with sales > 0 but ETL still emitted 0:
                           genuine pipeline bug.

Output: data/processed/week21_sales_drop_investigation.xlsx
"""
from __future__ import annotations
from pathlib import Path
import re

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"
WEEK21 = ROOT / "data" / "raw" / "sales" / "Week 21"
OUT    = ROOT / "data" / "processed" / "week21_sales_drop_investigation.xlsx"

DROPS = [
    ("Audio Array",    "UB-01 (AI-04, AM-S1, AA-21, AM-C2)"),
    ("White Mulberry", "WM-MDL-BK"),
    ("Nexlev",         "ST-03"),
    ("Tonor",          "TD510"),
    ("Audio Array",    "GB-01 (AM-C43 +AI-10)"),
    ("White Mulberry", "HDGS1ML"),
    ("Tonor",          "D5"),
    ("Audio Array",    "AM-C11X"),
    ("Nexlev",         "VC-04"),
    ("Audio Array",    "AM-W33"),
    ("Audio Array",    "AK-37-W"),
    ("Audio Array",    "AM-W22 Duplicate"),
    ("Tonor",          "TC40S"),
    ("Nexlev",         "TIC-04"),
    ("Audio Array",    "AI_13"),
    ("Audio Array",    "AM-C47 Duplicate"),
    ("Tonor",          "TM310"),
    ("Audio Array",    "AM-C7"),
    ("Nexlev",         "GS-04"),
    ("Nexlev",         "VC-03 HEPA"),
    ("Nexlev",         "ETC-08-BL"),
    ("Audio Array",    "AM-C13"),
]

BRAND_DIR = {
    "Audio Array":    "Audio_Array",
    "Nexlev":         "Nexlev",
    "Tonor":          "Tonor",
    "White Mulberry": "White_Mulberry",
}


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()

def _norm_lc(s) -> str:
    return _norm(s).lower()


# ── Master lookup ────────────────────────────────────────────────────────
m = pd.read_excel(MASTER)
m.columns = m.columns.str.strip()
for c in ("FBA SKU", "Original SKU", "ASIN", "Brand", "Model", "Variation ASINs"):
    if c in m.columns:
        m[c] = m[c].map(_norm)


def master_identifiers(brand: str, model: str) -> dict:
    sub = m[(m["Brand"].str.strip().str.lower() == brand.lower())
            & (m["Model"].str.strip().str.lower() == model.lower())]
    if sub.empty:
        return {"rows_in_master": 0, "skus": set(), "asins": set()}
    skus, asins = set(), set()
    for _, r in sub.iterrows():
        for c in ("FBA SKU", "Original SKU"):
            v = _norm(r.get(c))
            if v:
                skus.add(v)
        for c in ("ASIN",):
            v = _norm(r.get(c))
            if v:
                asins.add(v)
        # Variation ASINs is a comma/space-separated string in master
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x:
                    asins.add(x)
    return {"rows_in_master": len(sub), "skus": skus, "asins": asins}


# ── Raw Week 21 sales loaders ────────────────────────────────────────────
def load_amazon(brand_dir: str) -> pd.DataFrame:
    p = WEEK21 / brand_dir / "amazon_sales.xlsx"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_excel(p)
    df.columns = df.columns.str.strip()
    return df

def load_other(brand_dir: str) -> pd.DataFrame:
    p = WEEK21 / brand_dir / "other_channels.xlsx"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_excel(p)
    df.columns = df.columns.str.strip()
    return df


# ── Search a raw frame for any of the master identifiers ─────────────────
def find_in_raw(df: pd.DataFrame, model: str, skus: set, asins: set) -> dict:
    """Returns {matched_rows: int, by_model: int, by_sku: int, by_asin: int,
                  units: float, sales: float, matched_keys: list}"""
    if df.empty:
        return {"matched_rows": 0, "by_model": 0, "by_sku": 0, "by_asin": 0,
                "units": 0, "sales": 0, "matched_keys": []}

    # Locate columns (vary across amazon_sales vs other_channels)
    model_col = next((c for c in ("Model",) if c in df.columns), None)
    sku_col   = next((c for c in ("SKU",) if c in df.columns), None)
    asin_cols = [c for c in ("ASIN", "(Parent) ASIN", "(Child) ASIN") if c in df.columns]
    units_col = next((c for c in ("units_ordered", "Qty") if c in df.columns), None)
    sales_col = next((c for c in ("ordered_product_sales", "Sale Amount") if c in df.columns), None)

    mask_total = pd.Series(False, index=df.index)
    by_model = 0; by_sku = 0; by_asin = 0
    matched_keys = []

    if model_col:
        m_match = df[model_col].astype(str).str.strip().str.lower() == model.lower()
        by_model = int(m_match.sum())
        if by_model:
            matched_keys.append(f"Model={model}")
        mask_total = mask_total | m_match

    if sku_col and skus:
        s_match = df[sku_col].astype(str).str.strip().isin(skus)
        by_sku = int(s_match.sum())
        if by_sku:
            matched_keys.append(f"SKU∈{sorted(skus)[:3]}{'…' if len(skus) > 3 else ''}")
        mask_total = mask_total | s_match

    for ac in asin_cols:
        if not asins:
            continue
        a_match = df[ac].astype(str).str.strip().isin(asins)
        n = int(a_match.sum())
        if n:
            by_asin += n
            matched_keys.append(f"{ac}∈{sorted(asins)[:3]}{'…' if len(asins) > 3 else ''}")
        mask_total = mask_total | a_match

    matched = df[mask_total]
    units = float(pd.to_numeric(matched[units_col], errors="coerce").fillna(0).sum()) if units_col else 0.0
    sales = float(pd.to_numeric(matched[sales_col], errors="coerce").fillna(0).sum()) if sales_col else 0.0
    return {"matched_rows": int(len(matched)), "by_model": by_model, "by_sku": by_sku,
            "by_asin": by_asin, "units": units, "sales": sales,
            "matched_keys": " | ".join(matched_keys)}


# ── Investigate each suspect ─────────────────────────────────────────────
report_rows = []
for brand, model in DROPS:
    ids = master_identifiers(brand, model)
    bdir = BRAND_DIR.get(brand)
    if not bdir:
        continue
    amzn  = find_in_raw(load_amazon(bdir),  model, ids["skus"], ids["asins"])
    other = find_in_raw(load_other(bdir),   model, ids["skus"], ids["asins"])

    total_sales = amzn["sales"] + other["sales"]
    total_units = amzn["units"] + other["units"]

    # Classify
    if ids["rows_in_master"] == 0:
        verdict = "MASTER MISSING"
    elif amzn["matched_rows"] == 0 and other["matched_rows"] == 0:
        verdict = "A · TRUE DROP — absent from raw"
    elif total_sales > 0 or total_units > 0:
        verdict = "C · ETL BUG — raw has sales, snapshot says 0"
    else:
        verdict = "B · Raw row present, zero sales (catalog only)"

    report_rows.append({
        "Brand":             brand,
        "Model":             model,
        "Verdict":           verdict,
        "Master rows":       ids["rows_in_master"],
        "Master SKUs":       ", ".join(sorted(ids["skus"])) if ids["skus"] else "—",
        "Master ASINs":      ", ".join(sorted(ids["asins"])) if ids["asins"] else "—",
        "Amzn raw rows":     amzn["matched_rows"],
        "Amzn units":        round(amzn["units"], 2),
        "Amzn sales (₹)":    round(amzn["sales"], 2),
        "Amzn matched via":  amzn["matched_keys"],
        "Other raw rows":    other["matched_rows"],
        "Other units":       round(other["units"], 2),
        "Other sales (₹)":   round(other["sales"], 2),
        "Other matched via": other["matched_keys"],
    })


# ── Print + write Excel ──────────────────────────────────────────────────
print(f"{'Brand':<16} {'Model':<40} {'Verdict':<48} Amzn  Other")
print("-" * 130)
for r in report_rows:
    print(f"{r['Brand']:<16} {r['Model'][:40]:<40} {r['Verdict']:<48} "
          f"{r['Amzn raw rows']:>4} ₹{r['Amzn sales (₹)']:>8.0f}  "
          f"{r['Other raw rows']:>3} ₹{r['Other sales (₹)']:>8.0f}")

# Sheet 2: master detail per dropped model (so the operator can copy IDs)
detail_rows = []
for brand, model in DROPS:
    sub = m[(m["Brand"].str.strip().str.lower() == brand.lower())
            & (m["Model"].str.strip().str.lower() == model.lower())]
    for _, r in sub.iterrows():
        detail_rows.append({
            "Brand":           brand,
            "Model":           model,
            "FBA SKU":         r.get("FBA SKU", ""),
            "Original SKU":    r.get("Original SKU", ""),
            "ASIN":            r.get("ASIN", ""),
            "Variation ASINs": r.get("Variation ASINs", ""),
            "BAU":             r.get("BAU", ""),
            "ASIN Type":       r.get("ASIN Type", ""),
        })

OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    pd.DataFrame(report_rows).to_excel(w, sheet_name="Investigation", index=False)
    pd.DataFrame(detail_rows).to_excel(w, sheet_name="Master detail",  index=False)

print(f"\n📊 Report: {OUT.relative_to(ROOT)}")
