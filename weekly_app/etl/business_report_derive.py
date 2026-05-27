"""
Derive business_report_weekN.xlsx by combining Amazon 3P + 1P sales.

Going forward the operator stops maintaining business_report_weekN.xlsx
by hand.  This ETL derives it from the two raw sources they already
upload every week:

    Amazon 3P side  →  data/raw/sales/Week N/<Brand>/amazon_sales.xlsx
                        (the Seller-Central Business Report —
                         sessions, page views, units_ordered, sales)

    Amazon 1P side  →  data/raw/sales/Week N/<Brand>/other_channels.xlsx
                        sheet "1p Sales" (just SKU/ASIN/Model/Qty/Sale)

For every ASIN that appears in either source we emit ONE row with:

    SKU, Model, (Parent) ASIN, (Child) ASIN, Title,
    Sessions - Total + B2B,        (from Amazon 3P only)
    Session/Page-view %s,          (from Amazon 3P only)
    units_ordered = 3P + 1P,
    ordered_product_sales = 3P + 1P,
    …plus the other columns the legacy schema carries (filled in
    where Amazon 3P has data, 0 where it doesn't).

Output: data/ams_weekly_data/<Brand>/business_report_weekN.xlsx
        (legacy schema, single sheet — step1 business_ads_weekly_etl
        consumes it unchanged).
"""
from __future__ import annotations
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_SALES  = ROOT / "data" / "raw" / "sales"
AMS_ROOT   = ROOT / "data" / "ams_weekly_data"
MASTER     = ROOT / "data" / "master" / "sku_master.xlsx"

# The 1P sheet inside other_channels.xlsx (operator standard naming;
# match case-insensitively to absorb minor casing drift).
ONE_P_SHEET_CANDIDATES = {"1p sales", "1p", "amazon 1p", "amazon 1p sales"}

# Legacy business_report column order — must match what step1 expects.
LEGACY_COLS = [
    "SKU", "Model", "(Parent) ASIN", "(Child) ASIN", "Title",
    "Sessions - Total", "Sessions - Total - B2B",
    "Session Percentage - Total", "Session Percentage - Total - B2B",
    "Page Views - Total", "Page Views - Total - B2B",
    "Page Views Percentage - Total", "Page Views Percentage - Total - B2B",
    "Featured Offer Percentage", "Featured Offer Percentage - B2B",
    "units_ordered", "Units Ordered - B2B",
    "Unit Session Percentage", "Unit Session Percentage - B2B",
    "ordered_product_sales", "Ordered Product Sales - B2B",
    "Total Order Items", "Total Order Items - B2B",
]

# Numeric columns the merge needs to fill with 0 when one side is missing.
NUMERIC_COLS = [
    "Sessions - Total", "Sessions - Total - B2B",
    "Session Percentage - Total", "Session Percentage - Total - B2B",
    "Page Views - Total", "Page Views - Total - B2B",
    "Page Views Percentage - Total", "Page Views Percentage - Total - B2B",
    "Featured Offer Percentage", "Featured Offer Percentage - B2B",
    "units_ordered", "Units Ordered - B2B",
    "Unit Session Percentage", "Unit Session Percentage - B2B",
    "ordered_product_sales", "Ordered Product Sales - B2B",
    "Total Order Items", "Total Order Items - B2B",
]


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


# ── Master ASIN → (SKU, Model) lookup (used for 1P-only ASINs whose
# sessions don't appear in Amazon 3P) ──
def build_asin_meta_map() -> dict[str, dict]:
    if not MASTER.exists():
        return {}
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()
    out: dict[str, dict] = {}
    for _, r in m.iterrows():
        asin = _norm(r.get("ASIN"))
        if not asin:
            continue
        out[asin] = {
            "SKU":   _norm(r.get("FBA SKU")),
            "Model": _norm(r.get("Model")),
            "Title": _norm(r.get("Model")),  # master has no Title; use Model as best fallback
        }
        v = _norm(r.get("Variation ASINs"))
        if v:
            for x in re.split(r"[,\s/|;]+", v):
                x = x.strip()
                if x and x not in out:
                    out[x] = out[asin]
    return out


# ── Read amazon_sales (3P side) and key it by (Child) ASIN ────────────
# (Child) ASIN is the unit-of-sale identifier — same key the 1P Sales
# sheet uses in its ASIN column.  Using (Parent) ASIN here would cause
# a cross-product when a parent groups multiple children: 1P's units
# for the parent would be attributed to EACH child row.
def load_amazon_3p(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=LEGACY_COLS + ["_asin_key"])
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()
    # Add columns the legacy schema expects but the raw export might omit
    for c in LEGACY_COLS:
        if c not in df.columns:
            df[c] = 0 if c in NUMERIC_COLS else ""
    df = df[LEGACY_COLS].copy()
    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    # Key on (Child) ASIN — falls back to (Parent) only if Child is blank.
    df["_asin_key"] = df["(Child) ASIN"].map(_norm)
    blank = df["_asin_key"] == ""
    if blank.any():
        df.loc[blank, "_asin_key"] = df.loc[blank, "(Parent) ASIN"].map(_norm)
    df = df[df["_asin_key"] != ""]

    # Aggregate so each child ASIN appears at most once on this side.
    # Some SC business reports have multiple rows per ASIN (e.g. split
    # across days) — sum them up before the merge with 1P so the join
    # can't multiply units.
    text_cols = [c for c in LEGACY_COLS if c not in NUMERIC_COLS]
    agg_spec  = {c: "first" for c in text_cols}
    agg_spec.update({c: "sum" for c in NUMERIC_COLS})
    df = df.groupby("_asin_key", as_index=False).agg(agg_spec)
    return df


# ── Read other_channels' 1P sheet (1P side) and aggregate per ASIN ─────
def load_amazon_1p(path: Path) -> pd.DataFrame:
    """Returns {ASIN → (units, sales)} aggregated from the '1p Sales' sheet."""
    if not path.exists():
        return pd.DataFrame(columns=["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"])
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        return pd.DataFrame(columns=["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"])

    one_p_sheets = [
        s for s in xl.sheet_names
        if s.strip().lower() in ONE_P_SHEET_CANDIDATES
    ]
    if not one_p_sheets:
        return pd.DataFrame(columns=["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"])

    frames = []
    for s in one_p_sheets:
        df = pd.read_excel(path, sheet_name=s)
        df.columns = df.columns.str.strip()
        if "ASIN" not in df.columns:
            continue
        df["_asin_key"] = df["ASIN"].map(_norm)
        df = df[df["_asin_key"] != ""]
        for c in ("Qty", "Sale Amount"):
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        sub = df[["_asin_key", "Qty", "Sale Amount"]].copy()
        sub["sku_1p"]   = df.get("SKU", "").astype(str).str.strip() if "SKU" in df.columns else ""
        sub["model_1p"] = df.get("Model", "").astype(str).str.strip() if "Model" in df.columns else ""
        frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"])

    combined = pd.concat(frames, ignore_index=True)
    agg = (combined.groupby("_asin_key", as_index=False)
                   .agg(units_1p=("Qty", "sum"),
                        sales_1p=("Sale Amount", "sum"),
                        sku_1p=("sku_1p", "first"),
                        model_1p=("model_1p", "first")))
    return agg


# ── Combine 3P + 1P into the legacy business_report shape ──────────────
def derive_business_report(brand_dir: str, week_num: int,
                            asin_meta: dict[str, dict]) -> pd.DataFrame:
    amazon_path = RAW_SALES / f"Week {week_num}" / brand_dir / "amazon_sales.xlsx"
    other_path  = RAW_SALES / f"Week {week_num}" / brand_dir / "other_channels.xlsx"

    three = load_amazon_3p(amazon_path)
    one   = load_amazon_1p(other_path)

    if three.empty and one.empty:
        return pd.DataFrame(columns=LEGACY_COLS)

    # Outer-join on ASIN key so 1P-only ASINs still produce a row.
    merged = three.merge(one, on="_asin_key", how="outer", suffixes=("", "_o"))

    # For 1P-only ASINs the SKU/Model/Title cells from Amazon 3P are NaN
    # — fill from the 1P sheet first, then from master if still missing.
    for col_dst, col_src in (("SKU", "sku_1p"), ("Model", "model_1p")):
        if col_dst in merged.columns and col_src in merged.columns:
            mask = merged[col_dst].isna() | (merged[col_dst] == "")
            merged.loc[mask, col_dst] = merged.loc[mask, col_src]

    # Master fallback for rows still missing identifiers
    def _fill_from_master(row):
        asin = row["_asin_key"]
        if asin in asin_meta:
            meta = asin_meta[asin]
            if not row.get("SKU"):       row["SKU"]   = meta["SKU"]
            if not row.get("Model"):     row["Model"] = meta["Model"]
            if not row.get("Title"):     row["Title"] = meta["Title"]
            if not row.get("(Parent) ASIN") and not row.get("(Child) ASIN"):
                row["(Parent) ASIN"] = asin
                row["(Child) ASIN"]  = asin
        return row
    merged = merged.apply(_fill_from_master, axis=1)

    # Numeric NaNs from the outer join → 0
    for c in NUMERIC_COLS:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)
    merged["units_1p"] = pd.to_numeric(merged.get("units_1p", 0), errors="coerce").fillna(0)
    merged["sales_1p"] = pd.to_numeric(merged.get("sales_1p", 0), errors="coerce").fillna(0)

    # The headline merge: 3P + 1P → write back into the legacy fields.
    merged["units_ordered"]         = merged["units_ordered"] + merged["units_1p"]
    merged["ordered_product_sales"] = merged["ordered_product_sales"] + merged["sales_1p"]

    # If parent ASIN is still empty (1P-only row that had no master hit
    # either) fall back to the asin_key itself.
    blank = merged["(Parent) ASIN"].map(_norm) == ""
    merged.loc[blank, "(Parent) ASIN"] = merged.loc[blank, "_asin_key"]
    blank_c = merged["(Child) ASIN"].map(_norm) == ""
    merged.loc[blank_c, "(Child) ASIN"] = merged.loc[blank_c, "(Parent) ASIN"]

    # Trim text cols
    for c in ("SKU", "Model", "(Parent) ASIN", "(Child) ASIN", "Title"):
        merged[c] = merged[c].map(_norm)

    return merged[LEGACY_COLS]


# ── Main entry ─────────────────────────────────────────────────────────
def run_for_week(week_num: int) -> dict:
    summary = {"week": week_num, "per_brand": {}, "skipped": None}
    wk_dir = RAW_SALES / f"Week {week_num}"
    if not wk_dir.exists():
        summary["skipped"] = f"no raw folder at {wk_dir.relative_to(ROOT)}"
        return summary

    asin_meta = build_asin_meta_map()

    for brand_dir in sorted(p for p in wk_dir.iterdir() if p.is_dir()):
        brand_folder = brand_dir.name
        if not (brand_dir / "amazon_sales.xlsx").exists() and not (brand_dir / "other_channels.xlsx").exists():
            continue

        out_dir = AMS_ROOT / brand_folder
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"business_report_week{week_num}.xlsx"

        df = derive_business_report(brand_folder, week_num, asin_meta)
        if df.empty:
            print(f"   ⏭  {brand_folder}: no Amazon 3P or 1P data, skipped")
            continue

        if out.exists():
            bak = out.with_suffix(out.suffix + ".bak")
            if not bak.exists():
                shutil.copy2(out, bak)

        df.to_excel(out, index=False)
        units = float(df["units_ordered"].sum())
        sales = float(df["ordered_product_sales"].sum())
        sess  = float(df["Sessions - Total"].sum())
        summary["per_brand"][brand_folder] = {
            "rows": len(df), "units": units, "sales": sales, "sessions": sess,
            "file": str(out.relative_to(ROOT)),
        }
        print(f"   ✅ {brand_folder:<16}  {len(df):>4} rows  "
              f"units={int(units):>6,}  sales=₹{sales:>11,.0f}  sessions={int(sess):>7,}  "
              f"→ {out.relative_to(ROOT)}")

    return summary


def main() -> None:
    # Discover every Week N folder under raw/sales/ that exists.
    weeks: set[int] = set()
    if not RAW_SALES.exists():
        print(f"⚠ {RAW_SALES} not found")
        return
    for p in RAW_SALES.iterdir():
        if p.is_dir() and p.name.lower().startswith("week"):
            try:
                weeks.add(int(p.name.replace("Week", "").strip()))
            except Exception:
                pass

    if not weeks:
        print(f"⚠ No Week N folders under {RAW_SALES.relative_to(ROOT)}")
        return

    for wk in sorted(weeks):
        print(f"\n━━━ Week {wk} ━━━")
        s = run_for_week(wk)
        if s.get("skipped"):
            print(f"   ⏭ {s['skipped']}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import sys, traceback
        traceback.print_exc()
        sys.exit(1)
