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

import numpy as np
import pandas as pd

from weekly_app.etl._excel_safe import read_excel_safe

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
    m = read_excel_safe(MASTER)
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
    df = read_excel_safe(path)
    df.columns = df.columns.str.strip()
    # Schema bridge — SP-API Seller Sales emits title-case Amazon report
    # column names ("Units Ordered", "Ordered Product Sales", "Sessions"...)
    # while amazon_sales.xlsx (operator's Brand Analytics download) emits
    # snake_case ("units_ordered", "ordered_product_sales", "Sessions - Total"...).
    # LEGACY_COLS is the snake_case set; rename the SP-API headers in
    # place so downstream LEGACY_COLS slicing actually picks up the data.
    # Without this, 3P units/sales from SP-API silently become 0 in
    # business_report_weekN.xlsx (W26 lost ₹21 L Nexlev + ₹6 L WM + 18u Tonor 3P
    # this way until caught by the cross-route reconciliation).
    sp_api_renames = {
        "Units Ordered":         "units_ordered",
        "Ordered Product Sales": "ordered_product_sales",
        "Sessions":              "Sessions - Total",
        "Page Views":            "Page Views - Total",
    }
    df = df.rename(columns={k: v for k, v in sp_api_renames.items() if k in df.columns})
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
        df = read_excel_safe(path, sheet_name=s)
        df.columns = df.columns.str.strip()
        if "ASIN" not in df.columns:
            continue
        df["_asin_key"] = df["ASIN"].map(_norm)
        df = df[df["_asin_key"] != ""]
        for c in ("Qty", "Sale Amount"):
            if c not in df.columns:
                df[c] = 0
            # Strip currency formatting ("₹58,241.50") before parsing.
            # Some operator drops persist the cell as a currency string
            # (especially when copy-pasted from Vendor Central's display)
            # and the raw pd.to_numeric fails silently → row contributes
            # units but ₹0 sales.  W26 WM 1p Sales tripped this and the
            # AMS Trend page came out ₹1.65 L short vs Sales Trend.
            df[c] = (
                df[c].astype(str)
                     .str.replace(r"[^\d.\-]", "", regex=True)
            )
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


# ── Read SP-API Vendor Sales (1P) and aggregate per ASIN ───────────────
def load_amazon_1p_sp_api(path: Path) -> pd.DataFrame:
    """Returns {ASIN → (units, sales)} from the SP-API Vendor Sales
    file emitted by scripts/sp_vendor_sales_pull.py.  Same output shape
    as load_amazon_1p so derive_business_report's merge logic is
    unchanged.  Qty + Sale columns map to orderedUnits + orderedRevenue
    (the operator's canonical 1P "what Amazon bought" metrics)."""
    empty = pd.DataFrame(columns=["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"])
    if not path.exists():
        return empty
    try:
        df = read_excel_safe(path)
    except Exception:
        return empty
    df.columns = df.columns.str.strip()
    if "ASIN" not in df.columns or "Qty" not in df.columns:
        return empty
    df["_asin_key"] = df["ASIN"].map(_norm)
    df = df[df["_asin_key"] != ""]
    if df.empty:
        return empty
    df["units_1p"]  = pd.to_numeric(df["Qty"],  errors="coerce").fillna(0)
    df["sales_1p"]  = pd.to_numeric(df.get("Sale", 0), errors="coerce").fillna(0)
    df["sku_1p"]    = df.get("SKU", "").astype(str).str.strip()   if "SKU"   in df.columns else ""
    df["model_1p"]  = df.get("Model", "").astype(str).str.strip() if "Model" in df.columns else ""
    return (df[["_asin_key", "units_1p", "sales_1p", "sku_1p", "model_1p"]]
            .groupby("_asin_key", as_index=False)
            .agg(units_1p=("units_1p", "sum"),
                 sales_1p=("sales_1p", "sum"),
                 sku_1p=("sku_1p", "first"),
                 model_1p=("model_1p", "first")))


# ── Combine 3P + 1P into the legacy business_report shape ──────────────
def derive_business_report(brand_dir: str, week_num: int,
                            asin_meta: dict[str, dict]) -> pd.DataFrame:
    amazon_path     = RAW_SALES / f"Week {week_num}" / brand_dir / "amazon_sales.xlsx"
    other_path      = RAW_SALES / f"Week {week_num}" / brand_dir / "other_channels.xlsx"
    sp_api_path     = RAW_SALES / f"Week {week_num}" / brand_dir / "Vendor Sales (SP-API).xlsx"
    sp_seller_path  = RAW_SALES / f"Week {week_num}" / brand_dir / "Seller Sales (SP-API).xlsx"

    # 3P side: prefer operator's amazon_sales.xlsx; fall back to the
    # cron-pulled Seller Sales (SP-API).xlsx (same schema by design —
    # see scripts/sp_seller_sales_pull.OUTPUT_COLS).  Without this
    # fallback the entire AMS-Trend chain silently loses every brand's
    # 3P Amazon sales for any week the operator skipped the manual
    # amazon_sales.xlsx drop (W26 lost ₹76 L across 5 brands).
    three_source = amazon_path if amazon_path.exists() else (
        sp_seller_path if sp_seller_path.exists() else amazon_path
    )
    if three_source is sp_seller_path:
        print(f"  📡 SP-API 3P seller sales canonical for {brand_dir} W{week_num}")
    three = load_amazon_3p(three_source)

    # SP-API 1P canonical when present + non-zero; manual 1p Sales sheet
    # otherwise.  Empty SP-API must NOT silently zero out 1P (same
    # safety net as the sales / inventory ETL refactors).
    one = pd.DataFrame()
    if sp_api_path.exists():
        sp_one = load_amazon_1p_sp_api(sp_api_path)
        if not sp_one.empty and float(sp_one["units_1p"].sum()) > 0:
            one = sp_one
            print(f"  📡 SP-API 1P sales canonical for {brand_dir} W{week_num} "
                  f"({len(one)} ASINs, {int(one['units_1p'].sum())} units)")
    if one.empty:
        one = load_amazon_1p(other_path)

    if three.empty and one.empty:
        return pd.DataFrame(columns=LEGACY_COLS)

    # Outer-join on ASIN key so 1P-only ASINs still produce a row.
    merged = three.merge(one, on="_asin_key", how="outer", suffixes=("", "_o"))

    # For 1P-only ASINs the SKU/Model/Title cells from Amazon 3P are NaN
    # — fill from the 1P sheet first, then from master if still missing.
    # pandas 3.0+ raises TypeError when assigning a StringArray (with NaN)
    # into a float64 column.  Coerce the destination column to object dtype
    # first so the assignment is always type-safe across pandas versions.
    for col_dst, col_src in (("SKU", "sku_1p"), ("Model", "model_1p")):
        if col_dst in merged.columns and col_src in merged.columns:
            merged[col_dst] = merged[col_dst].astype(object)
            mask = merged[col_dst].isna() | (merged[col_dst] == "")
            merged.loc[mask, col_dst] = merged.loc[mask, col_src].astype(object)

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
    # Widen ASIN columns to `object` first — otherwise pandas 2.2+
    # refuses to setitem a StringArray into a float64 all-NaN column
    # (happens when the merge produced no non-null ASINs upstream).
    merged["(Parent) ASIN"] = merged["(Parent) ASIN"].astype("object")
    merged["(Child) ASIN"]  = merged["(Child) ASIN"].astype("object")
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
        # Skip brand folders that have NO usable sales source at all.
        # Vendor Sales (SP-API).xlsx counts as a valid 1P source even
        # when amazon_sales.xlsx and other_channels.xlsx are absent —
        # otherwise W24 would emit no business_report files for the
        # SP-API-only brands (AA + Tonor).  Seller Sales (SP-API).xlsx
        # is the 3P SP-API fallback for amazon_sales.xlsx (see
        # three_source resolution below) — missing it here means a
        # 3P-only brand (Nexlev) with only the SP-API seller pull
        # gets silently dropped from the whole business_report chain.
        # W29 lost Nexlev (₹29.5L / 909 units) this way.
        if (
            not (brand_dir / "amazon_sales.xlsx").exists()
            and not (brand_dir / "other_channels.xlsx").exists()
            and not (brand_dir / "Vendor Sales (SP-API).xlsx").exists()
            and not (brand_dir / "Seller Sales (SP-API).xlsx").exists()
        ):
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

        write_status = _write_idempotent(df, out)
        units = float(df["units_ordered"].sum())
        sales = float(df["ordered_product_sales"].sum())
        sess  = float(df["Sessions - Total"].sum())
        summary["per_brand"][brand_folder] = {
            "rows": len(df), "units": units, "sales": sales, "sessions": sess,
            "file": str(out.relative_to(ROOT)), "write": write_status,
        }
        marker = "✅" if write_status == "written" else "⏸"
        print(f"   {marker} {brand_folder:<16}  {len(df):>4} rows  "
              f"units={int(units):>6,}  sales=₹{sales:>11,.0f}  sessions={int(sess):>7,}  "
              f"[{write_status}] → {out.relative_to(ROOT)}")

    return summary


def _write_idempotent(df: pd.DataFrame, out: Path) -> str:
    """Write df to out only if the content actually changed.

    Excel xlsx writes are non-deterministic (timestamps, internal IDs)
    so comparing bytes always shows a diff even when data is identical.
    Instead we read the existing file back, normalize both sides the
    same way, and compare cell-by-cell.  Returns "written" if the file
    was touched, "unchanged" if it was already correct.
    """
    if not out.exists():
        df.to_excel(out, index=False)
        return "written"

    try:
        old = read_excel_safe(out)
    except Exception:
        # Existing file unreadable — overwrite.
        df.to_excel(out, index=False)
        return "written (prior unreadable)"

    if list(old.columns) != list(df.columns) or len(old) != len(df):
        df.to_excel(out, index=False)
        return "written"

    # Coerce both sides to the same dtype family per column so Excel's
    # text/number round-trip quirks don't trip false-positive diffs.
    same = True
    for c in df.columns:
        a = df[c]
        b = old[c]
        if pd.api.types.is_numeric_dtype(a) or pd.api.types.is_numeric_dtype(b):
            an = pd.to_numeric(a, errors="coerce").round(6)
            bn = pd.to_numeric(b, errors="coerce").round(6)
            # NaN == NaN should count as equal here.
            if not ((an == bn) | (an.isna() & bn.isna())).all():
                same = False
                break
        else:
            an = a.astype(str).str.strip().fillna("").replace({"nan": ""})
            bn = b.astype(str).str.strip().fillna("").replace({"nan": ""})
            if not (an == bn).all():
                same = False
                break

    if same:
        return "unchanged"
    df.to_excel(out, index=False)
    return "written"


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
