"""
Margin snapshot ETL — reads per-brand Excel exports from
`data/raw/margin/*.xlsx` and produces a single tidy CSV at
`data/processed/margin_snapshot.csv`.

Input expectation (one file per brand, brand name == filename without .xlsx):
    data/raw/margin/Audio Array.xlsx
    data/raw/margin/Nexlev.xlsx
    data/raw/margin/White Mulberry.xlsx
    ...

Each file has a sheet named "Margin Data" with 42 cols.  We keep the
operationally meaningful columns and drop the rest.

Fossil has no margin file — silently skipped if absent.

Run standalone:
    python weekly_app/etl/margin_snapshot.py

Or via the unified runner:
    python scripts/run_weekly_etl.py margin
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from weekly_app.etl._excel_safe import read_excel_safe

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = ROOT / "data" / "raw" / "margin"
OUT_FILE   = ROOT / "data" / "processed" / "margin_snapshot.csv"
INV_SNAP   = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
INBOUND    = ROOT / "data" / "processed" / "inbound_snapshot.csv"
SHEET_NAME = "Margin Data"

# Warehouse SOH = operator's local stock (AMPM + B2B-AMPM).  Matches the
# AMS Planning definition and inventory_dashboard's CHANNEL_TO_TYPE
# warehouse bucket.
WAREHOUSE_CHANNELS = {"ampm", "b2b-ampm", "b2b - ampm"}

# Columns we keep from the source — order = output column order.
KEEP_COLS = [
    "SKU", "ASIN", "Model",
    "Category - L1", "Category - L2",
    "Product Name",
    "Latest FOB",
    "NLC",
    "DP",
    "MRP",
    "BAU Deal SP",
    "BAU SP/MOP(MP)",
    "Gross Margin", "Gross Margin %",
    "Net Margin",   "Net Margin %",
]

# Friendly snake_case names the React app can rely on
RENAME = {
    "SKU":              "sku",
    "ASIN":             "asin",
    "Model":            "model",
    "Category - L1":    "category_l1",
    "Category - L2":    "category_l2",
    "Product Name":     "product_name",
    "Latest FOB":       "latest_fob",
    "NLC":              "nlc",
    "DP":               "dp",
    "MRP":              "mrp",
    "BAU Deal SP":      "bau_deal_sp",
    "BAU SP/MOP(MP)":   "bau_sp_mop",
    "Gross Margin":     "gross_margin",
    "Gross Margin %":   "gross_margin_pct",
    "Net Margin":       "net_margin",
    "Net Margin %":     "net_margin_pct",
}


# ── PRIMARY SOURCE since 2026-09-05: the MARGIN TOOL (operator: "weekly
# should take margin data from margin tool directly", refreshed weekly).
# The brand master sheets under margin_src/input/ are the live, edited-in-
# browser masters, and margins are RECOMPUTED here through the margin
# tool's own calculator engine — so this snapshot always equals what
# /margin shows, instead of a manually exported xlsx going stale in
# data/raw/margin/. That folder remains the FALLBACK if margin_src is
# unavailable (never below yesterday's behaviour). Cross-lane import is
# sanctioned in CLAUDE.md #2 (single source of margin truth).
MARGIN_TOOL_MASTERS = {
    "Audio Array":    ROOT / "margin_src" / "input" / "Audio Array" / "Audio Array Master Sheet.xlsx",
    "Nexlev":         ROOT / "margin_src" / "input" / "Nexlev" / "NexLev Master Sheet.xlsx",
    "White Mulberry": ROOT / "margin_src" / "input" / "White Mulberry" / "WM Master Sheet.xlsx",
}


def _global_params(xls: pd.ExcelFile) -> dict:
    """Exactly load_brand()'s Edit-Parameters parsing (margin_src/router.py)."""
    raw = pd.read_excel(xls, sheet_name="Edit Parameters", header=None)
    p = raw.iloc[:, :2].copy()
    p.columns = ["parameter", "value"]
    p["parameter"] = p["parameter"].astype(str).str.strip()
    p["value"] = pd.to_numeric(
        p["value"].astype(str)
        .str.replace("₹", "", regex=False).str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False).str.strip(), errors="coerce")

    def _pct(name, default):
        v = p.loc[p["parameter"] == name, "value"]
        if v.empty:
            return default
        x = float(v.iloc[0])
        return x * 100 if x <= 1 else x

    def _val(name, default):
        v = p.loc[p["parameter"] == name, "value"]
        return float(v.iloc[0]) if not v.empty else default

    return {
        "usd_rate": _val("Dollar Conversion Rate", 88.0),
        "surcharge_pct": _pct("Surcharge", 10.0),
        "gst_default_pct": _pct("GST %", 18.0),
        "smm_pct": 0.0,
        "overhead_pct": _pct("Business Overhead %", 5.0),
        "finance_pct": _pct("Cost of Finance %", 3.0),
    }


def _read_from_margin_tool(brand: str, path: Path) -> pd.DataFrame:
    """One brand's rows, margins recomputed through the margin tool engine."""
    from margin_src.services.calculator_factory import get_calculator

    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, sheet_name="Master")
    gp = _global_params(xls)
    df.columns = df.columns.str.strip()
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip()
    if "SMM" in df.columns and "BAU Deal SP" in df.columns:
        df["SMM"] = df["SMM"].fillna(0)
        df["SMM Cost"] = (df["SMM"] / 100) * df["BAU Deal SP"]
    else:
        df["SMM"] = 0
        df["SMM Cost"] = 0

    calc = get_calculator("cambium")
    rows = [calc.calculate_single(r, gp) for _, r in df.iterrows()]
    cd = pd.DataFrame(rows)
    df["DP"] = cd["dp"]
    df["Gross Margin"] = cd["gross_margin"]
    df["Gross Margin %"] = cd["gross_margin_pct"]
    df["Net Margin"] = cd["net_margin"]
    df["Net Margin %"] = cd["net_margin_pct"]

    available = [c for c in KEEP_COLS if c in df.columns]
    out = df[available].copy()
    for c in set(KEEP_COLS) - set(available):
        out[c] = None
    out = out[KEEP_COLS].rename(columns=RENAME)
    out["brand"] = brand
    return out


def _read_one(path: Path) -> pd.DataFrame:
    df = read_excel_safe(path, sheet_name=SHEET_NAME)
    available = [c for c in KEEP_COLS if c in df.columns]
    missing = set(KEEP_COLS) - set(available)
    if missing:
        print(f"  ⚠ {path.name}: missing columns {missing} — they'll be NaN")
    out = df[available].copy()
    for c in missing:
        out[c] = None
    out = out[KEEP_COLS]                       # enforce column order
    out = out.rename(columns=RENAME)
    out["brand"] = path.stem                    # filename (sans .xlsx) IS the brand
    return out


def _build_inventory_lookups() -> tuple[dict, dict]:
    """Returns (total_stock_per_asin, warehouse_soh_per_asin) for the
    latest week in inventory_model_snapshot.csv.
      total_stock = sum across ALL channels (Amazon, AMPM, B2B-AMPM, 1P,
                    YNT, Blinkit, Pipeline, Open Order — everything).
      warehouse_soh = sum across AMPM + B2B-AMPM only (operator warehouse).
    """
    if not INV_SNAP.exists():
        print(f"  ⚠ {INV_SNAP.name} missing — total_stock / soh will be 0")
        return {}, {}
    df = pd.read_csv(INV_SNAP)
    # latest week by week-num
    df["wn"] = df["week"].astype(str).str.extract(r"(\d+)").astype(float)
    latest = int(df["wn"].max())
    df = df[df["wn"] == latest].copy()
    df["asin_n"] = df["asin"].astype(str).str.strip().str.upper()
    df = df[df["asin_n"] != ""]
    df["chan_n"] = df["channel"].astype(str).str.strip().str.lower()

    total = df.groupby("asin_n")["inventory_units"].sum().astype(int).to_dict()
    wh    = df[df["chan_n"].isin(WAREHOUSE_CHANNELS)] \
                .groupby("asin_n")["inventory_units"].sum().astype(int).to_dict()
    return total, wh


def _build_inbound_lookup() -> tuple[dict, dict]:
    """Returns (am_soh_per_asin, am_intransit_per_asin) from
    inbound_snapshot.csv.  am_soh = stock already at Amazon FBA;
    am_intransit = operator's warehouse → Amazon shipment in flight."""
    if not INBOUND.exists():
        print(f"  ⚠ {INBOUND.name} missing — am_soh / am_intransit will be 0")
        return {}, {}
    df = pd.read_csv(INBOUND)
    df["asin_n"] = df["asin"].astype(str).str.strip().str.upper()
    df = df[df["asin_n"] != ""]
    am_soh = pd.to_numeric(df["am_soh"], errors="coerce").fillna(0).astype(int).groupby(df["asin_n"]).sum().to_dict()
    am_it  = pd.to_numeric(df["am_intransit"], errors="coerce").fillna(0).astype(int).groupby(df["asin_n"]).sum().to_dict()
    return am_soh, am_it


def run_margin_etl() -> int:
    frames = []

    # PRIMARY: margin tool masters + engine (live, browser-edited source)
    for brand, path in MARGIN_TOOL_MASTERS.items():
        if not path.exists():
            print(f"  ⚠ margin tool master missing for {brand}: {path.name}")
            continue
        print(f"📥 {brand}: margin tool master ({path.name}) — engine recompute")
        try:
            frames.append(_read_from_margin_tool(brand, path))
        except Exception as exc:
            print(f"  ❌ {brand} via margin tool failed: {exc}")

    # FALLBACK: the legacy manually-exported xlsx drops, only for brands the
    # margin tool didn't cover this run.
    covered = {f["brand"].iloc[0] for f in frames if len(f)}
    if RAW_DIR.exists():
        for f in sorted(RAW_DIR.glob("*.xlsx")):
            if f.name.startswith("~$") or f.stem in covered:
                continue
            print(f"📥 Reading legacy export {f.name}…")
            try:
                frames.append(_read_one(f))
            except Exception as exc:
                print(f"  ❌ {f.name} failed: {exc}")

    if not frames:
        print("⚠ No usable margin sources — exiting")
        return 0

    out = pd.concat(frames, ignore_index=True)

    # Final tidy: brand first, then everything else.
    out = out[["brand"] + [c for c in out.columns if c != "brand"]]

    # Drop rows with no SKU AND no Model — they're noise.
    before = len(out)
    out = out[out["sku"].notna() | out["model"].notna()]
    dropped = before - len(out)
    if dropped:
        print(f"🧹 Dropped {dropped} rows with no SKU/Model")

    # Join inventory + inbound columns by ASIN.
    #   total_stock  = every channel summed (inventory_model_snapshot, latest week)
    #   soh          = AMPM + B2B-AMPM (operator warehouse, matches AMS Planning)
    #   am_soh       = stock already at Amazon FBA (inbound_snapshot)
    #   am_intransit = warehouse → Amazon shipments in flight (inbound_snapshot)
    total_stock, wh_soh    = _build_inventory_lookups()
    am_soh_map, am_it_map  = _build_inbound_lookup()
    asin_n = out["asin"].astype(str).str.strip().str.upper()
    out["total_stock"]  = asin_n.map(total_stock).fillna(0).astype(int)
    out["soh"]          = asin_n.map(wh_soh).fillna(0).astype(int)
    out["am_soh"]       = asin_n.map(am_soh_map).fillna(0).astype(int)
    out["am_intransit"] = asin_n.map(am_it_map).fillna(0).astype(int)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(out):,} rows → {OUT_FILE.relative_to(ROOT)}")
    print(f"   Brands: {sorted(out['brand'].unique().tolist())}")
    matched = int((out["total_stock"] > 0).sum())
    print(f"   Inventory joined: {matched:,}/{len(out):,} ASINs with stock > 0")
    return len(out)


if __name__ == "__main__":
    try:
        run_margin_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
