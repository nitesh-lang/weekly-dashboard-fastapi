"""
Amazon SOH + Intransit snapshot ETL — combines three sources to give one
truthful per-ASIN view of "how many units does Amazon have / will have".

Two metrics surfaced to AMS Planning:
    am_soh        Current Amazon-side sellable stock
    am_intransit  Units en route to Amazon (not yet on the shelf)

Brands aren't one-size-fits-all.  Source selection by brand:

  ┌──────────────────────────────────────┬───────────────────────────┐
  │ Brand                                │ Source                    │
  ├──────────────────────────────────────┼───────────────────────────┤
  │ Audio Array, White Mulberry  (1P)    │ am_soh:       weekly                                       │
  │                                      │   inventory_model_snapshot                                 │
  │                                      │   (latest week, brand+model)                               │
  │                                      │ am_intransit: PO files                                     │
  │                                      │   (Open PO + In-Transit, by ASIN)                          │
  ├──────────────────────────────────────┼───────────────────────────┤
  │ Nexlev, Tonor, others        (3P)    │ am_soh:       FBA inventory                                │
  │                                      │   (afn-total − afn-unsellable)                             │
  │                                      │ am_intransit: FBA inventory                                │
  │                                      │   (afn-inbound-working + afn-inbound-shipped)              │
  └──────────────────────────────────────┴───────────────────────────┘

Sources:
    data/raw/inbound/inventory_amazon_*.csv     FBA inventory exports (3P)
    data/raw/inbound/In_Transit_PO data*.xlsx   Vendor Central PO + intransit (1P)
    data/processed/inventory_model_snapshot.csv weekly inventory aggregate (1P SOH)
    data/master/sku_master.xlsx                 ASIN → brand mapping

Output:
    data/processed/inbound_snapshot.csv
    Columns: asin, sku, am_soh, am_intransit
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = ROOT / "data" / "raw" / "inbound"
INV_MODEL  = ROOT / "data" / "processed" / "inventory_model_snapshot.csv"
MASTER     = ROOT / "data" / "master" / "sku_master.xlsx"
OUT_FILE   = ROOT / "data" / "processed" / "inbound_snapshot.csv"

# Brands whose Amazon presence is dominantly 1P — pull SOH + intransit from
# the 1P sources instead of FBA.  Case-insensitive match against sku_master.
ONE_P_BRANDS = {"audio array", "white mulberry", "tonor"}

# FBA file columns (standard Seller-Central "Manage Inventory" export)
COL_ASIN       = "asin"
COL_SKU        = "sku"
COL_TOTAL      = "afn-total-quantity"            # N
COL_UNSELLABLE = "afn-unsellable-quantity"       # L
COL_INB_WORK   = "afn-inbound-working-quantity"  # P
COL_INB_SHIP   = "afn-inbound-shipped-quantity"  # Q

# PO file columns (Vendor Central export — operator's "In_Transit_PO data*.xlsx")
PO_COL_ASIN     = "ASIN"
PO_COL_SKU      = "SKU"
PO_COL_QTY      = "Accepted quantity"   # falls back to "Quantity Requested"
PO_COL_STATUS   = "Delivery Status"     # "Open PO" or "In-Transit"


# ─────────────────────────────────────────────────────────────────────────
# Source loaders
# ─────────────────────────────────────────────────────────────────────────

def _load_fba_files() -> pd.DataFrame:
    """Combine all FBA inventory CSVs into one frame keyed by ASIN.
    Returns columns: asin, sku, am_soh, am_intransit.
    Dedupes by ASIN keeping the row with the highest combined inventory."""
    files = sorted([p for p in RAW_DIR.glob("inventory_amazon_*.csv") if not p.name.startswith("~")])
    if not files:
        return pd.DataFrame(columns=["asin", "sku", "am_soh", "am_intransit"])

    frames = []
    for f in files:
        print(f"📥 Reading FBA inventory: {f.name}…")
        df = pd.read_csv(f, dtype=str)
        needed = {COL_ASIN, COL_SKU, COL_TOTAL, COL_UNSELLABLE, COL_INB_WORK, COL_INB_SHIP}
        if not needed.issubset(set(df.columns)):
            print(f"  ⚠ missing required columns — skipping")
            continue
        out = df[[COL_ASIN, COL_SKU, COL_TOTAL, COL_UNSELLABLE, COL_INB_WORK, COL_INB_SHIP]].copy()
        out.columns = ["asin", "sku", "_total", "_unsellable", "_p", "_q"]
        out["asin"] = out["asin"].astype(str).str.strip()
        out["sku"]  = out["sku"].astype(str).str.strip()
        out = out[(out["asin"] != "") & (out["asin"].str.lower() != "nan")]
        for c in ("_total", "_unsellable", "_p", "_q"):
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
        out["am_soh"]       = (out["_total"] - out["_unsellable"]).clip(lower=0).astype(int)
        out["am_intransit"] = (out["_p"] + out["_q"]).astype(int)
        frames.append(out[["asin", "sku", "am_soh", "am_intransit"]])

    if not frames:
        return pd.DataFrame(columns=["asin", "sku", "am_soh", "am_intransit"])
    combo = pd.concat(frames, ignore_index=True)
    combo["_freshness"] = combo["am_soh"] + combo["am_intransit"]
    combo = combo.sort_values("_freshness", ascending=False)
    return combo.drop_duplicates(subset=["asin"], keep="first").reset_index(drop=True)[
        ["asin", "sku", "am_soh", "am_intransit"]
    ]


def _load_po_files() -> dict[str, int]:
    """{asin → total intransit qty} from Vendor Central PO files.
    Sums Open PO + In-Transit accepted quantities — both are units committed
    to Amazon that the operator wants to see in the AM Intransit column."""
    files = sorted([p for p in RAW_DIR.glob("In_Transit_PO*.xlsx") if not p.name.startswith("~")])
    if not files:
        return {}

    rows = []
    for f in files:
        print(f"📥 Reading 1P PO file: {f.name}…")
        try:
            df = pd.read_excel(f)
        except Exception as e:
            print(f"  ⚠ failed to read: {e}")
            continue
        df.columns = [str(c).strip() for c in df.columns]
        if PO_COL_ASIN not in df.columns:
            print(f"  ⚠ no ASIN column — skipping")
            continue
        if df.empty:
            print(f"  (empty file — 0 rows)")
            continue
        # Pick the qty column — prefer Accepted, fall back to Requested.
        qty_col = PO_COL_QTY if PO_COL_QTY in df.columns else "Quantity Requested"
        if qty_col not in df.columns:
            print(f"  ⚠ no qty column — skipping")
            continue
        df["_asin"] = df[PO_COL_ASIN].astype(str).str.strip()
        df["_qty"]  = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
        # No filter on Delivery Status — Open PO + In-Transit are BOTH treated
        # as "en route", per the operator's spec.
        df = df[(df["_asin"] != "") & (df["_asin"].str.lower() != "nan")]
        rows.append(df[["_asin", "_qty"]])

    if not rows:
        return {}
    combo = pd.concat(rows, ignore_index=True)
    by_asin = combo.groupby("_asin", as_index=False)["_qty"].sum()
    return {row["_asin"]: int(row["_qty"]) for _, row in by_asin.iterrows()}


def _load_1p_soh() -> dict[tuple[str, str], int]:
    """{(brand_lower, model_lower) → inventory_units at latest week}
    from the weekly inventory_model_snapshot."""
    if not INV_MODEL.exists():
        return {}
    df = pd.read_csv(INV_MODEL)
    if df.empty or not {"week", "brand", "model", "inventory_units"}.issubset(df.columns):
        return {}
    # Pick latest week numerically (Week 1 < Week 20)
    def _wk_num(w):
        try: return int(str(w).replace("Week", "").strip())
        except Exception: return -1
    df["_wk"] = df["week"].apply(_wk_num)
    df = df[df["_wk"] >= 0]
    if df.empty:
        return {}
    latest = int(df["_wk"].max())
    print(f"📥 1P SOH source: inventory_model_snapshot latest week = Week {latest}")
    df = df[df["_wk"] == latest].copy()
    df["_b"] = df["brand"].astype(str).str.strip().str.lower()
    df["_m"] = df["model"].astype(str).str.strip().str.lower()
    df["_u"] = pd.to_numeric(df["inventory_units"], errors="coerce").fillna(0).astype(int)
    return {(row["_b"], row["_m"]): row["_u"] for _, row in df.iterrows() if row["_b"] and row["_m"]}


def _load_master_map() -> dict[str, tuple[str, str, str]]:
    """{asin → (sku, brand_lower, model_lower)}.  Used to:
       1. tag each ASIN with its brand so we can pick the right source
       2. join 1P SOH which is keyed on (brand, model)
    """
    if not MASTER.exists():
        return {}
    m = pd.read_excel(MASTER)
    m.columns = m.columns.str.strip()
    rn = {"FBA SKU": "sku", "ASIN": "asin", "Brand": "brand", "Model": "model"}
    m = m.rename(columns={k: v for k, v in rn.items() if k in m.columns})
    if "asin" not in m.columns or "brand" not in m.columns:
        return {}
    out: dict[str, tuple[str, str, str]] = {}
    for _, r in m.iterrows():
        a = str(r.get("asin", "")).strip()
        if not a or a.lower() == "nan":
            continue
        sku   = str(r.get("sku",   "")).strip()
        brand = str(r.get("brand", "")).strip()
        model = str(r.get("model", "")).strip()
        out[a] = (sku, brand.lower(), model.lower())
    return out


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def run_inbound_etl() -> int:
    if not RAW_DIR.exists():
        print(f"⚠ Inbound raw folder not found at {RAW_DIR}")
        return 0

    fba          = _load_fba_files()
    po_lookup    = _load_po_files()
    one_p_soh    = _load_1p_soh()
    master_map   = _load_master_map()

    print()
    print(f"   FBA-side ASINs:       {len(fba):,}")
    print(f"   PO file ASINs:        {len(po_lookup):,}")
    print(f"   1P SOH model entries: {len(one_p_soh):,}")
    print(f"   Master ASIN map:      {len(master_map):,}")
    print()

    # Build the per-ASIN final row.  Walk every ASIN we know about from any
    # source so a 1P-only ASIN (no FBA entry) still gets a row.
    all_asins: set[str] = set(fba["asin"].tolist()) | set(po_lookup.keys()) | set(master_map.keys())

    rows = []
    for asin in all_asins:
        sku, brand_lower, model_lower = master_map.get(asin, ("", "", ""))
        is_1p = brand_lower in ONE_P_BRANDS

        fba_row = fba[fba["asin"] == asin]
        fba_soh       = int(fba_row["am_soh"].iloc[0])       if not fba_row.empty else 0
        fba_intransit = int(fba_row["am_intransit"].iloc[0]) if not fba_row.empty else 0
        fba_sku       = str(fba_row["sku"].iloc[0])          if not fba_row.empty else ""

        if is_1p:
            am_soh       = one_p_soh.get((brand_lower, model_lower), 0)
            am_intransit = int(po_lookup.get(asin, 0))
        else:
            am_soh       = fba_soh
            am_intransit = fba_intransit

        rows.append({
            "asin":         asin,
            "sku":          sku or fba_sku,
            "am_soh":       int(am_soh),
            "am_intransit": int(am_intransit),
        })

    if not rows:
        print("⚠ Nothing to write")
        return 0

    out = pd.DataFrame(rows).sort_values("asin").reset_index(drop=True)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(out):,} ASINs → {OUT_FILE.relative_to(ROOT)}")
    print()
    print(f"   Total AM SOH:        {int(out['am_soh'].sum()):,}")
    print(f"   Total AM Intransit:  {int(out['am_intransit'].sum()):,}")
    print(f"   ASINs with SOH > 0:        {int((out['am_soh'] > 0).sum()):,}")
    print(f"   ASINs with Intransit > 0:  {int((out['am_intransit'] > 0).sum()):,}")

    # Per-brand sanity check so the operator can verify routing
    if master_map:
        out["_brand"] = out["asin"].map(lambda a: master_map.get(a, ("", "", ""))[1])
        by_brand = out.groupby("_brand").agg(
            asins=("asin", "count"),
            soh=("am_soh", "sum"),
            intransit=("am_intransit", "sum"),
        ).sort_values("soh", ascending=False)
        print()
        print("   By brand:")
        for b, r in by_brand.iterrows():
            tag = " (1P)" if b in ONE_P_BRANDS else ""
            print(f"     {(b or '(unmapped)'):<18}{tag:<6}  {int(r['asins']):>4} ASINs   SOH {int(r['soh']):>6,}   Intransit {int(r['intransit']):>6,}")
    return len(out)


if __name__ == "__main__":
    try:
        run_inbound_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
