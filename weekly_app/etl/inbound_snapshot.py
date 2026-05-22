"""
Amazon SOH + Intransit snapshot — combines FBA (3P) and 1P sources into
one truthful per-ASIN view.  Brands don't gate the source: a 1P-dominant
brand can still have some FBA stock, and vice versa.  We sum everything
that's on Amazon's side (or en route to it).

Output metrics (one row per ASIN):
    am_soh        = (FBA: afn-total − afn-unsellable)
                  + (1P channel inventory, latest week, brand+model)
    am_intransit  = (FBA: afn-inbound-working + afn-inbound-shipped)
                  + (PO file Accepted quantity, all rows for the ASIN —
                     both "Open PO" and "In-Transit" statuses)

Sources:
    data/raw/inbound/inventory_amazon_*.csv     FBA inventory exports
    data/raw/inbound/In_Transit_PO data*.xlsx   Vendor Central PO + intransit (1P)
    inventory_dashboard.load_all_inventory()    raw weekly inventory w/ channels
                                                (filtered to channel = "1P")
    data/master/sku_master.xlsx                 ASIN → brand + model mapping

Output:
    data/processed/inbound_snapshot.csv  (asin, sku, am_soh, am_intransit)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).resolve().parent.parent.parent
RAW_DIR    = ROOT / "data" / "raw" / "inbound"
MASTER     = ROOT / "data" / "master" / "sku_master.xlsx"
OUT_FILE   = ROOT / "data" / "processed" / "inbound_snapshot.csv"

# FBA file columns (Seller-Central "Manage Inventory" export)
FBA_ASIN       = "asin"
FBA_SKU        = "sku"
FBA_TOTAL      = "afn-total-quantity"           # N
FBA_UNSELLABLE = "afn-unsellable-quantity"      # L
FBA_INB_WORK   = "afn-inbound-working-quantity" # P
FBA_INB_SHIP   = "afn-inbound-shipped-quantity" # Q

# PO file columns (Vendor Central export — "In_Transit_PO data*.xlsx")
PO_ASIN   = "ASIN"
PO_SKU    = "SKU"
PO_QTY    = "Accepted quantity"        # falls back to "Quantity Requested"
PO_STATUS = "Delivery Status"          # Open PO + In-Transit are BOTH counted

# The 1P channel name as it appears in the raw weekly inventory snapshot.
ONE_P_CHANNEL = "1p"


# ─────────────────────────────────────────────────────────────────────────
# Source loaders
# ─────────────────────────────────────────────────────────────────────────

def _load_fba() -> dict[str, dict]:
    """{asin → {sku, fba_soh, fba_intransit}} from FBA inventory CSVs.
    Dedupes by ASIN keeping the freshest non-zero snapshot if the ASIN
    appears in multiple files (e.g. WM.csv / viomi.csv dup rows)."""
    files = sorted([p for p in RAW_DIR.glob("inventory_amazon_*.csv") if not p.name.startswith("~")])
    rows = []
    for f in files:
        print(f"📥 FBA inventory: {f.name}")
        try:
            df = pd.read_csv(f, dtype=str)
        except Exception as e:
            print(f"  ⚠ read failed: {e}")
            continue
        needed = {FBA_ASIN, FBA_SKU, FBA_TOTAL, FBA_UNSELLABLE, FBA_INB_WORK, FBA_INB_SHIP}
        if not needed.issubset(set(df.columns)):
            print(f"  ⚠ missing columns, skipping")
            continue
        sub = df[[FBA_ASIN, FBA_SKU, FBA_TOTAL, FBA_UNSELLABLE, FBA_INB_WORK, FBA_INB_SHIP]].copy()
        sub.columns = ["asin", "sku", "_total", "_unsellable", "_p", "_q"]
        sub["asin"] = sub["asin"].astype(str).str.strip()
        sub["sku"]  = sub["sku"].astype(str).str.strip()
        sub = sub[(sub["asin"] != "") & (sub["asin"].str.lower() != "nan")]
        for c in ("_total", "_unsellable", "_p", "_q"):
            sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0)
        sub["fba_soh"]       = (sub["_total"] - sub["_unsellable"]).clip(lower=0).astype(int)
        sub["fba_intransit"] = (sub["_p"] + sub["_q"]).astype(int)
        rows.append(sub[["asin", "sku", "fba_soh", "fba_intransit"]])
    if not rows:
        return {}
    combo = pd.concat(rows, ignore_index=True)
    combo["_freshness"] = combo["fba_soh"] + combo["fba_intransit"]
    combo = combo.sort_values("_freshness", ascending=False).drop_duplicates(subset=["asin"], keep="first")
    return {
        r["asin"]: {"sku": r["sku"], "fba_soh": int(r["fba_soh"]), "fba_intransit": int(r["fba_intransit"])}
        for _, r in combo.iterrows()
    }


def _load_po_intransit() -> dict[str, int]:
    """{asin → 1P PO intransit qty} — sum of Accepted quantity across
    every PO line for the ASIN (Open PO + In-Transit both treated as
    en-route units, per operator spec)."""
    files = sorted([p for p in RAW_DIR.glob("In_Transit_PO*.xlsx") if not p.name.startswith("~")])
    rows = []
    for f in files:
        print(f"📥 1P PO file: {f.name}")
        try:
            df = pd.read_excel(f)
        except Exception as e:
            print(f"  ⚠ read failed: {e}")
            continue
        df.columns = [str(c).strip() for c in df.columns]
        if PO_ASIN not in df.columns:
            print(f"  ⚠ no ASIN column, skipping")
            continue
        if df.empty:
            print(f"  (empty — 0 rows)")
            continue
        qty_col = PO_QTY if PO_QTY in df.columns else "Quantity Requested"
        df["_asin"] = df[PO_ASIN].astype(str).str.strip()
        df["_qty"]  = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
        df = df[(df["_asin"] != "") & (df["_asin"].str.lower() != "nan")]
        rows.append(df[["_asin", "_qty"]])
    if not rows:
        return {}
    combo = pd.concat(rows, ignore_index=True)
    by_asin = combo.groupby("_asin", as_index=False)["_qty"].sum()
    return {row["_asin"]: int(row["_qty"]) for _, row in by_asin.iterrows()}


def _load_1p_soh() -> dict[tuple[str, str], int]:
    """{(brand_lower, model_lower) → 1P channel inventory units at the
    latest available week}.

    Uses inventory_dashboard.load_all_inventory() so we share the same
    channel-aware loader the Inventory page already trusts, then filters
    to channel = '1P'."""
    try:
        from weekly_app.routes.inventory_dashboard import load_all_inventory
    except Exception as e:
        print(f"⚠ Couldn't import inventory loader: {e}")
        return {}
    df = load_all_inventory()
    if df is None or df.empty or "channel" not in df.columns:
        return {}

    def _wk(w):
        try:    return int(str(w).replace("Week", "").strip())
        except Exception: return -1
    df = df.copy()
    df["_wk"] = df["week"].apply(_wk)
    df = df[df["_wk"] >= 0]
    if df.empty:
        return {}
    latest = int(df["_wk"].max())
    print(f"📥 1P SOH source: load_all_inventory channel = '1P' @ Week {latest}")

    # Filter to 1P channel, latest week
    df = df[df["_wk"] == latest]
    df = df[df["channel"].astype(str).str.strip().str.lower() == ONE_P_CHANNEL]
    df["_b"] = df["brand"].astype(str).str.strip().str.lower()
    df["_m"] = df["model"].astype(str).str.strip().str.lower()
    df["_u"] = pd.to_numeric(df["inventory_units"], errors="coerce").fillna(0).astype(int)
    g = df.groupby(["_b", "_m"], as_index=False)["_u"].sum()
    return {(r["_b"], r["_m"]): int(r["_u"]) for _, r in g.iterrows() if r["_b"] and r["_m"]}


def _load_master_map() -> dict[str, tuple[str, str, str]]:
    """{asin → (sku, brand_lower, model_lower)}."""
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
        out[a] = (
            str(r.get("sku", "")).strip(),
            str(r.get("brand", "")).strip().lower(),
            str(r.get("model", "")).strip().lower(),
        )
    return out


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def run_inbound_etl() -> int:
    if not RAW_DIR.exists():
        print(f"⚠ Inbound raw folder not found at {RAW_DIR}")
        return 0

    fba_lookup   = _load_fba()
    po_lookup    = _load_po_intransit()
    onep_soh     = _load_1p_soh()
    master_map   = _load_master_map()

    print()
    print(f"   FBA ASINs:            {len(fba_lookup):,}")
    print(f"   PO file ASINs:        {len(po_lookup):,}")
    print(f"   1P SOH model entries: {len(onep_soh):,}")
    print(f"   Master ASIN map:      {len(master_map):,}")
    print()

    # Union of every ASIN we know about so a 1P-only ASIN that's not in
    # the FBA file still gets a row.
    all_asins = set(fba_lookup.keys()) | set(po_lookup.keys()) | set(master_map.keys())

    rows = []
    for asin in all_asins:
        sku, brand_l, model_l = master_map.get(asin, ("", "", ""))
        fba = fba_lookup.get(asin, {})
        fba_soh       = int(fba.get("fba_soh",       0))
        fba_intransit = int(fba.get("fba_intransit", 0))
        one_p_soh     = int(onep_soh.get((brand_l, model_l), 0)) if (brand_l and model_l) else 0
        po_intransit  = int(po_lookup.get(asin, 0))

        am_soh       = fba_soh + one_p_soh
        am_intransit = fba_intransit + po_intransit

        rows.append({
            "asin":         asin,
            "sku":          sku or fba.get("sku", ""),
            "am_soh":       am_soh,
            "am_intransit": am_intransit,
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

    # Per-brand sanity check
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
            print(f"     {(b or '(unmapped)'):<18}  {int(r['asins']):>4} ASINs   SOH {int(r['soh']):>7,}   Intransit {int(r['intransit']):>6,}")
    return len(out)


if __name__ == "__main__":
    try:
        run_inbound_etl()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
