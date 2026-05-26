"""
Week-landing audit — runs after a weekly data refresh to catch
anomalies before the operator looks at the dashboard.

Checks (each → its own Excel sheet):
  1. Snapshot freshness: does every snapshot include the latest week?
  2. Sales model drops: models with W(latest-1) sales but 0 this week
  3. Sales WoW swings: per-model GMV change > ±50%
  4. Inventory model drops: models in last week's inventory but missing
     this week (split by type — AMPM / Amazon / 1P / In-Transit)
  5. AMPM specifically: brands where total AMPM stock dropped > 30% WoW
  6. Master coverage: ASINs / SKUs in master with NO presence in any
     downstream snapshot (sales, inventory, returns, inbound, margin)
  7. Cross-snapshot orphans:
        ASINs in {returns, inbound, margin} but not in master
        SKUs in {sales} but not in master
  8. Within-snapshot casing dupes (same identifier with different
     casing — sneaks past normal joins)

Read-only.  Output: data/processed/week_landing_audit.xlsx
"""
from __future__ import annotations
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "data" / "processed" / "week_landing_audit.xlsx"

# ── helpers ──────────────────────────────────────────────────────────────
def _wnum(w) -> int:
    try:
        return int(str(w).replace("Week", "").strip())
    except Exception:
        return -1

def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()

def _df(rows, cols):
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


# ── load everything ──────────────────────────────────────────────────────
print("📥 Loading snapshots…")
master = pd.read_excel(ROOT / "data" / "master" / "sku_master.xlsx")
master.columns = master.columns.str.strip()
for c in ("FBA SKU", "Original SKU", "ASIN", "Brand", "Model"):
    if c in master.columns:
        master[c] = master[c].map(_norm)

sales       = pd.read_csv(ROOT / "data" / "processed" / "weekly_sales_snapshot.csv")
inv_model   = pd.read_csv(ROOT / "data" / "processed" / "inventory_model_snapshot.csv")
inv_ams     = pd.read_csv(ROOT / "data" / "processed" / "inventory_ams_snapshot.csv")
returns_df  = pd.read_csv(ROOT / "data" / "processed" / "returns_snapshot.csv")
inbound     = pd.read_csv(ROOT / "data" / "processed" / "inbound_snapshot.csv")
margin      = pd.read_csv(ROOT / "data" / "processed" / "margin_snapshot.csv")

for df in (sales, inv_model, inv_ams):
    if "week" in df.columns:
        df["_wk"] = df["week"].apply(_wnum)

LATEST = max(sales["_wk"].max(), inv_model["_wk"].max())
PRIOR  = LATEST - 1
SALES_WINDOW = 12                                     # last-N-weeks scope for "active SKU" checks
SALES_FROM   = LATEST - SALES_WINDOW + 1              # inclusive
print(f"   Latest week: W{LATEST}  ·  Prior week: W{PRIOR}  ·  Sales window: W{SALES_FROM}–W{LATEST}\n")


# ─────────────────────────────────────────────────────────────────────────
# 1. FRESHNESS
# ─────────────────────────────────────────────────────────────────────────
print("(1) Snapshot freshness")
freshness_rows = []
def _last_wk(df, label):
    if "_wk" in df.columns:
        wk = int(df["_wk"].max())
    elif "week" in df.columns:
        wk = max(_wnum(w) for w in df["week"].dropna())
    else:
        wk = None
    ok = wk == LATEST
    print(f"   {'✓' if ok else '⚠'} {label}: latest week = W{wk}")
    freshness_rows.append({"Snapshot": label, "Latest Week": wk, "OK?": "yes" if ok else "NO"})

_last_wk(sales,     "weekly_sales_snapshot")
_last_wk(inv_model, "inventory_model_snapshot")
_last_wk(inv_ams,   "inventory_ams_snapshot")
print()


# ─────────────────────────────────────────────────────────────────────────
# 2. SALES MODEL DROPS (W{prior} had sales, W{latest} = 0)
# ─────────────────────────────────────────────────────────────────────────
print(f"(2) Models with W{PRIOR} sales but 0 in W{LATEST}")
sales_pivot = (sales[sales["_wk"].isin([PRIOR, LATEST])]
               .groupby(["brand", "model", "_wk"], as_index=False)["gmv"].sum()
               .pivot(index=["brand", "model"], columns="_wk", values="gmv")
               .fillna(0).reset_index())
sales_pivot.columns = ["brand", "model", "gmv_prior", "gmv_latest"] \
    if {PRIOR, LATEST}.issubset(sales_pivot.columns) else list(sales_pivot.columns)
if "gmv_prior" in sales_pivot.columns and "gmv_latest" in sales_pivot.columns:
    drops = sales_pivot[(sales_pivot["gmv_prior"] > 0) & (sales_pivot["gmv_latest"] <= 0)].copy()
    drops = drops.sort_values("gmv_prior", ascending=False)
    print(f"   {len(drops)} models with prior-week GMV >0 but current week = 0")
    sales_drops_rows = drops.to_dict("records")
else:
    print("   (data layout unexpected, skipped)")
    sales_drops_rows = []
print()


# ─────────────────────────────────────────────────────────────────────────
# 3. SALES WoW SWINGS (> ±50% on models with > ₹10k prior-week GMV)
# ─────────────────────────────────────────────────────────────────────────
print(f"(3) Sales WoW swings (per model, ≥ ±50% change)")
if "gmv_prior" in sales_pivot.columns:
    sw = sales_pivot[sales_pivot["gmv_prior"] >= 10_000].copy()
    sw["pct_change"] = ((sw["gmv_latest"] - sw["gmv_prior"]) / sw["gmv_prior"] * 100).round(1)
    swings = sw[(sw["pct_change"] <= -50) | (sw["pct_change"] >= 50)].copy()
    swings = swings.sort_values("pct_change")
    print(f"   {len(swings)} models with > ±50% WoW change (prior GMV ≥ ₹10k)")
    swing_rows = swings.to_dict("records")
else:
    swing_rows = []
print()


# ─────────────────────────────────────────────────────────────────────────
# 4 & 5. INVENTORY TYPE-LEVEL DROPS (via raw loader) — needs type column
#
# Loads the full multi-week multi-type inventory frame via the same
# canonical loader the FastAPI Inventory page uses, then slices by
# type (warehouse / 1p / in-transit / unsellable / amazon).
# ─────────────────────────────────────────────────────────────────────────
print(f"(4) Inventory drops by type (W{PRIOR} → W{LATEST})")
print(f"(5) AMPM warehouse stock — brand-level swings")
inv_drops_rows = []
ampm_rows      = []
try:
    import sys
    sys.path.insert(0, str(ROOT))
    from weekly_app.routes.inventory_dashboard import load_all_inventory   # noqa: E402
    raw_inv = load_all_inventory()
except Exception as e:
    print(f"   ⚠ Couldn't load raw inventory: {e}")
    raw_inv = None

if raw_inv is not None and not raw_inv.empty:
    raw = raw_inv.copy()
    raw["_wk"]   = pd.to_numeric(raw.get("week_num"), errors="coerce")
    raw["_type"] = raw["type"].astype(str).str.strip().str.lower()
    raw          = raw[raw["_wk"].isin([PRIOR, LATEST])]

    # (4) per-type, per-model drops to 0
    for t in sorted(raw["_type"].dropna().unique()):
        sub = raw[raw["_type"] == t]
        pv  = (sub.groupby(["brand", "model", "_wk"], as_index=False)["inventory_units"].sum()
                  .pivot(index=["brand", "model"], columns="_wk", values="inventory_units")
                  .fillna(0).reset_index())
        if PRIOR not in pv.columns or LATEST not in pv.columns:
            continue
        pv.columns = ["brand", "model", "units_prior", "units_latest"]
        dr = pv[(pv["units_prior"] > 0) & (pv["units_latest"] <= 0)].copy()
        if len(dr):
            dr["type"] = t
            dr = dr[["type", "brand", "model", "units_prior", "units_latest"]]
            print(f"   {t:<24}  {len(dr)} models dropped to 0")
            inv_drops_rows.extend(dr.to_dict("records"))
    print(f"   total: {len(inv_drops_rows)} (type, model) drops to 0")

    # (5) AMPM = warehouse — brand-level swings
    ampm = raw[raw["_type"] == "warehouse"]
    bp = (ampm.groupby(["brand", "_wk"], as_index=False)["inventory_units"].sum()
              .pivot(index="brand", columns="_wk", values="inventory_units")
              .fillna(0).reset_index())
    if PRIOR in bp.columns and LATEST in bp.columns:
        bp.columns = ["brand", "units_prior", "units_latest"]
        bp["pct_change"] = ((bp["units_latest"] - bp["units_prior"]) /
                            bp["units_prior"].replace(0, pd.NA) * 100).round(1)
        print()
        for _, r in bp.iterrows():
            change = r["pct_change"]
            marker = " ⚠ DROP" if pd.notna(change) and change <= -30 else ""
            print(f"   AMPM {str(r['brand']):<18}  {int(r['units_prior']):>7,} → {int(r['units_latest']):>7,}  ({change}%){marker}")
        ampm_rows = bp.to_dict("records")
print()


# ─────────────────────────────────────────────────────────────────────────
# 6. MASTER COVERAGE — ASINs / SKUs in master with no downstream presence
# ─────────────────────────────────────────────────────────────────────────
print("(6) Master coverage — fully unused ASINs/SKUs")
master_asins = set(master["ASIN"].dropna()) - {""}
master_skus  = set(master["FBA SKU"].dropna()) - {""}
master_models = set((master["Brand"].astype(str).str.strip() + "|" + master["Model"].astype(str).str.strip())
                    .replace("|", "", regex=False)) if "Model" in master.columns else set()

# Sales presence is scoped to the last 12 weeks (window=SALES_WINDOW).
# A SKU that sold 6 months ago shouldn't count as "active" — operator
# wants the audit to reflect the current commercial state.
recent_sales = sales[sales["_wk"] >= SALES_FROM]
asins_in_sales    = set(recent_sales["sku"].astype(str).str.strip()) if "sku" in recent_sales.columns else set()
asins_in_inv      = set()  # inv keys by model, not asin
asins_in_returns  = set(returns_df["asin"].astype(str).str.strip()) if "asin" in returns_df.columns else set()
asins_in_inbound  = set(inbound["asin"].astype(str).str.strip()) if "asin" in inbound.columns else set()
asins_in_margin   = set(margin["asin"].astype(str).str.strip()) if "asin" in margin.columns else set()

skus_in_sales = set(recent_sales["sku"].astype(str).str.strip()) if "sku" in recent_sales.columns else set()

unused_skus  = master_skus  - skus_in_sales
unused_asins = master_asins - asins_in_returns - asins_in_inbound - asins_in_margin
print(f"   SKUs in master with no sale in last {SALES_WINDOW} weeks:   {len(unused_skus)}")
print(f"   ASINs in master absent from all 3 downstream:        {len(unused_asins)}")

unused_sku_rows = []
for sku in sorted(unused_skus):
    row = master[master["FBA SKU"] == sku].head(1)
    if not row.empty:
        r = row.iloc[0]
        unused_sku_rows.append({
            "SKU": sku, "ASIN": r.get("ASIN", ""), "Brand": r.get("Brand", ""),
            "Model": r.get("Model", ""), "BAU": r.get("BAU", ""),
        })
unused_asin_rows = []
for asin in sorted(unused_asins):
    row = master[master["ASIN"] == asin].head(1)
    if not row.empty:
        r = row.iloc[0]
        unused_asin_rows.append({
            "ASIN": asin, "SKU": r.get("FBA SKU", ""), "Brand": r.get("Brand", ""),
            "Model": r.get("Model", ""), "BAU": r.get("BAU", ""),
        })
print()


# ─────────────────────────────────────────────────────────────────────────
# 7. CROSS-SNAPSHOT ORPHANS — identifier appears in snapshot but NOT master
# ─────────────────────────────────────────────────────────────────────────
print("(7) Cross-snapshot orphans (in snapshot but absent from master)")
orphans = []
def _orphan_set(snap_asins: set[str], snap_label: str):
    miss = snap_asins - master_asins - {""}
    print(f"   {snap_label}: {len(miss)} orphan ASINs")
    for a in sorted(miss):
        orphans.append({"Snapshot": snap_label, "ASIN": a})

_orphan_set(asins_in_returns,  "returns_snapshot")
_orphan_set(asins_in_inbound,  "inbound_snapshot")
_orphan_set(asins_in_margin,   "margin_snapshot")

sku_orphans = []
sku_orphans_set = skus_in_sales - master_skus - {""}
print(f"   sales SKUs not in master: {len(sku_orphans_set)}")
for s in sorted(sku_orphans_set):
    sku_orphans.append({"Snapshot": "weekly_sales_snapshot", "SKU": s})
print()


# ─────────────────────────────────────────────────────────────────────────
# 8. WITHIN-SNAPSHOT CASING DUPES (same identifier, different casing)
# ─────────────────────────────────────────────────────────────────────────
print("(8) Within-snapshot casing dupes")
casing_rows = []
def _check_casing(df: pd.DataFrame, col: str, label: str):
    if col not in df.columns:
        return
    bag: dict[str, set[str]] = {}
    for v in df[col].dropna().astype(str):
        v = v.strip()
        if v:
            bag.setdefault(v.lower(), set()).add(v)
    dupes = {k: vs for k, vs in bag.items() if len(vs) > 1}
    if dupes:
        print(f"   {label} · {col}: {len(dupes)} case dupes")
        for k, vs in dupes.items():
            casing_rows.append({"Snapshot": label, "Column": col, "Variants": " | ".join(sorted(vs))})

_check_casing(sales,     "model", "weekly_sales_snapshot")
_check_casing(sales,     "brand", "weekly_sales_snapshot")
_check_casing(inv_model, "model", "inventory_model_snapshot")
_check_casing(inv_ams,   "model", "inventory_ams_snapshot")
print()


# ─────────────────────────────────────────────────────────────────────────
# Summary tab
# ─────────────────────────────────────────────────────────────────────────
summary = [
    {"Category": "Snapshot freshness",            "Count": sum(1 for r in freshness_rows if r["OK?"] != "yes"),
     "Action": "Re-run ETL for the lagging snapshot"},
    {"Category": "Sales model drops to 0",        "Count": len(sales_drops_rows),
     "Action": "Validate WoW: missing source file? out of stock?"},
    {"Category": "Sales WoW swings ≥ ±50%",       "Count": len(swing_rows),
     "Action": "Spot-check: legitimate seasonality vs. data error"},
    {"Category": "Inv model drops to 0",          "Count": len(inv_drops_rows),
     "Action": "Check raw inventory file for missing rows"},
    {"Category": f"Master SKUs unsold in last {SALES_WINDOW} wks", "Count": len(unused_sku_rows),
     "Action": "Possibly retired SKUs — or master typo / out of stock"},
    {"Category": "Master ASINs unused everywhere","Count": len(unused_asin_rows),
     "Action": "Possibly retired — or never launched"},
    {"Category": "Cross-snapshot orphan ASINs",   "Count": len(orphans),
     "Action": "Add to master OR remove from snapshot source"},
    {"Category": "Sales SKUs not in master",      "Count": len(sku_orphans),
     "Action": "Add SKU to master"},
    {"Category": "Within-snapshot casing dupes",  "Count": len(casing_rows),
     "Action": "Normalize casing in source files"},
]

# ─────────────────────────────────────────────────────────────────────────
# Excel report
# ─────────────────────────────────────────────────────────────────────────
OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    _df(summary, ["Category", "Count", "Action"]).to_excel(w, sheet_name="Summary", index=False)
    _df(freshness_rows, ["Snapshot", "Latest Week", "OK?"]).to_excel(w, sheet_name="1 Freshness", index=False)
    _df(sales_drops_rows,
        ["brand", "model", "gmv_prior", "gmv_latest"]
       ).to_excel(w, sheet_name="2 Sales drops to 0", index=False)
    _df(swing_rows,
        ["brand", "model", "gmv_prior", "gmv_latest", "pct_change"]
       ).to_excel(w, sheet_name="3 Sales WoW swings", index=False)
    _df(inv_drops_rows,
        ["type", "brand", "model", "units_prior", "units_latest"]
       ).to_excel(w, sheet_name="4 Inv drops to 0", index=False)
    _df(ampm_rows,
        ["brand", "units_prior", "units_latest", "pct_change"]
       ).to_excel(w, sheet_name="5 AMPM brand swings", index=False)
    _df(unused_sku_rows,
        ["SKU", "ASIN", "Brand", "Model", "BAU"]
       ).to_excel(w, sheet_name="6a Master SKUs no sales", index=False)
    _df(unused_asin_rows,
        ["ASIN", "SKU", "Brand", "Model", "BAU"]
       ).to_excel(w, sheet_name="6b Master ASINs unused", index=False)
    _df(orphans,   ["Snapshot", "ASIN"]).to_excel(w, sheet_name="7a ASIN orphans",  index=False)
    _df(sku_orphans, ["Snapshot", "SKU"]).to_excel(w, sheet_name="7b SKU orphans",  index=False)
    _df(casing_rows, ["Snapshot", "Column", "Variants"]).to_excel(w, sheet_name="8 Casing dupes", index=False)

print(f"📊 Excel report: {OUT.relative_to(ROOT)}")
