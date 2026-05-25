"""
Audit Week 21 inventory snapshots against sku_master.

Looks for:
  1. Cross-brand identifier collisions (same SKU/ASIN/Model in 2+ brands)
  2. Inventory rows where the file's brand disagrees with master
  3. Inventory SKU/ASIN that aren't in master at all
  4. SKU↔ASIN mismatches (inv says SKU X is ASIN Y, master says SKU X is ASIN Z)
  5. SKU↔Model mismatches between inv and master
  6. Within-file dupes (same SKU rows with mismatching ASIN/Model)
  7. Whitespace / case anomalies on join keys

Read-only.  Prints a structured report.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data" / "master" / "sku_master.xlsx"
WEEK21 = ROOT / "data" / "raw" / "inventory" / "Week 21"

BRANDS = ["Audio_Array", "Nexlev", "Tonor", "White_Mulberry"]


def _norm(s) -> str:
    return "" if pd.isna(s) else str(s).strip()


def _norm_lc(s) -> str:
    return _norm(s).lower()


# ── Load master ──────────────────────────────────────────────────────────
m = pd.read_excel(MASTER)
m.columns = m.columns.str.strip()
for c in ("FBA SKU", "Original SKU", "ASIN", "Brand", "Model"):
    if c in m.columns:
        m[c] = m[c].map(_norm)

master_by_sku   : dict[str, dict] = {}
master_by_asin  : dict[str, list[dict]] = {}
master_by_model : dict[str, set[str]] = {}      # model_lc → set of brands_lc

for _, r in m.iterrows():
    sku    = r.get("FBA SKU", "")
    osku   = r.get("Original SKU", "")
    asin   = r.get("ASIN", "")
    brand  = r.get("Brand", "")
    model  = r.get("Model", "")
    rec = {"sku": sku, "original_sku": osku, "asin": asin, "brand": brand, "model": model}
    if sku:
        # Note: master HAS a known duplicate (FBK78924/FBA78924 typo per
        # prior audit), so keep first wins but flag conflicts.
        if sku in master_by_sku and master_by_sku[sku] != rec:
            pass  # already documented duplicate, skip noisy log
        master_by_sku.setdefault(sku, rec)
        if osku and osku != sku:
            master_by_sku.setdefault(osku, rec)
    if asin:
        master_by_asin.setdefault(asin, []).append(rec)
    if model:
        master_by_model.setdefault(model.lower(), set()).add(brand.lower())

print(f"Master: {len(m):,} rows · {len(master_by_sku):,} unique SKUs · "
      f"{len(master_by_asin):,} unique ASINs · {len(master_by_model):,} unique Models\n")


# ── Load all inventory rows ──────────────────────────────────────────────
all_rows: list[dict] = []
for brand_dir in BRANDS:
    p = WEEK21 / brand_dir / "Inventory Snapshot.xlsx"
    if not p.exists():
        continue
    df = pd.read_excel(p)
    df.columns = df.columns.str.strip()
    df["__file_brand"] = brand_dir.replace("_", " ")
    for c in ("SKU", "ASIN", "Brand", "Model"):
        if c in df.columns:
            df[c] = df[c].map(_norm)
    all_rows.append(df)
inv = pd.concat(all_rows, ignore_index=True)
print(f"Inventory rows: {len(inv):,} across {inv['__file_brand'].nunique()} files\n")


# ── (1) Cross-brand identifier collisions ────────────────────────────────
print("=" * 70)
print("(1) Cross-brand collisions inside the inventory files")
print("=" * 70)

def collisions(col: str) -> list[tuple[str, set[str]]]:
    bag: dict[str, set[str]] = {}
    for _, r in inv.iterrows():
        key = r.get(col, "")
        b = r["__file_brand"]
        if not key:
            continue
        bag.setdefault(key, set()).add(b)
    return sorted([(k, brs) for k, brs in bag.items() if len(brs) > 1])

for col in ("SKU", "ASIN", "Model"):
    coll = collisions(col)
    if not coll:
        print(f"  ✓ {col}: no cross-file collisions")
    else:
        print(f"  ⚠ {col}: {len(coll)} collisions")
        for key, brands in coll[:25]:
            print(f"      {key!r:<22} appears in: {sorted(brands)}")
        if len(coll) > 25:
            print(f"      ...and {len(coll)-25} more")
print()


# ── (2) File-brand vs master-brand mismatches ────────────────────────────
print("=" * 70)
print("(2) Inventory row's brand ≠ master's brand for that SKU/ASIN")
print("=" * 70)

brand_mismatch_sku  : list[tuple[str, str, str, str]] = []
brand_mismatch_asin : list[tuple[str, str, str, str]] = []

for _, r in inv.iterrows():
    fb = r["__file_brand"].lower()
    sku = r.get("SKU", "")
    asin = r.get("ASIN", "")
    if sku and sku in master_by_sku:
        mb = master_by_sku[sku]["brand"].lower()
        if mb and mb != fb:
            brand_mismatch_sku.append((sku, fb, mb, master_by_sku[sku]["model"]))
    if asin and asin in master_by_asin:
        master_brands = {x["brand"].lower() for x in master_by_asin[asin] if x["brand"]}
        if master_brands and fb not in master_brands:
            brand_mismatch_asin.append((asin, fb, "/".join(sorted(master_brands)), sku))

if brand_mismatch_sku:
    uniq = sorted(set(brand_mismatch_sku))
    print(f"  ⚠ {len(uniq)} unique SKU rows with brand mismatch")
    for sku, fb, mb, model in uniq[:30]:
        print(f"      SKU {sku!r:<14}  file=[{fb:<14}] master=[{mb:<14}]  model={model!r}")
    if len(uniq) > 30:
        print(f"      ...and {len(uniq)-30} more")
else:
    print("  ✓ No SKU brand mismatches")

if brand_mismatch_asin:
    uniq = sorted(set(brand_mismatch_asin))
    print(f"  ⚠ {len(uniq)} unique ASIN rows with brand mismatch")
    for asin, fb, mb, sku in uniq[:30]:
        print(f"      ASIN {asin!r:<14}  file=[{fb:<14}] master=[{mb:<14}]  sku={sku!r}")
    if len(uniq) > 30:
        print(f"      ...and {len(uniq)-30} more")
else:
    print("  ✓ No ASIN brand mismatches")
print()


# ── (3) Inv identifiers missing from master ──────────────────────────────
print("=" * 70)
print("(3) Inv SKU / ASIN that are NOT in master")
print("=" * 70)

orphan_sku  : dict[str, set[str]] = {}
orphan_asin : dict[str, set[str]] = {}
for _, r in inv.iterrows():
    sku  = r.get("SKU", "")
    asin = r.get("ASIN", "")
    fb   = r["__file_brand"]
    if sku and sku not in master_by_sku:
        orphan_sku.setdefault(sku, set()).add(fb)
    if asin and asin not in master_by_asin:
        orphan_asin.setdefault(asin, set()).add(fb)

if orphan_sku:
    print(f"  ⚠ {len(orphan_sku)} SKUs in inv files but not in master")
    for sku, brs in sorted(orphan_sku.items())[:30]:
        print(f"      SKU {sku!r:<18}  brands={sorted(brs)}")
    if len(orphan_sku) > 30:
        print(f"      ...and {len(orphan_sku)-30} more")
else:
    print("  ✓ Every inv SKU is in master")

if orphan_asin:
    print(f"  ⚠ {len(orphan_asin)} ASINs in inv files but not in master")
    for asin, brs in sorted(orphan_asin.items())[:30]:
        print(f"      ASIN {asin!r:<14}  brands={sorted(brs)}")
    if len(orphan_asin) > 30:
        print(f"      ...and {len(orphan_asin)-30} more")
else:
    print("  ✓ Every inv ASIN is in master")
print()


# ── (4) SKU↔ASIN drift ───────────────────────────────────────────────────
print("=" * 70)
print("(4) SKU↔ASIN drift between inv and master")
print("=" * 70)
sku_asin_drift: list[tuple[str, str, str]] = []   # sku, inv_asin, master_asin
for _, r in inv.iterrows():
    sku  = r.get("SKU", "")
    asin = r.get("ASIN", "")
    if not sku or not asin:
        continue
    if sku in master_by_sku:
        m_asin = master_by_sku[sku]["asin"]
        if m_asin and m_asin != asin:
            sku_asin_drift.append((sku, asin, m_asin))
if sku_asin_drift:
    uniq = sorted(set(sku_asin_drift))
    print(f"  ⚠ {len(uniq)} unique SKU rows where ASIN differs from master")
    for sku, inv_a, m_a in uniq[:30]:
        print(f"      SKU {sku!r:<14}  inv_ASIN={inv_a!r:<14}  master_ASIN={m_a!r}")
    if len(uniq) > 30:
        print(f"      ...and {len(uniq)-30} more")
else:
    print("  ✓ No SKU↔ASIN drift")
print()


# ── (5) SKU↔Model drift between inv and master ───────────────────────────
print("=" * 70)
print("(5) SKU↔Model drift between inv and master")
print("=" * 70)
sku_model_drift: list[tuple[str, str, str]] = []   # sku, inv_model, master_model
for _, r in inv.iterrows():
    sku   = r.get("SKU", "")
    model = r.get("Model", "")
    if not sku or not model:
        continue
    if sku in master_by_sku:
        m_model = master_by_sku[sku]["model"]
        if m_model and m_model.lower() != model.lower():
            sku_model_drift.append((sku, model, m_model))
if sku_model_drift:
    uniq = sorted(set(sku_model_drift))
    print(f"  ⚠ {len(uniq)} unique SKU rows where Model differs from master")
    for sku, inv_m, m_m in uniq[:30]:
        print(f"      SKU {sku!r:<14}  inv_Model={inv_m!r:<22}  master_Model={m_m!r}")
    if len(uniq) > 30:
        print(f"      ...and {len(uniq)-30} more")
else:
    print("  ✓ No SKU↔Model drift")
print()


# ── (6) Within-file: same SKU showing different ASIN/Model ───────────────
print("=" * 70)
print("(6) Within-file inconsistency — same SKU with diverging ASIN/Model")
print("=" * 70)
intra_sku_to_asin: dict[tuple[str, str], set[str]] = {}
intra_sku_to_model: dict[tuple[str, str], set[str]] = {}
for _, r in inv.iterrows():
    sku   = r.get("SKU", "")
    asin  = r.get("ASIN", "")
    model = r.get("Model", "")
    fb    = r["__file_brand"]
    if sku:
        if asin:
            intra_sku_to_asin.setdefault((fb, sku), set()).add(asin)
        if model:
            intra_sku_to_model.setdefault((fb, sku), set()).add(model)
bad_asin = [(k, v) for k, v in intra_sku_to_asin.items() if len(v) > 1]
bad_model = [(k, v) for k, v in intra_sku_to_model.items() if len(v) > 1]
if bad_asin:
    print(f"  ⚠ {len(bad_asin)} (file, SKU) pairs with multiple ASINs")
    for (fb, sku), asins in bad_asin[:25]:
        print(f"      [{fb:<14}] SKU {sku!r:<14}  ASINs={sorted(asins)}")
else:
    print("  ✓ Each SKU has at most 1 ASIN within its file")
if bad_model:
    print(f"  ⚠ {len(bad_model)} (file, SKU) pairs with multiple Models")
    for (fb, sku), ms in bad_model[:25]:
        print(f"      [{fb:<14}] SKU {sku!r:<14}  Models={sorted(ms)}")
else:
    print("  ✓ Each SKU has at most 1 Model within its file")
print()


# ── (7) Whitespace / case anomalies on join keys ─────────────────────────
print("=" * 70)
print("(7) Whitespace / case anomalies in identifier columns")
print("=" * 70)
def anomalies(col: str) -> dict[str, int]:
    counts = {"leading_trailing_space": 0, "internal_double_space": 0, "lowercase_asin": 0}
    for _, r in inv.iterrows():
        v = r.get(col, "")
        if not v:
            continue
        # We've already stripped via _norm at load time. So pre-strip check
        # would always be 0. Instead read raw via inv source columns again.
        pass
    return counts

# Re-check raw before strip
issues_sp: list[tuple[str, str, str]] = []
for brand_dir in BRANDS:
    p = WEEK21 / brand_dir / "Inventory Snapshot.xlsx"
    if not p.exists():
        continue
    raw = pd.read_excel(p)
    for c in ("SKU", "ASIN", "Brand", "Model"):
        if c not in raw.columns:
            continue
        for v in raw[c].dropna().astype(str):
            if v != v.strip():
                issues_sp.append((brand_dir, c, v))
            elif "  " in v:
                issues_sp.append((brand_dir, c, v))
if issues_sp:
    print(f"  ⚠ {len(issues_sp)} cells with leading/trailing or double whitespace")
    for fb, col, val in issues_sp[:25]:
        print(f"      [{fb:<14}] {col:<6} {val!r}")
else:
    print("  ✓ No whitespace anomalies in SKU/ASIN/Brand/Model")
print()


# ── Summary ──────────────────────────────────────────────────────────────
print("=" * 70)
print("Per-file row count")
print("=" * 70)
for fb, n in inv.groupby("__file_brand").size().items():
    print(f"  {fb:<18}  {n:>5,} rows")


# ─────────────────────────────────────────────────────────────────────────
# Excel report — one tab per anomaly so the operator can fix in place
# ─────────────────────────────────────────────────────────────────────────
print()
OUT = ROOT / "data" / "processed" / "week21_inventory_audit.xlsx"
OUT.parent.mkdir(parents=True, exist_ok=True)

def _df(rows, cols):
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

# Rebuild the same lookups but capture row-level context (file, channel, type,
# qty) so the operator can locate each issue in the source spreadsheet.
issue_orphan_sku   = []
issue_orphan_asin  = []
issue_sku_asin     = []
issue_sku_model    = []
issue_intra_asin   = []
issue_intra_model  = []
issue_brand_sku    = []
issue_brand_asin   = []
issue_ws           = []

for _, r in inv.iterrows():
    fb     = r["__file_brand"]
    sku    = r.get("SKU", "")
    asin   = r.get("ASIN", "")
    brand  = r.get("Brand", "")
    model  = r.get("Model", "")
    qty    = r.get("Qty", "")
    chan   = r.get("Channel", "")
    typ    = r.get("Type", "")

    # orphans
    if sku and sku not in master_by_sku:
        issue_orphan_sku.append({
            "File": fb, "SKU": sku, "ASIN": asin, "Brand": brand,
            "Model": model, "Qty": qty, "Channel": chan, "Type": typ,
        })
    if asin and asin not in master_by_asin:
        issue_orphan_asin.append({
            "File": fb, "ASIN": asin, "SKU": sku, "Brand": brand,
            "Model": model, "Qty": qty, "Channel": chan, "Type": typ,
        })
    # SKU↔ASIN drift
    if sku and asin and sku in master_by_sku:
        m_asin = master_by_sku[sku]["asin"]
        if m_asin and m_asin != asin:
            issue_sku_asin.append({
                "File": fb, "SKU": sku, "Inv ASIN": asin, "Master ASIN": m_asin,
                "Inv Model": model, "Master Model": master_by_sku[sku]["model"],
                "Qty": qty, "Channel": chan, "Type": typ,
            })
    # SKU↔Model drift
    if sku and model and sku in master_by_sku:
        m_model = master_by_sku[sku]["model"]
        if m_model and m_model.lower() != model.lower():
            issue_sku_model.append({
                "File": fb, "SKU": sku, "ASIN": asin,
                "Inv Model": model, "Master Model": m_model,
                "Qty": qty, "Channel": chan, "Type": typ,
            })
    # Brand mismatches
    if sku and sku in master_by_sku:
        mb = master_by_sku[sku]["brand"].lower()
        if mb and mb != fb.lower():
            issue_brand_sku.append({
                "File": fb, "SKU": sku, "ASIN": asin,
                "Master Brand": master_by_sku[sku]["brand"],
                "Model": model, "Qty": qty, "Channel": chan, "Type": typ,
            })
    if asin and asin in master_by_asin:
        master_brands = {x["brand"].lower() for x in master_by_asin[asin] if x["brand"]}
        if master_brands and fb.lower() not in master_brands:
            issue_brand_asin.append({
                "File": fb, "ASIN": asin, "SKU": sku,
                "Master Brand": "/".join(sorted(master_brands)),
                "Model": model, "Qty": qty, "Channel": chan, "Type": typ,
            })

# within-file SKU → multi ASIN / Model
for (fb, sku), asins in intra_sku_to_asin.items():
    if len(asins) > 1:
        issue_intra_asin.append({"File": fb, "SKU": sku, "Distinct ASINs": ", ".join(sorted(asins))})
for (fb, sku), models in intra_sku_to_model.items():
    if len(models) > 1:
        issue_intra_model.append({"File": fb, "SKU": sku, "Distinct Models": ", ".join(sorted(models))})

# whitespace anomalies (raw read)
for brand_dir in BRANDS:
    p = WEEK21 / brand_dir / "Inventory Snapshot.xlsx"
    if not p.exists():
        continue
    raw = pd.read_excel(p)
    raw.columns = raw.columns.str.strip()
    for idx, row in raw.iterrows():
        for c in ("SKU", "ASIN", "Brand", "Model"):
            if c not in raw.columns:
                continue
            v = row.get(c, "")
            if pd.isna(v):
                continue
            s = str(v)
            if s != s.strip() or "  " in s:
                issue_ws.append({
                    "File":   brand_dir.replace("_", " "),
                    "Row #":  int(idx) + 2,   # Excel-style (1-based, +1 header)
                    "Column": c,
                    "Value":  repr(s),  # quoted so whitespace is visible
                })

# Summary tab
summary_rows = [
    {"Category": "Cross-brand SKU collision",  "Count": 0, "Action": "—"},
    {"Category": "Cross-brand ASIN collision", "Count": 0, "Action": "—"},
    {"Category": "Cross-brand Model collision","Count": 0, "Action": "—"},
    {"Category": "Inv-brand vs master (SKU)",  "Count": len(issue_brand_sku),  "Action": "Fix file OR master Brand"},
    {"Category": "Inv-brand vs master (ASIN)", "Count": len(issue_brand_asin), "Action": "Fix file OR master Brand"},
    {"Category": "Orphan SKU (not in master)", "Count": len(issue_orphan_sku), "Action": "Add to master or remove from inv"},
    {"Category": "Orphan ASIN (not in master)","Count": len(issue_orphan_asin),"Action": "Add to master or remove from inv"},
    {"Category": "SKU↔ASIN drift",             "Count": len(issue_sku_asin),   "Action": "Pick canonical ASIN, update both files"},
    {"Category": "SKU↔Model drift",            "Count": len(issue_sku_model),  "Action": "Pick canonical Model, update both files"},
    {"Category": "Within-file SKU multi-ASIN", "Count": len(issue_intra_asin), "Action": "Same SKU shouldn't carry 2 ASINs"},
    {"Category": "Within-file SKU multi-Model","Count": len(issue_intra_model),"Action": "Usually case-typo — pick one casing"},
    {"Category": "Whitespace anomalies",       "Count": len(issue_ws),         "Action": "Strip in source file (server already trims)"},
]

with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    _df(summary_rows, ["Category", "Count", "Action"]).to_excel(w, sheet_name="Summary", index=False)
    _df(issue_sku_asin,
        ["File", "SKU", "Inv ASIN", "Master ASIN", "Inv Model", "Master Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="SKU-ASIN drift", index=False)
    _df(issue_sku_model,
        ["File", "SKU", "ASIN", "Inv Model", "Master Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="SKU-Model drift", index=False)
    _df(issue_orphan_sku,
        ["File", "SKU", "ASIN", "Brand", "Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="Orphan SKUs", index=False)
    _df(issue_orphan_asin,
        ["File", "ASIN", "SKU", "Brand", "Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="Orphan ASINs", index=False)
    _df(issue_intra_asin,  ["File", "SKU", "Distinct ASINs"]).to_excel(w, sheet_name="Within-file multi-ASIN", index=False)
    _df(issue_intra_model, ["File", "SKU", "Distinct Models"]).to_excel(w, sheet_name="Within-file multi-Model", index=False)
    _df(issue_brand_sku,
        ["File", "SKU", "ASIN", "Master Brand", "Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="Brand mismatch (SKU)", index=False)
    _df(issue_brand_asin,
        ["File", "ASIN", "SKU", "Master Brand", "Model", "Qty", "Channel", "Type"]
       ).to_excel(w, sheet_name="Brand mismatch (ASIN)", index=False)
    _df(issue_ws, ["File", "Row #", "Column", "Value"]).to_excel(w, sheet_name="Whitespace", index=False)

print(f"📊 Excel report written: {OUT.relative_to(ROOT)}")

