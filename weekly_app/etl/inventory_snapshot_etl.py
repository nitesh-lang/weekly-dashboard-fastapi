import pandas as pd
from pathlib import Path
import re

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]
INVENTORY_BASE_DIR = BASE_DIR / "data" / "raw" / "inventory"

week_dirs = [
    d for d in INVENTORY_BASE_DIR.iterdir()
    if d.is_dir() and d.name.lower().startswith("week")
]

if not week_dirs:
    raise FileNotFoundError(f"No week folders found in {INVENTORY_BASE_DIR}")

RAW_INVENTORY_DIR = sorted(
    week_dirs,
    key=lambda d: int(re.search(r"(\d+)", d.name).group(1))
)[-1]

print(f"Using inventory week folder: {RAW_INVENTORY_DIR}")
OUTPUT_FILE = BASE_DIR / "data" / "processed" / "inventory_ams_snapshot.csv"


# =========================================================
# AUTO-DETECT LATEST WEEK (ADDITIVE, SAFE)
# =========================================================

if not RAW_INVENTORY_DIR.exists():
    base_dir = RAW_INVENTORY_DIR.parent
    if base_dir.exists():
        week_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.lower().startswith("week")]
        if week_dirs:
            RAW_INVENTORY_DIR = sorted(
                week_dirs,
                key=lambda d: int(re.search(r"(\d+)", d.name).group(1)) if re.search(r"(\d+)", d.name) else -1
            )[-1]
            print(f"ℹ️ Auto-selected inventory week folder: {RAW_INVENTORY_DIR}")
    if not RAW_INVENTORY_DIR.exists():
        raise FileNotFoundError(f"Inventory directory not found: {RAW_INVENTORY_DIR}")

# =========================================================
# LOAD RAW INVENTORY (ALL BRANDS)
# =========================================================

frames = []

for brand_dir in RAW_INVENTORY_DIR.iterdir():
    if not brand_dir.is_dir():
        continue

    inv_file = brand_dir / "Inventory Snapshot.xlsx"
    if inv_file.exists():
        temp = pd.read_excel(inv_file)
        temp.columns = temp.columns.str.lower().str.strip()
        temp["brand"] = brand_dir.name
        frames.append(temp)
    else:
        print(f"⚠️ Missing operator Inventory Snapshot.xlsx for brand: {brand_dir.name}")

    # Defensive supplement: ALSO read Seller FBA Inventory (SP-API).xlsx
    # when present.  Without this, if the operator drops the manual
    # Inventory Snapshot but it doesn't have Amazon-channel rows (the
    # operator may rely on the SP-API auto-pull instead), Amazon FBA
    # stock disappears from inventory_ams_snapshot.csv.  The same gap
    # already bit inventory_model_snapshot.py for W26 — pre-empting
    # the same hit here.
    fba_file = brand_dir / "Seller FBA Inventory (SP-API).xlsx"
    if fba_file.exists():
        try:
            fba = pd.read_excel(fba_file)
        except Exception:
            fba = None
        if fba is not None and not fba.empty:
            fba.columns = fba.columns.str.lower().str.strip()
            if "inventory" in fba.columns and "model" in fba.columns:
                fba["qty"]     = pd.to_numeric(fba["inventory"], errors="coerce").fillna(0)
                fba["channel"] = "AMAZON"
                if "type" not in fba.columns:
                    fba["type"] = "FBA"
                if "week" not in fba.columns and "Week" in pd.read_excel(fba_file, nrows=0).columns:
                    fba["week"] = pd.read_excel(fba_file)["Week"]
                fba["brand"] = brand_dir.name
                frames.append(fba)

    # Same defensive supplement for the 1P side — read Vendor SOH
    # (SP-API).xlsx when present so 1P inventory survives even if the
    # operator stops dropping Inventory Snapshot.xlsx.  The file is
    # already in the (lowercased) channel=qty schema downstream
    # expects, so we just need to filter to the 1P channel rows and
    # tag the brand.
    vendor_soh_file = brand_dir / "Vendor SOH (SP-API).xlsx"
    if vendor_soh_file.exists():
        try:
            vsoh = pd.read_excel(vendor_soh_file)
        except Exception:
            vsoh = None
        if vsoh is not None and not vsoh.empty:
            vsoh.columns = vsoh.columns.str.lower().str.strip()
            if "channel" in vsoh.columns and "qty" in vsoh.columns and "model" in vsoh.columns:
                vsoh_1p = vsoh[vsoh["channel"].astype(str).str.strip().str.lower() == "1p"].copy()
                if not vsoh_1p.empty:
                    vsoh_1p["brand"] = brand_dir.name
                    frames.append(vsoh_1p)

if not frames:
    raise RuntimeError("No inventory files found for any brand")

df = pd.concat(frames, ignore_index=True)
print("RAW INVENTORY ROWS (ALL BRANDS):", len(df))

# =========================================================
# BACKFILL MODEL + CATEGORIES FROM sku_master (ASIN-keyed)
# =========================================================
# Operator's Inventory Snapshot.xlsx ships with SKU + ASIN + Brand +
# Qty + Channel populated, but Model + category_l0/l1/l2 are routinely
# blank — they're expected to come from sku_master.  Without this
# backfill the groupby below collapses everything into a single
# "Model=NAN" row and inventory_amazon ends up 0 portfolio-wide
# (audit check 3 fires).
if "asin" in df.columns:
    sku_master_file = BASE_DIR / "data" / "master" / "sku_master.xlsx"
    if sku_master_file.exists():
        _m = pd.read_excel(sku_master_file)
        _m.columns = _m.columns.str.strip()
        _need = ["ASIN", "Model", "category_l0", "category_l1", "category_l2"]
        _m = _m[[c for c in _need if c in _m.columns]].copy()
        rename = {"ASIN": "asin", "Model": "_m_model",
                  "category_l0": "_m_cl0", "category_l1": "_m_cl1", "category_l2": "_m_cl2"}
        _m = _m.rename(columns={k: v for k, v in rename.items() if k in _m.columns})
        _m["asin"] = _m["asin"].astype(str).str.strip()
        if "_m_model" in _m.columns:
            _m["_m_model"] = _m["_m_model"].astype(str).str.strip().str.upper()
        _m = _m.drop_duplicates(subset=["asin"])
        df["asin"] = df["asin"].astype(str).str.strip()
        df = df.merge(_m, on="asin", how="left")
        # Fill Model where blank ("nan"/"none"/empty)
        if "_m_model" in df.columns and "model" in df.columns:
            _blank = (df["model"].isna()
                      | df["model"].astype(str).str.strip().str.lower().isin(["", "nan", "none"]))
            df.loc[_blank, "model"] = df.loc[_blank, "_m_model"]
        # Fill categories where blank — adds the column if absent
        for raw_col, m_col in [("category_l0", "_m_cl0"),
                                ("category_l1", "_m_cl1"),
                                ("category_l2", "_m_cl2")]:
            if m_col not in df.columns:
                continue
            if raw_col not in df.columns:
                df[raw_col] = df[m_col]
            else:
                # Widen raw_col to object so we can assign strings even
                # when the column was created as all-NaN float64.
                df[raw_col] = df[raw_col].astype("object")
                _blank = (df[raw_col].isna()
                          | df[raw_col].astype(str).str.strip().str.lower().isin(["", "nan", "none"]))
                df.loc[_blank, raw_col] = df.loc[_blank, m_col]
        # Drop helper columns
        df = df.drop(columns=[c for c in ["_m_model", "_m_cl0", "_m_cl1", "_m_cl2"]
                              if c in df.columns])

# =========================================================
# REQUIRED COLUMN CHECK (SAFE)
# =========================================================

required_raw_cols = {"channel", "type", "qty", "week", "model"}
missing_cols = required_raw_cols - set(df.columns)

if missing_cols:
    raise ValueError(f"Missing required columns in inventory file: {missing_cols}")

# ---------------------------------------------------------
# NORMALIZE CORE FIELDS (NO LOGIC CHANGE)
# ---------------------------------------------------------

df["channel"] = df["channel"].astype(str).str.upper().str.strip()
df["type"] = df["type"].astype(str).str.upper().str.replace("-", " ").str.strip()
df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)

# =========================================================
# >>> CRITICAL FIX: NORMALIZE WEEK ("Week 52" → 52)
# =========================================================

def normalize_week(val):
    """
    Extract numeric week from values like:
    - 'Week 52'
    - 'W52'
    - '52'
    """
    if pd.isna(val):
        return None
    match = re.search(r"(\d+)", str(val))
    return int(match.group(1)) if match else None

df["week"] = df["week"].apply(normalize_week)

# Fallback: Week 18+ raw exports leave the `week` column blank.
# Derive it from the folder name (e.g. "Week 20" → 20).
folder_week_match = re.search(r"(\d+)", RAW_INVENTORY_DIR.name)
if folder_week_match:
    df["week"] = df["week"].fillna(int(folder_week_match.group(1)))

# normalize model
df["model"] = df["model"].astype(str).str.strip().str.upper()

# =========================================================
# HARD FILTER INVALID ROWS (SAFE)
# =========================================================

before_rows = len(df)

df = df[df["week"].notna()]
# Drop rows whose Model is missing/blank OR is the stringified
# 'NAN'/'NONE' (left over after astype(str) on a NaN cell).  Pre-fix
# these slipped through and collapsed the groupby into a single
# Model="NAN" row, zeroing out inventory_amazon for the latest week.
_m_norm = df["model"].astype(str).str.strip().str.lower()
df = df[df["model"].notna() & (df["model"] != "") & ~_m_norm.isin(["nan", "none"])]

df["week"] = df["week"].astype("Int64")

after_rows = len(df)

print(f"ROWS AFTER WEEK/MODEL CLEAN: {after_rows} (dropped {before_rows - after_rows})")

if df.empty:
    print("⚠️ WARNING: Inventory file has no valid rows after cleaning.")
    empty_cols = [
        "week",
        "model",
        "Model",
        "inventory_ampm",
        "inventory_1p",
        "inventory_amazon",
        "inventory_total_amazon",
        "pipeline_orders",
        "inv_units_model",
    ]
    pd.DataFrame(columns=empty_cols).to_csv(OUTPUT_FILE, index=False)
    print(f"Empty AMS inventory snapshot written to: {OUTPUT_FILE}")
    exit(0)

# =========================================================
# DERIVE AMS CHANNEL (LOGIC PRESERVED)
# =========================================================

def derive_ams_channel(row):
    # pipeline logic unchanged
    # Defensive: row["type"] can be NaN/float when an inventory line
    # lacks a type tag (e.g. _audit folder or malformed raw row).  Coerce
    # to str before the substring check so this never crashes the whole ETL.
    t = row.get("type")
    if not isinstance(t, str):
        t = "" if (t is None or pd.isna(t)) else str(t)
    if any(k in t for k in ["TRANSIT", "OPEN", "PIPELINE"]):
         return "PIPELINE"
    if row["channel"] in ["AMPM", "AMAZON", "1P"]:
        return row["channel"]
    return "OTHER"

df["ams_channel"] = df.apply(derive_ams_channel, axis=1)

# =========================================================
# PIVOT → MODEL LEVEL (AMS CONSUMER GRAIN)
# =========================================================

pivot = (
    df.pivot_table(
        index=["week", "model"],
        columns="ams_channel",
        values="qty",
        aggfunc=lambda x: x.iloc[-1] if x.name == "PIPELINE" else x.sum(),
        fill_value=0
    )
    .reset_index()
)

# =========================================================
# ENSURE REQUIRED CHANNELS EXIST (SAFE)
# =========================================================

required_channels = ["AMPM", "1P", "AMAZON", "PIPELINE"]

for ch in required_channels:
    if ch not in pivot.columns:
        pivot[ch] = 0

# =========================================================
# RENAME CHANNEL COLUMNS (AMS CONTRACT)
# =========================================================

pivot = pivot.rename(columns={
    "AMPM": "inventory_ampm",
    "1P": "inventory_1p",
    "AMAZON": "inventory_amazon",
    "PIPELINE": "pipeline_orders"
})

# =========================================================
# APPLY LOCKED BUSINESS RULES (NO CHANGE)
# =========================================================

pivot["inventory_total_amazon"] = (
    pivot["inventory_ampm"]
    + pivot["inventory_1p"]
    + pivot["inventory_amazon"]
)

pivot["inv_units_model"] = pivot["inventory_total_amazon"]

# =========================================================
# CREATE AMS JOIN KEY (NO CHANGE)
# =========================================================

pivot["Model"] = pivot["model"].astype(str).str.strip().str.upper()
pivot = pivot.drop(columns=["model"])

# =========================================================
# FINAL COLUMN ORDER (EXPLICIT, STABLE)
# =========================================================

pivot = pivot[
    [
        "week",
        "Model",
        "inventory_ampm",
        "inventory_1p",
        "inventory_amazon",
        "inventory_total_amazon",
        "pipeline_orders",
        "inv_units_model"
    ]
]


# =========================================================
# AMS SCHEMA GUARANTEE (ADDITIVE, NO LOGIC CHANGE)
# Ensures compatibility with ams_trend.py loader
# =========================================================

required_ams_cols = [
    "week",
    "Model",
    "inventory_ampm",
    "inventory_1p",
    "inventory_amazon",
    "inventory_total_amazon",
    "pipeline_orders",
    "inv_units_model",
]

for c in required_ams_cols:
    if c not in pivot.columns:
        pivot[c] = 0

# enforce dtypes expected by merge_asof
pivot["week"] = pd.to_numeric(pivot["week"], errors="coerce")
pivot["Model"] = pivot["Model"].astype(str).str.strip().str.upper()

# =========================================================
# WRITE OUTPUT
# =========================================================

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
pivot.to_csv(OUTPUT_FILE, index=False)

print(f"AMS inventory snapshot written to: {OUTPUT_FILE}")
print("FINAL ROW COUNT:", len(pivot))
