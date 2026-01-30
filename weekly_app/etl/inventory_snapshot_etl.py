import pandas as pd
from pathlib import Path
import re

# =========================================================
# CONFIG
# =========================================================

RAW_INVENTORY_DIR = Path(
    r"G:\Other computers\My Laptop\D\Nitesh\Weekly Report - B2B + B2C\FastAPI\data\raw\inventory\Week 3"
)

OUTPUT_FILE = Path(
    r"D:\Nitesh\Weekly Report - B2B + B2C\FastAPI\data\processed\inventory_ams_snapshot.csv"
)


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
    if not inv_file.exists():
        print(f"⚠️ Missing inventory file for brand: {brand_dir.name}")
        continue

    temp = pd.read_excel(inv_file)
    temp.columns = temp.columns.str.lower().str.strip()
    temp["brand"] = brand_dir.name
    frames.append(temp)

if not frames:
    raise RuntimeError("No inventory files found for any brand")

df = pd.concat(frames, ignore_index=True)
print("RAW INVENTORY ROWS (ALL BRANDS):", len(df))

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
df["type"] = df["type"].astype(str).str.upper().str.strip()
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

# normalize model
df["model"] = df["model"].astype(str).str.strip().str.upper()

# =========================================================
# HARD FILTER INVALID ROWS (SAFE)
# =========================================================

before_rows = len(df)

df = df[df["week"].notna()]
df = df[df["model"].notna() & (df["model"] != "")]

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
    if row["type"] in ["IN-TRANSIT", "OPEN ORDER", "PIPELINE"]:
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
        aggfunc="sum",
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

pivot["Model"] = pivot["model"]

# =========================================================
# FINAL COLUMN ORDER (EXPLICIT, STABLE)
# =========================================================

pivot = pivot[
    [
        "week",
        "model",
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
