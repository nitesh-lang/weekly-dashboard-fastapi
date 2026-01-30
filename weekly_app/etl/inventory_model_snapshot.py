# ============================================================
# INVENTORY MODEL SNAPSHOT – AUTO ETL (MODEL + BRAND + WEEK)
# ============================================================

from pathlib import Path
import pandas as pd
import re

# ------------------------------------------------------------
# BASE PATHS
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_INV_DIR = BASE_DIR / "data" / "raw" / "inventory"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

OUT_FILE = PROCESSED_DIR / "inventory_model_snapshot.csv"


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def extract_week(value):
    """
    Extracts 'Week XX' from:
    - Week column
    - Folder name
    """
    if pd.isna(value):
        return None
    m = re.search(r"\d+", str(value))
    return f"Week {int(m.group())}" if m else None


def extract_brand(file_path: Path):
    """
    Brand detection priority:
    1. Filename
    2. Parent folder
    """
    ref = f"{file_path.parent.name} {file_path.stem}".lower()

    if "nexlev" in ref:
        return "Nexlev"
    if "white" in ref or "mulberry" in ref:
        return "White Mulberry"
    if "audio" in ref or "array" in ref:
        return "Audio Array"
    if "am" in ref:
        return "AMPM"

    return "Unknown"


# ------------------------------------------------------------
# MAIN ETL
# ------------------------------------------------------------
def run_inventory_etl():
    """
    Inventory rules (LOCKED):
    - Brand + Model is master
    - Week-wise inventory
    - Inventory Units = SUM(Qty)
    - Inventory Value = 0
    """

    if not RAW_INV_DIR.exists():
        print("⚠ INVENTORY RAW DIR NOT FOUND – SKIPPING")
        return

    records = []

    # --------------------------------------------------------
    # RECURSIVE FILE SCAN
    # --------------------------------------------------------
    for f in RAW_INV_DIR.rglob("*.xlsx"):
        try:
            df = pd.read_excel(f)
        except Exception:
            continue

        df.columns = [c.strip().lower() for c in df.columns]

        # mandatory columns
        if "model" not in df.columns or "qty" not in df.columns:
            continue

        # ----------------------------------------------------
        # BRAND
        # ----------------------------------------------------
        df["brand"] = extract_brand(f)

        # ----------------------------------------------------
        # MODEL
        # ----------------------------------------------------
        df["model"] = (
            df["model"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        # ----------------------------------------------------
        # QTY
        # ----------------------------------------------------
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)

        # ----------------------------------------------------
        # WEEK
        # ----------------------------------------------------
        if "week" in df.columns:
            df["week"] = df["week"].apply(extract_week)
        else:
            df["week"] = extract_week(f.parent.name)

        df = df.dropna(subset=["week"])

        if df.empty:
            continue

        # ----------------------------------------------------
        # AGGREGATION (SINGLE PASS – NO DOUBLE COUNT)
        # ----------------------------------------------------
        grp = (
            df.groupby(
                ["week", "brand", "model"],
                as_index=False
            )
            .agg(
                inventory_units=("qty", "sum")
            )
        )

        grp["inventory_value"] = 0

        records.append(grp)

    # --------------------------------------------------------
    # FINAL OUTPUT
    # --------------------------------------------------------
    if not records:
        print("⚠ NO VALID INVENTORY FILES FOUND")
        return

    out = pd.concat(records, ignore_index=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)

    print("✅ INVENTORY MODEL SNAPSHOT GENERATED")
    print(f"📦 Rows written: {len(out)}")
    print(f"📁 Output: {OUT_FILE}")


# ------------------------------------------------------------
# RUN
# ------------------------------------------------------------
if __name__ == "__main__":
    run_inventory_etl()
