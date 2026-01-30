import pandas as pd
from pathlib import Path

from weekly_app.core.week import get_current_week

# =========================
# PATHS
# =========================
RAW_INVENTORY = Path("data/raw/inventory")
MASTER_FILE = Path("data/master/sku_master.xlsx")

PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = PROCESSED / "weekly_inventory_snapshot.csv"


# =========================
# HELPERS
# =========================
def norm(c: str) -> str:
    return (
        str(c)
        .lower()
        .strip()
        .replace("₹", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .replace(" ", "_")
    )


def detect_column(columns, keywords):
    for kw in keywords:
        for col in columns:
            if kw in col:
                return col
    return None


# =========================
# LOAD SKU MASTER
# =========================
def load_sku_master() -> pd.DataFrame:
    df = pd.read_excel(MASTER_FILE)
    df.columns = [norm(c) for c in df.columns]

    if "sku" not in df.columns:
        if "fba_sku" in df.columns:
            df = df.rename(columns={"fba_sku": "sku"})
        else:
            raise ValueError("SKU master must contain SKU or FBA SKU")

    # Normalize categories
    category_map = {}
    for col in df.columns:
        if "category" in col and "l0" in col:
            category_map[col] = "category_l0"
        elif "category" in col and "l1" in col:
            category_map[col] = "category_l1"
        elif "category" in col and "l2" in col:
            category_map[col] = "category_l2"

    df = df.rename(columns=category_map)

    for col in ["category_l0", "category_l1", "category_l2"]:
        if col not in df.columns:
            df[col] = ""

    required = {"sku", "brand", "nlc"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"SKU master missing columns: {missing}")

    return df


# =========================
# MAIN INVENTORY ETL
# =========================
def run_inventory_etl():
    week = get_current_week()
    week_start = str(week["week_start"])

    inv_dir = RAW_INVENTORY / week_start
    if not inv_dir.exists():
        raise FileNotFoundError(f"No inventory folder for week {week_start}")

    files = list(inv_dir.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError("No inventory Excel file found")

    inventory_file = files[0]

    df = pd.read_excel(inventory_file)
    if df.empty:
        raise ValueError("Inventory file is empty")

    df.columns = [norm(c) for c in df.columns]

    # Detect columns
    sku_col = detect_column(df.columns, ["sku"])
    qty_col = detect_column(df.columns, ["count", "qty", "quantity", "inventory"])
    channel_col = detect_column(df.columns, ["channel"])
    type_col = detect_column(df.columns, ["type", "location", "warehouse"])
    asin_col = detect_column(df.columns, ["asin"])
    model_col = detect_column(df.columns, ["model"])

    if not (sku_col and qty_col and channel_col):
        raise ValueError("Inventory must contain SKU, Count/Qty, and Channel")

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    # ==================================================
    # 🔑 CRITICAL FIX: GROUP ONLY ON REQUIRED KEYS
    # ==================================================
    grouped = (
        df.groupby(
            [sku_col, channel_col],
            dropna=False,
            as_index=False
        )
        .agg(
            inventory_units=(qty_col, "sum"),
            inventory_type=(type_col, "first") if type_col else (sku_col, "first"),
            asin=(asin_col, "first") if asin_col else (sku_col, "first"),
            model_name=(model_col, "first") if model_col else (sku_col, "first"),
        )
    )

    grouped = grouped.rename(
        columns={
            sku_col: "sku",
            channel_col: "channel",
        }
    )

    grouped["week_start"] = week_start

    # Ensure optional columns always exist
    for col in ["asin", "model_name", "inventory_type"]:
        if col not in grouped.columns:
            grouped[col] = ""

    # =========================
    # JOIN SKU MASTER
    # =========================
    sku_master = load_sku_master()

    final = grouped.merge(
        sku_master,
        how="left",
        on="sku",
        suffixes=("_inv", "_master"),
    )

    # -------------------------
    # ASIN STANDARDIZATION
    # -------------------------
    if "asin_inv" in final.columns or "asin_master" in final.columns:
        final["asin"] = (
            final.get("asin_inv", "")
            .replace("", pd.NA)
            .fillna(final.get("asin_master", ""))
        )
    elif "asin" not in final.columns:
        final["asin"] = ""

    final = final.drop(
        columns=[c for c in ["asin_inv", "asin_master"] if c in final.columns],
        errors="ignore",
    )

    # =========================
    # RECONCILIATION FLAGS
    # =========================
    final["sku_status"] = final["brand"].apply(
        lambda x: "MAPPED" if pd.notna(x) else "UNMAPPED"
    )

    final["nlc"] = final["nlc"].fillna(0)
    final["inventory_value"] = final["inventory_units"] * final["nlc"]

    # =========================
    # EXPORT UNMAPPED
    # =========================
    unmapped = final[final["sku_status"] == "UNMAPPED"]
    if not unmapped.empty:
        unmapped_file = PROCESSED / f"unmapped_inventory_skus_{week_start}.csv"
        unmapped.to_csv(unmapped_file, index=False)

    # =========================
    # FINAL OUTPUT
    # =========================
    final = final[
        [
            "week_start",
            "sku",
            "asin",
            "brand",
            "model_name",
            "channel",
            "inventory_type",
            "inventory_units",
            "nlc",
            "inventory_value",
            "sku_status",
            "category_l0",
            "category_l1",
            "category_l2",
        ]
    ]

    final.to_csv(OUTPUT_FILE, index=False)
    return final
