import pandas as pd
from pathlib import Path
from weekly_app.core.week import get_current_week

# =========================
# PATHS
# =========================
RAW_SALES = Path("data/raw/sales")
MASTER_FILE = Path("data/master/sku_master.xlsx")

PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = PROCESSED / "weekly_sales_snapshot.csv"


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


def detect_column(cols, keys):
    for k in keys:
        for c in cols:
            if k in c:
                return c
    return None


# =========================
# LOAD SKU MASTER
# =========================
def load_sku_master() -> pd.DataFrame:
    df = pd.read_excel(MASTER_FILE)
    df.columns = [norm(c) for c in df.columns]

    if "fba_sku" in df.columns:
        df = df.rename(columns={"fba_sku": "sku"})

    required = {"sku", "asin", "brand", "nlc"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"SKU master missing columns: {missing}")

    for c in ["category_l0", "category_l1", "category_l2"]:
        if c not in df.columns:
            df[c] = ""

    return df


# =========================
# AMAZON PARSER
# =========================
def parse_amazon(df: pd.DataFrame, week_start: str):
    df.columns = [norm(c) for c in df.columns]

    asin = detect_column(df.columns, ["asin"])
    sku = detect_column(df.columns, ["sku"])
    qty = detect_column(df.columns, ["quantity"])
    price = detect_column(df.columns, ["item_price"])

    if not all([asin, qty, price]):
        return None

    df[qty] = pd.to_numeric(df[qty], errors="coerce").fillna(0)
    df[price] = pd.to_numeric(df[price], errors="coerce").fillna(0)

    out = (
        df.groupby([asin, sku], as_index=False)
        .agg(units_sold=(qty, "sum"), sales_sp=(price, "sum"))
    )

    out["channel"] = "Amazon"
    out["week_start"] = week_start
    return out


# =========================
# OTHER CHANNEL PARSER
# =========================
def parse_other(df: pd.DataFrame, channel: str, week_start: str):
    df.columns = [norm(c) for c in df.columns]

    sku = detect_column(df.columns, ["sku"])
    qty = detect_column(df.columns, ["qty", "quantity", "units"])
    sales = detect_column(df.columns, ["sale_amount", "sales", "amount"])

    if not all([sku, qty, sales]):
        return None

    df[qty] = pd.to_numeric(df[qty], errors="coerce").fillna(0)
    df[sales] = pd.to_numeric(df[sales], errors="coerce").fillna(0)

    out = (
        df.groupby(sku, as_index=False)
        .agg(units_sold=(qty, "sum"), sales_sp=(sales, "sum"))
    )

    out["channel"] = channel
    out["week_start"] = week_start
    out["asin"] = None
    return out


# =========================
# MAIN ETL
# =========================
def run_sales_auto_etl():
    week = get_current_week()
    week_start = str(week["week_start"])

    sales_dir = RAW_SALES / week_start
    if not sales_dir.exists():
        raise FileNotFoundError(f"No sales files for {week_start}")

    sku_master = load_sku_master()
    rows = []

    for file in sales_dir.glob("*.xlsx"):
        xls = pd.ExcelFile(file)

        if "amazon" in file.stem.lower():
            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sh)
                parsed = parse_amazon(df, week_start)
                if parsed is not None:
                    rows.append(parsed)
        else:
            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sh)
                parsed = parse_other(df, sh.strip(), week_start)
                if parsed is not None:
                    rows.append(parsed)

    sales = pd.concat(rows, ignore_index=True)

    # =========================
    # JOIN SKU MASTER
    # =========================
    final = sales.merge(
        sku_master,
        how="left",
        on=["asin", "sku"],
    )

    final["sku_status"] = final["brand"].notna().map(
        {True: "MAPPED", False: "UNMAPPED"}
    )

    final["nlc"] = final["nlc"].fillna(0)
    final["sales_nlc"] = final["units_sold"] * final["nlc"]

    final = final[
        [
            "week_start",
            "channel",
            "sku",
            "asin",
            "brand",
            "model_no",
            "category_l0",
            "category_l1",
            "category_l2",
            "units_sold",
            "sales_sp",
            "sales_nlc",
            "nlc",
            "sku_status",
        ]
    ]

    final.to_csv(OUTPUT_FILE, index=False)
    return final
