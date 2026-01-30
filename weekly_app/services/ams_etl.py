import pandas as pd
from pathlib import Path

# ==================================================
# OUTPUT
# ==================================================
OUTPUT_FILE = Path("data/processed/weekly_ams_snapshot.csv")

# ==================================================
# HELPERS
# ==================================================
def norm(c):
    return (
        str(c)
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .strip()
    )


def week_start_from_date(series):
    d = pd.to_datetime(series, errors="coerce")
    return (d - pd.to_timedelta(d.dt.weekday, unit="d")).dt.date.astype(str)


# ==================================================
# LOAD + NORMALIZE AMS FILE
# ==================================================
def load_and_normalize(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [norm(c) for c in df.columns]

    # ------------------------------
    # IDENTITY (SAFE & EXPLICIT)
    # ------------------------------
    if "sku" not in df.columns:
        df["sku"] = ""

    if "asin" not in df.columns:
        df["asin"] = ""

    df["sku"] = df["sku"].astype(str).str.strip()
    df["asin"] = df["asin"].astype(str).str.strip()

    df["join_key"] = df["sku"]
    df.loc[df["join_key"] == "", "join_key"] = df["asin"]

    # ------------------------------
    # DATE
    # ------------------------------
    date_col = None
    for c in ["date", "start_date", "day"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise ValueError(f"No date column found in {path.name}")

    df["week_start"] = week_start_from_date(df[date_col])

    # ------------------------------
    # METRICS (SAFE)
    # ------------------------------
    def safe(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0)
        return 0

    df["spend"] = safe("spend")
    df["sales"] = safe("sales")
    df["orders"] = safe("orders")
    df["clicks"] = safe("clicks")
    df["impressions"] = safe("impressions")

    # ------------------------------
    # CONSTANTS
    # ------------------------------
    df["brand"] = ""
    df["channel"] = "AMS"

    return df[
        [
            "week_start",
            "brand",
            "sku",
            "asin",
            "join_key",
            "channel",
            "spend",
            "sales",
            "orders",
            "clicks",
            "impressions",
        ]
    ]


# ==================================================
# RUN AMS ETL
# ==================================================
def run_ams_etl(uploaded_files: list[Path]):
    frames = []

    for f in uploaded_files:
        frames.append(load_and_normalize(f))

    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)

    final = (
        df.groupby(
            ["week_start", "brand", "sku", "asin", "join_key", "channel"],
            as_index=False,
        )
        .agg(
            spend=("spend", "sum"),
            sales=("sales", "sum"),
            orders=("orders", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
    )

    # ------------------------------
    # KPIs
    # ------------------------------
    final["cpc"] = final["spend"] / final["clicks"].replace(0, pd.NA)
    final["acos"] = final["spend"] / final["sales"].replace(0, pd.NA)
    final["roas"] = final["sales"] / final["spend"].replace(0, pd.NA)

    final = final.fillna(0)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ AMS weekly snapshot written → {OUTPUT_FILE}")
