import pandas as pd
import os
import warnings

from .category_alias import alias_map_for_file, apply_alias, collapse_rows

# =========================
# CONSTANTS
# =========================
GST_RATE = 0.18

PLANNING_FOLDER = os.path.join("data", "planning", "nexlev")

# =========================
# COMMON HELPERS
# =========================
def to_sparkline_svg(values, width=110, height=28):
    if not values or len(values) < 2 or all(v == 0 for v in values):
        return ""
    mn, mx = min(values), max(values)
    rng = (mx - mn) or 1
    n = len(values)
    step = width / (n - 1)
    third = max(1, n // 3)
    first_avg = sum(values[:third]) / third
    last_avg = sum(values[-third:]) / third
    if last_avg > first_avg * 1.05:
        color, fill = "#0f9b6e", "rgba(15,155,110,0.12)"
    elif last_avg < first_avg * 0.95:
        color, fill = "#dc2626", "rgba(220,38,38,0.10)"
    else:
        color, fill = "#4f6ef7", "rgba(79,110,247,0.10)"
    pad = 3
    pts = []
    for i, v in enumerate(values):
        x = i * step
        y = height - ((v - mn) / rng) * (height - pad * 2) - pad
        pts.append(f"{x:.1f},{y:.1f}")
    line = " ".join(pts)
    area = f"0,{height} " + line + f" {width},{height}"
    last_x, last_y = pts[-1].split(",")
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'preserveAspectRatio="none" style="display:block;overflow:visible;">'
        f'<polygon fill="{fill}" stroke="none" points="{area}"/>'
        f'<polyline fill="none" stroke="{color}" stroke-width="1.6" '
        f'stroke-linecap="round" stroke-linejoin="round" points="{line}"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="2" fill="{color}"/>'
        f'</svg>'
    )

def norm(c):
    return (
        str(c)
        .lower()
        .replace("\ufeff", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
        .strip()
    )

def clean_money(x):
    if pd.isna(x):
        return 0.0
    return float(
        str(x)
        .replace("₹", "")
        .replace(",", "")
        .replace("INR", "")
        .strip()
    )

def empty_df():
    return pd.DataFrame()

# =========================
# PLANNING FILE RESOLVER
# =========================
def get_planning_file_for_date(d):
    if not isinstance(d, pd.Timestamp):
        d = pd.to_datetime(d)

    month = d.strftime("%b")
    year = d.strftime("%Y")
    filename = f"ASIN Planning file - {month} {year}.xlsx"
    return os.path.join(PLANNING_FOLDER, filename)

# =========================
# FILE LOADER
# =========================
def load_file(source, sheet_name=0, skiprows=0):
    try:
        if hasattr(source, "filename"):
            source.file.seek(0)
            name = source.filename.lower()

            if name.endswith(".csv") or name.endswith(".txt"):
                return pd.read_csv(source.file, skiprows=skiprows)

            if name.endswith(".xlsx") or name.endswith(".xls"):
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=UserWarning,
                        module="openpyxl",
                    )
                    import io
                    content = source.file.read()
                    return pd.read_excel(
                    io.BytesIO(content),
                    sheet_name=sheet_name,
                    skiprows=skiprows,
                     engine="openpyxl",
                     )

        if isinstance(source, str) and os.path.exists(source):
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=UserWarning,
                    module="openpyxl",
                )
                return pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl")

    except Exception as e:
        print("LOAD_FILE_ERROR:", e)

    return empty_df()

# =========================
# PLANNING DATA
# =========================
def _category_alias_for(ref_date):
    """Alias map built from BOTH tabs' category columns for this month.

    Nexlev's 12 categories have no word-order duplicates today, so this is a
    no-op here — it is wired up so the two brands behave identically and a
    future "Care Hair" typed next to "Hair Care" is caught the same way.
    """
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        return {}

    def _columns():
        names = []
        for sheet in ("Main", "Category"):
            raw = load_file(path, sheet_name=sheet)
            if raw is None or raw.empty:
                continue
            raw.columns = [norm(c) for c in raw.columns]
            if "category" in raw.columns:
                col = raw["category"]
                if hasattr(col, "columns"):
                    col = col.iloc[:, 0]
                names.append(col.tolist())
        return names

    return alias_map_for_file(path, _columns)


def category_alias_map(ref_date):
    """Public accessor: {raw category spelling -> canonical spelling}.

    The routers use this to normalise category names that reach the payload
    from somewhere other than the planning file (the sku_master fallback on
    the ASIN table), so one product is never labelled "Dynamic Microphone" in
    one table and "Microphone Dynamic" in the next.
    """
    return _category_alias_for(ref_date)


def load_planning_main(ref_date):
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        print("PLANNING FILE MISSING:", path)
        return empty_df()

    df = load_file(path, sheet_name="Main")
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]

    if "asin" not in df.columns:
        return empty_df()

    df["asin"] = df["asin"].astype(str).str.upper().str.strip()
    # SAFETY GUARD: a hand-edited plan could accidentally list the same ASIN
    # twice, which would double-count that product in the ASIN table. Keep the
    # first occurrence only. (No-op when there are no duplicates.)
    df = df.drop_duplicates(subset="asin", keep="first")
    # Rename word-order/case variants of a category to one spelling so the
    # ASIN->category map agrees with the Category tab's targets.
    df = apply_alias(df, _category_alias_for(ref_date))
    return df

def load_planning_category(ref_date):
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        return empty_df()

    df = load_file(path, sheet_name="Category")
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]
    # Fold variant rows of one category together, SUMMING their goals.
    df = collapse_rows(df, _category_alias_for(ref_date))
    return df

# =========================
# CORE INGESTION
# =========================
def build_rows(file, account, sales_date, is_vendor):
    df = load_file(file, skiprows=1 if is_vendor else 0)
    return build_rows_from_df(df, account, sales_date, is_vendor)


def build_rows_from_df(df, account, sales_date, is_vendor):
    """Transform an already-loaded DataFrame of raw per-ASIN sales into ledger
    rows. Shared by the Excel upload path (build_rows) and the automated
    SP-API sync endpoint (/api/sync-sales) so both produce identical numbers.

    Expected (case/space-insensitive) columns:
      Seller Central: parentASIN|ASIN, Ordered Product Sales, Units Ordered
      Vendor Central: ASIN, Ordered Revenue, Units Ordered|Shipped Units
    """
    if df is None or df.empty:
        return empty_df()

    df = df.copy()
    df.columns = [norm(c) for c in df.columns]

    if "parentasin" in df.columns:
        df["ASIN"] = df["parentasin"]
    elif "asin" in df.columns:
        df["ASIN"] = df["asin"]
    else:
        return empty_df()

    df["ASIN"] = df["ASIN"].astype(str).str.upper().str.strip()
    # Drop obvious junk rows (blank / totals rows) but NOT real ASINs.
    df = df[~df["ASIN"].isin(["", "NAN", "NONE", "NAT"])]
    if df.empty:
        return empty_df()

    # SILENT-DROP FIX
    # -----------------------------------------------------------------
    # We used to keep ONLY ASINs present in this month's planning file and
    # silently discard everything else. When a planning file shrank (e.g. the
    # Jul sheet had 37 ASINs vs Jun's 91) that quietly erased ~56 selling
    # ASINs' revenue from the dashboard with zero warning.
    #
    # We now KEEP every sold ASIN. Out-of-plan ASINs simply carry no target
    # and are surfaced separately ("No Plan") on the dashboard. We only log
    # the mismatch so it is visible in the server logs.
    plan_main = load_planning_main(sales_date)
    if not plan_main.empty:
        allowed_asins = set(plan_main["asin"])
        out_of_plan = sorted(set(df["ASIN"]) - allowed_asins)
        if out_of_plan:
            preview = ", ".join(out_of_plan[:10]) + ("…" if len(out_of_plan) > 10 else "")
            print(f"[build_rows] {account}: {len(out_of_plan)} ASIN(s) NOT in plan for "
                  f"{pd.to_datetime(sales_date).date()} — KEPT (no target): {preview}")
    else:
        print(f"[build_rows] {account}: planning file MISSING for "
              f"{pd.to_datetime(sales_date).date()} — ingesting sales without target validation")

    # =========================
    # SELLER CENTRAL LOGIC
    # =========================
    if not is_vendor:
        if "orderedproductsales" not in df.columns:
            return empty_df()

        # 🔒 STRICT BUSINESS RULE
        # ALL ACCOUNTS → ONLY B2C (ignore B2B for planning consistency)
        # Audio Array → ONLY B2C
        # Cambium Retail / Viomi → B2C + B2B
        df["sales"] = df["orderedproductsales"].apply(clean_money)

        # B2B sales are intentionally ignored for all Seller Central accounts

        df["net_sales"] = df["sales"] / (1 + GST_RATE)

    # =========================
    # VENDOR CENTRAL
    # =========================
    else:
        if "orderedrevenue" not in df.columns:
            return empty_df()

        df["sales"] = df["orderedrevenue"].apply(clean_money)
        df["net_sales"] = df["sales"]

    df["date"] = pd.to_datetime(sales_date)
    df["account"] = account

    # Units Ordered (B2C only — exclude B2B column)
    if not is_vendor:
        units_col = next(
            (c for c in df.columns if "unitsordered" in c and "b2b" not in c), None
        )
    else:
        units_col = next(
            (c for c in df.columns if "unitsordered" in c or "shippedunits" in c), None
        )
    df["units"] = pd.to_numeric(df[units_col], errors="coerce").fillna(0).astype(int) if units_col else 0

    return df[["date", "account", "ASIN", "sales", "net_sales", "units"]]

# =========================
# KPI
# =========================
def calculate_kpis(df, ref_date):
    zeros = {
        "monthly_target": 0,
        "target_till": 0,
        "actual": 0,
        "achievement": 0,
        "pace": 0,
    }
    if df.empty:
        return zeros

    months = sorted(df["date"].dropna().dt.to_period("M").unique())
    if not months:
        return zeros

    monthly_target_sum = 0.0
    target_till_sum = 0.0
    months_with_plan = []

    for m_period in months:
        m_start = m_period.to_timestamp()
        plan = load_planning_main(m_start)
        if plan.empty:
            continue
        # Match the month-goal column tolerant of abbreviated ("jul") or
        # full-name ("july") month spellings in the planning file header.
        abbr = m_start.strftime("%b").lower()          # e.g. "jul"
        full = m_start.strftime("%B").lower()          # e.g. "july"
        candidates = {f"{abbr}goalprojected", f"{full}goalprojected"}
        monthly_col = next((c for c in candidates if c in plan.columns), None)
        if monthly_col is None or "perdaygoalprojected" not in plan.columns:
            continue
        months_with_plan.append(m_period)
        monthly_target_sum += float(plan[monthly_col].sum())
        days_this_month = df[df["date"].dt.to_period("M") == m_period]["date"].nunique()
        target_till_sum += float(plan["perdaygoalprojected"].sum()) * days_this_month

    if not months_with_plan:
        return zeros

    df_aligned = df[df["date"].dt.to_period("M").isin(months_with_plan)]
    actual = float(df_aligned["net_sales"].sum())

    return {
        "monthly_target": round(monthly_target_sum, 1),
        "target_till": round(target_till_sum, 1),
        "actual": round(actual, 1),
        "achievement": actual / monthly_target_sum if monthly_target_sum else 0,
        "pace": actual / target_till_sum if target_till_sum else 0,
    }

# =========================
# DAY / MTD / WEEK
# =========================
def _per_day_target_by_month(periods):
    """Map each pd.Period('M') to its plan's summed perdaygoalprojected. Skips months with no plan."""
    out = {}
    for m_period in periods:
        plan = load_planning_main(m_period.to_timestamp())
        if plan.empty or "perdaygoalprojected" not in plan.columns:
            continue
        out[m_period] = float(plan["perdaygoalprojected"].sum())
    return out

def day_wise_performance(df, ref_date):
    if df.empty:
        return []

    months = sorted(df["date"].dropna().dt.to_period("M").unique())
    rate_by_month = _per_day_target_by_month(months)
    if not rate_by_month:
        return []

    agg = {"net_sales": "sum"}
    if "units" in df.columns:
        agg["units"] = "sum"
    d = df.groupby("date", as_index=False).agg(agg)
    if "units" not in d.columns:
        d["units"] = 0

    d["target"] = d["date"].dt.to_period("M").map(rate_by_month)
    d = d[d["target"].notna()].copy()
    if d.empty:
        return []
    d["actual"] = d["net_sales"]
    d["achieved"] = (d["net_sales"] / d["target"]).round(2)
    return d.to_dict("records")

def mtd_chart(df, ref_date):
    empty = {"labels": [], "actual": [], "target": []}
    if df.empty:
        return empty

    months = sorted(df["date"].dropna().dt.to_period("M").unique())
    rate_by_month = _per_day_target_by_month(months)
    if not rate_by_month:
        return empty

    d = df.groupby("date", as_index=False)["net_sales"].sum().sort_values("date").reset_index(drop=True)
    d["target"] = d["date"].dt.to_period("M").map(rate_by_month)
    d = d[d["target"].notna()].reset_index(drop=True)
    if d.empty:
        return empty

    d["actual"]     = d["net_sales"]
    d["cum_actual"] = d["actual"].cumsum()
    d["cum_target"] = d["target"].cumsum()

    return {
        "labels": d["date"].dt.strftime("%d %b").tolist(),
        "actual": d["cum_actual"].round(1).tolist(),
        "target": d["cum_target"].round(1).tolist(),
    }

def week_wise(df):
    if df.empty:
        return "<p>No data</p>"

    return (
        df.assign(week=df["date"].dt.to_period("W").astype(str))
        .groupby("week", as_index=False)["net_sales"]
        .sum()
        .round(1)
        .to_html(index=False, classes="table table-striped table-bordered table-sm")
    )

# =========================
# FILTER
# =========================
def filter_by_date_range(df, f, t):
    return df[(df["date"] >= f) & (df["date"] <= t)]

# =========================
# ASIN TARGET VS ACTUAL
# =========================
def asin_target_vs_actual(ledger, f, t):
    ledger_filtered = filter_by_date_range(ledger, f, t).copy()
    if ledger_filtered.empty:
        return []

    plan = load_planning_main(f)
    if plan.empty:
        return []

    # SAFETY GUARD: if a plan file is ever missing the per-day goal column,
    # don't crash — treat targets as 0 so ASINs and actuals still show.
    if "perdaygoalprojected" not in plan.columns:
        plan = plan.copy()
        plan["perdaygoalprojected"] = 0

    days = ledger_filtered["date"].nunique()
    plan["period_target"] = plan["perdaygoalprojected"] * days

    # Target UNITS must scale with the selected period, exactly like the ₹ target.
    # 'tu' is the per-day unit goal (= monthly unit goal / days-in-month), so the
    # period target = tu * days. Older sheets without a 'tu' column stay at 0.
    if "tu" in plan.columns:
        plan["period_target_units"] = pd.to_numeric(plan["tu"], errors="coerce").fillna(0) * days
    else:
        plan["period_target_units"] = 0

    # Net sales per ASIN
    actual = ledger_filtered.groupby("ASIN", as_index=False)["net_sales"].sum()

    # Units ordered = number of daily rows per ASIN (each row = 1 day upload)
    units = ledger_filtered.groupby("ASIN", as_index=False)["net_sales"].count()
    units = units.rename(columns={"net_sales": "units_ordered"})

    # Also aggregate real units if available
    if "units" in ledger_filtered.columns and ledger_filtered["units"].sum() > 0:
        units_real = ledger_filtered.groupby("ASIN", as_index=False)["units"].sum()
        units_real = units_real.rename(columns={"units": "units_ordered"})
    else:
        units_real = units  # fallback to row-count proxy

    merged = plan.merge(actual, left_on="asin", right_on="ASIN", how="left")
    merged = merged.merge(units_real, left_on="asin", right_on="ASIN", how="left")

    merged["net_sales"]     = merged["net_sales"].fillna(0)
    merged["units_ordered"] = merged["units_ordered"].fillna(0).astype(int)

    # Daily series per ASIN for sparklines
    daily = ledger_filtered.pivot_table(
        index="ASIN", columns="date", values="net_sales", aggfunc="sum", fill_value=0
    )
    spark_map = {}
    if not daily.empty:
        full_range = pd.date_range(daily.columns.min(), daily.columns.max())
        daily = daily.reindex(columns=full_range, fill_value=0)
        for asin_val in daily.index:
            spark_map[str(asin_val)] = to_sparkline_svg(daily.loc[asin_val].tolist())

    # norm() strips '#' so 'Model#' becomes 'model' in the DataFrame
    model_col = next((c for c in ["model#", "model", "modelno", "modelnumber", "sku"] if c in plan.columns), None)

    rows = []
    for _, r in merged.iterrows():
        target = round(float(r["period_target"]), 1)
        actual_val = round(float(r["net_sales"]), 1)
        ach = round((actual_val / target * 100), 1) if target else 0.0
        model_no = ""
        if model_col:
            raw = r.get(model_col, "")
            if pd.notna(raw) and str(raw).strip() not in ("", "nan"):
                model_no = str(raw).strip()
        asin_str = str(r["asin"])
        rows.append({
            "asin":          asin_str,
            "model_no":      model_no,
            "category":      str(r.get("category", "")),
            "product_name":  str(r.get("productname", "")),
            "target":        target,
            "actual":        actual_val,
            "units_ordered": int(r["units_ordered"]),
            "target_units": int(round(float(r.get("period_target_units", 0) or 0))),
            "achievement":   ach,
            "sparkline":     spark_map.get(asin_str, ""),
            "out_of_plan":   False,
        })

    # ---- OUT-OF-PLAN ASINs (silent-drop fix) ----
    # ASINs that actually sold in the period but are NOT in this month's plan.
    # They have no target; we still list them (target = 0, "No Plan") so their
    # revenue is visible and the ASIN table reconciles with the KPI total.
    plan_asins = set(plan["asin"].astype(str))
    extra = actual[~actual["ASIN"].astype(str).isin(plan_asins)]
    for _, r in extra.iterrows():
        asin_str = str(r["ASIN"])
        actual_val = round(float(r["net_sales"]), 1)
        u_series = units_real.loc[units_real["ASIN"].astype(str) == asin_str, "units_ordered"]
        rows.append({
            "asin":          asin_str,
            "model_no":      "",
            "category":      "— No Plan —",
            "product_name":  "",
            "target":        0.0,
            "actual":        actual_val,
            "units_ordered": int(u_series.iloc[0]) if len(u_series) else 0,
            "target_units":  0,
            "achievement":   0.0,
            "sparkline":     spark_map.get(asin_str, ""),
            "out_of_plan":   True,
        })

    return rows

# =========================
# CATEGORY TARGET VS ACTUAL
# =========================
def category_target_vs_actual(ledger, f, t):
    ledger = filter_by_date_range(ledger, f, t)
    ledger = ledger.copy()
    if ledger.empty:
        return []

    plan_main = load_planning_main(f)
    plan_cat = load_planning_category(f)
    if plan_main.empty or plan_cat.empty:
        return []

    # SAFETY GUARD: don't crash if the Category sheet is missing its per-day
    # goal column — treat category targets as 0 so actuals still show.
    if "perdaygoal" not in plan_cat.columns:
        plan_cat = plan_cat.copy()
        plan_cat["perdaygoal"] = 0

    asin_category = plan_main.set_index("asin")["category"].to_dict()
    # Out-of-plan ASINs have no category — bucket them so their revenue is not
    # silently dropped from the category totals (pandas groupby drops NaN keys).
    ledger["category"] = ledger["ASIN"].map(asin_category).fillna("— No Plan —")

    actual = ledger.groupby("category", as_index=False)["net_sales"].sum()

    days = ledger["date"].nunique()
    plan_cat["period_target"] = plan_cat["perdaygoal"] * days

    # Outer join so the "No Plan" bucket (in actual but not in plan_cat) is kept.
    merged = plan_cat.merge(actual, on="category", how="outer")
    merged["perdaygoal"]     = pd.to_numeric(merged["perdaygoal"], errors="coerce").fillna(0)
    merged["period_target"]  = pd.to_numeric(merged["period_target"], errors="coerce").fillna(0)
    merged["net_sales"]      = pd.to_numeric(merged["net_sales"], errors="coerce").fillna(0)

    rows = []
    for _, r in merged.iterrows():
        target = round(float(r["period_target"]), 1)
        actual_val = round(float(r["net_sales"]), 1)
        per_day = round(float(r["perdaygoal"]), 1)
        ach = round((actual_val / target * 100), 1) if target > 0 else 0.0
        rows.append({
            "category":   str(r["category"]),
            "per_day":    per_day,
            "target":     target,
            "actual":     actual_val,
            "achievement": ach,
            "is_total":   False,
        })

    # ── Total row ──
    if rows:
        tot_per_day = round(sum(r["per_day"] for r in rows), 1)
        tot_target  = round(sum(r["target"]  for r in rows), 1)
        tot_actual  = round(sum(r["actual"]  for r in rows), 1)
        tot_ach     = round((tot_actual / tot_target * 100), 1) if tot_target > 0 else 0.0
        rows.append({
            "category":    "Total",
            "per_day":     tot_per_day,
            "target":      tot_target,
            "actual":      tot_actual,
            "achievement": tot_ach,
            "is_total":    True,
        })

    return rows

# =========================
# DATA INTEGRITY & VALIDATION (READ-ONLY)
# =========================
def validation_summary(ledger, f, t):
    """
    Read-only validation helper.
    Does NOT mutate data.
    Used only for dashboard reconciliation & audit visibility.
    """
    try:
        # Defensive copy
        df = ledger.copy()

        # Apply same filter logic
        if f is not None and t is not None:
            df = df[(df["date"] >= f) & (df["date"] <= t)]

        if df.empty:
            return None

        # ---------- ASIN VALIDATION ----------
        plan = load_planning_main(f or df["date"].max())
        out_of_plan_sales = 0.0
        if plan.empty:
            extra_asins = 0
        else:
            plan_asins = set(plan["asin"].unique())
            oop_mask = ~df["ASIN"].isin(plan_asins)
            extra_asins = int(df.loc[oop_mask, "ASIN"].nunique())
            out_of_plan_sales = round(float(df.loc[oop_mask, "net_sales"].sum()), 1)

        # ---------- AUDIO ARRAY (NEXLEV) DATA-HYGIENE CHECK ----------
        # Nexlev is Seller Central B2C: net_sales should be sales / 1.18 (GST
        # stripped), so net_sales/sales ≈ 0.847. Legacy/raw rows where GST was
        # NOT stripped show net_sales ≈ sales (ratio ≈ 1.0) — those are the real
        # anomalies. The old check (sales != net_sales) flagged EVERY row,
        # because GST always makes them differ: a permanent false alarm.
        audio_array_b2b_rows = 0
        aa = df[(df["account"] == "Nexlev") & (df["sales"] > 0)]
        if not aa.empty:
            ratio = aa["net_sales"] / aa["sales"]
            audio_array_b2b_rows = int((ratio > 0.95).sum())

        # ---------- RECONCILIATION ----------
        kpi_actual = round(df["net_sales"].sum(), 1)

        day_sum = (
            df.groupby("date", as_index=False)["net_sales"]
            .sum()["net_sales"]
            .sum()
        )
        day_sum = round(day_sum, 1)

        difference = round(kpi_actual - day_sum, 1)

        # ---------- REASON ----------
        reasons = []
        if extra_asins > 0:
            reasons.append(
                f"{extra_asins} ASIN(s) sold but not in the planning file "
                f"(₹{out_of_plan_sales:,.0f} kept as 'No Plan')."
            )
        if audio_array_b2b_rows > 0:
            reasons.append("Historical Audio Array B2B rows detected.")
        if difference != 0 and not reasons:
            reasons.append("Difference due to partial month / missing days in selection.")

        reason_text = " ".join(reasons) if reasons else "All validations passed. Data is consistent."

        return {
            "extra_asins": extra_asins,
            "out_of_plan_sales": out_of_plan_sales,
            "audio_array_b2b_rows": audio_array_b2b_rows,
            "kpi_actual": kpi_actual,
            "daywise_sum": day_sum,
            "difference": difference,
            "reason": reason_text,
        }

    except Exception as e:
        print("VALIDATION_ERROR:", e)
        return None

# =========================
# 3-MONTH MODEL TREND
# =========================
def model_trend_3months(ledger, ref_date):
    if ledger.empty:
        return None

    ref = pd.to_datetime(ref_date)
    today = pd.Timestamp.today().normalize()

    # Walk backward from ref's month and pick up to 3 months that have a
    # planning file. Include the current partial month so the operator sees
    # month-to-date alongside the last two complete months.
    periods = []
    cursor = pd.Period(ref, freq="M")
    for _ in range(24):
        m_start = cursor.to_timestamp()
        m_end = cursor.to_timestamp(how="end").normalize()
        if os.path.exists(get_planning_file_for_date(m_start)):
            periods.append((m_start, m_end, m_start.strftime("%b %Y")))
            if len(periods) == 3:
                break
        cursor = cursor - 1

    if len(periods) < 3:
        return None

    periods.reverse()  # oldest → newest
    month_labels = [p[2] for p in periods]

    asin_model    = {}
    asin_category = {}
    for m_start, _, _ in periods:
        plan = load_planning_main(m_start)
        if plan.empty:
            continue
        model_col = next((c for c in plan.columns if "model" in c), None)
        for _, row in plan.iterrows():
            asin_val = str(row.get("asin", "")).upper().strip()
            if not asin_val:
                continue
            if model_col and str(row.get(model_col, "")) not in ("", "nan"):
                asin_model[asin_val] = str(row[model_col]).strip()
            if "category" in plan.columns and str(row.get("category", "")) not in ("", "nan"):
                asin_category[asin_val] = str(row["category"]).strip()

    if not asin_model:
        return None

    ledger = ledger.copy()
    ledger["model"]    = ledger["ASIN"].map(asin_model).fillna("Unknown")
    ledger["category"] = ledger["ASIN"].map(asin_category).fillna("—")

    sales_by_month = {}
    units_by_month = {}
    days_by_month: dict[str, int] = {}

    for m_start, m_end, label in periods:
        sl = ledger[(ledger["date"] >= m_start) & (ledger["date"] <= m_end)]
        days_by_month[label] = int(sl["date"].dt.normalize().nunique()) if not sl.empty else 0
        sales_agg = sl.groupby("model")["net_sales"].sum()
        if "units" in sl.columns and sl["units"].sum() > 0:
            units_agg = sl.groupby("model")["units"].sum()
        else:
            units_agg = sl.groupby("model").size()
        for model in set(list(sales_agg.index) + list(units_agg.index)):
            if model == "Unknown":
                continue
            sales_by_month.setdefault(model, {})[label] = round(float(sales_agg.get(model, 0)), 0)
            units_by_month.setdefault(model, {})[label] = int(units_agg.get(model, 0))

    if not sales_by_month:
        return None

    rows = []
    for model in sorted(sales_by_month, key=lambda m: sum(sales_by_month[m].values()), reverse=True):
        s1 = sales_by_month[model].get(month_labels[0], 0)
        s2 = sales_by_month[model].get(month_labels[1], 0)
        s3 = sales_by_month[model].get(month_labels[2], 0)
        u1 = units_by_month.get(model, {}).get(month_labels[0], 0)
        u2 = units_by_month.get(model, {}).get(month_labels[1], 0)
        u3 = units_by_month.get(model, {}).get(month_labels[2], 0)

        if s1 == 0 and s2 == 0 and s3 == 0:
            continue

        sales_trend_pct = round(((s3 - s2) / s2) * 100, 1) if s2 > 0 else 0
        if sales_trend_pct > 5:
            trend_arrow, trend_class = "↑", "good"
        elif sales_trend_pct < -5:
            trend_arrow, trend_class = "↓", "bad"
        else:
            trend_arrow, trend_class = "→", "warn"

        cat = asin_category.get(
            next((a for a, m in asin_model.items() if m == model), ""), "—"
        )

        rows.append({
            "model": model, "category": cat,
            "s1": s1, "s2": s2, "s3": s3,
            "u1": u1, "u2": u2, "u3": u3,
            "sales_trend_pct": sales_trend_pct,
            "trend_arrow": trend_arrow,
            "trend_class": trend_class,
        })

    if not rows:
        return None

    top_rows   = rows[:12]
    top_labels = [r["model"] for r in top_rows]
    palette    = ["#4f6ef7","#0f9b6e","#f59e0b","#ef4444","#7c5cbf","#0891b2"]

    def make_datasets(keys, colors):
        return [{"label": month_labels[i], "data": [r[k] for r in top_rows],
                 "backgroundColor": colors[i]+"bb", "borderColor": colors[i], "borderWidth":1}
                for i, k in enumerate(keys)]

    return {
        "months":      month_labels,
        "days":        [days_by_month.get(l, 0) for l in month_labels],
        "rows":        rows,
        "sales_chart": {"labels": top_labels, "datasets": make_datasets(["s1","s2","s3"], palette[:3])},
        "units_chart": {"labels": top_labels, "datasets": make_datasets(["u1","u2","u3"], palette[3:])},
    }

# =========================
# MONTH-WISE ASIN COMPARISON
# =========================
def monthwise_asin_table(ledger):
    if ledger.empty:
        return "<p style='padding:32px 20px;color:#94a3b8;font-size:13px;'>No data in ledger yet.</p>"

    df = ledger.copy()
    df["month"] = df["date"].dt.to_period("M")
    pivot = (
        df.groupby(["ASIN", "month"])["net_sales"]
        .sum()
        .unstack(fill_value=0)
    )
    pivot.columns = [pd.Period(col).strftime("%b %Y") for col in pivot.columns]
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False).reset_index()
    month_cols = [c for c in pivot.columns if c not in ("ASIN", "Total")]
    rows_html = ""
    for _, row in pivot.iterrows():
        total = row["Total"]
        cells = ""
        for m in month_cols:
            val = row[m]
            cell_content = "&#8377;{:,.0f}".format(val) if val > 0 else "<span style='color:#d1d5db;'>&#8212;</span>"
            cells += "<td style='text-align:right;'>" + cell_content + "</td>"
        rows_html += "<tr><td>" + str(row["ASIN"]) + "</td>" + cells + "<td style='text-align:right;'>&#8377;{:,.0f}</td></tr>".format(total)
    header_cells = "".join("<th style='text-align:right;'>" + m + "</th>" for m in month_cols)
    return "<table><thead><tr><th>ASIN</th>" + header_cells + "<th style='text-align:right;'>Total</th></tr></thead><tbody>" + rows_html + "</tbody></table>"


def monthwise_asin_chart_data(ledger):
    empty = {"labels": [], "asins": [], "data": []}
    if ledger.empty:
        return empty
    df = ledger.copy()
    df["month"] = df["date"].dt.to_period("M")
    pivot = (
        df.groupby(["ASIN", "month"])["net_sales"]
        .sum()
        .unstack(fill_value=0)
    )
    pivot.columns = [pd.Period(c).strftime("%b %Y") for c in pivot.columns]
    pivot["_total"] = pivot.sum(axis=1)
    top = pivot.nlargest(8, "_total").drop(columns="_total")
    asins = [str(a) for a in top.index]
    data  = [top.iloc[i].tolist() for i in range(len(top))]
    return {"labels": list(top.columns), "asins": asins, "data": data}
