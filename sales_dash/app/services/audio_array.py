import pandas as pd
import os
import warnings

from .category_alias import alias_map_for_file, apply_alias, collapse_rows

# =========================
# CONSTANTS
# =========================
GST_RATE = 0.18
MONTHLY_TARGET = 17147488
WORKING_DAYS = 31
PER_DAY_TARGET = MONTHLY_TARGET / WORKING_DAYS

PLANNING_FOLDER = "data/planning/audio_array"

# =========================
# COMMON HELPERS
# =========================
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

def parse_units(series):
    """Units as int, tolerating Excel's text-formatted thousands separators.

    Plain pd.to_numeric turns "1,234" into NaN -> 0, silently zeroing real
    units. Vendor Central reports render Ordered Units with separators once
    volume crosses 1,000, so this was a silent zero, not an error.
    """
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce").fillna(0).astype(int)

def clean_money(x):
    if pd.isna(x):
        return 0.0
    raw = str(x)
    s = (
        raw.replace("₹", "")
        .replace(",", "")
        .replace("INR", "")
        .strip()
    )
    if s in ("", "-", "--", "N/A", "NA", "n/a"):
        return 0.0
    # Accounting-style negatives: "(1,234)" means -1234.
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1].strip()
    try:
        val = float(s)
    except ValueError:
        # A bare float() here used to raise and 500 the entire upload with no
        # indication of which cell was at fault. Reachable from the ASIN_DROP
        # logging path too, which runs on UNFILTERED rows -- so junk in a row
        # that was never going to be ingested could kill the whole upload.
        print(f"[clean_money] UNPARSEABLE money value {raw!r} — treated as 0")
        return 0.0
    return -val if neg else val

def dedupe_normed_columns(df, where):
    """Drop duplicate column names left over after norm(), keeping the first.

    The planning workbook is re-typed by hand every month, so two headers can
    collide once norm() strips spaces/dashes -- e.g. a tab carrying both
    "Per Day Goal" (revenue/day) and "Per Day Goal " (units/day, trailing space)
    normalises both to "perdaygoal". A duplicated label makes df["perdaygoal"]
    return a DataFrame instead of a Series, and the category table then dies
    with "Cannot set a DataFrame with multiple columns to the single column
    period_target" -- the whole tab 500s.

    Keeping the FIRST occurrence preserves the historical meaning: the revenue
    per-day goal is always the earlier column, the unit goal the later one.
    """
    dupes = df.columns[df.columns.duplicated()].unique().tolist()
    if not dupes:
        return df
    print(f"[planning] {where}: duplicate column name(s) after normalising "
          f"{dupes} — keeping the first of each (check the sheet headers)")
    return df.loc[:, ~df.columns.duplicated(keep="first")]

def empty_df():
    return pd.DataFrame()

# =========================
# SPARKLINE SVG (inline, no JS, no canvas)
# =========================
def to_sparkline_svg(values, width=110, height=24):
    """
    Build a tiny inline SVG sparkline from a sequence of numeric values.
    Returns '—' if there's nothing meaningful to plot. Color auto-picks
    green/red/blue based on first-third vs last-third trend.
    """
    if values is None:
        return "—"
    try:
        vals = [float(v) for v in values if v is not None and not pd.isna(v)]
    except Exception:
        return "—"
    if len(vals) < 2:
        return "—"

    lo, hi = min(vals), max(vals)
    span = hi - lo if hi > lo else 1.0
    n = len(vals)

    # Trend color: compare last third vs first third
    third = max(1, n // 3)
    first_avg = sum(vals[:third]) / third
    last_avg  = sum(vals[-third:]) / third
    if first_avg > 0 and last_avg > first_avg * 1.05:
        stroke, fill = "#15803d", "#15803d"
    elif first_avg > 0 and last_avg < first_avg * 0.95:
        stroke, fill = "#b91c1c", "#b91c1c"
    else:
        stroke, fill = "#4f6ef7", "#4f6ef7"

    pad = 2
    inner_w = width - pad * 2
    inner_h = height - pad * 2

    pts = []
    for i, v in enumerate(vals):
        x = pad + (i / (n - 1)) * inner_w
        y = pad + inner_h - ((v - lo) / span) * inner_h
        pts.append(f"{x:.1f},{y:.1f}")

    poly_line = " ".join(pts)
    area_path = f"M {pad},{pad + inner_h} L " + " L ".join(pts) + f" L {pad + inner_w},{pad + inner_h} Z"
    last_x, last_y = pts[-1].split(",")

    return (
        f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        f'preserveAspectRatio="none" style="display:inline-block;vertical-align:middle;">'
        f'<path d="{area_path}" fill="{fill}" fill-opacity="0.12" stroke="none"/>'
        f'<polyline points="{poly_line}" fill="none" stroke="{stroke}" stroke-width="1.5" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="2" fill="{stroke}"/>'
        f'</svg>'
    )

# =========================
# PLANNING FILE RESOLVER
# =========================
def get_planning_file_for_date(d):
    if not isinstance(d, pd.Timestamp):
        d = pd.to_datetime(d)

    month = d.strftime("%b")
    year = d.strftime("%Y")
    filename = f"ASIN Planning file - {month} {year}.xlsx"
    exact = os.path.join(PLANNING_FOLDER, filename)
    if os.path.exists(exact):
        return exact
    import re
    want = re.sub(r"\s+", " ", filename).strip().lower()
    try:
        names = os.listdir(PLANNING_FOLDER)
    except OSError:
        return exact

    for name in names:
        if re.sub(r"\s+", " ", name).strip().lower() == want:
            return os.path.join(PLANNING_FOLDER, name)

    # Broader fallback: the workbook is hand-saved monthly, so the spelling
    # drifts beyond whitespace ("July" for "Jul", reordered words). Accept any
    # workbook whose name carries both the 3-letter month and the year.
    mon3 = month.lower()
    matches = sorted(
        n for n in names
        # "~$..." are Excel lock files created while the workbook is open; they
        # are not readable workbooks and must never be picked up.
        if not n.startswith("~$") and n.lower().endswith((".xlsx", ".xls"))
        and mon3 in norm(n) and year in norm(n)
    )
    if matches:
        print(f"[planning] exact name not found for {month} {year}; using '{matches[0]}'")
        if len(matches) > 1:
            print(f"[planning] WARNING: {len(matches)} files match {month} {year}: {matches}")
        return os.path.join(PLANNING_FOLDER, matches[0])

    return exact

# =========================
# PLANNING FILE CACHE (mtime-keyed)
# =========================
# Planning .xlsx files are immutable until replaced. Caching by (path, mtime, sheet)
# turns ~10 Excel opens per render into 1 read per file per replacement, which is
# the single biggest warm-render speedup on Render free tier.
_PLAN_CACHE = {}

def _read_planning_excel(path, sheet_name):
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return empty_df()
    key = (path, mtime, sheet_name)
    cached = _PLAN_CACHE.get(key)
    if cached is not None:
        return cached.copy()  # caller may mutate; hand out a copy

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    except Exception as e:
        print("PLAN_CACHE_READ_ERROR:", path, sheet_name, e)
        return empty_df()

    # Drop any older entries for this same path (mtime changed → invalidated).
    for stale in [k for k in _PLAN_CACHE if k[0] == path and k[1] != mtime]:
        _PLAN_CACHE.pop(stale, None)

    _PLAN_CACHE[key] = df
    return df.copy()

# =========================
# FILE LOADER
# =========================
def load_file(source, sheet_name=0, skiprows=0):
    try:
        if hasattr(source, "filename"):
            source.file.seek(0)
            name = source.filename.lower()

            if name.endswith(".csv") or name.endswith(".txt"):
                return pd.read_csv(source.file)

            if name.endswith(".xlsx") or name.endswith(".xls"):
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=UserWarning,
                        module="openpyxl",
                    )
                    return pd.read_excel(
                        source.file,
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
# Alternative tab names seen (or plausibly typed) in hand-saved planning files.
SHEET_ALIASES = {
    "main":     ["main", "mainsheet", "asin", "asins", "asinplan", "plan", "planning", "sheet1"],
    "category": ["category", "categories", "cat", "categorysheet", "categoryplan"],
}
_SHEET_NAME_CACHE = {}

def resolve_sheet_name(path, wanted):
    """Return the actual tab name in `path` corresponding to `wanted`.

    The planning workbook is re-created by hand each month, so the tab can come
    back as "MAIN", "Main " or "Sheet1". Reading a literal sheet_name made the
    whole month load as empty. Cached by (path, mtime) like _read_planning_excel.
    """
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    key = (path, mtime, wanted)
    if key in _SHEET_NAME_CACHE:
        return _SHEET_NAME_CACHE[key]

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            # Context manager required: an unclosed ExcelFile keeps an OS handle
            # on the workbook, which on Windows blocks replacing next month's file.
            with pd.ExcelFile(path, engine="openpyxl") as xl:
                sheets = xl.sheet_names
    except Exception as e:
        print(f"[planning] could not read sheet list from {path}: {e}")
        return None

    by_norm = {norm(s): s for s in sheets}
    found = None
    if wanted in by_norm:
        found = by_norm[wanted]
    else:
        for alias in SHEET_ALIASES.get(wanted, []):
            if alias in by_norm:
                found = by_norm[alias]
                print(f"[planning] tab '{wanted}' not found; using '{found}'")
                break
        else:
            for flat, actual in by_norm.items():
                if wanted in flat or flat in wanted:
                    found = actual
                    print(f"[planning] tab '{wanted}' not found; using closest match '{actual}'")
                    break

    if found is None:
        print(f"[planning] NO tab matching '{wanted}' in {os.path.basename(path)} — tabs present: {sheets}")

    for stale in [k for k in _SHEET_NAME_CACHE if k[0] == path and k[1] != mtime]:
        _SHEET_NAME_CACHE.pop(stale, None)
    _SHEET_NAME_CACHE[key] = found
    return found


# Categories that are one product line but share no words, so no automatic
# rule can pair them — these are business calls, listed explicitly.
# (variant, surviving name). Matching is order/case/plural-insensitive, so
# "Accessories" also catches "Accessory"/"accessories".
#
# Direction is deliberate: the survivor is the larger, more specific line
# (Aug 2026 targets — Microphone Accessories 7.80 L vs Accessories 0.61 L;
# DJ Headphone 7.13 L vs Headphone 0.61 L). Only these exact names fold in;
# "Wireless Accessories", "Monitor Speakers Accessories" and "Headphone
# Amplifier" are separate categories and are NOT touched.
CATEGORY_MERGES = [
    ("Accessories", "Microphone Accessories"),
    ("Headphone", "DJ Headphone"),
]


def _category_alias_for(ref_date):
    """Alias map built from BOTH tabs' category columns for this month.

    Built from the union so the Main tab and the Category tab can never pick
    different winners — if they did, the ledger's categories (mapped via Main)
    and the targets (from Category) would stop joining and the duplicate rows
    would come straight back. Both reads hit _read_planning_excel's
    (path, mtime, sheet) cache, so this costs no extra Excel opens.
    """
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        return {}

    def _columns():
        names = []
        for wanted in ("main", "category"):
            sheet = resolve_sheet_name(path, wanted)
            if sheet is None:
                continue
            raw = _read_planning_excel(path, sheet_name=sheet)
            if raw.empty:
                continue
            raw.columns = [norm(c) for c in raw.columns]
            if "category" in raw.columns:
                col = raw["category"]
                # A duplicate normalised header hands back a DataFrame, not a
                # Series (the Aug 2026 sheet has done this) — take column one.
                if hasattr(col, "columns"):
                    col = col.iloc[:, 0]
                names.append(col.tolist())
        return names

    return alias_map_for_file(path, _columns, manual=CATEGORY_MERGES)


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

    sheet = resolve_sheet_name(path, "main")
    if sheet is None:
        return empty_df()

    df = _read_planning_excel(path, sheet_name=sheet)
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]
    df = dedupe_normed_columns(df, "Main sheet")

    if "asin" not in df.columns:
        print(f"[planning] tab '{sheet}' has no ASIN column — columns: {list(df.columns)}")
        return empty_df()

    df["asin"] = df["asin"].astype(str).str.upper().str.strip()

    # Drop placeholder / blank ASIN rows (e.g. "-", "", "NAN"). These are future-product
    # roadmap entries in the planning sheet that carry no goal and must not appear as
    # dashboard line items or enter the allowed-ASIN set.
    df = df[~df["asin"].isin(["", "-", "NAN", "NONE"])]

    # Collapse accidental duplicate ASINs (keep first) so the outer-join in the ASIN
    # target table can't double-count a single ASIN's actual sales.
    df = df.drop_duplicates(subset="asin", keep="first")

    # Word-order/case variants of one category ("Condenser Microphone" vs
    # "Microphone Condenser") are renamed to a single spelling here, so every
    # ASIN->category lookup built off this frame agrees with the Category tab.
    df = apply_alias(df, _category_alias_for(ref_date))

    return df

def load_planning_category(ref_date):
    path = get_planning_file_for_date(ref_date)
    if not os.path.exists(path):
        return empty_df()

    sheet = resolve_sheet_name(path, "category")
    if sheet is None:
        return empty_df()

    df = _read_planning_excel(path, sheet_name=sheet)
    if df.empty:
        return empty_df()

    df.columns = [norm(c) for c in df.columns]
    df = dedupe_normed_columns(df, "Category sheet")
    # Mirror the duplicate guard on the Main sheet (line ~325). Without this, a
    # category listed twice in the hand-edited Category tab duplicates that
    # category's actual through the merge and inflates the Total row, while the
    # KPI and ASIN table stay correct.
    if "category" in df.columns:
        df["category"] = df["category"].astype(str).str.strip()
        before = len(df)
        df = df.drop_duplicates(subset="category", keep="first")
        if len(df) < before:
            print(f"[planning] Category sheet had {before - len(df)} duplicate "
                  f"category row(s) — keeping first of each")
        # Now fold the word-order/case variants of one category into a single
        # row, SUMMING their goals — the two rows are two real halves of the
        # month's target for that category, not a repeated row. Runs after the
        # exact-duplicate drop above so a row typed twice is never summed.
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
      Vendor Central: ASIN, Ordered Revenue, Ordered Units|Shipped Units
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
    df = df[~df["ASIN"].isin(["", "-", "NAN", "NONE", "NAT"])]
    if df.empty:
        return empty_df()

    # SILENT-DROP FIX (ported from nexlev commit 11cf45d)
    # -----------------------------------------------------------------
    # We used to keep ONLY ASINs present in this month's planning file and
    # discard the rest -- and discard EVERY row when the planning file was
    # missing. It was logged, but the revenue still never reached the ledger,
    # so the dashboard disagreed with Amazon and no one could tell from the UI.
    #
    # We now KEEP every sold ASIN. Out-of-plan ASINs simply carry no target and
    # are surfaced separately ("No Plan"). Only the mismatch is logged.
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

        # 🔒 STRICT BUSINESS RULE: B2C only for planning consistency
        df["sales"] = df["orderedproductsales"].apply(clean_money)
        df["net_sales"] = df["sales"] / (1 + GST_RATE)

        # Units Ordered (B2C only — exclude B2B column).
        # Tolerate both header orderings: "Units Ordered" -> "unitsordered"
        # and "Ordered Units" -> "orderedunits".
        units_col = next(
            (c for c in df.columns
             if ("unitsordered" in c or "orderedunits" in c) and "b2b" not in c),
            None
        )
        if not units_col:
            print(f"[build_rows] {account}: no units column found — units recorded as 0. "
                  f"Columns: {list(df.columns)}")
        df["units"] = parse_units(df[units_col]) if units_col else 0

    # =========================
    # VENDOR CENTRAL
    # =========================
    else:
        # REVENUE BASIS — shipped first, ordered second.
        #
        # Ordered Revenue books a PO cancellation as a negative amount on the
        # day Amazon cancels, not against the day the PO was raised. One
        # cancelled order (B0GN97RFPR, +₹4.79 L on 03-08-2026, -₹3.56 L on
        # 05-08-2026) is enough to push an entire day's dashboard figure
        # negative, because AA is ~85% 1P. Shipped Revenue is the same retail
        # scale and cannot go negative that way — a cancelled PO simply never
        # ships. So prefer it whenever the report carries it.
        #
        # shippedCogs is WHOLESALE, not retail, so it sits below both of the
        # above and is only taken when nothing else is present.
        _rev_pref = [
            ("shippedrevenue", "shippedunits"),
            ("orderedrevenue", "orderedunits"),
            ("shippedcogs", "shippedunits"),
        ]
        rev_col = units_pref = None
        for _rev, _units in _rev_pref:
            match = next((c for c in df.columns if _rev in c and "b2b" not in c), None)
            if match is not None:
                rev_col, units_pref = match, _units
                break
        if rev_col is None:
            return empty_df()
        if "shippedcogs" in rev_col:
            print(f"[build_rows] {account}: using Shipped COGS (wholesale) — no "
                  f"shipped/ordered revenue column in this source.")

        df["sales"] = df[rev_col].apply(clean_money)
        df["net_sales"] = df["sales"]

        # Vendor Central units column.
        # Units MUST match the revenue basis chosen above, otherwise a day
        # reports shipped revenue against ordered units. Note: after norm()
        # the header "Ordered Units" -> "orderedunits" (NOT "unitsordered").
        def _pick_units_col(cols):
            # Exclude B2B variants if present.
            matched = next((c for c in cols if units_pref in c and "b2b" not in c), None)
            if matched:
                return matched
            # Basis-matching column absent — take the other one rather than
            # recording zero units, but say so, since the mix is not exact.
            other = "orderedunits" if units_pref == "shippedunits" else "shippedunits"
            fallback = next((c for c in cols if other in c and "b2b" not in c), None)
            if fallback:
                print(f"[build_rows] {account}: no '{units_pref}' column — "
                      f"units taken from '{fallback}', which does not match the "
                      f"'{rev_col}' revenue basis.")
            return fallback

        units_col = _pick_units_col(df.columns)
        if not units_col:
            print(f"[build_rows] {account}: no units column found — units recorded as 0. "
                  f"Columns: {list(df.columns)}")
        df["units"] = parse_units(df[units_col]) if units_col else 0

    df["date"] = pd.to_datetime(sales_date)
    df["account"] = account

    return df[["date", "account", "ASIN", "sales", "net_sales", "units"]]

# =========================
# KPI
# =========================
def calculate_kpis(df, ref_date):
    if df.empty:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    plan = load_planning_main(ref_date)
    if plan.empty:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    days = df["date"].nunique()

    month = pd.to_datetime(ref_date).strftime("%b").lower()
    monthly_col = f"{month}goalprojected"

    if monthly_col not in plan.columns or "perdaygoalprojected" not in plan.columns:
        return {
            "monthly_target": 0,
            "target_till": 0,
            "actual": 0,
            "achievement": 0,
            "pace": 0,
        }

    monthly_target = plan[monthly_col].sum()
    per_day_target = plan["perdaygoalprojected"].sum()

    target_till = per_day_target * days
    actual = df["net_sales"].sum()

    return {
        "monthly_target": round(monthly_target, 1),
        "target_till": round(target_till, 1),
        "actual": round(actual, 1),
        "achievement": actual / target_till if target_till else 0,
        "pace": actual / target_till if target_till else 0,
    }

# =========================
# DAY / MTD / WEEK
# =========================
def day_wise_performance(df, ref_date):
    if df.empty:
        return []

    plan = load_planning_main(ref_date)
    if plan.empty or "perdaygoalprojected" not in plan.columns:
        return []

    per_day_target = plan["perdaygoalprojected"].sum()

    agg = df.groupby("date", as_index=False).agg(
        net_sales=("net_sales", "sum"),
        units=("units", "sum")
    )
    agg["actual"]  = agg["net_sales"]
    agg["target"]  = per_day_target
    agg["achieved"] = (agg["net_sales"] / per_day_target).round(2)
    # Cast to native Python types so Jinja's |tojson can serialize np.int64 / NaN safely.
    records = agg.to_dict("records")
    out = []
    for r in records:
        out.append({
            "date":      r.get("date"),
            "net_sales": float(r.get("net_sales", 0) or 0),
            "units":     int(r.get("units", 0) or 0),
            "actual":    float(r.get("actual", 0) or 0),
            "target":    float(r.get("target", 0) or 0),
            "achieved":  float(r.get("achieved", 0) or 0),
        })
    return out

def mtd_chart(df, ref_date):
    if df.empty:
        return {"labels": [], "actual": [], "target": []}

    plan = load_planning_main(ref_date)
    if plan.empty or "perdaygoalprojected" not in plan.columns:
        return {"labels": [], "actual": [], "target": []}

    per_day_target = plan["perdaygoalprojected"].sum()

    d = df.groupby("date", as_index=False)["net_sales"].sum()
    d["cum_actual"] = d["net_sales"].cumsum()
    d["cum_target"] = per_day_target * (d.index + 1)

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
    ledger = filter_by_date_range(ledger, f, t)
    ledger = ledger.copy()
    if ledger.empty:
        return "<p>No data</p>"

    plan = load_planning_main(f)
    if plan.empty:
        return "<p>No planning data</p>"

    days = ledger["date"].nunique()
    plan["period_target"] = plan["perdaygoalprojected"] * days

    actual = ledger.groupby("ASIN", as_index=False).agg(
        net_sales=("net_sales", "sum"),
        units=("units", "sum")
    )

    # Daily pivot: per-ASIN net_sales per day, sorted, used for sparkline.
    daily = (
        ledger.groupby(["ASIN", "date"], as_index=False)["net_sales"]
        .sum()
        .pivot(index="ASIN", columns="date", values="net_sales")
        .fillna(0)
        .sort_index(axis=1)
    )
    sparkline_map = {asin: to_sparkline_svg(row.tolist()) for asin, row in daily.iterrows()}

    # Outer-join so ledger ASINs missing from the planning file are still visible —
    # otherwise the ASIN-table "Actual" total silently lags the KPI Actual.
    merged = plan.merge(actual, left_on="asin", right_on="ASIN", how="outer")
    # Coalesce the ASIN identifier across the two join keys.
    merged["asin"] = merged["asin"].fillna(merged.get("ASIN"))
    # Fill numeric NaNs from the outer join with 0; leave string columns alone for "—" handling below.
    for _col in ["period_target", "perdaygoalprojected", "net_sales", "units", "monthlyunit", "perdaytu"]:
        if _col in merged.columns:
            merged[_col] = pd.to_numeric(merged[_col], errors="coerce").fillna(0)

    model_col   = next((c for c in merged.columns if "model" in c and c != "asin"), None)

    out = pd.DataFrame({
        "ASIN":          merged["asin"].apply(lambda x: f'<a href="https://www.amazon.in/dp/{x}" target="_blank" style="color:#4f6ef7;font-weight:600;text-decoration:none;">{x}</a>'),
        "Model":         merged[model_col].fillna("—") if model_col else "—",
        "Category":      merged["category"].fillna("—") if "category" in merged.columns else "—",
        "Target":        merged["period_target"].round(1),
        "Actual":        merged["net_sales"].round(1),
        "Units":         merged["units"].fillna(0).astype(int),
        "Monthly Unit":  merged["monthlyunit"].fillna(0).astype(int) if "monthlyunit" in merged.columns else 0,
        "Per Day TU":    merged["perdaytu"].round(1) if "perdaytu" in merged.columns else 0,
        "Achievement %": (merged["net_sales"] / merged["period_target"].replace(0, float("nan")) * 100).round(1).fillna(0),
        "Trend":         merged["asin"].map(sparkline_map).fillna("—"),
    })

    out["Achievement %"] = out["Achievement %"].astype(str) + "%"
    return out.to_html(index=False, classes="table table-striped table-bordered table-sm", escape=False)

# =========================
# CATEGORY TARGET VS ACTUAL
# =========================
def category_target_vs_actual(ledger, f, t):
    ledger = filter_by_date_range(ledger, f, t)
    ledger = ledger.copy()
    if ledger.empty:
        return "<p>No data</p>"

    plan_main = load_planning_main(f)
    plan_cat = load_planning_category(f)
    if plan_main.empty or plan_cat.empty:
        return "<p>No planning data</p>"

    asin_category = plan_main.set_index("asin")["category"].to_dict()
    # ASINs sold but absent from the plan map to NaN, and groupby would silently
    # drop them -- so their revenue vanished from this table while still counting
    # in the KPI. Bucket them as "No Plan" and outer-join so the category total
    # reconciles with the ledger.
    ledger["category"] = ledger["ASIN"].map(asin_category).fillna("No Plan")

    actual = ledger.groupby("category", as_index=False).agg(
        net_sales=("net_sales", "sum"),
        units=("units", "sum")
    )

    days = ledger["date"].nunique()
    plan_cat["period_target"] = plan_cat["perdaygoal"] * days

    # Category names are hand-typed on BOTH tabs, so the same category can differ
    # only by case -- the Aug 2026 Main sheet had "Microphone conference" while
    # the Category tab had "Microphone Conference". An exact-string merge splits
    # that into two rows: the planned row keeps the full target but loses that
    # ASIN's sales (so it reads as under-performing), and a phantom 0-target row
    # appears beside it. Join on a case-folded key and show the Category tab's
    # spelling, falling back to the ledger's for sales-only rows ("No Plan").
    _catkey = lambda s: s.astype(str).str.strip().str.casefold()
    plan_cat = plan_cat.assign(_catkey=_catkey(plan_cat["category"]))
    plan_cat = plan_cat.drop_duplicates(subset="_catkey", keep="first")
    actual = (
        actual.assign(_catkey=_catkey(actual["category"]))
              .groupby("_catkey", as_index=False)
              .agg(category=("category", "first"),
                   net_sales=("net_sales", "sum"),
                   units=("units", "sum"))
    )

    merged = plan_cat.merge(actual, on="_catkey", how="outer", suffixes=("", "_actual"))
    merged["category"] = merged["category"].fillna(merged["category_actual"])
    merged = merged.drop(columns=["_catkey", "category_actual"])
    merged["perdaygoal"] = merged["perdaygoal"].fillna(0)
    merged["period_target"] = merged["period_target"].fillna(0)
    merged = merged.fillna(0)

    # Category target-units column. Older sheets used "tu"; current sheets store it as
    # "monthlyunit". Prefer whichever exists so the TU column stops showing all-zeros.
    _tu_col = "tu" if "tu" in merged.columns else ("monthlyunit" if "monthlyunit" in merged.columns else None)

    out = pd.DataFrame({
        "Category":      merged["category"],
        "Per Day Target": merged["perdaygoal"].round(1),
        "Target":        merged["period_target"].round(1),
        "Actual":        merged["net_sales"].round(1),
        "Units":         merged["units"].fillna(0).astype(int),
        "TU":            merged[_tu_col].fillna(0).astype(int) if _tu_col else 0,
        "Achievement %": (merged["net_sales"] / merged["period_target"].replace(0, float("nan")) * 100).round(1).fillna(0),
    })

    out["Achievement %"] = out["Achievement %"].astype(str) + "%"
    return out.to_html(index=False, classes="table table-striped table-bordered table-sm", escape=False)

# =========================
# CATEGORY DONUT DATA (JSON-friendly)
# =========================
def category_donut_data(ledger, f, t):
    """Return labels/values/targets/achievements for the category donut chart."""
    if ledger is None or ledger.empty or f is None or t is None:
        return None

    sliced = filter_by_date_range(ledger, f, t).copy()
    if sliced.empty:
        return None

    plan_main = load_planning_main(f)
    plan_cat  = load_planning_category(f)
    if plan_main.empty or plan_cat.empty:
        return None

    asin_category = plan_main.set_index("asin")["category"].to_dict()
    # Keep out-of-plan sales visible as their own slice rather than dropping
    # them, so the donut sums to the same total as the KPI header.
    sliced["category"] = sliced["ASIN"].map(asin_category).fillna("No Plan")
    if sliced.empty:
        return None

    actual = sliced.groupby("category", as_index=False).agg(
        net_sales=("net_sales", "sum"),
        units=("units", "sum"),
    )

    days = sliced["date"].nunique()
    plan_cat = plan_cat.copy()
    plan_cat["period_target"] = plan_cat["perdaygoal"] * days

    # Outer join so the "No Plan" bucket (sales whose ASIN isn't in the plan)
    # keeps its slice; a left join silently dropped it and the donut then
    # summed to less than the KPI header.
    merged = plan_cat.merge(actual, on="category", how="outer").fillna(0)
    merged = merged.sort_values("net_sales", ascending=False)

    labels       = merged["category"].astype(str).tolist()
    # NOTE: key is `series` (not `values`) so Jinja attr access doesn't hit dict.values method.
    series       = [round(float(v), 1) for v in merged["net_sales"].tolist()]
    targets      = [round(float(v), 1) for v in merged["period_target"].tolist()]
    units_list   = [int(v) for v in merged["units"].tolist()]
    achievements = [
        round((v / tgt * 100), 1) if tgt > 0 else 0.0
        for v, tgt in zip(series, targets)
    ]

    total_actual = round(sum(series), 1)
    total_target = round(sum(targets), 1)
    overall_pct  = round((total_actual / total_target * 100), 1) if total_target > 0 else 0.0

    return {
        "labels":       labels,
        "series":       series,
        "targets":      targets,
        "units":        units_list,
        "achievements": achievements,
        "total_actual": total_actual,
        "total_target": total_target,
        "overall_pct":  overall_pct,
    }

# ======================================================
# 🔧 ONE-TIME HISTORICAL CORRECTION (MANUAL USE ONLY)
# ======================================================
def correct_audio_array_b2b_history(engine):
    with engine.begin() as conn:
        conn.execute("""
            UPDATE ledger
            SET net_sales = net_sales / (1 + 0) -- placeholder
            WHERE account = 'Audio Array';
        """)
    print("⚠️ This function is a placeholder. Use controlled script for correction.")

# =========================
# N-MONTH MODEL TREND (with walk-back gating)
# =========================
def model_trend_3months(ledger, ref_date, months=3, min_days=7):
    """
    Build a model-level trend across the last `months` months ending at ref_date.
    If the requested ref month has fewer than `min_days` days of ledger coverage,
    walk back to the most recent month that does, so the comparison is meaningful.
    """
    if ledger.empty:
        return None

    months = max(2, min(int(months or 3), 12))   # clamp 2..12
    ref = pd.to_datetime(ref_date)

    # ── Walk-back: if requested ref month is sparse, slide window back to last
    #    fully-populated month so the "current" column isn't empty/misleading.
    month_day_counts = ledger.groupby(ledger["date"].dt.to_period("M"))["date"].nunique()
    ref_period = ref.to_period("M")
    if ref_period not in month_day_counts.index or month_day_counts.loc[ref_period] < min_days:
        qualified = month_day_counts[month_day_counts >= min_days]
        if not qualified.empty and qualified.index[-1] != ref_period:
            ref = qualified.index[-1].to_timestamp(how="end")

    # MonthBegin(0) is non-anchored — for any non-month-begin ref it rolls forward,
    # which used to push every period one month into the future. Use DateOffset(months=i).
    ref_month_start = pd.to_datetime(ref).replace(day=1)
    periods = []
    for i in range(months - 1, -1, -1):
        m_start = ref_month_start - pd.DateOffset(months=i)
        m_end   = m_start + pd.offsets.MonthEnd(1)
        periods.append((m_start, m_end, m_start.strftime("%b %Y")))

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

    # Every model that appears in ANY plan inside the window — guarantees the row
    # shows up in the table even when it had zero sales across all periods.
    known_models = {m for m in asin_model.values() if m and m != "Unknown"}

    for m_start, m_end, label in periods:
        sl = ledger[(ledger["date"] >= m_start) & (ledger["date"] <= m_end)]
        days_by_month[label] = int(sl["date"].dt.normalize().nunique()) if not sl.empty else 0
        if sl.empty:
            sales_agg = pd.Series(dtype=float)
            units_agg = pd.Series(dtype=int)
        else:
            sales_agg = sl.groupby("model")["net_sales"].sum()
            if "units" in sl.columns and sl["units"].sum() > 0:
                units_agg = sl.groupby("model")["units"].sum()
            else:
                units_agg = sl.groupby("model").size()

        # Union of activity models and all plan models — keeps zero-sales models visible.
        period_models = set(sales_agg.index) | set(units_agg.index) | known_models
        for model in period_models:
            if not model or model == "Unknown":
                continue
            sales_by_month.setdefault(model, {})[label] = round(float(sales_agg.get(model, 0)), 0)
            units_by_month.setdefault(model, {})[label] = int(units_agg.get(model, 0))

    if not sales_by_month:
        return None

    rows = []
    for model in sorted(sales_by_month, key=lambda m: sum(sales_by_month[m].values()), reverse=True):
        sales_series = [float(sales_by_month[model].get(lbl, 0) or 0) for lbl in month_labels]
        units_series = [int(units_by_month.get(model, {}).get(lbl, 0) or 0) for lbl in month_labels]

        # Trend = last vs previous (last two periods)
        prev = sales_series[-2] if len(sales_series) >= 2 else 0
        curr = sales_series[-1] if len(sales_series) >= 1 else 0
        sales_trend_pct = round(((curr - prev) / prev) * 100, 1) if prev > 0 else 0

        if sales_trend_pct > 5:
            trend_arrow, trend_class = "↑", "good"
        elif sales_trend_pct < -5:
            trend_arrow, trend_class = "↓", "bad"
        else:
            trend_arrow, trend_class = "→", "warn"

        cat = asin_category.get(
            next((a for a, m in asin_model.items() if m == model), ""), "—"
        )

        # Back-compat fields s1..s3 / u1..u3 for any consumer still expecting them.
        compat = {}
        for idx in range(3):
            src_idx = idx + max(0, len(sales_series) - 3)  # last 3 periods
            compat[f"s{idx+1}"] = sales_series[src_idx] if src_idx < len(sales_series) else 0
            compat[f"u{idx+1}"] = units_series[src_idx] if src_idx < len(units_series) else 0

        rows.append({
            "model": model, "category": cat,
            "sales": sales_series, "units": units_series,
            "sales_trend_pct": sales_trend_pct,
            "trend_arrow": trend_arrow,
            "trend_class": trend_class,
            **compat,
        })

    if not rows:
        return None

    top_rows   = rows[:12]
    top_labels = [r["model"] for r in top_rows]
    palette    = ["#c7d2fe","#7c8cf2","#4f6ef7","#fbcfe8","#a78bfa","#7c3aed",
                  "#fed7aa","#fb923c","#ea580c","#bbf7d0","#4ade80","#15803d"]

    def make_datasets(field):
        # One dataset per period, color ramp end = newest.
        n = len(month_labels)
        colors = palette[max(0, len(palette) - n):][:n]
        return [{"label": month_labels[i],
                 "data": [r[field][i] if i < len(r[field]) else 0 for r in top_rows],
                 "backgroundColor": colors[i] + "bb",
                 "borderColor": colors[i],
                 "borderWidth": 1}
                for i in range(n)]

    return {
        "months":      month_labels,
        "days":        [days_by_month.get(l, 0) for l in month_labels],
        "rows":        rows,
        "sales_chart": {"labels": top_labels, "datasets": make_datasets("sales")},
        "units_chart": {"labels": top_labels, "datasets": make_datasets("units")},
    }

# =========================
# DATA INTEGRITY & VALIDATION (READ-ONLY)
# =========================
def asin_target_vs_actual_json(ledger, f, t):
    """Structured (list[dict]) variant of asin_target_vs_actual so the merged
    dashboard router can consume it directly. Uses IDENTICAL math to the
    original — only the output shape differs."""
    ledger = filter_by_date_range(ledger, f, t).copy()
    if ledger.empty:
        return []

    plan = load_planning_main(f)
    if plan.empty:
        return []
    if "perdaygoalprojected" not in plan.columns:
        plan = plan.copy()
        plan["perdaygoalprojected"] = 0

    days = ledger["date"].nunique()
    plan["period_target"] = plan["perdaygoalprojected"] * days

    actual = ledger.groupby("ASIN", as_index=False).agg(
        net_sales=("net_sales", "sum"),
        units=("units", "sum"),
    )
    merged = plan.merge(actual, left_on="asin", right_on="ASIN", how="outer")
    merged["asin"] = merged["asin"].fillna(merged.get("ASIN"))
    for c in ("period_target", "perdaygoalprojected", "net_sales", "units",
              "monthlyunit", "perdaytu"):
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    plan_asins = set(plan["asin"].astype(str))
    model_col = next((c for c in merged.columns if "model" in c and c != "asin"), None)

    # Per-ASIN daily sparklines — same math the Nexlev services uses.
    daily = (
        ledger.pivot_table(index="ASIN", columns="date", values="net_sales", aggfunc="sum", fill_value=0)
    )
    spark_map: dict[str, str] = {}
    if not daily.empty:
        full_range = pd.date_range(daily.columns.min(), daily.columns.max())
        daily = daily.reindex(columns=full_range, fill_value=0)
        for asin_val in daily.index:
            spark_map[str(asin_val)] = to_sparkline_svg(daily.loc[asin_val].tolist())

    rows = []
    for _, r in merged.iterrows():
        target = round(float(r.get("period_target", 0) or 0), 1)
        actual_val = round(float(r.get("net_sales", 0) or 0), 1)
        ach = round((actual_val / target * 100), 1) if target else 0.0
        asin = str(r["asin"])
        rows.append({
            "asin":          asin,
            "model_no":      str(r.get(model_col, "")) if model_col else "",
            "category":      str(r.get("category", "")) if "category" in merged.columns else "",
            "product_name":  str(r.get("productname", "")) if "productname" in merged.columns else "",
            "target":        target,
            "actual":        actual_val,
            "units_ordered": int(r.get("units", 0) or 0),
            "target_units":  int(round(float(r.get("monthlyunit", 0) or 0))),
            "achievement":   ach,
            "sparkline":     spark_map.get(asin, ""),
            "out_of_plan":   asin not in plan_asins,
        })
    return rows


def category_target_vs_actual_json(ledger, f, t):
    """Structured variant of category_target_vs_actual — same math, list[dict]."""
    ledger = filter_by_date_range(ledger, f, t).copy()
    if ledger.empty:
        return []

    plan_main = load_planning_main(f)
    plan_cat = load_planning_category(f)
    if plan_main.empty or plan_cat.empty:
        return []

    asin_category = plan_main.set_index("asin")["category"].to_dict()
    ledger["category"] = ledger["ASIN"].map(asin_category).fillna("Uncategorised")

    days = ledger["date"].nunique()
    actual = ledger.groupby("category", as_index=False)["net_sales"].sum()

    plan_cat = plan_cat.copy()
    # AA's Category sheet has `perdaygoal`; Nexlev's has `perdaygoal-projected`.
    # After norm() strips spaces but not hyphens, both look for "perdaygoal"
    # prefix. Fall back to a month-goal column (`junegoal`, `julgoal`,
    # `monthlygoalprojected`) prorated by days-in-view / days-in-month.
    perday_col = next(
        (c for c in plan_cat.columns if c.startswith("perdaygoal") or c == "perdaygoal"),
        None,
    )
    monthly_col = next(
        (c for c in plan_cat.columns if "monthlygoal" in c or (c.endswith("goal") and c != "perdaygoal")),
        None,
    )
    if "category" not in plan_cat.columns or (perday_col is None and monthly_col is None):
        return []

    if perday_col:
        plan_cat["period_target"] = pd.to_numeric(plan_cat[perday_col], errors="coerce").fillna(0) * days
    else:
        m = pd.to_datetime(f) if f is not None else pd.Timestamp.today()
        end_of_month = (m + pd.offsets.MonthEnd(0)).day
        plan_cat["period_target"] = (
            pd.to_numeric(plan_cat[monthly_col], errors="coerce").fillna(0) * (days / end_of_month)
        )

    merged = plan_cat.merge(actual, on="category", how="outer")
    merged["net_sales"] = pd.to_numeric(merged["net_sales"], errors="coerce").fillna(0)
    merged["period_target"] = pd.to_numeric(merged["period_target"], errors="coerce").fillna(0)

    out = []
    for _, r in merged.iterrows():
        target = round(float(r["period_target"]), 1)
        actual_val = round(float(r["net_sales"]), 1)
        ach = round((actual_val / target * 100), 1) if target else 0.0
        out.append({
            "category":   str(r["category"]),
            "target":     target,
            "actual":     actual_val,
            "achievement": ach,
        })
    return out


def monthwise_asin_chart_data(ledger):
    """Same shape as the nexlev service — top 8 ASINs by net_sales, monthly.
    Ported so the merged dashboard's Monthwise card populates for AA too."""
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
    return {
        "labels": list(top.columns),
        "asins": [str(a) for a in top.index],
        "data": [top.iloc[i].tolist() for i in range(len(top))],
    }


def validation_summary(ledger, f, t, header_actual=None):
    """Cross-check the dashboard against the ledger.

    `header_actual` is the KPI figure actually rendered to the user. Pass it so
    the panel compares the headline against ledger ground truth; without it the
    check degrades to ledger-vs-ledger and can only ever report 0.
    """
    try:
        df = ledger.copy()

        if f is not None and t is not None:
            df = df[(df["date"] >= f) & (df["date"] <= t)]

        if df.empty:
            return None

        plan = load_planning_main(f or df["date"].max())
        if plan.empty:
            extra_asins = 0
        else:
            ledger_asins = set(df["ASIN"].unique())
            plan_asins = set(plan["asin"].unique())
            extra_asins = len(ledger_asins - plan_asins)

        # Seller Central rows have GST stripped (net/sales ~= 0.847). The old
        # check (sales != net_sales) flagged EVERY row, because GST always makes
        # them differ -- a permanent false alarm that trains people to ignore
        # this panel. Rows where GST was NOT stripped (ratio ~= 1.0) are the
        # real anomalies.
        audio_array_b2b_rows = 0
        aa = df[(df["account"] == "Audio Array") & (df["sales"] > 0)]
        if not aa.empty:
            ratio = aa["net_sales"] / aa["sales"]
            audio_array_b2b_rows = int((ratio > 0.95).sum())

        # The ledger's own total -- ground truth, before any planning-file logic.
        ledger_sum = round(df["net_sales"].sum(), 1)

        # NOTE: this used to compare df["net_sales"].sum() against the same
        # figure grouped by date -- identical by definition, so `difference` was
        # ALWAYS 0.0 and the panel always read "All validations passed". The
        # only comparison that catches a real problem is ledger sum vs the
        # figure actually rendered in the KPI header, which is computed from the
        # planning file and CAN silently collapse to 0.
        kpi_actual = ledger_sum if header_actual is None else round(float(header_actual), 1)
        difference = round(kpi_actual - ledger_sum, 1)

        reasons = []
        if extra_asins > 0:
            reasons.append("Ledger contains ASINs not present in planning file.")
        if audio_array_b2b_rows > 0:
            reasons.append("Historical Audio Array B2B rows detected.")
        # A real mismatch between the headline and the ledger means the KPI path
        # dropped revenue (missing/renamed planning column, month with no plan).
        # Say so plainly instead of "all validations passed".
        if difference != 0:
            reasons.insert(0, (
                f"⚠ KPI header (₹{kpi_actual:,.1f}) does NOT match the ledger "
                f"(₹{ledger_sum:,.1f}) — difference ₹{difference:,.1f}. "
                f"Check that this month's planning file exists and its goal "
                f"column is named as expected."
            ))

        reason_text = " ".join(reasons) if reasons else "All validations passed. Data is consistent."

        return {
            "extra_asins": extra_asins,
            "audio_array_b2b_rows": audio_array_b2b_rows,
            "kpi_actual": kpi_actual,
            "daywise_sum": ledger_sum,
            "difference": difference,
            # bool() not numpy.bool_ — the latter is not JSON-serialisable.
            "reconciled": bool(difference == 0),
            "reason": reason_text,
        }

    except Exception as e:
        print("VALIDATION_ERROR:", e)
        return None
