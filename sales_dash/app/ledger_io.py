"""Shared ledger read/write. Brand column threads through every path.

Row shape matches what the ported services expect
(LEDGER_COLUMNS = ["date", "account", "ASIN", "sales", "net_sales", "units"])
so nothing in `services/*.py` had to change.
"""
from __future__ import annotations

import time

import pandas as pd
from sqlalchemy import text

from . import sku_master
from .database import engine

LEDGER_COLUMNS = ["date", "account", "ASIN", "sales", "net_sales", "units", "model", "category_l2"]

# Per-brand ledger DataFrame cache. Cleared automatically after every
# save_ledger/delete_day so uploads and SP-API pulls surface immediately.
_LEDGER_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_LEDGER_TTL_SEC = 20


def _invalidate(brand: str | None = None) -> None:
    if brand is None:
        _LEDGER_CACHE.clear()
    else:
        _LEDGER_CACHE.pop(brand, None)

# Alias fold-in — same physical Amazon account has drifted labels over time,
# and the ledger PK is (brand, date, account, asin), so two labels → double
# count. Every write funnels through this so it can never split again.
_ACCOUNT_ALIASES = {
    "viomi": "Viomi By Cambium",
    "viomi by cambium": "Viomi By Cambium",
}


def canonical_account(name) -> str | None:
    if name is None:
        return name
    s = str(name).strip()
    return _ACCOUNT_ALIASES.get(s.lower(), s)


def load_ledger(brand: str) -> pd.DataFrame:
    now = time.time()
    hit = _LEDGER_CACHE.get(brand)
    if hit and (now - hit[0]) < _LEDGER_TTL_SEC:
        return hit[1].copy(deep=False)
    with engine.begin() as conn:
        df = pd.read_sql(
            text(
                "SELECT date, account, asin AS \"ASIN\", sales::float, net_sales::float, units::float, model, category_l2 "
                "FROM ledger WHERE brand = :b"
            ),
            conn,
            params={"b": brand},
            parse_dates=["date"],
        )
    if df.empty:
        result = pd.DataFrame(columns=LEDGER_COLUMNS)
    else:
        # Downcast numeric columns so the 60k-row AA ledger stops eating
        # ~15 MB per copy under pandas 3 default float64.
        df["sales"] = pd.to_numeric(df["sales"], errors="coerce", downcast="float").fillna(0)
        df["net_sales"] = pd.to_numeric(df["net_sales"], errors="coerce", downcast="float").fillna(0)
        df["units"] = pd.to_numeric(df["units"], errors="coerce", downcast="integer").fillna(0)
        # NOTE: account/ASIN deliberately stay plain object dtype. They were
        # briefly `category` for memory, but categorical groupby semantics
        # differ across pandas versions (2.x defaults observed=False → every
        # category level materialises in every groupby, padding asin_rows
        # with ~450 zero rows; pandas 3 defaults observed=True). Plain
        # strings behave identically on every version — correctness over
        # the few MB saved.
        for c in ("model", "category_l2"):
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)
        result = df[LEDGER_COLUMNS]
    _LEDGER_CACHE[brand] = (now, result)
    return result.copy(deep=False)


def save_ledger(brand: str, rows: pd.DataFrame, conn=None) -> None:
    """Upsert rows into the ledger for `brand`.

    Pass an open transaction as `conn` to atomically combine with a
    replace-day DELETE (see routers/ledger.py).
    """
    if rows.empty:
        return

    rows = rows.copy()
    rows.columns = ["date", "account", "asin", "sales", "net_sales", "units"]
    rows["account"] = rows["account"].map(canonical_account)
    rows["date"] = pd.to_datetime(rows["date"]).dt.date

    # Aggregate duplicate (date, account, asin) — GET_SALES_AND_TRAFFIC_REPORT
    # returns child-item rows that share a parent, and we key by parent.
    rows = rows.groupby(["date", "account", "asin"], as_index=False).agg(
        {"sales": "sum", "net_sales": "sum", "units": "sum"}
    )

    from .database import IS_SQLITE
    upsert_sql = (
        """
        INSERT INTO ledger (brand, date, account, asin, sales, net_sales, units, model, category_l2)
        VALUES (:brand, :date, :account, :asin, :sales, :net_sales, :units, :model, :category_l2)
        ON CONFLICT (brand, date, account, asin)
        DO UPDATE SET
            sales       = EXCLUDED.sales,
            net_sales   = EXCLUDED.net_sales,
            units       = EXCLUDED.units,
            model       = COALESCE(NULLIF(EXCLUDED.model, ''), ledger.model),
            category_l2 = COALESCE(NULLIF(EXCLUDED.category_l2, ''), ledger.category_l2)
        """
        if not IS_SQLITE
        else
        """
        INSERT INTO ledger (brand, date, account, asin, sales, net_sales, units, model, category_l2)
        VALUES (:brand, :date, :account, :asin, :sales, :net_sales, :units, :model, :category_l2)
        ON CONFLICT (brand, date, account, asin)
        DO UPDATE SET
            sales       = excluded.sales,
            net_sales   = excluded.net_sales,
            units       = excluded.units,
            model       = COALESCE(NULLIF(excluded.model, ''), ledger.model),
            category_l2 = COALESCE(NULLIF(excluded.category_l2, ''), ledger.category_l2)
        """
    )

    sku_map = sku_master.lookup_map()

    def _write(c):
        for _, r in rows.iterrows():
            payload = r.to_dict()
            payload["brand"] = brand
            asin_key = str(payload.get("asin", "")).strip().upper()
            enrich = sku_map.get(asin_key) or {}
            payload["model"] = enrich.get("model", "") or ""
            payload["category_l2"] = enrich.get("category_l2", "") or ""
            c.execute(text(upsert_sql), payload)

    if conn is not None:
        _write(conn)
    else:
        with engine.begin() as own:
            _write(own)
    _invalidate(brand)


def day_totals(brand: str, sales_date, accounts: list[str] | None = None, conn=None) -> dict[str, dict]:
    """What is ALREADY stored for (brand, day), per account.

    Read straight from the DB rather than through load_ledger() — that one is
    cached for a TTL, and a maturity check against stale numbers would be
    worse than no check at all.
    """
    accounts = [canonical_account(a) for a in (accounts or []) if a]
    sql = (
        "SELECT account, COALESCE(SUM(sales), 0) AS sales, "
        "COALESCE(SUM(units), 0) AS units, COUNT(*) AS rows "
        "FROM ledger WHERE brand = :b AND date = :d"
    )
    params: dict = {"b": brand, "d": sales_date}
    if accounts:
        placeholders = ",".join(f":a{i}" for i in range(len(accounts)))
        sql += f" AND account IN ({placeholders})"
        for i, a in enumerate(accounts):
            params[f"a{i}"] = a
    sql += " GROUP BY account"

    def _run(c):
        return {
            r["account"]: {
                "sales": float(r["sales"] or 0),
                "units": float(r["units"] or 0),
                "rows": int(r["rows"] or 0),
            }
            for r in c.execute(text(sql), params).mappings().all()
        }

    if conn is not None:
        return _run(conn)
    with engine.begin() as own:
        return _run(own)


def delete_day(brand: str, sales_date, accounts: list[str], conn=None) -> None:
    """Replace-day: drop all rows for (brand, day, account∈accounts) so a
    re-run of the same day's sync is idempotent."""
    accounts = [canonical_account(a) for a in accounts if a]
    if not accounts:
        return

    def _run(c):
        placeholders = ",".join(f":a{i}" for i in range(len(accounts)))
        params = {"b": brand, "d": sales_date}
        for i, a in enumerate(accounts):
            params[f"a{i}"] = a
        c.execute(
            text(
                f"DELETE FROM ledger "
                f"WHERE brand = :b AND date = :d AND account IN ({placeholders})"
            ),
            params,
        )

    if conn is not None:
        _run(conn)
    else:
        with engine.begin() as own:
            _run(own)
    _invalidate(brand)
