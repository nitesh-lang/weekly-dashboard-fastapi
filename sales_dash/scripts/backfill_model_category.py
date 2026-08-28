"""One-shot: fill ledger.model + ledger.category_l2 for every row.

Two-tier lookup:
  1. sku_master.xlsx (ASIN → Model / category_l2 / brand — canonical)
  2. planning workbooks per brand+month (ASIN → category + model#)
     covers any planned ASIN missing from sku_master

Idempotent — only touches rows where the enrichment is currently empty.
Runs against Neon directly (bypasses the API).
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DEST_URL = os.getenv(
    "DEST_URL",
    "postgresql://neondb_owner:npg_DFo7HqWST0hs"
    "@ep-noisy-rain-aztnfk88.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require",
)
MASTER = Path(__file__).resolve().parent.parent / "backend" / "data" / "master" / "sku_master.xlsx"
PLANNING_ROOT = Path(__file__).resolve().parent.parent / "backend" / "data" / "planning"


def load_planning_fallbacks() -> dict[str, tuple[str, str]]:
    """Union of every brand's every-month plan → ASIN → (model, category)."""
    out: dict[str, tuple[str, str]] = {}
    for brand_dir in PLANNING_ROOT.iterdir():
        if not brand_dir.is_dir():
            continue
        for xlsx in sorted(brand_dir.glob("ASIN Planning file - *.xlsx")):
            try:
                p = pd.read_excel(xlsx)
            except Exception:
                continue
            p.columns = [str(c).strip().lower().replace(" ", "") for c in p.columns]
            if "asin" not in p.columns:
                continue
            model_col = next((c for c in p.columns if "model" in c), None)
            cat_col = "category" if "category" in p.columns else None
            for _, row in p.iterrows():
                a = str(row["asin"]).strip().upper()
                if not a or a in out:
                    continue
                mo = str(row.get(model_col) or "").strip() if model_col else ""
                ca = str(row.get(cat_col) or "").strip() if cat_col else ""
                if mo.lower() in ("", "nan"):
                    mo = ""
                if ca.lower() in ("", "nan"):
                    ca = ""
                if mo or ca:
                    out[a] = (mo, ca)
    return out


def main() -> int:
    def _clean(v) -> str:
        s = str(v or "").strip()
        return "" if s.lower() in ("", "nan", "none", "nat", "0", "0.0") else s

    m = pd.read_excel(MASTER)
    m.columns = [str(c).strip() for c in m.columns]
    m["ASIN"] = m["ASIN"].astype(str).str.strip().str.upper()
    m = m[m["ASIN"].ne("")].drop_duplicates(subset=["ASIN"], keep="first")

    tuples = [
        (
            row["ASIN"],
            _clean(row.get("Model")),
            _clean(row.get("category_l2")) or _clean(row.get("category_l1")) or _clean(row.get("category_l0")),
        )
        for _, row in m.iterrows()
    ]
    tuples = [(a, mo, c) for a, mo, c in tuples if mo or c]
    print(f"sku_master: {len(tuples)} ASINs with model or category (l2→l1→l0)")

    # Second tier: planning files (any ASIN not in sku_master)
    plan_fb = load_planning_fallbacks()
    master_asins = {t[0] for t in tuples}
    plan_extras = [(a, mo, ca) for a, (mo, ca) in plan_fb.items() if a not in master_asins]
    print(f"planning fallbacks (ASINs missing from sku_master): {len(plan_extras)}")
    tuples.extend(plan_extras)

    with psycopg2.connect(DEST_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ledger WHERE model IS NULL OR model = ''")
        need_model = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ledger WHERE category_l2 IS NULL OR category_l2 = ''")
        need_cat = cur.fetchone()[0]
        print(f"ledger rows needing model: {need_model:,}  category_l2: {need_cat:,}")

        cur.execute("CREATE TEMP TABLE _sm (asin TEXT PRIMARY KEY, model TEXT, category_l2 TEXT)")
        execute_values(cur, "INSERT INTO _sm (asin, model, category_l2) VALUES %s", tuples, page_size=500)

        cur.execute(
            """
            UPDATE ledger l SET
                model = COALESCE(NULLIF(l.model, ''), s.model),
                category_l2 = COALESCE(NULLIF(l.category_l2, ''), s.category_l2)
            FROM _sm s
            WHERE l.asin = s.asin
              AND ((l.model IS NULL OR l.model = '')
                OR (l.category_l2 IS NULL OR l.category_l2 = ''))
            """
        )
        touched = cur.rowcount
        print(f"updated {touched:,} ledger rows")

        cur.execute("SELECT COUNT(*) FROM ledger WHERE model IS NULL OR model = ''")
        still_no_model = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ledger WHERE category_l2 IS NULL OR category_l2 = ''")
        still_no_cat = cur.fetchone()[0]
        print(f"after: still-missing model={still_no_model:,}  category_l2={still_no_cat:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
