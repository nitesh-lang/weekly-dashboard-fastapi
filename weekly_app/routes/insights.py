"""
Insights brief — narrative, prose-style weekly summary for operators
who don't want to read tables of numbers.

Serves the Markdown brief that `scripts/build_weekly_brief.py`
generates on every weekly ETL run.  No LLM call at view time — no
ANTHROPIC_API_KEY required to render this page.

The brand picker / regenerate button on the frontend still works:
- ?brand=X → filters the brand-briefs section to that brand
- ?force=true → regenerates the brief on the fly (useful when the
  operator pushes a fresh data drop and doesn't want to wait for the
  cron to rebuild).  Falls back to the stored file on error.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/insights", tags=["insights"])

AI_CONTEXT_JSON = Path("data/processed/ai_context.json")
BRIEF_MD        = Path("data/processed/weekly_brief.md")

def _filter_by_brand(md: str, brand: str) -> str:
    """Trim the Brand briefs section to one brand.  Keeps all other
    sections intact so the headline / movers context stays visible."""
    target = brand.strip().lower()
    if not target or target == "all":
        return md
    out_lines = []
    in_brand_briefs = False
    keep_block = True
    for line in md.splitlines():
        if line.startswith("## "):
            in_brand_briefs = (line.strip().lower() == "## brand briefs")
            keep_block = True
            out_lines.append(line)
            continue
        if in_brand_briefs and line.startswith("**"):
            # `**Audio Array** ...` — keep only the matching brand
            m = re.match(r"\*\*([^*]+)\*\*", line)
            keep_block = bool(m and m.group(1).strip().lower() == target)
        if not in_brand_briefs or keep_block:
            out_lines.append(line)
    return "\n".join(out_lines)


def _regenerate_if_possible() -> bool:
    """Run scripts/build_weekly_brief.py in-process.  Returns True on
    success, False if the script raised or required snapshots are
    missing — caller falls back to the stored file."""
    try:
        # Import lazily to avoid a hard dep at module load time.
        import importlib.util
        script_path = Path(__file__).resolve().parent.parent.parent / "scripts" / "build_weekly_brief.py"
        if not script_path.exists():
            return False
        spec = importlib.util.spec_from_file_location("_build_weekly_brief", script_path)
        if not spec or not spec.loader:
            return False
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.main() == 0
    except Exception as e:
        print(f"[insights] regenerate failed: {e!r}")
        return False


def _brief_module():
    import importlib.util
    script_path = Path(__file__).resolve().parent.parent.parent / "scripts" / "build_weekly_brief.py"
    spec = importlib.util.spec_from_file_location("_build_weekly_brief", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# (params, snapshot mtime) -> md.  A sliced brief takes ~1-2s to compute;
# the cache makes flipping between filters instant within a data refresh.
_slice_cache: dict = {}


@router.get("/brief")
def get_brief(
    brand: Optional[str] = Query(None, description="Recompute the WHOLE brief for one brand"),
    week: Optional[int] = Query(None, description="Brief for a specific week number"),
    asin_type: Optional[str] = Query(None, description="Core / Medium / Tail / EOL / New …"),
    category: Optional[str] = Query(None, description="category_l0 slice"),
    subcategory: Optional[str] = Query(None, description="category_l1 slice"),
    model: Optional[str] = Query(None, description="Single-model brief (model code)"),
    force: bool = Query(False, description="Regenerate from snapshots before serving"),
):
    def _active(v: Optional[str]) -> bool:
        return bool(v) and v.strip().lower() != "all"

    filtered = any([
        _active(brand), week is not None, _active(asin_type),
        _active(category), _active(subcategory), _active(model),
    ])

    if filtered:
        # Sliced briefs are computed in memory and NEVER written to
        # weekly_brief.md — the stored file stays the canonical
        # all-brands latest-week document the cron owns.
        sales_csv = Path("data/processed/weekly_sales_snapshot.csv")
        mtime = int(sales_csv.stat().st_mtime) if sales_csv.exists() else 0
        key = (brand or "all", week, asin_type or "all", category or "all",
               subcategory or "all", model or "all", mtime)
        if not force and key in _slice_cache:
            md = _slice_cache[key]
        else:
            try:
                md = _brief_module().build_brief(week=week, brand=brand,
                                                 asin_type=asin_type,
                                                 category=category,
                                                 subcategory=subcategory,
                                                 model=model)
                _slice_cache.clear() if len(_slice_cache) > 40 else None
                _slice_cache[key] = md
            except Exception as e:
                raise HTTPException(500, f"sliced brief failed: {e}")
        now = int(time.time())
        return {"markdown": md, "cached": key in _slice_cache and not force,
                "generated_at": now, "context_mtime": mtime,
                "brand": brand or "all", "week": week,
                "asin_type": asin_type or "all", "category": category or "all",
                "subcategory": subcategory or "all", "model": model or "all"}

    if force:
        _regenerate_if_possible()

    if not BRIEF_MD.exists():
        # Try once more to build it — first-time deploy where no cron
        # has run yet.  Cheap fallback: synthesize a placeholder.
        if not _regenerate_if_possible() or not BRIEF_MD.exists():
            return JSONResponse(
                {"error": "Brief not generated yet — run `python scripts/build_weekly_brief.py` "
                          "(or trigger the weekly cron) to create data/processed/weekly_brief.md."},
                status_code=503,
            )

    try:
        md = BRIEF_MD.read_text(encoding="utf-8")
    except Exception as e:
        raise HTTPException(500, f"weekly_brief.md unreadable: {e}")

    mtime = int(BRIEF_MD.stat().st_mtime)
    return {
        "markdown":      md,
        "cached":        not force,
        "generated_at":  mtime,
        "context_mtime": mtime,
        "brand":         "all",
        "week":          None,
        "asin_type":     "all",
    }


@router.get("/meta")
def get_meta():
    """Filter options for the insights page, straight from the snapshot —
    weeks (desc), brands (Fossil-excluded like the brief itself), the ASIN
    types actually present, and a compact (brand, category, model) index so
    the Category / Model pickers can narrow to the selected brand instead of
    offering an 800-entry flat list.  One source of truth: the data."""
    import pandas as pd
    empty = {"weeks": [], "brands": [], "asin_types": [], "model_index": []}
    p = Path("data/processed/weekly_sales_snapshot.csv")
    if not p.exists():
        return empty
    try:
        df = pd.read_csv(p, usecols=lambda c: c in
                         ("week", "brand", "asin_type",
                          "category_l0", "category_l1", "model"))
    except Exception:
        return empty
    wn = pd.to_numeric(df["week"].astype(str).str.extract(r"(\d+)", expand=False),
                       errors="coerce").dropna().astype(int)
    # fillna BEFORE astype(str): on newer pandas astype(str) PRESERVES NaN
    # instead of stringifying it, and a NaN reaching the JSON encoder 500s
    # the whole endpoint ("Out of range float values are not JSON
    # compliant") — see feedback_pd_na_breaks_json_response.
    df["brand"] = df["brand"].fillna("").astype(str).str.strip()
    df = df[df["brand"].str.lower() != "fossil"]          # brief excludes Fossil by rule
    brands = sorted(b for b in df["brand"].unique() if b and b.lower() != "nan")
    types = []
    if "asin_type" in df.columns:
        types = sorted(t for t in df["asin_type"].fillna("").astype(str).str.strip().unique()
                       if t and t.lower() not in ("nan", "none", ""))
    index: list = []
    if {"category_l0", "category_l1", "model"}.issubset(df.columns):
        def _clean(col):
            v = df[col].fillna("").astype(str).str.strip()
            return v.where(~v.str.lower().isin(("nan", "none", "")), "")
        quad = (pd.DataFrame({"b": df["brand"], "c": _clean("category_l0"),
                              "s": _clean("category_l1"), "m": _clean("model")})
                .query("m != ''").drop_duplicates()
                .sort_values(["b", "c", "s", "m"]))
        # Belt and braces: nothing non-string may reach the JSON encoder.
        index = [[x if isinstance(x, str) else "" for x in row]
                 for row in quad.values.tolist()]
    return {"weeks": sorted(wn.unique().tolist(), reverse=True),
            "brands": brands, "asin_types": types, "model_index": index}


@router.get("/brands")
def get_brand_list():
    """Brand names the brief actually covers.  Parsed from the brief
    itself so the picker stays in sync with the script's exclusion
    rules (Fossil dropped, etc.) — no risk of offering a brand the
    brief can't filter to."""
    if not BRIEF_MD.exists():
        return {"brands": []}
    try:
        md = BRIEF_MD.read_text(encoding="utf-8")
    except Exception:
        return {"brands": []}
    # Brand briefs section opens with `**BrandName** ...` lines.
    brands: list[str] = []
    in_brand_briefs = False
    for line in md.splitlines():
        ls = line.strip()
        if ls.startswith("## "):
            in_brand_briefs = (ls.lower() == "## brand briefs")
            continue
        if in_brand_briefs and ls.startswith("**"):
            m = re.match(r"\*\*([^*]+)\*\*", ls)
            if m:
                b = m.group(1).strip()
                if b and b not in brands:
                    brands.append(b)
    return {"brands": brands}
