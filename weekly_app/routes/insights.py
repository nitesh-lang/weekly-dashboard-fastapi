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


@router.get("/brief")
def get_brief(
    brand: Optional[str] = Query(None, description="Restrict the brand-briefs section to one brand"),
    force: bool = Query(False, description="Regenerate from snapshots before serving"),
):
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

    md = _filter_by_brand(md, brand or "")
    mtime = int(BRIEF_MD.stat().st_mtime)
    return {
        "markdown":      md,
        "cached":        not force,
        "generated_at":  mtime,
        "context_mtime": mtime,
        "brand":         brand or "all",
    }


@router.get("/brands")
def get_brand_list():
    """Brand names available in the current ai_context for the picker."""
    if not AI_CONTEXT_JSON.exists():
        return {"brands": []}
    try:
        ctx = json.loads(AI_CONTEXT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"brands": []}
    seen = set()
    for entry in ctx.get("top_models_last4", []):
        b = (entry.get("brand") or "").strip()
        if b: seen.add(b)
    return {"brands": sorted(seen)}
