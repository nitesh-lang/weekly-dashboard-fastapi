"""Keepa uploads from the browser — BSR CSVs (Buybox) + variation export.

The data of record for both targets lives IN GIT (Render's disk is wiped on
every deploy), so an upload here becomes a COMMIT on main via the GitHub
API. That commit auto-triggers the Render deploy, which rebuilds the
/buybox bundle (and refreshes data/master/keepa_variations.csv for the
Variation Performance page). Browser → commit → live, ~5 minutes, no
terminal involved.

Targets:
  POST /api/keepa-upload/bsr          — per-brand Keepa CSVs (or one ZIP of
                                        them) → buybox_src/data/BSR/<Brand>/
                                        <yy-mm-dd>.csv + regenerated
                                        src/bsr_snapshots.json
  POST /api/keepa-upload/variations   — the parent/child ASIN export →
                                        data/master/keepa_variations.csv
  GET  /api/keepa-upload/status       — latest BSR date per brand + counts

Auth: any non-viewer weekly login (admins + operators).
Requires env GITHUB_TOKEN with repo write on the weekly repo; without it
the endpoints answer 503 with instructions instead of failing weirdly.
"""
from __future__ import annotations

import base64
import io
import os
import re
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

import requests as _rq
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile

from weekly_app.core import auth_users

router = APIRouter(prefix="/api/keepa-upload", tags=["keepa-upload"])

ROOT = Path(__file__).resolve().parent.parent.parent
BSR_DIR = ROOT / "buybox_src" / "data" / "BSR"
VARIATIONS_CSV = ROOT / "data" / "master" / "keepa_variations.csv"

REPO = "nitesh-lang/weekly-dashboard-fastapi"
IST = timezone(timedelta(hours=5, minutes=30))

# Keepa export filename → BSR brand folder. The operator's system names —
# "Nexlev", "Tonor", "Audio Array", "White Mulberry" — match EXACTLY first
# (case-insensitive); the fuzzy keys below catch variants like "Audio array".
EXACT_BRANDS = {
    "nexlev": "Nexlev",
    "tonor": "Tonor",
    "audio array": "Audio Array",
    "white mulberry": "White Mulberry",
}
BRAND_FOLDERS = {
    "nexlev": "Nexlev",
    "audio": "Audio Array",
    "tonor": "Tonor",
    "white": "White Mulberry",
    "mulberry": "White Mulberry",
}


def require_uploader(request: Request) -> str:
    email = request.session.get("user_email")
    if not email:
        raise HTTPException(401, "Login required")
    if auth_users.get_role(email) == "viewer":
        raise HTTPException(403, "Viewer role cannot upload data")
    return email


# ── GitHub single-commit helper (blobs → tree → commit → ref) ─────────────

def _gh(path: str, method: str = "GET", body: dict | None = None) -> dict:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        raise HTTPException(503, "GITHUB_TOKEN not configured on this service — "
                                 "set it in Render env to enable browser uploads.")
    r = _rq.request(method, f"https://api.github.com{path}",
                    headers={"Authorization": f"Bearer {token}",
                             "Accept": "application/vnd.github+json"},
                    json=body, timeout=60)
    if r.status_code >= 300:
        raise HTTPException(502, f"GitHub API {r.status_code} on {path}: {r.text[:200]}")
    return r.json()


def commit_files(files: dict[str, bytes], message: str, author_email: str) -> Optional[str]:
    """One commit on main containing `files` ({repo-relative-path: bytes}).
    Returns the commit sha, or None when nothing actually changed."""
    head = _gh(f"/repos/{REPO}/git/ref/heads/main")["object"]["sha"]
    base_commit = _gh(f"/repos/{REPO}/git/commits/{head}")

    tree_entries = []
    for path, content in files.items():
        blob = _gh(f"/repos/{REPO}/git/blobs", "POST",
                   {"content": base64.b64encode(content).decode(), "encoding": "base64"})
        tree_entries.append({"path": path.replace("\\", "/"), "mode": "100644",
                             "type": "blob", "sha": blob["sha"]})

    tree = _gh(f"/repos/{REPO}/git/trees", "POST",
               {"base_tree": base_commit["tree"]["sha"], "tree": tree_entries})
    if tree["sha"] == base_commit["tree"]["sha"]:
        return None                      # byte-identical upload — no commit

    commit = _gh(f"/repos/{REPO}/git/commits", "POST", {
        "message": message,
        "tree": tree["sha"],
        "parents": [head],
        "author": {"name": "keepa-upload (dashboard)",
                   "email": author_email or "uploads@cambiumretail.com",
                   "date": datetime.now(timezone.utc).isoformat()},
    })
    _gh(f"/repos/{REPO}/git/refs/heads/main", "PATCH",
        {"sha": commit["sha"], "force": False})
    return commit["sha"]


# ── validation helpers ────────────────────────────────────────────────────

def _brand_for(filename: str) -> Optional[str]:
    low = Path(filename).stem.strip().lower()
    if low in EXACT_BRANDS:
        return EXACT_BRANDS[low]
    for key, folder in BRAND_FOLDERS.items():
        if key in low:
            return folder
    return None


def _looks_like_keepa_bsr(content: bytes) -> bool:
    head = content[:2048].decode("utf-8-sig", "replace")
    return "ASIN" in head and "Sales Rank" in head


def _collect_csvs(uploads: List[UploadFile]) -> dict[str, bytes]:
    """Flatten uploads (CSVs and/or ZIPs of CSVs) → {brand_folder: bytes}."""
    out: dict[str, bytes] = {}
    unmatched: list[str] = []
    for up in uploads:
        raw = up.file.read()
        names_blobs: list[tuple[str, bytes]] = []
        if (up.filename or "").lower().endswith(".zip") or raw[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                for n in z.namelist():
                    if n.lower().endswith(".csv"):
                        names_blobs.append((n, z.read(n)))
        else:
            names_blobs.append((up.filename or "upload.csv", raw))
        for name, blob in names_blobs:
            brand = _brand_for(name)
            if brand is None:
                unmatched.append(name)
                continue
            if not _looks_like_keepa_bsr(blob):
                raise HTTPException(422, f"{name}: doesn't look like a Keepa export "
                                         "(no ASIN / Sales Rank columns in header)")
            out[brand] = blob
    if unmatched:
        raise HTTPException(422, f"Couldn't map to a brand: {', '.join(unmatched[:4])}. "
                                 "Name files like Nexlev.csv / Audio array.csv / "
                                 "Tonor.csv / White Mulberry.csv")
    if not out:
        raise HTTPException(422, "No CSVs found in the upload.")
    return out


# ── endpoints ─────────────────────────────────────────────────────────────

@router.get("/status")
def status(request: Request):
    require_uploader(request)
    brands = {}
    if BSR_DIR.exists():
        for d in sorted(BSR_DIR.iterdir()):
            if d.is_dir():
                dates = sorted(p.stem for p in d.glob("*.csv"))
                brands[d.name] = dates[-1] if dates else None
    variations = None
    if VARIATIONS_CSV.exists():
        variations = {"rows": max(sum(1 for _ in VARIATIONS_CSV.open(encoding="utf-8", errors="replace")) - 1, 0)}
    return {"bsr_latest": brands, "variations": variations,
            "github_configured": bool(os.environ.get("GITHUB_TOKEN", "").strip()),
            "today": datetime.now(IST).strftime("%y-%m-%d")}


@router.post("/bsr")
def upload_bsr(request: Request,
               files: List[UploadFile] = File(...),
               date: Optional[str] = Query(None, description="yy-mm-dd; default today IST")):
    email = require_uploader(request)
    day = date or datetime.now(IST).strftime("%y-%m-%d")
    if not re.fullmatch(r"\d{2}-\d{2}-\d{2}", day):
        raise HTTPException(422, "date must be yy-mm-dd")

    per_brand = _collect_csvs(files)

    # DELIBERATELY no snapshot regeneration here: running
    # build_bsr_snapshots.py in-request read the whole 78MB BSR store and
    # OOM-killed the 512MB instance (server_failed 2026-08-31 08:01 → the
    # operator's 502). The commit below triggers a Render deploy, and the
    # BUILD step regenerates bsr_snapshots.json from ALL committed CSVs
    # (see render.yaml) with the build container's own resources.
    commit_payload = {
        f"buybox_src/data/BSR/{brand}/{day}.csv": blob
        for brand, blob in per_brand.items()
    }

    sha = commit_files(commit_payload,
                       f"data(buybox): BSR upload {day} via dashboard "
                       f"({', '.join(sorted(per_brand))}) [skip ci]", email)
    return {"ok": True, "date": day, "brands": sorted(per_brand),
            "commit": sha,
            "note": ("No change — these files were already up to date." if sha is None
                     else "Committed. Render is rebuilding /buybox — live in ~5 minutes.")}


@router.post("/variations")
def upload_variations(request: Request, file: UploadFile = File(...)):
    email = require_uploader(request)
    raw = file.file.read()
    head = raw[:4096].decode("utf-8-sig", "replace")
    if "ASIN" not in head or "Variation ASINs" not in head:
        raise HTTPException(422, "Doesn't look like the variation export "
                                 "(needs ASIN + 'Variation ASINs' columns).")
    VARIATIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
    VARIATIONS_CSV.write_bytes(raw)
    sha = commit_files({"data/master/keepa_variations.csv": raw},
                       "data(master): keepa variations upload via dashboard [skip ci]",
                       email)
    return {"ok": True, "commit": sha,
            "note": ("No change — identical to the current file." if sha is None
                     else "Committed. Variation Performance refreshes with the next "
                          "deploy (~5 minutes).")}
