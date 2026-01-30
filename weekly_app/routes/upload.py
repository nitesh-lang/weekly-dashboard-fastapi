from fastapi import APIRouter, Request, UploadFile, Form, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from weekly_app.core.week import get_current_week
from weekly_app.services.ams_etl import run_ams_etl

router = APIRouter(prefix="/upload", tags=["Upload"])

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

BASE_RAW = Path("data/raw")

UPLOAD_TYPES = ["sales", "inventory", "ams"]
BRANDS = ["Nexlev", "Audio Array", "Viomi"]
ALLOWED_EXTENSIONS = [".xlsx", ".csv"]


# =====================================================
# GET: Upload Page
# =====================================================
@router.get("", response_class=HTMLResponse)
def upload_page(request: Request):
    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "upload_types": UPLOAD_TYPES,
            "brands": BRANDS,
        },
    )


# =====================================================
# POST: Handle Upload
# =====================================================
@router.post("")
async def handle_upload(
    upload_type: str = Form(...),
    brand: str = Form(...),
    files: list[UploadFile] = File(...),  # 🔥 FIX HERE
):
    # -------------------------------
    # VALIDATION: No files
    # -------------------------------
    if not files or all(f.filename == "" for f in files):
        return RedirectResponse(
            "/upload?error=No file selected. Please choose a file.",
            status_code=303,
        )

    # -------------------------------
    # VALIDATION: File extensions
    # -------------------------------
    invalid_files = [
        f.filename
        for f in files
        if not any(f.filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)
    ]

    if invalid_files:
        return RedirectResponse(
            f"/upload?error=Invalid file type: {', '.join(invalid_files)}. Only XLSX or CSV allowed.",
            status_code=303,
        )

    # -------------------------------
    # SAVE FILES
    # -------------------------------
    week = get_current_week()
    week_start = str(week["week_start"])

    target_dir = BASE_RAW / upload_type / week_start
    target_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[Path] = []
    uploaded_count = 0

    for file in files:
        contents = await file.read()
        path = target_dir / file.filename
        path.write_bytes(contents)
        saved_files.append(path)
        uploaded_count += 1

    # -------------------------------
    # AMS AUTO-ETL
    # -------------------------------
    if upload_type == "ams":
        run_ams_etl(saved_files)

    return RedirectResponse(
        f"/upload?success=1&count={uploaded_count}&week={week_start}",
        status_code=303,
    )
