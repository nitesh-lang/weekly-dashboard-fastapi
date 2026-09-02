from pathlib import Path

from fastapi.templating import Jinja2Templates

# Absolute path: this app runs INSIDE the Weekly service whose CWD is the
# repo root, so a bare "templates" would resolve to the wrong directory.
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))
