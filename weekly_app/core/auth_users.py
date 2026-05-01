"""File-backed user store at data/users.json.

Schema:
{
  "users": [
    {
      "email":               "info@cambiumretail.com",
      "password_hash":       "$2b$...",
      "created_at":          "2026-04-30T...Z",
      "password_updated_at": null
    }
  ]
}
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

USERS_FILE = Path("data/users.json")

# Single source of truth for the bootstrap account. seed_user.py and the
# startup auto-seed in main.py both use these.
INITIAL_USER_EMAIL = "info@cambiumretail.com"
INITIAL_USER_PASSWORD = "Cambium@109"  # initial only — change via /reset-password


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load() -> Dict[str, Any]:
    if not USERS_FILE.exists():
        return {"users": []}
    try:
        return json.loads(USERS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"users": []}


def _save(data: Dict[str, Any]) -> None:
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def find_user(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    norm = email.strip().lower()
    for u in _load().get("users", []):
        if u.get("email", "").strip().lower() == norm:
            return u
    return None


def create_user(email: str, password_hash: str) -> Dict[str, Any]:
    data = _load()
    norm = email.strip().lower()
    if any(u.get("email", "").strip().lower() == norm for u in data["users"]):
        raise ValueError(f"User already exists: {norm}")
    user = {
        "email": norm,
        "password_hash": password_hash,
        "created_at": _now_iso(),
        "password_updated_at": None,
    }
    data["users"].append(user)
    _save(data)
    return user


def update_password(email: str, password_hash: str) -> bool:
    data = _load()
    norm = email.strip().lower()
    for u in data["users"]:
        if u.get("email", "").strip().lower() == norm:
            u["password_hash"] = password_hash
            u["password_updated_at"] = _now_iso()
            _save(data)
            return True
    return False


def ensure_initial_user() -> bool:
    """Create the bootstrap user if absent. Idempotent — safe to call on
    every startup. Returns True if a user was created, False if it already
    existed.
    """
    from weekly_app.core import security  # local import to avoid circular
    if find_user(INITIAL_USER_EMAIL):
        return False
    create_user(INITIAL_USER_EMAIL, security.hash_password(INITIAL_USER_PASSWORD))
    return True
