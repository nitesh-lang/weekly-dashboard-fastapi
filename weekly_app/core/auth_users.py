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

Notes:
- _load() is wrapped in a short-lived in-memory cache (~30s) because the
  Google-Drive virtual filesystem on Windows occasionally throws
  `OSError: [WinError 433] A device which does not exist was specified`
  when stat'd from a child process.  Without the cache, every login
  request rolled the dice on that flake.  Auto-invalidated on _save().
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

USERS_FILE = Path("data/users.json")
_CACHE_TTL_SECONDS = 30.0

# Single source of truth for the bootstrap account. seed_user.py and the
# startup auto-seed in main.py both use these.
INITIAL_USER_EMAIL = "info@cambiumretail.com"
INITIAL_USER_PASSWORD = "Cambium@109"  # initial only — change via /reset-password


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# Cache: (loaded_at_monotonic, data).  None means "never loaded".
_cache: Optional[tuple] = None


def _load_uncached() -> Dict[str, Any]:
    """Single attempt to load + parse the users file.  Retries the FS call
    a couple of times to ride out the Google-Drive WinError 433 hiccup.
    Returns {"users": []} only on hard parse error, never on FS flake."""
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            if not USERS_FILE.exists():
                return {"users": []}
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except OSError as e:
            last_exc = e
            time.sleep(0.05 * (attempt + 1))   # 50ms, 100ms backoff
        except Exception:
            return {"users": []}
    # All retries failed — surface the FS error so the caller can decide.
    raise last_exc if last_exc else RuntimeError("users.json unreadable")


def _load() -> Dict[str, Any]:
    global _cache
    now = time.monotonic()
    if _cache is not None and (now - _cache[0]) < _CACHE_TTL_SECONDS:
        return _cache[1]
    try:
        data = _load_uncached()
    except Exception:
        # If the file is genuinely unreadable but we have a cached copy,
        # return the stale one rather than 500ing.  Better than nothing.
        if _cache is not None:
            return _cache[1]
        return {"users": []}
    _cache = (now, data)
    return data


def _save(data: Dict[str, Any]) -> None:
    global _cache
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    # Invalidate cache so the next _load() picks up the freshly written file.
    _cache = (time.monotonic(), data)


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
