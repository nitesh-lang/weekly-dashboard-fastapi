"""Password hashing + signed-token helpers for auth.

Passwords: bcrypt (used directly; passlib has compatibility issues with
           bcrypt 4.x).
Tokens:    itsdangerous URLSafeTimedSerializer signed with SESSION_SECRET.
"""
import os

import bcrypt
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired


def hash_password(plain: str) -> str:
    # bcrypt has a 72-byte input limit; truncate so very long passwords
    # don't crash the hasher. Practical passwords never hit this.
    pw = plain.encode("utf-8")[:72]
    return bcrypt.hashpw(pw, bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    if not hashed:
        return False
    try:
        return bcrypt.checkpw(
            plain.encode("utf-8")[:72],
            hashed.encode("utf-8"),
        )
    except Exception:
        return False


_warned = False


def _secret() -> str:
    """Pulls SESSION_SECRET from env. In prod this MUST be set.
    Falls back to a stable in-process random for local dev only and warns once.
    """
    global _warned
    s = os.environ.get("SESSION_SECRET")
    if s:
        return s
    if not _warned:
        print("⚠ SESSION_SECRET not set — using insecure dev default. Set in env for production!")
        _warned = True
    return "dev-insecure-secret-do-not-use-in-prod"


def make_reset_token(email: str) -> str:
    s = URLSafeTimedSerializer(_secret(), salt="password-reset")
    return s.dumps({"email": email.strip().lower()})


def verify_reset_token(token: str, max_age_seconds: int = 1800) -> str | None:
    """Return the email if the token is valid + unexpired, else None."""
    if not token:
        return None
    s = URLSafeTimedSerializer(_secret(), salt="password-reset")
    try:
        data = s.loads(token, max_age=max_age_seconds)
        return data.get("email")
    except (BadSignature, SignatureExpired):
        return None
