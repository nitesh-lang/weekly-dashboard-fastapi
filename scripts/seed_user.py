"""One-time bootstrap to create the initial dashboard user.

Usage (from project root):
    python scripts/seed_user.py

Idempotent — safe to re-run; it will skip if the user already exists.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weekly_app.core import security, auth_users  # noqa: E402

EMAIL = "info@cambiumretail.com"
PASSWORD = "Cambium@109"


def main():
    if auth_users.find_user(EMAIL):
        print(f"User already exists: {EMAIL} — nothing to do.")
        return
    auth_users.create_user(EMAIL, security.hash_password(PASSWORD))
    print(f"Seeded user: {EMAIL}")


if __name__ == "__main__":
    main()
