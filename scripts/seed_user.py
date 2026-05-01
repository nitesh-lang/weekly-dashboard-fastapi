"""One-time bootstrap to create the initial dashboard user.

Usage (from project root):
    python scripts/seed_user.py

Idempotent — safe to re-run; it will skip if the user already exists.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weekly_app.core import auth_users  # noqa: E402


def main():
    if auth_users.ensure_initial_user():
        print(f"Seeded user: {auth_users.INITIAL_USER_EMAIL}")
    else:
        print(f"User already exists: {auth_users.INITIAL_USER_EMAIL} — nothing to do.")


if __name__ == "__main__":
    main()
