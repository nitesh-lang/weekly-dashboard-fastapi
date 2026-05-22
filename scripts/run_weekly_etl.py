"""
One-command local ETL runner — replaces invoking the six scripts manually:

    python weekly_app/etl/business_ads_weekly_etl.py
    python weekly_app/etl/step3_ads_aggregation.py
    python weekly_app/etl/step4_join_business_ads.py
    python weekly_app/etl/step5_add_category_mapping.py
    python weekly_app/etl/inventory_model_snapshot.py
    python weekly_app/etl/inventory_snapshot_etl.py

This script runs them in subprocesses (they are imperative top-level scripts
without main() functions, so subprocess.run is the right invocation).
Each script's stdout/stderr is streamed live so you see progress as it
happens.  If any step fails, the runner exits non-zero — useful for CI/cron
or your own sanity-check workflow.

Usage (from repo root):
    python scripts/run_weekly_etl.py            # run all six in order
    python scripts/run_weekly_etl.py inv        # just inventory_snapshot_etl
    python scripts/run_weekly_etl.py step3 step4  # specific steps

Render does NOT run this — the production server only serves the committed
outputs.  See DEPLOY.md.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = REPO_ROOT / "weekly_app" / "etl"

# Canonical pipeline order — same as the operator's documented sequence.
PIPELINE = [
    ("biz_ads",    "business_ads_weekly_etl.py"),
    ("step3",      "step3_ads_aggregation.py"),
    ("step4",      "step4_join_business_ads.py"),
    ("step5",      "step5_add_category_mapping.py"),
    ("inv_model",  "inventory_model_snapshot.py"),
    ("inv_snap",   "inventory_snapshot_etl.py"),
    ("margin",     "margin_snapshot.py"),         # reads data/raw/margin/*.xlsx
    ("returns",    "returns_snapshot.py"),        # reads data/raw/returns/*.csv
]

ALIAS = {name: script for name, script in PIPELINE}
# Friendly extra aliases so partial runs are easy to type
ALIAS["inv"] = "inventory_snapshot_etl.py"
ALIAS["model"] = "inventory_model_snapshot.py"


def run_one(name: str, script: str) -> int:
    path = ETL_DIR / script
    if not path.exists():
        print(f"[ETL] ❌ {script} not found at {path}")
        return 2
    print(f"\n{'=' * 64}\n[ETL] ▶ {name}: {script}\n{'=' * 64}")
    completed = subprocess.run(
        [sys.executable, str(path)],
        cwd=REPO_ROOT,
        env={**__import__('os').environ, "PYTHONIOENCODING": "utf-8"},
    )
    if completed.returncode != 0:
        print(f"[ETL] ❌ {name} exited with code {completed.returncode}")
    else:
        print(f"[ETL] ✓ {name} done")
    return completed.returncode


def main():
    requested = [a.lower() for a in sys.argv[1:]]

    if not requested:
        plan = PIPELINE
    else:
        plan = []
        for arg in requested:
            if arg not in ALIAS:
                print(f"[ETL] Unknown stage '{arg}'. Available: {list(ALIAS)}")
                sys.exit(2)
            # de-dup but preserve order
            script = ALIAS[arg]
            entry = (arg, script)
            if entry not in plan:
                plan.append(entry)

    failures: list[str] = []
    for name, script in plan:
        code = run_one(name, script)
        if code != 0:
            failures.append(name)
            # Halt the pipeline so later steps don't run against bad data
            print(f"[ETL] Stopping pipeline because {name} failed")
            break

    if failures:
        print(f"\n[ETL] ❌ Pipeline failed at: {failures}")
        sys.exit(1)

    print("\n[ETL] ✅ All stages OK")


if __name__ == "__main__":
    main()
