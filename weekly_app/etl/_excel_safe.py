"""xlsx reader that survives Linux-CI openpyxl truncation.

Background (2026-07-06 postmortem):
    Some xlsx files that Windows Excel writes get silently truncated
    when read by pandas + openpyxl on the CI Linux runner.  The
    Model column (or another column with all-empty cells + shared-
    strings quirks) gets dropped, or values shift, causing dedup
    downstream to collapse hundreds of rows into a handful.  This is
    what caused today's cascade — inventory_model_snapshot regression
    guard tripped on Linux-only W5/W6 drops that never happen on
    Windows.

Fix strategy:
    Prefer the Rust-based `python-calamine` engine, which handles
    xlsx more reliably.  Fall back to openpyxl if calamine can't read
    the file (rare, but a few odd xlsx variants trip it).  Callers
    who used `pd.read_excel(file, **kwargs)` can drop-in replace
    with `read_excel_safe(file, **kwargs)`.

Cross-platform contract:
    Same rows, same dtypes, same column order across Windows dev
    machines and Linux CI runners.  If the two engines disagree on
    what a file contains, calamine wins (matches Windows Excel
    behavior more closely).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def read_excel_safe(file: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Read xlsx with calamine, fall back to pandas default on error.

    Accepts every kwarg `pd.read_excel` does.  If the caller passed an
    explicit `engine=`, respect it — some xlsx-shaped files need
    openpyxl for specific features calamine doesn't expose.
    """
    if "engine" in kwargs:
        return pd.read_excel(file, **kwargs)
    try:
        return pd.read_excel(file, engine="calamine", **kwargs)
    except Exception:
        # Calamine unavailable, or an xlsx variant it doesn't grok.
        # openpyxl fallback keeps us running (this is the pre-2026-07-06
        # behavior — worst case we're no worse off than before).
        return pd.read_excel(file, **kwargs)
