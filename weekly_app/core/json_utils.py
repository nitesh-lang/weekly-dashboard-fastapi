"""
Shared JSON serialization helpers.

Pandas returns NaN / Infinity for missing or divide-by-zero numerics, and
Starlette's `JSONResponse` (which uses stdlib `json.dumps`) refuses to
serialize them — raising `ValueError: Out of range float values are not
JSON compliant` at request time.

The fix is to walk the payload once before handing it to `JSONResponse`
and replace any NaN/Inf with `None` (JSON `null`).  Every route that
returns DataFrame-derived data should wrap its dict in `clean_nan(...)`.
"""
from __future__ import annotations

import math
from typing import Any


def clean_nan(obj: Any) -> Any:
    """Recursively replace NaN / Inf floats with None.

    - dicts and lists are walked in place (new container returned, originals untouched)
    - floats are checked with math.isnan / math.isinf
    - other types pass through unchanged
    """
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(clean_nan(v) for v in obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj
