"""
Defensive data normalization for join-key columns.

Operator-edited Excel files often contain stray whitespace ("Nexlev ",
"Spares "), case inconsistencies, and the literal strings "nan" / "None"
that pandas writes when it stringifies a NaN.  When these values appear
in JOIN keys (brand, model, asin, sku), they silently break merges —
6 SKUs of "Nexlev " never appear under "Nexlev", and the operator only
notices because totals look wrong.

`normalize_keys()` is the one-line guard that goes right after every
read of the master file (or any snapshot whose join keys come from it).
It strips whitespace and coerces "nan"/"None" sentinels to empty
string so downstream `.eq()` and `.isin()` checks work as the operator
expects.

Why explicit + opt-in instead of automatic on every read:
  - We want to keep NaN-ness on NON-key columns (numeric, dates) so the
    rest of the code can detect missing values.
  - Explicit calls make the join-correctness intent visible at the read
    site — anyone touching the loader can see at a glance which columns
    are guarded.
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd


# Standard set of join-key columns across the operator's data.  Any of
# these that exists in the frame will be normalized.
DEFAULT_KEY_COLS: tuple[str, ...] = (
    "asin", "ASIN", "Asin",
    "sku", "SKU", "FBA SKU", "Original SKU",
    "brand", "Brand",
    "model", "Model", "Model No.",
    "category_l0", "category_l1", "category_l2",
    "Category L0", "Category L1", "Category L2",
)


def normalize_keys(df: pd.DataFrame, cols: Iterable[str] | None = None) -> pd.DataFrame:
    """Strip whitespace + collapse 'nan' / 'None' sentinels to '' on the
    named columns (or DEFAULT_KEY_COLS if none provided).  Returns the
    same frame mutated in place; the return value is just so callers
    can chain.

    Safe to call when a column is missing — just skipped.
    Safe to call on numeric columns — only object-dtype values are touched.
    """
    targets = list(cols) if cols is not None else list(DEFAULT_KEY_COLS)
    for c in targets:
        if c not in df.columns:
            continue
        if df[c].dtype != object:
            continue
        df[c] = (
            df[c]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "NaN": "", "NONE": ""})
        )
    return df
