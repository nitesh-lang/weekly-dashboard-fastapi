"""Canonical category names for the hand-typed planning workbooks.

THE PROBLEM
-----------
Category names are typed by hand on both the Main and Category tabs, so the
same category gets entered with its words in a different order on different
rows. The Aug 2026 Audio Array plan carries all of these:

    Microphone Condenser   /  Condenser Microphone
    Microphone Conference  /  Conference Microphone  /  Microphone conference
    Microphone Dynamic     /  Dynamic Microphone     /  Microphone dynamic

Nothing downstream knew they were one category, so the dashboard showed two
rows for each: the month's target was split across the pair and BOTH rows read
as achieving against a partial goal. Aug 2026 showed "Microphone Condenser
16.40 L / 15.68 L / 95.6%" sitting a few rows above "Condenser Microphone
2.95 L / 2.59 L / 88.0%" — one category, reported twice, neither figure the
real one.

Case alone was already handled (a casefold merge inside
category_target_vs_actual). Word order was not, and the casefold merge only
ran in that one function, so the donut and the JSON table the UI actually
renders still split. This module normalises both, and the loaders apply it at
the point the plan is read, so every consumer sees a single name.

WHAT COUNTS AS THE SAME CATEGORY
--------------------------------
Two names match when their words are equal ignoring order, case and a trailing
plural: "Condenser Microphone" == "Microphone Condenser", and "Mixers" ==
"Mixer". Checked against every planning workbook on disk, Dec 2025 → Aug 2026,
plus all 212 sku_master category names: it collapses the three microphone
groups above, the "Microphone Wireless"/"Wireless Microphone" pair, and the
two plural drifts ("Mixers"→"Mixer" from Jun 2026, "Monitor Speaker
Accessories"→"Monitor Speakers Accessories" in Aug 2026). Nothing else.

The plural pairs never occur in the SAME month — they are month-to-month
renames — so they were not splitting a row today. Folding them keeps one
category from reading as two different products when months are compared,
and catches the day someone types "Microphone Accessory" beside
"Microphone Accessories". Nexlev is untouched either way (12 categories in,
12 out, figures byte-identical).

WHICH SPELLING SURVIVES
-----------------------
The brand's house style — the variant whose FIRST word is the most common
first word across that month's category vocabulary. Audio Array names nine
categories "Microphone <type>" (Accessories, Lapel, Wireless Handheld, …) and
exactly one each "Condenser/Conference/Dynamic Microphone", so "Microphone
Condenser" wins 9-to-1.

Deliberately NOT "whichever variant has the bigger target": that order can
flip between months — Aug 2026 has Microphone Dynamic at 19,074/day against
Dynamic Microphone at 17,378 — and a category that renames itself mid-year is
worse than either spelling. The leading-word margin has been 8:1 or 9:1 in
every month on file, so this rule is stable.
"""
from __future__ import annotations

import os
import re

import pandas as pd

_WORD = re.compile(r"[a-z0-9]+")
_BLANK = {"", "nan", "none", "-", "—"}


def _words(name) -> list[str]:
    return _WORD.findall(str(name).casefold())


def _singular(word: str) -> str:
    """Fold a trailing plural so "Mixers" and "Mixer" are one category.

    Plurals drift between months in the hand-typed plans: Audio Array called
    it "Mixers" Dec 2025 - May 2026 and "Mixer" from Jun 2026, and
    "Monitor Speaker Accessories" became "Monitor Speakers Accessories" in
    Aug 2026. Same category, two names, so month-on-month comparison reads
    them as different products.

    Deliberately conservative — a trailing "s" and the -ies/-y swap, nothing
    more. The "ss" guard matters: without it "Wireless" becomes "Wireles",
    which is harmless on its own but starts inventing stems. Checked against
    all 212 sku_master category names and every planning workbook on file:
    this merges nothing that the word-order rule did not already merge,
    beyond the two genuine pairs above.
    """
    if len(word) > 4 and word.endswith("ies"):
        return word[:-3] + "y"
    if len(word) > 3 and word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def canon_key(name) -> str:
    """Order-, case- and plural-insensitive identity of a category name."""
    return " ".join(sorted(_singular(w) for w in _words(name)))


def _lead(name) -> str:
    w = _words(name)
    return w[0] if w else ""


def build_alias_map(*name_sources, manual=()) -> dict[str, str]:
    """{raw spelling -> surviving spelling} for one month's vocabulary.

    Pass every column the month's categories are typed into (Main tab AND
    Category tab). Building the map from the union is what guarantees both
    sheets pick the SAME winner — if each sheet chose on its own vocabulary
    they could disagree, which would re-split the very rows this fixes.

    `manual` is an iterable of (variant, winner) name pairs for categories
    that are one product line but share no words, so no rule above can pair
    them — "Accessories" into "Microphone Accessories". Those are business
    calls, not spelling accidents, so they are declared per brand rather than
    guessed. Matching is itself order/case/plural-insensitive, so declaring
    "Accessories" also catches "Accessory" and "accessories".
    """
    seen: list[str] = []
    for src in name_sources:
        if src is None:
            continue
        for raw in src:
            name = str(raw).strip()
            if name.casefold() in _BLANK:
                continue
            if name not in seen:
                seen.append(name)

    lead_freq: dict[str, int] = {}
    for name in seen:
        lead_freq[_lead(name)] = lead_freq.get(_lead(name), 0) + 1

    groups: dict[str, list[str]] = {}
    for name in seen:
        groups.setdefault(canon_key(name), []).append(name)

    # Automatic winner per group: most-common leading word, first-seen breaks
    # a tie so the result never depends on dict/row ordering.
    winner_of: dict[str, str] = {
        key: min(variants, key=lambda v: (-lead_freq[_lead(v)], seen.index(v)))
        for key, variants in groups.items()
    }

    # Manual merges fold one whole group into another. Applied after the
    # automatic pass so the declared winner overrides the house-style pick,
    # and chained so ("A","B") plus ("B","C") lands everything on C.
    redirect: dict[str, str] = {}
    for variant, target in manual:
        vk, tk = canon_key(variant), canon_key(target)
        if vk == tk or vk not in groups or tk not in groups:
            continue  # a pair not present this month is simply inactive
        redirect[vk] = tk
    for key in list(redirect):
        seen_hops = {key}
        dest = redirect[key]
        while dest in redirect and dest not in seen_hops:
            seen_hops.add(dest)
            dest = redirect[dest]
        redirect[key] = dest

    merged: dict[str, list[str]] = {}
    for key, variants in groups.items():
        merged.setdefault(redirect.get(key, key), []).extend(variants)

    alias: dict[str, str] = {}
    for key, variants in merged.items():
        if len(variants) == 1:
            continue  # nothing to rename
        winner = winner_of[key]
        for v in variants:
            if v != winner:
                alias[v] = winner
        print(
            f"[planning] category variants merged -> {winner!r}: "
            + ", ".join(repr(v) for v in variants if v != winner)
        )
    return alias


def _dedupe_columns(df):
    """Drop repeated column labels (keep first).

    Both loaders normalise headers before calling in here, and a hand-edited
    sheet can normalise two different headers onto one name — Nexlev's Category
    tab carries `perdaygoal` twice. A repeated label makes df[name] hand back a
    DataFrame instead of a Series, which breaks the dtype check and the groupby
    below. brands.py applies exactly this rule to the loader's result a moment
    later, so keeping the first is not a new opinion about the data.
    """
    if df is None or df.empty:
        return df
    dupes = df.columns.duplicated()
    if not dupes.any():
        return df
    print(f"[planning] duplicate column(s) on the category sheet, keeping first: "
          f"{sorted(set(df.columns[dupes]))}")
    return df.loc[:, ~dupes]


def apply_alias(df, alias: dict[str, str], col: str = "category"):
    """Rewrite `col` to canonical spellings. Row count unchanged."""
    if df is None or df.empty or not alias:
        return df
    df = _dedupe_columns(df)
    if col not in df.columns:
        return df
    df = df.copy()
    df[col] = df[col].astype(str).str.strip()
    df[col] = df[col].map(lambda v: alias.get(v, v))
    return df


def collapse_rows(df, alias: dict[str, str], col: str = "category"):
    """Canonicalise `col`, then fold the variant rows into one.

    Numeric columns are SUMMED — the two rows carry two real halves of one
    month's goal, so the merged category's target is their total (Aug 2026
    Condenser: 102,519 + 18,414 = 120,933/day). Everything else keeps the
    winner's first value. Groups that never had a duplicate are passed through
    untouched, so this cannot disturb a category that was already fine.

    NOTE: exact-duplicate rows are the caller's problem — load_planning_category
    drops those first (keep-first), because an identical row typed twice is a
    data-entry slip, not two halves of a goal, and summing it would inflate.
    """
    if df is None or df.empty or not alias:
        return df
    df = _dedupe_columns(df)
    if col not in df.columns:
        return df

    df = apply_alias(df, alias, col=col)
    dupe_names = set(alias.values())
    merging = df[col].isin(dupe_names)
    if not merging.any():
        return df

    numeric = [
        c for c in df.columns
        if c != col and pd.api.types.is_numeric_dtype(df[c])
    ]
    # min_count=1 keeps an all-NaN column NaN instead of turning it into 0.
    agg = {
        c: ((lambda s: s.sum(min_count=1)) if c in numeric else "first")
        for c in df.columns if c != col
    }
    folded = df[merging].groupby(col, as_index=False, sort=False).agg(agg)
    out = pd.concat([df[~merging], folded], ignore_index=True)
    return out[df.columns.tolist()]


# Planning workbooks are immutable until replaced, and the map is rebuilt on
# every load_planning_* call, so memoise it by (path, mtime). Without this the
# merge banner also prints on every single call, which buries the real logs.
_MAP_CACHE: dict[tuple, dict[str, str]] = {}


def alias_map_for_file(path, read_name_columns, manual=()) -> dict[str, str]:
    """Cached build_alias_map for one planning workbook.

    `read_name_columns` is called only on a cache miss and must return an
    iterable of category-name iterables (one per tab).
    """
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {}
    key = (path, mtime, tuple(manual))
    hit = _MAP_CACHE.get(key)
    if hit is not None:
        return hit

    alias = build_alias_map(*read_name_columns(), manual=manual)
    # Drop entries for an older revision of the same file.
    for stale in [k for k in _MAP_CACHE if k[0] == path and k[1] != mtime]:
        _MAP_CACHE.pop(stale, None)
    _MAP_CACHE[key] = alias
    return alias
