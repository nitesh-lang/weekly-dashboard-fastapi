"""Working-session store for the margin calculator.

Upstream this was a plain module dict — correct there because the old
service ran a SINGLE uvicorn worker.  The Weekly service runs `--workers 2`
(render.yaml), so a per-process dict makes every follow-up request that
lands on the other worker fail with "Invalid session" (~50% of calls).

Both workers share one container, so the store is disk-backed: every write
pickles to a shared temp directory; reads go through an in-process cache
that is VALIDATED AGAINST THE FILE'S MTIME, so a mutation persisted by the
other worker is picked up instead of served stale.  Mutating routes MUST
write back explicitly (`SESSION_STORE[sid] = session`) after changing the
df/params they got from `.get()` — reference semantics only persist within
one worker.

Entries expire after SESSION_TTL (sliding, refreshed on write) so
DataFrames don't accumulate forever in the 512MB shared instance.
"""
from __future__ import annotations

import os
import pickle
import re
import tempfile
import time
from pathlib import Path

SESSION_TTL = 12 * 60 * 60  # seconds
_SAFE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")

_DIR = Path(os.environ.get("MARGIN_SESSION_DIR",
                           Path(tempfile.gettempdir()) / "margin_sessions"))


class _DiskSessionStore:
    def __init__(self) -> None:
        # key -> (disk mtime at load/save time, value)
        self._cache: dict[str, tuple[float, dict]] = {}
        _DIR.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        if not _SAFE_KEY.match(key):
            raise KeyError(key)
        return _DIR / f"{key}.pkl"

    def _prune(self) -> None:
        cutoff = time.time() - SESSION_TTL
        try:
            for p in _DIR.glob("*.pkl"):
                if p.stat().st_mtime < cutoff:
                    p.unlink(missing_ok=True)
                    self._cache.pop(p.stem, None)
        except OSError:
            pass

    def __setitem__(self, key: str, value: dict) -> None:
        self._prune()
        path = self._path(key)
        tmp = path.with_suffix(f".tmp{os.getpid()}")
        with open(tmp, "wb") as f:
            pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, path)
        try:
            self._cache[key] = (path.stat().st_mtime, value)
        except OSError:
            self._cache.pop(key, None)

    def get(self, key: str, default=None):
        if not key or not _SAFE_KEY.match(key):
            return default
        path = self._path(key)
        try:
            mtime = path.stat().st_mtime
        except OSError:
            self._cache.pop(key, None)
            return default
        if mtime < time.time() - SESSION_TTL:
            return default
        cached = self._cache.get(key)
        if cached is not None and cached[0] == mtime:
            return cached[1]
        try:
            with open(path, "rb") as f:
                value = pickle.load(f)
        except (OSError, pickle.PickleError, EOFError):
            return default
        self._cache[key] = (mtime, value)
        return value

    def __getitem__(self, key: str) -> dict:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None


SESSION_STORE = _DiskSessionStore()
