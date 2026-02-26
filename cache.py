"""
cache.py — In-Memory DataFrame Cache

Caches pandas DataFrames keyed by (file_path, mtime) so repeated API
calls don't re-read CSVs from disk.  Thread-safe via a simple lock.
Also provides a computed_at timestamp for all cached responses.
"""

import os
import threading
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


class _CSVCache:
    def __init__(self):
        self._store: dict = {}
        self._lock  = threading.Lock()

    def get(self, path: str) -> Optional[pd.DataFrame]:
        """Return cached DataFrame if file has not changed, else None."""
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        key   = (path, mtime)
        with self._lock:
            return self._store.get(key)

    def set(self, path: str, df: pd.DataFrame) -> None:
        """Cache a DataFrame for the current mtime of path."""
        if not os.path.exists(path):
            return
        mtime = os.path.getmtime(path)
        key   = (path, mtime)
        with self._lock:
            # Evict stale entries for the same path
            stale = [k for k in self._store if k[0] == path and k != key]
            for k in stale:
                del self._store[k]
            self._store[key] = df

    def load(self, path: str) -> pd.DataFrame:
        """
        Load CSV, using cache if the file has not changed.
        Raises FileNotFoundError if the file does not exist.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found.")
        cached = self.get(path)
        if cached is not None:
            return cached
        df = pd.read_csv(path)
        df = df.where(pd.notnull(df), None)
        self.set(path, df)
        return df

    def invalidate(self, path: Optional[str] = None) -> None:
        """Invalidate a specific path (or all entries if path is None)."""
        with self._lock:
            if path is None:
                self._store.clear()
            else:
                to_del = [k for k in self._store if k[0] == path]
                for k in to_del:
                    del self._store[k]

    def stats(self) -> dict:
        with self._lock:
            return {
                "cached_files": len(self._store),
                "paths": [k[0] for k in self._store],
            }


# Singleton instance — import this in routers
csv_cache = _CSVCache()


def computed_at() -> str:
    """Return current UTC time as ISO string for API responses."""
    return datetime.now(timezone.utc).isoformat()
