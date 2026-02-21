from __future__ import annotations

import sqlite3
from pathlib import Path


class KVState:
    """
    Tiny persistent key/value store backed by sqlite.

    Used for monotonic counters (e.g. notify seq) so restarts don't reset ordering.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL,
              updated_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def get_int(self, key: str, default: int = 0) -> int:
        cur = self._conn.execute("SELECT v FROM kv WHERE k=? LIMIT 1;", (key,))
        row = cur.fetchone()
        if not row:
            return int(default)
        try:
            return int(row[0])
        except Exception:
            return int(default)

    def set_int(self, key: str, value: int) -> None:
        self._conn.execute(
            "INSERT INTO kv(k, v, updated_at) VALUES(?, ?, unixepoch()) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at;",
            (key, str(int(value))),
        )
        self._conn.commit()

    def incr_int(self, key: str, by: int = 1) -> int:
        # No concurrent writers in this tool; keep logic simple.
        cur = self.get_int(key, 0) + int(by)
        self.set_int(key, cur)
        return cur

