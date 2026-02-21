from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


class StateDB:
    """
    Minimal de-dupe store.

    We only persist a rolling set of recent message hashes per chat, to avoid
    re-appending when we re-scan the same visible message window.
    """

    def __init__(self, path: str | Path, *, keep_per_chat: int = 300) -> None:
        self.path = str(path)
        self.keep_per_chat = keep_per_chat
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat TEXT NOT NULL,
              hash TEXT NOT NULL,
              created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_seen_chat_hash ON seen(chat, hash);"
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def has(self, chat: str, h: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM seen WHERE chat = ? AND hash = ? LIMIT 1;", (chat, h)
        )
        return cur.fetchone() is not None

    def add_many(self, chat: str, hashes: Iterable[str]) -> None:
        rows = [(chat, h) for h in hashes]
        if not rows:
            return
        self._conn.executemany("INSERT INTO seen(chat, hash) VALUES(?, ?);", rows)
        self._purge_chat(chat)
        self._conn.commit()

    def _purge_chat(self, chat: str) -> None:
        # Keep only the most recent N seen hashes per chat.
        self._conn.execute(
            """
            DELETE FROM seen
            WHERE chat = ?
              AND id NOT IN (
                SELECT id FROM seen
                WHERE chat = ?
                ORDER BY id DESC
                LIMIT ?
              );
            """,
            (chat, chat, self.keep_per_chat),
        )

