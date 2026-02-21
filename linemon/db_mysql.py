from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Optional


def _getenv(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v is not None else default


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str

    @staticmethod
    def from_env() -> "MySQLConfig":
        host = (_getenv("DB_HOST") or "").strip()
        user = (_getenv("DB_USER") or "").strip()
        password = _getenv("DB_PASSWORD") or ""
        dbname = (_getenv("DB_NAME") or "").strip()
        port_s = (_getenv("DB_PORT") or "3306").strip()
        if not host or not user or not dbname:
            raise RuntimeError(
                "Missing DB config. Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME (and optionally DB_PORT)."
            )
        try:
            port = int(port_s)
        except Exception as e:
            raise RuntimeError(f"Invalid DB_PORT: {port_s!r}") from e
        return MySQLConfig(host=host, port=port, user=user, password=password, dbname=dbname)


class MySQLStore:
    """
    Minimal MySQL persistence for rooms/messages/attachments.

    We avoid heavy ORMs here because this tool runs in a UI automation loop; keep
    dependency surface small and operations explicit.
    """

    def __init__(self, cfg: MySQLConfig) -> None:
        self.cfg = cfg
        self._conn = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        import pymysql  # type: ignore

        # Autocommit keeps the loop simple (no long-lived transactions).
        self._conn = pymysql.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=self.cfg.dbname,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=8,
            read_timeout=20,
            write_timeout=20,
        )

    @property
    def conn(self):
        if self._conn is None:
            self.connect()
        # Remote MySQL connections can be dropped by the server/network (esp. after long idle).
        # PyMySQL supports ping(reconnect=True) which transparently re-establishes the connection.
        try:
            self._conn.ping(reconnect=True)  # type: ignore[union-attr]
        except Exception:
            # Best-effort: hard reconnect once.
            try:
                self.close()
            except Exception:
                pass
            self.connect()
            try:
                self._conn.ping(reconnect=True)  # type: ignore[union-attr]
            except Exception:
                pass
        return self._conn

    def close(self) -> None:
        c = self._conn
        self._conn = None
        if c is not None:
            try:
                c.close()
            except Exception:
                pass

    def ensure_schema(self) -> None:
        self.connect()
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS rooms (
              id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
              canonical_name VARCHAR(255) NOT NULL,
              raw_name VARCHAR(255) NOT NULL,
              kind VARCHAR(16) NOT NULL DEFAULT 'chat',
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uq_rooms_canonical (canonical_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS messages (
              id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
              room_id BIGINT NOT NULL,
              signature CHAR(40) NOT NULL,
              sender VARCHAR(255) NOT NULL,
              content TEXT NOT NULL,
              kind VARCHAR(16) NOT NULL DEFAULT 'text',
              is_image TINYINT(1) NOT NULL DEFAULT 0,
              captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uq_messages_room_sig (room_id, signature),
              KEY ix_messages_room_time (room_id, captured_at),
              CONSTRAINT fk_messages_room FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS attachments (
              id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
              room_id BIGINT NULL,
              message_id BIGINT NULL,
              sha256 CHAR(64) NOT NULL,
              source_path TEXT NOT NULL,
              original_name VARCHAR(255) NULL,
              ext VARCHAR(16) NULL,
              media_type VARCHAR(96) NULL,
              bytes BIGINT NULL,
              r2_bucket VARCHAR(128) NULL,
              r2_key TEXT NULL,
              r2_etag VARCHAR(255) NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uq_attachments_sha (sha256),
              KEY ix_attachments_room (room_id),
              KEY ix_attachments_msg (message_id),
              CONSTRAINT fk_attachments_room FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE SET NULL,
              CONSTRAINT fk_attachments_msg FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS message_ai (
              id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
              message_id BIGINT NOT NULL,
              attachment_id BIGINT NULL,
              model VARCHAR(96) NOT NULL,
              prompt TEXT NOT NULL,
              result_json JSON NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uq_message_ai_message (message_id),
              KEY ix_message_ai_attachment (attachment_id),
              CONSTRAINT fk_message_ai_message FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
              CONSTRAINT fk_message_ai_attachment FOREIGN KEY (attachment_id) REFERENCES attachments(id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
        ]
        cur = self.conn.cursor()
        try:
            for q in ddl:
                cur.execute(q)
        finally:
            cur.close()

    def upsert_room(self, *, canonical_name: str, raw_name: str, kind: str = "chat") -> int:
        """
        Insert or update a room and return its id.

        Uses LAST_INSERT_ID trick so we always get an id with one round-trip.
        """
        self.connect()
        q = (
            "INSERT INTO rooms (canonical_name, raw_name, kind) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE raw_name=VALUES(raw_name), kind=VALUES(kind), id=LAST_INSERT_ID(id)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(q, (canonical_name, raw_name, kind))
            rid = int(cur.lastrowid)
            return rid
        finally:
            cur.close()

    def upsert_message(
        self,
        *,
        room_id: int,
        signature: str,
        sender: str,
        content: str,
        kind: str,
        is_image: bool,
    ) -> int:
        """
        Insert or ignore a message; return its id (existing or new).
        """
        self.connect()
        q = (
            "INSERT INTO messages (room_id, signature, sender, content, kind, is_image, captured_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, NOW()) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(q, (room_id, signature, sender, content, kind, 1 if is_image else 0))
            mid = int(cur.lastrowid)
            return mid
        finally:
            cur.close()

    def upsert_attachment(
        self,
        *,
        sha256: str,
        source_path: str,
        original_name: str | None,
        ext: str | None,
        media_type: str | None,
        bytes_len: int | None,
        room_id: int | None,
        message_id: int | None,
        r2_bucket: str | None,
        r2_key: str | None,
        r2_etag: str | None,
    ) -> int:
        self.connect()
        q = (
            "INSERT INTO attachments (sha256, source_path, original_name, ext, media_type, bytes, room_id, message_id, r2_bucket, r2_key, r2_etag) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "room_id=COALESCE(attachments.room_id, VALUES(room_id)), "
            "message_id=COALESCE(attachments.message_id, VALUES(message_id)), "
            "r2_bucket=COALESCE(attachments.r2_bucket, VALUES(r2_bucket)), "
            "r2_key=COALESCE(attachments.r2_key, VALUES(r2_key)), "
            "r2_etag=COALESCE(attachments.r2_etag, VALUES(r2_etag)), "
            "original_name=COALESCE(attachments.original_name, VALUES(original_name)), "
            "ext=COALESCE(attachments.ext, VALUES(ext)), "
            "media_type=COALESCE(attachments.media_type, VALUES(media_type)), "
            "bytes=COALESCE(attachments.bytes, VALUES(bytes)), "
            "id=LAST_INSERT_ID(id)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(
                q,
                (
                    sha256,
                    source_path,
                    original_name,
                    ext,
                    media_type,
                    bytes_len,
                    room_id,
                    message_id,
                    r2_bucket,
                    r2_key,
                    r2_etag,
                ),
            )
            return int(cur.lastrowid)
        finally:
            cur.close()

    def attachment_exists(self, sha256: str) -> bool:
        self.connect()
        q = "SELECT 1 FROM attachments WHERE sha256=%s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (sha256,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def attachment_r2_info(self, sha256: str) -> tuple[str | None, str | None, str | None] | None:
        """
        Return (r2_bucket, r2_key, r2_etag) for a given sha, or None if no row.
        """
        self.connect()
        q = "SELECT r2_bucket, r2_key, r2_etag FROM attachments WHERE sha256=%s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (sha256,))
            row = cur.fetchone()
            if not row:
                return None
            return (row[0], row[1], row[2])
        finally:
            cur.close()

    def backfill_unlinked_attachments_from_filename(self, *, room_id: int, limit: int = 250) -> int:
        """
        Repair older rows where an attachment was uploaded (room_id set) but not linked to a message.

        We only update rows with message_id IS NULL, and only when we can extract a plausible
        message_id from original_name and confirm the message exists in the same room.
        """
        self.connect()
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, original_name
                FROM attachments
                WHERE room_id=%s
                  AND message_id IS NULL
                  AND original_name IS NOT NULL
                ORDER BY id DESC
                LIMIT %s
                """,
                (int(room_id), int(limit)),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        # Filename shape produced by wcmon:
        #   <room>_<message_id>_<sigprefix>....(png/jpg/etc)
        # Room may contain underscores, so find the numeric id preceding a sigprefix.
        rx = re.compile(r"_(\\d+)_([0-9a-f]{10})", re.IGNORECASE)

        linked = 0
        for att_id, original_name in rows:
            try:
                s = str(original_name or "")
            except Exception:
                continue
            m = rx.search(s)
            if not m:
                continue
            try:
                mid = int(m.group(1))
            except Exception:
                continue

            cur2 = self.conn.cursor()
            try:
                cur2.execute(
                    "SELECT 1 FROM messages WHERE id=%s AND room_id=%s AND kind='image' LIMIT 1",
                    (int(mid), int(room_id)),
                )
                if cur2.fetchone() is None:
                    continue
                cur2.execute(
                    "UPDATE attachments SET message_id=%s WHERE id=%s AND message_id IS NULL",
                    (int(mid), int(att_id)),
                )
                if int(cur2.rowcount or 0) > 0:
                    linked += 1
            finally:
                cur2.close()
        return int(linked)

    def message_has_attachment(self, message_id: int) -> bool:
        self.connect()
        q = "SELECT 1 FROM attachments WHERE message_id=%s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(message_id),))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def message_sender(self, message_id: int) -> str | None:
        self.connect()
        q = "SELECT sender FROM messages WHERE id=%s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(message_id),))
            row = cur.fetchone()
            if not row:
                return None
            try:
                return str(row[0])
            except Exception:
                return None
        finally:
            cur.close()

    def update_message_sender_if_unknown(self, *, message_id: int, sender: str) -> int:
        """
        Update sender for an existing message row only when it's still unknown.

        We intentionally avoid changing sender for rows that already have a non-placeholder
        value to reduce the risk of corrupting historical data in case of signature collisions.
        """
        self.connect()
        s = (sender or "").strip()
        if not s:
            return 0
        q = "UPDATE messages SET sender=%s WHERE id=%s AND (sender='' OR sender='(unknown)')"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (s, int(message_id)))
            return int(cur.rowcount or 0)
        finally:
            cur.close()

    def message_ai_exists(self, message_id: int) -> bool:
        self.connect()
        q = "SELECT 1 FROM message_ai WHERE message_id=%s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(message_id),))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def upsert_message_ai(
        self,
        *,
        message_id: int,
        attachment_id: int | None,
        model: str,
        prompt: str,
        result_json: str,
    ) -> int:
        self.connect()
        q = (
            "INSERT INTO message_ai (message_id, attachment_id, model, prompt, result_json) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "attachment_id=COALESCE(message_ai.attachment_id, VALUES(attachment_id)), "
            "model=VALUES(model), "
            "prompt=VALUES(prompt), "
            "result_json=VALUES(result_json), "
            "id=LAST_INSERT_ID(id)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(message_id), attachment_id, model, prompt, result_json))
            return int(cur.lastrowid)
        finally:
            cur.close()

    def clear_other_attachments_for_message(self, *, message_id: int, keep_attachment_id: int) -> int:
        """
        Disassociate older attachments from a message, keeping only the newest one we just saved.

        This avoids UI confusion when we re-capture attachments (e.g. after improving image export).
        """
        self.connect()
        q = "UPDATE attachments SET message_id=NULL WHERE message_id=%s AND id<>%s"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(message_id), int(keep_attachment_id)))
            return int(cur.rowcount or 0)
        finally:
            cur.close()

    def latest_image_message_id(self, *, room_id: int) -> int | None:
        """
        Return the newest (highest id) image message id for a room, or None.
        """
        self.connect()
        q = "SELECT id FROM messages WHERE room_id=%s AND kind='image' ORDER BY id DESC LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (int(room_id),))
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            cur.close()

    def ping(self) -> float:
        """
        Return server time() as a quick connectivity check.
        """
        self.connect()
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT UNIX_TIMESTAMP()")
            row = cur.fetchone()
            if not row:
                return time.time()
            return float(row[0])
        finally:
            cur.close()
