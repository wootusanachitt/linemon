from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import pymysql
from pymysql.cursors import DictCursor

from .config import settings


def _require_db_config() -> None:
    if not settings.db_host or not settings.db_user or not settings.db_name:
        raise RuntimeError(
            "Database config is missing. Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME (or LINEMON_DB_*)."
        )


@contextmanager
def db_cursor() -> Iterator[DictCursor]:
    _require_db_config()
    conn = pymysql.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=8,
        read_timeout=20,
        write_timeout=20,
        cursorclass=DictCursor,
    )
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def _to_iso(dt: Any) -> str | None:
    if isinstance(dt, datetime):
        return dt.isoformat()
    return None


def list_rooms(*, limit: int = 100) -> list[dict[str, Any]]:
    q = """
        SELECT
            r.id,
            r.canonical_name,
            r.raw_name,
            r.updated_at,
            lm.id AS last_message_id,
            lm.sender AS last_sender,
            lm.content AS last_content,
            lm.kind AS last_kind,
            lm.is_image AS last_is_image,
            lm.captured_at AS last_captured_at
        FROM rooms r
        LEFT JOIN (
            SELECT
                m1.room_id,
                m1.id,
                m1.sender,
                m1.content,
                m1.kind,
                m1.is_image,
                m1.captured_at
            FROM messages m1
            INNER JOIN (
                SELECT room_id, MAX(id) AS max_id
                FROM messages
                GROUP BY room_id
            ) mx ON mx.room_id = m1.room_id AND mx.max_id = m1.id
        ) lm ON lm.room_id = r.id
        ORDER BY COALESCE(lm.captured_at, r.updated_at) DESC, r.id DESC
        LIMIT %s
    """
    with db_cursor() as cur:
        cur.execute(q, (max(1, min(limit, 500)),))
        rows = cur.fetchall() or []

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": int(row["id"]),
                "canonical_name": str(row.get("canonical_name") or ""),
                "raw_name": str(row.get("raw_name") or ""),
                "updated_at": _to_iso(row.get("updated_at")),
                "last_message": {
                    "id": int(row["last_message_id"]) if row.get("last_message_id") is not None else None,
                    "sender": str(row.get("last_sender") or ""),
                    "content": str(row.get("last_content") or ""),
                    "kind": str(row.get("last_kind") or ""),
                    "is_image": bool(row.get("last_is_image") or False),
                    "captured_at": _to_iso(row.get("last_captured_at")),
                },
            }
        )
    return out


def list_messages(
    *,
    room_id: int,
    limit: int = 150,
    after_id: int | None = None,
    before_id: int | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [int(room_id)]
    where = "WHERE m.room_id = %s"
    if after_id is not None and after_id > 0:
        where += " AND m.id > %s"
        params.append(int(after_id))
    if before_id is not None and before_id > 0:
        where += " AND m.id < %s"
        params.append(int(before_id))

    q = f"""
        SELECT
            m.id,
            m.room_id,
            m.sender,
            m.content,
            m.kind,
            m.is_image,
            m.captured_at,
            a.id AS attachment_id,
            a.r2_bucket,
            a.r2_key,
            a.media_type,
            a.ext
        FROM messages m
        LEFT JOIN attachments a
            ON a.id = (
                SELECT a2.id
                FROM attachments a2
                WHERE a2.message_id = m.id
                ORDER BY a2.id DESC
                LIMIT 1
            )
        {where}
        ORDER BY m.id DESC
        LIMIT %s
    """
    params.append(max(1, min(limit, 500)))

    with db_cursor() as cur:
        cur.execute(q, tuple(params))
        rows = cur.fetchall() or []

    rows.reverse()
    out: list[dict[str, Any]] = []
    for row in rows:
        r2_key = str(row.get("r2_key") or "")
        attachment_url = None
        if r2_key and settings.r2_public_base_url:
            attachment_url = settings.r2_public_base_url.rstrip("/") + "/" + r2_key.lstrip("/")

        out.append(
            {
                "id": int(row["id"]),
                "room_id": int(row["room_id"]),
                "sender": str(row.get("sender") or ""),
                "content": str(row.get("content") or ""),
                "kind": str(row.get("kind") or ""),
                "is_image": bool(row.get("is_image") or False),
                "captured_at": _to_iso(row.get("captured_at")),
                "attachment": {
                    "id": int(row["attachment_id"]) if row.get("attachment_id") is not None else None,
                    "media_type": str(row.get("media_type") or ""),
                    "ext": str(row.get("ext") or ""),
                    "r2_bucket": str(row.get("r2_bucket") or ""),
                    "r2_key": r2_key,
                    "url": attachment_url,
                },
            }
        )
    return out
