from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from linemon.db_mysql import MySQLConfig, MySQLStore
from linemon.r2_uploader import R2Config, R2Uploader, guess_mime, sha256_bytes, sha256_file
from linemon.screen_capture import Rect, ScreenGrabber
from linemon.wechat_files import WeChatFiles


def _safe_ext_from_name(name: str) -> str:
    s = (name or "").strip()
    if "." not in s:
        return ""
    ext = s.rsplit(".", 1)[-1].lower()
    if len(ext) > 12:
        return ""
    return ext


def _allowed_upload_by_ext(ext: str) -> bool:
    e = (ext or "").lower().lstrip(".")
    if not e:
        return False
    if e in {"pdf", "doc", "docx", "xls", "xlsx"}:
        return True
    # images: allow common formats; WeChat .dat is decoded to a real image extension.
    if e in {"jpg", "jpeg", "png", "gif", "bmp", "webp", "tif", "tiff", "heic"}:
        return True
    return False


@dataclass
class Persistor:
    mysql: MySQLStore
    r2: Optional[R2Uploader]
    wechat_files: WeChatFiles
    grabber: ScreenGrabber

    @staticmethod
    def from_env() -> "Persistor":
        mysql = MySQLStore(MySQLConfig.from_env())
        r2: Optional[R2Uploader] = None
        try:
            r2 = R2Uploader(R2Config.from_env())
        except Exception:
            r2 = None
        wf = WeChatFiles()
        return Persistor(mysql=mysql, r2=r2, wechat_files=wf, grabber=ScreenGrabber())

    def ensure_ready(self) -> None:
        self.mysql.ensure_schema()
        # Best-effort connect to R2 so failures are early and obvious.
        if self.r2 is not None:
            try:
                self.r2.connect()
            except Exception:
                # Don't hard-fail capture if R2 is down; we still store text to DB.
                self.r2 = None

    def close(self) -> None:
        try:
            self.mysql.close()
        except Exception:
            pass

    def clear_other_attachments_for_message(self, *, message_id: int, keep_attachment_id: int) -> int:
        try:
            return self.mysql.clear_other_attachments_for_message(
                message_id=int(message_id),
                keep_attachment_id=int(keep_attachment_id),
            )
        except Exception:
            return 0

    def latest_image_message_id(self, *, room_id: int) -> int | None:
        try:
            return self.mysql.latest_image_message_id(room_id=int(room_id))
        except Exception:
            return None

    def upsert_room(self, *, canonical_name: str, raw_name: str, kind: str = "chat") -> int:
        return self.mysql.upsert_room(canonical_name=canonical_name, raw_name=raw_name, kind=kind)

    def message_has_attachment(self, message_id: int) -> bool:
        try:
            return self.mysql.message_has_attachment(int(message_id))
        except Exception:
            return False

    def message_sender(self, message_id: int) -> str | None:
        try:
            v = self.mysql.message_sender(int(message_id))
        except Exception:
            v = None
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    def save_message(
        self,
        *,
        room_id: int,
        signature: str,
        sender: str,
        content: str,
        msg_type: str,
        is_image: bool,
    ) -> int:
        return self.mysql.upsert_message(
            room_id=room_id,
            signature=signature,
            sender=sender,
            content=content,
            kind=msg_type,
            is_image=is_image,
        )

    def update_message_sender_if_unknown(self, *, message_id: int, sender: str) -> int:
        try:
            return int(self.mysql.update_message_sender_if_unknown(message_id=int(message_id), sender=str(sender)))
        except Exception:
            return 0

    def upload_and_record_file(
        self,
        *,
        room_id: int | None,
        message_id: int | None,
        path: Path,
        original_name: str | None = None,
        ext: str | None = None,
        media_type: str | None = None,
    ) -> int | None:
        """
        Upload an allowed attachment to R2 (if configured) and upsert an attachments row.
        """
        try:
            if not path.exists() or not path.is_file():
                return None
        except Exception:
            return None

        if ext is None:
            ext = path.suffix.lower().lstrip(".")
        if not _allowed_upload_by_ext(ext):
            return None

        try:
            sha = sha256_file(path)
        except Exception:
            return None

        try:
            size = path.stat().st_size
        except Exception:
            size = None

        existing_r2_key = None
        try:
            info = self.mysql.attachment_r2_info(sha)
            existing_r2_key = (info[1] if info else None)
        except Exception:
            existing_r2_key = None

        bucket = None
        key = None
        etag = None
        if self.r2 is not None and not existing_r2_key:
            # Key by sha256 to avoid collisions.
            base_key = f"attachments/{sha[:2]}/{sha}.{ext}"
            try:
                key, etag = self.r2.put_file(
                    key=base_key, path=path, content_type=media_type or guess_mime(path)
                )
                bucket = self.r2.cfg.bucket
            except Exception:
                bucket = None
                key = None
                etag = None

        return self.mysql.upsert_attachment(
            sha256=sha,
            source_path=str(path),
            original_name=original_name or path.name,
            ext=ext,
            media_type=media_type or guess_mime(path),
            bytes_len=size,
            room_id=room_id,
            message_id=message_id,
            r2_bucket=bucket,
            r2_key=key,
            r2_etag=etag,
        )

    def upload_and_record_image_png_bytes(
        self,
        *,
        room_id: int | None,
        message_id: int | None,
        data: bytes,
        source_path: str | None = None,
        original_name: str | None = None,
    ) -> int | None:
        """
        Upload PNG bytes (typically a UI bubble capture) to R2 and upsert an attachments row.

        This is used when we already have the exact bytes we want to store and need a stable sha256.
        """
        if not data:
            return None
        sha = sha256_bytes(data)
        media_type = "image/png"
        ext = "png"

        existing_r2_key = None
        try:
            info = self.mysql.attachment_r2_info(sha)
            existing_r2_key = (info[1] if info else None)
        except Exception:
            existing_r2_key = None

        bucket = None
        key = None
        etag = None
        if self.r2 is not None and not existing_r2_key:
            base_key = f"images/{sha[:2]}/{sha}.{ext}"
            try:
                key, etag = self.r2.put_bytes(key=base_key, data=data, content_type=media_type)
                bucket = self.r2.cfg.bucket
            except Exception:
                bucket = None
                key = None
                etag = None

        return self.mysql.upsert_attachment(
            sha256=sha,
            source_path=source_path or f"uia_capture://bubble/{sha}.{ext}",
            original_name=original_name or f"bubble.{ext}",
            ext=ext,
            media_type=media_type,
            bytes_len=len(data),
            room_id=room_id,
            message_id=message_id,
            r2_bucket=bucket,
            r2_key=key,
            r2_etag=etag,
        )

    def upload_and_record_image_dat(
        self,
        *,
        room_id: int | None,
        message_id: int | None,
        dat_path: Path,
    ) -> int | None:
        """
        Decode WeChat .dat image, upload decoded bytes, and upsert attachment row.
        """
        dec = self.wechat_files.decode_wechat_dat(dat_path)
        if dec is None:
            return None
        sha = sha256_bytes(dec.data)
        existing_r2_key = None
        try:
            info = self.mysql.attachment_r2_info(sha)
            existing_r2_key = (info[1] if info else None)
        except Exception:
            existing_r2_key = None

        bucket = None
        key = None
        etag = None
        if self.r2 is not None and not existing_r2_key:
            base_key = f"images/{sha[:2]}/{sha}.{dec.ext}"
            try:
                key, etag = self.r2.put_bytes(key=base_key, data=dec.data, content_type=dec.media_type)
                bucket = self.r2.cfg.bucket
            except Exception:
                bucket = None
                key = None
                etag = None

        return self.mysql.upsert_attachment(
            sha256=sha,
            source_path=str(dat_path),
            original_name=dat_path.name,
            ext=dec.ext,
            media_type=dec.media_type,
            bytes_len=len(dec.data),
            room_id=room_id,
            message_id=message_id,
            r2_bucket=bucket,
            r2_key=key,
            r2_etag=etag,
        )

    def upload_and_record_image_capture(
        self,
        *,
        room_id: int | None,
        message_id: int | None,
        rect: tuple[int, int, int, int],
        original_name: str | None = None,
    ) -> int | None:
        """
        Capture the on-screen pixels of a WeChat image bubble and upload as PNG.

        This is more reliable than guessing which .dat file corresponds to a UI bubble.
        """
        try:
            left, top, right, bottom = rect
        except Exception:
            return None
        if right <= left or bottom <= top:
            return None

        try:
            img_bgr = self.grabber.grab_bgr(Rect(left=left, top=top, right=right, bottom=bottom))
        except Exception:
            return None

        try:
            import cv2  # type: ignore

            ok, buf = cv2.imencode(".png", img_bgr)
            if not ok:
                return None
            data = bytes(buf.tobytes())
        except Exception:
            return None

        return self.upload_and_record_image_png_bytes(
            room_id=room_id,
            message_id=message_id,
            data=data,
            original_name=original_name or "bubble.png",
        )
