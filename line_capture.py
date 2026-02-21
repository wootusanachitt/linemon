from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import unicodedata

from linemon.config import Config
from linemon.state import StateDB
from linemon.uia_wechat import get_idle_seconds, sanitize_filename, sha1_hex
from linemon.uia_line import LineUIA
from linemon.badge_ocr import BadgeOCR, BadgeOCRConfig
from linemon.env import load_env
from linemon.persist import Persistor
from linemon.notifier import Notifier
from linemon.login_banner import LoginBanner
from linemon.single_instance import acquire_mutex, release_mutex
from linemon.r2_uploader import sha256_bytes, sha256_file
from linemon.screen_capture import Rect as CaptureRect
from linemon.vision_client import VisionClient, VisionConfig


def _force_utf8_stdio() -> None:
    # When stdout/stderr are redirected on Windows, default encodings can be legacy (cp1252),
    # which will crash on CN text. Replace unencodable characters instead of crashing.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def _apply_linemon_env_defaults() -> None:
    """
    Map LINEMON_* environment values to the shared keys used by persistence modules.

    This keeps the LINE runner isolated from wcmon without rewriting lower-level modules.
    """
    mapping = [
        ("DB_HOST", "LINEMON_DB_HOST"),
        ("DB_PORT", "LINEMON_DB_PORT"),
        ("DB_USER", "LINEMON_DB_USER"),
        ("DB_PASSWORD", "LINEMON_DB_PASSWORD"),
        ("DB_NAME", "LINEMON_DB_NAME"),
        ("NOTIFY_URL", "LINEMON_NOTIFY_URL"),
        ("NOTIFY_TOKEN", "LINEMON_NOTIFY_TOKEN"),
        ("NOTIFY_SOURCE", "LINEMON_NOTIFY_SOURCE"),
    ]
    for dst, src in mapping:
        v = (os.environ.get(src) or "").strip()
        if v:
            os.environ[dst] = v
    # Sensible default source for frontend notifications in the LINE app.
    if not (os.environ.get("NOTIFY_SOURCE") or "").strip():
        os.environ["NOTIFY_SOURCE"] = "line-monitor"


def _match_allowlist(display_name: str, allowlist: set[str]) -> str | None:
    """
    WeChat UIA sometimes returns a merged string (chat name + preview text).
    To avoid missing intended chats, treat an allowlist entry as a match if the
    display name equals it or starts with it (word boundary).
    """
    dn = (display_name or "").strip()
    if not dn:
        return None
    for a in allowlist:
        aa = (a or "").strip()
        if not aa:
            continue
        if dn == aa:
            return aa
        if dn.startswith(aa):
            if len(dn) == len(aa):
                return aa
            # Require a boundary to reduce accidental prefix collisions.
            nxt = dn[len(aa)]
            if nxt.isspace() or nxt in {"-", "—", "(", "[", ":"}:
                return aa
    return None


def _normalize_chat_lookup(raw: str) -> str:
    s = (raw or "").replace("\r", "").strip()
    if not s:
        return ""
    # Row names are often formatted as "Name\\nPreview...".
    s = s.split("\n", 1)[0].strip()
    s = _canonical_chat_title(s)
    return s.lower()


def _ensure_dirs(cfg: Config) -> None:
    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)
    if cfg.debug:
        Path(cfg.debug_dump_dir).mkdir(parents=True, exist_ok=True)


def _append_lines(path: Path, lines: list[str]) -> None:
    if not lines:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use BOM on first write so legacy Windows tools (e.g. PowerShell 5 Get-Content)
    # don't misinterpret UTF-8 chat logs containing CJK.
    enc = "utf-8-sig" if not path.exists() else "utf-8"
    with path.open("a", encoding=enc, newline="\n") as f:
        for ln in lines:
            f.write(ln)
            if not ln.endswith("\n"):
                f.write("\n")


_TRAILING_COUNT_RE = re.compile(r"^(.*?)[\(（](\d[\d,]*)[\)）]$")


def _ensure_room_header(path: Path, *, raw_room_name: str, canonical_room_name: str) -> None:
    """
    If the log file does not exist yet, add a small header block so it's easy to
    verify we're logging the *correct* room (raw header text can include member counts).
    """
    if path.exists():
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    hdr = [
        f"# created_at: {ts}",
        f"# room_raw: {raw_room_name}",
        f"# room_canonical: {canonical_room_name}",
        "# ---",
    ]
    _append_lines(path, hdr)


@dataclass
class _PersistDelta:
    room_id: int | None
    inserted_messages: int
    message_ids: list[int]
    attachment_ids: list[int]


def _persist_messages_to_db(
    persistor: Persistor,
    *,
    uia: LineUIA | None = None,
    vision: VisionClient | None = None,
    image_export_dir: str | None = None,
    raw_room_name: str,
    canonical_room_name: str,
    messages: list,
    state_db: StateDB | None = None,
    image_since_epoch: float | None = None,
    force_attachments: bool = False,
) -> _PersistDelta:
    """
    Persist messages into MySQL. Returns inserted_count (best-effort).
    Uses local state sqlite (if provided) to reduce duplicate writes.
    """
    try:
        room_id = persistor.upsert_room(
            canonical_name=canonical_room_name,
            raw_name=raw_room_name,
            kind="chat",
        )
    except Exception as e:
        print(f"[warn] DB room upsert failed for {canonical_room_name!r}: {e}", file=sys.stderr)
        return _PersistDelta(room_id=None, inserted_messages=0, message_ids=[], attachment_ids=[])

    inserted = 0
    new_message_ids: list[int] = []
    attachment_ids: list[int] = []

    def _capture_png_bytes(rect_t: tuple[int, int, int, int]) -> bytes | None:
        try:
            left, top, right, bottom = rect_t
        except Exception:
            return None
        if right <= left or bottom <= top:
            return None
        try:
            img_bgr = persistor.grabber.grab_bgr(CaptureRect(left=left, top=top, right=right, bottom=bottom))
        except Exception:
            return None
        try:
            import cv2  # type: ignore

            ok, buf = cv2.imencode(".png", img_bgr)
            if not ok:
                return None
            return bytes(buf.tobytes())
        except Exception:
            return None

    def _encode_png_bytes(img_bgr) -> bytes | None:
        if img_bgr is None:
            return None
        try:
            import cv2  # type: ignore

            ok, buf = cv2.imencode(".png", img_bgr)
            if not ok:
                return None
            return bytes(buf.tobytes())
        except Exception:
            return None

    def _maybe_describe_image_and_save(
        *,
        message_id: int,
        attachment_id: int | None,
        png_bytes: bytes,
    ) -> None:
        if vision is None:
            return
        if not png_bytes:
            return
        try:
            if persistor.mysql.message_ai_exists(int(message_id)):
                return
        except Exception:
            # If we can't check, don't risk repeated calls.
            return
        base_prompt = (
            os.environ.get("VISION_IMAGE_PROMPT", "").strip()
            or (
                "explain about the image. if it contains some important text on the image especially shipment/courier tracking number, "
                "must always extract them. if the image contains a table of data, extract it into markdown format to preserve table structure"
            )
        )
        prompt = (
            base_prompt.strip()
            + "\n\nReturn ONLY valid JSON with this schema:\n"
            + "{\n"
            + '  "description": string,\n'
            + '  "extracted_text": string,\n'
            + '  "table_markdown": string,\n'
            + '  "tracking_numbers": [string]\n'
            + "}\n"
            + "Use empty string/[] if nothing is found. If there is a table, put it in table_markdown as markdown; otherwise use empty string. "
            + "Ensure tracking numbers are included when present."
        )
        try:
            import cv2  # type: ignore
            import numpy as np  # type: ignore

            arr = np.frombuffer(png_bytes, dtype=np.uint8)
            img_bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        except Exception:
            img_bgr = None
        if img_bgr is None:
            return
        try:
            result = vision.describe_image_json(img_bgr=img_bgr, prompt=prompt, max_tokens=900)
            result_s = json.dumps(result, ensure_ascii=False)
        except Exception as e:
            # Don't fail capture if AI is down. Keep logs small.
            try:
                print(f"[warn] vision failed message_id={int(message_id)}: {e}", file=sys.stderr)
            except Exception:
                pass
            return
        try:
            persistor.mysql.upsert_message_ai(
                message_id=int(message_id),
                attachment_id=int(attachment_id) if attachment_id else None,
                model=str(vision.cfg.model),
                prompt=prompt,
                result_json=result_s,
            )
        except Exception as e:
            try:
                print(f"[warn] DB save message_ai failed message_id={int(message_id)}: {e}", file=sys.stderr)
            except Exception:
                pass

    # Resolving sender names requires UI clicks (avatar -> profile popup). Keep it scoped to
    # a small tail window to reduce disruption and runtime cost.
    sender_resolve_tail = 8

    for idx, m in enumerate(messages):
        seen_before = False
        chosen_sig = m.signature
        if state_db is not None:
            try:
                legacy_sig = getattr(m, "legacy_signature", None)
                seen_before = bool(state_db.has(canonical_room_name, m.signature))
                is_image = bool(getattr(m, "is_image", False) or (getattr(m, "msg_type", "text") == "image"))
                if not is_image and not seen_before and legacy_sig and state_db.has(canonical_room_name, legacy_sig):
                    # Keep legacy fallback for non-image messages only.
                    # Image payloads are often normalized to the same placeholder text (e.g. "[Image]"),
                    # so legacy hashes can collide for distinct images.
                    seen_before = True
            except Exception:
                seen_before = False

        # Attachment hints (also needed for sender resolution).
        try:
            msg_type = getattr(m, "msg_type", "text") or "text"
            attach_name = getattr(m, "attachment_name", None)
            rect = getattr(m, "rect", None)
            wrapper = getattr(m, "wrapper", None)
        except Exception:
            msg_type = "text"
            attach_name = None
            rect = None
            wrapper = None

        sender_for_db = (getattr(m, "sender", "") or "").strip() or "(unknown)"
        resolved_sender: str | None = None

        try:
            mid = persistor.save_message(
                room_id=room_id,
                signature=chosen_sig,
                sender=sender_for_db,
                content=m.text or "",
                msg_type=getattr(m, "msg_type", "text") or ("image" if getattr(m, "is_image", False) else "text"),
                is_image=bool(getattr(m, "is_image", False)),
            )
        except Exception as e:
            print(f"[warn] DB message upsert failed for {canonical_room_name!r}: {e}", file=sys.stderr)
            continue

        # Resolve sender for the most recent visible messages when it's unknown.
        # Note: do this AFTER we have a message_id, and skip if the DB already has a real sender
        # to avoid repeated UI clicks on every scan.
        if (
            uia is not None
            and wrapper is not None
            and sender_for_db in {"", "(unknown)"}
            and idx >= max(0, len(messages) - int(sender_resolve_tail))
        ):
            need = True
            try:
                cur_sender = persistor.message_sender(int(mid))
                if cur_sender and cur_sender not in {"(unknown)", ""}:
                    need = False
            except Exception:
                need = True
            if need:
                try:
                    resolved_sender = uia.resolve_sender_from_message_item_avatar(wrapper, timeout_seconds=1.2)
                except Exception:
                    resolved_sender = None
                if resolved_sender:
                    try:
                        persistor.update_message_sender_if_unknown(
                            message_id=int(mid), sender=str(resolved_sender)
                        )
                    except Exception:
                        pass
        if not seen_before:
            inserted += 1
            new_message_ids.append(mid)

        if state_db is not None:
            try:
                sigs: list[str] = [chosen_sig]
                legacy_sig = getattr(m, "legacy_signature", None)
                if (
                    legacy_sig
                    and not getattr(m, "is_image", False)
                    and legacy_sig != chosen_sig
                ):
                    sigs.append(legacy_sig)
                state_db.add_many(canonical_room_name, sigs)
            except Exception:
                pass

        if msg_type == "file" and attach_name:
            try:
                p = persistor.wechat_files.find_doc_by_name(str(attach_name))
            except Exception:
                p = None
            if p is not None:
                aid = persistor.upload_and_record_file(
                    room_id=room_id,
                    message_id=mid,
                    path=p,
                    original_name=str(attach_name),
                )
                if aid:
                    attachment_ids.append(int(aid))

        if msg_type == "image" or bool(getattr(m, "is_image", False)):
            # Capture images by opening the viewer and taking a screen capture of the viewer window.
            # This avoids WeChat "Save" automation and is more reliable than full-screen capture.
            try:
                message_has_attachment = bool(persistor.message_has_attachment(int(mid)))
            except Exception:
                message_has_attachment = False

            if (not force_attachments) and seen_before:
                # If we already processed this message but attachment was never recorded,
                # still try to capture it now to recover missed images.
                if message_has_attachment:
                    continue

            before_att_count = len(attachment_ids)
            exported: Path | None = None
            png_bytes: bytes | None = None
            sha: str | None = None
            png_source_path: str | None = None

            # Preferred: open viewer and screenshot the viewer image area.
            if uia is not None and wrapper is not None:
                viewer = None
                try:
                    viewer = uia.open_image_viewer_from_message_item(
                        wrapper,
                        timeout_seconds=2.0,
                        maximize_window=bool(force_attachments),
                    )
                except Exception:
                    viewer = None

                if viewer is not None:
                    try:
                        # Give the viewer time to decode/render the full image.
                        time.sleep(2.0)
                        rect_v = uia.viewer_best_capture_rect(viewer)
                        # Prefer capturing the viewer window by HWND (PrintWindow) so other windows
                        # (including the WeChat main window) can't cover it in the screenshot.
                        hwnd = None
                        try:
                            hwnd = int(getattr(viewer, "handle", 0) or 0)
                        except Exception:
                            hwnd = None
                        if not hwnd:
                            try:
                                hwnd = int(getattr(getattr(viewer, "element_info", None), "handle", 0) or 0)
                            except Exception:
                                hwnd = None

                        img_bgr = None
                        if hwnd:
                            try:
                                img_bgr = persistor.grabber.grab_hwnd_bgr(hwnd)
                            except Exception:
                                img_bgr = None

                        if img_bgr is not None:
                            # Crop to the best image-area rect when we have it.
                            if rect_v:
                                try:
                                    import win32gui  # type: ignore

                                    wl, wt, wr, wb = win32gui.GetWindowRect(int(hwnd))
                                    x0 = max(0, int(rect_v[0] - wl))
                                    y0 = max(0, int(rect_v[1] - wt))
                                    x1 = min(int(img_bgr.shape[1]), int(rect_v[2] - wl))
                                    y1 = min(int(img_bgr.shape[0]), int(rect_v[3] - wt))
                                    if x1 > x0 and y1 > y0:
                                        img_bgr = img_bgr[y0:y1, x0:x1].copy()
                                except Exception:
                                    pass
                            png_bytes = _encode_png_bytes(img_bgr)
                        elif rect_v:
                            # Fallback: visible screen capture of the rect (requires viewer to be on top).
                            png_bytes = _capture_png_bytes(rect_v)

                        if png_bytes:
                            sha = sha256_bytes(png_bytes)
                            png_source_path = f"uia_capture://viewer/{sha}.png"
                    except Exception:
                        pass
                    try:
                        uia.close_image_viewer(viewer)
                    except Exception:
                        pass

            # Fallback: capture the bubble pixels and encode as PNG.
            if sha is None and rect:
                try:
                    png_bytes = _capture_png_bytes(rect)
                    if png_bytes:
                        sha = sha256_bytes(png_bytes)
                except Exception:
                    png_bytes = None
                    sha = None

            # If UIA list-item signatures collide, we can end up upserting an *older* message row here.
            # When we detect we're looking at a different image payload (new sha) while the message row
            # already has an attachment, create a synthetic message row keyed by sha so the new image
            # shows up in the DB instead of overwriting/losing it.
            target_mid = int(mid)
            if sha and seen_before and message_has_attachment:
                try:
                    att_exists = bool(persistor.mysql.attachment_exists(sha))
                except Exception:
                    att_exists = False
                if not att_exists:
                    try:
                        syn_sig = sha1_hex(f"{m.sender or '(unknown)'}\nimage\nsha256:{sha}\nsynthetic")
                        syn_seen = False
                        if state_db is not None:
                            try:
                                syn_seen = bool(state_db.has(canonical_room_name, syn_sig))
                            except Exception:
                                syn_seen = False
                        syn_mid = persistor.save_message(
                            room_id=room_id,
                            signature=syn_sig,
                            sender=m.sender or "(unknown)",
                            content=m.text or "[Image]",
                            msg_type="image",
                            is_image=True,
                        )
                        target_mid = int(syn_mid)
                        if not syn_seen:
                            inserted += 1
                            new_message_ids.append(int(syn_mid))
                        if state_db is not None:
                            try:
                                state_db.add_many(canonical_room_name, [syn_sig])
                            except Exception:
                                pass
                    except Exception:
                        target_mid = int(mid)

            if exported is not None:
                try:
                    ext = exported.suffix.lower().lstrip(".")
                    aid = persistor.upload_and_record_file(
                        room_id=room_id,
                        message_id=target_mid,
                        path=exported,
                        original_name=exported.name,
                        ext=ext or None,
                    )
                    if aid:
                        attachment_ids.append(int(aid))
                    continue
                except Exception:
                    pass

            if png_bytes:
                try:
                    base = f"{sanitize_filename(canonical_room_name)[:40]}_{chosen_sig[:10]}_{int(time.time())}"
                    aid = persistor.upload_and_record_image_png_bytes(
                        room_id=room_id,
                        message_id=target_mid,
                        data=png_bytes,
                        source_path=png_source_path,
                        original_name=f"{base}.png",
                    )
                    if aid:
                        attachment_ids.append(int(aid))
                        _maybe_describe_image_and_save(
                            message_id=int(target_mid),
                            attachment_id=int(aid),
                            png_bytes=png_bytes,
                        )
                except Exception:
                    pass

            if len(attachment_ids) == before_att_count:
                try:
                    print(
                        f"[warn] image save failed room={canonical_room_name!r} message_id={int(mid)}",
                        file=sys.stderr,
                    )
                except Exception:
                    pass

    # Best-effort repair: older runs could upload attachments (and even R2 keys) without linking
    # them back to message_id. Use our filename convention to backfill links.
    try:
        linked = int(
            persistor.mysql.backfill_unlinked_attachments_from_filename(
                room_id=int(room_id), limit=250
            )
        )
        if linked > 0:
            try:
                print(f"[info] backfilled_attachments room={canonical_room_name!r} linked={linked}")
            except Exception:
                pass
    except Exception:
        pass

    return _PersistDelta(
        room_id=int(room_id),
        inserted_messages=int(inserted),
        message_ids=new_message_ids,
        attachment_ids=attachment_ids,
    )

def _sweep_recent_attachments(persistor: Persistor, *, since_epoch: float) -> tuple[float, list[int]]:
    """
    LINE build: no local file-tree sweeper yet.

    We still capture media from visible message bubbles during per-chat persistence.
    """
    return time.time(), []


def _vision_from_env(*, verbose: bool = False) -> VisionClient | None:
    """
    Return a VisionClient if configured, else None.

    Required env vars:
    - VISION_BASE_URL
    - VISION_MODEL
    - VISION_API_KEY
    """
    base_url = (os.environ.get("VISION_BASE_URL") or "").strip()
    model = (os.environ.get("VISION_MODEL") or "").strip()
    api_key = (os.environ.get("VISION_API_KEY") or "").strip()
    if not base_url and not model and not api_key:
        return None
    if not base_url or not model or not api_key:
        if verbose:
            try:
                print(
                    "[warn] vision disabled: set VISION_BASE_URL, VISION_MODEL, VISION_API_KEY in .env",
                    file=sys.stderr,
                )
            except Exception:
                pass
        return None
    try:
        timeout_s = float((os.environ.get("VISION_TIMEOUT_SECONDS") or "45").strip() or "45")
    except Exception:
        timeout_s = 45.0
    cfg = VisionConfig(
        kind="openai_compatible",
        base_url=base_url,
        model=model,
        api_key_env="VISION_API_KEY",
        require_api_key=True,
        timeout_seconds=float(timeout_s),
    )
    return VisionClient(cfg)


def _canonical_chat_title(title: str) -> str:
    """
    WeChat group headers commonly show "GroupName(8)" (member count).
    Use a stable filename/key by stripping a trailing "(digits)" suffix.
    """
    t = (title or "").strip()
    # Strip invisible "format" characters that can break suffix matching.
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Cf").strip()
    # List rows often append metadata on a second line; keep the chat name only.
    parts = [p.strip() for p in t.splitlines() if p.strip()]
    if parts:
        t = parts[0]
    m = _TRAILING_COUNT_RE.match(t)
    if m:
        return (m.group(1) or "").strip() or t
    return t


def _parse_utc_offset(s: str) -> timezone:
    """
    Parse a UTC offset like '+07:00' into a datetime.timezone.
    """
    s = (s or "").strip()
    m = re.fullmatch(r"([+-])(\d{1,2})(?::?(\d{2}))?", s)
    if not m:
        raise ValueError(f"Invalid UTC offset: {s!r}")
    sign = 1 if m.group(1) == "+" else -1
    hh = int(m.group(2))
    mm = int(m.group(3) or "0")
    return timezone(sign * timedelta(hours=hh, minutes=mm))


def _parse_hhmm(s: str) -> tuple[int, int]:
    s = (s or "").strip()
    m = re.fullmatch(r"(\d{1,2})[:.](\d{2})", s)
    if not m:
        raise ValueError(f"Invalid time (HH:MM): {s!r}")
    return int(m.group(1)), int(m.group(2))


def _in_run_window(cfg: Config) -> tuple[bool, float]:
    """
    Return (in_window, sleep_seconds_until_next_window_if_outside).
    Window is evaluated in cfg.run_window_tz_offset.
    """
    tz = _parse_utc_offset(cfg.run_window_tz_offset)
    sh, sm = _parse_hhmm(cfg.run_window_start)
    eh, em = _parse_hhmm(cfg.run_window_end)

    now = datetime.now(timezone.utc).astimezone(tz)
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = now.replace(hour=eh, minute=em, second=0, microsecond=0)

    # Assumes a same-day window (e.g. 06:00-23:00).
    if start <= now < end:
        return True, 0.0
    if now < start:
        return False, max(1.0, (start - now).total_seconds())
    # now >= end
    start_next = start + timedelta(days=1)
    return False, max(1.0, (start_next - now).total_seconds())


def _next_poll_seconds(cfg: Config) -> float:
    lo = max(0.05, float(cfg.poll_interval_seconds_min))
    hi = max(lo, float(cfg.poll_interval_seconds_max))
    return random.uniform(lo, hi)


_IMAGE_PREVIEW_RE = re.compile(
    # Chat-list preview markers that suggest the latest unread message is an image.
    #
    # Note: keep this regex ASCII-only; use \\u escapes for CJK.
    r"(?i)(\[\s*(photo|image|picture|pic)\s*\]|\bphoto\b|\bimage\b|\bpicture\b|\bpic\b|\u56fe\u7247|\u7167\u7247)"
)


def _row_suggests_image_preview(s: str) -> bool:
    try:
        return bool(_IMAGE_PREVIEW_RE.search((s or "").strip()))
    except Exception:
        return False


def _scroll_message_list_to_bottom_best_effort(
    uia: LineUIA,
    *,
    max_steps: int = 40,
    stable_rounds: int = 3,
    sleep_seconds: float = 0.06,
) -> None:
    """
    Best-effort: wheel-scroll the message list down towards the newest messages.

    WeChat can open a chat at an older scroll position; UIA only sees visible rows,
    so we can otherwise miss the unread message that got marked read by activation.
    """
    try:
        msg_list = uia._find_message_list()
        r = msg_list.rectangle()
        w = max(1, int(r.right - r.left))
        h = max(1, int(r.bottom - r.top))
        coords = (max(5, w - 20), max(5, h - 20))
    except Exception:
        return
    try:
        msg_list.set_focus()
    except Exception:
        pass

    prev_last = ""
    stable = 0
    for _ in range(max(1, int(max_steps))):
        try:
            msgs = uia.extract_recent_messages(max_messages=30)
            last = msgs[-1].signature if msgs else ""
        except Exception:
            last = ""
        if last and last == prev_last:
            stable += 1
        else:
            stable = 0
        prev_last = last
        if stable >= max(1, int(stable_rounds)):
            break
        try:
            # Negative wheel_dist scrolls down (towards newer messages) in pywinauto.
            msg_list.wheel_mouse_input(wheel_dist=-12, coords=coords)
        except Exception:
            try:
                msg_list.type_keys("^{END}")
            except Exception:
                pass
        time.sleep(float(sleep_seconds))


def _open_and_verify(uia: LineUIA, ent, *, title_hint: str, previous_header: str) -> None:
    """
    Click a chat list row and verify the header switches to the expected chat.

    WeChat's UIA list items can be virtualized/reused; also clicks may be ignored if
    the window isn't focused yet. Verification prevents us from extracting the wrong chat.
    """
    hint = (title_hint or "").strip()
    # Best-effort: strip ellipsis from chat list title hints.
    if hint.endswith("..."):
        hint = hint[:-3].rstrip()
    hint_key = _normalize_chat_lookup(hint)
    prev_key = _normalize_chat_lookup(previous_header)

    # Retry a few times with refreshed entry when needed.
    prev_selected = ""
    try:
        prev_selected = (uia.get_selected_chat_row_text() or "").strip()
    except Exception:
        prev_selected = ""
    for _ in range(3):
        try:
            # If WeChat can't be brought to the foreground, avoid physical click_input
            # (it could click the wrong app). UIA Select/Invoke can still work.
            uia.open_chat(ent, allow_click=uia.is_foreground())
        except Exception:
            # One retry will happen below after refresh.
            pass

        # Verify using the chat header text.
        for _ in range(12):
            if not hint:
                return
            try:
                hdr = (uia.get_header_chat_name() or "").strip()
            except Exception:
                hdr = ""
            hdr_key = _normalize_chat_lookup(hdr)
            if hdr_key and (hdr_key == hint_key or hdr_key.startswith(hint_key) or hint_key.startswith(hdr_key)):
                return
            # Some WeChat builds don't expose a reliable header via UIA; fall back to the selected chat row.
            try:
                sel = (uia.get_selected_chat_row_text() or "").strip()
            except Exception:
                sel = ""
            sel_key = _normalize_chat_lookup(sel)
            if sel_key and (sel_key == hint_key or sel_key.startswith(hint_key) or hint_key.startswith(sel_key)):
                return
            if prev_selected and sel and sel != prev_selected:
                return
            # Fallback: accept any header change (UIA title strings can be noisy/garbled).
            if prev_key and hdr_key and hdr_key != prev_key:
                return
            time.sleep(0.12)

        # Refresh the chat list entry by scanning again and re-binding `ent` to a fresh wrapper.
        try:
            entries = uia.list_chat_list_entries(limit=60)
        except Exception:
            continue
        if hint:
            ent2 = next((e for e in entries if e.row_text.startswith(hint)), None)
            if ent2 is not None:
                ent = ent2
                continue
        if uia.single_window_mode and uia.is_foreground():
            try:
                # Some builds don't switch with single-click.
                ent.wrapper.double_click_input()
                time.sleep(0.08)
            except Exception:
                pass

    raise RuntimeError(f"Could not verify chat switch for hint={title_hint!r}")


def _extract_messages_with_retries(
    uia: LineUIA,
    *,
    max_messages: int,
    chat_title: str,
    attempts: int = 5,
    sleep_seconds: float = 0.22,
    verbose: bool = False,
    raise_on_error: bool = False,
) -> list:
    """
    Extract recent messages with short delays between attempts.

    This is intentionally tolerant: some WeChat builds need a few paint cycles
    after activating a chat before UIA exposes the latest rows.
    """
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            msgs = uia.extract_recent_messages(max_messages=max_messages)
        except Exception as e:
            last_err = e
            # WeChat UIA can intermittently go "empty" (no Lists found) until the main window is
            # minimized/restored. Try a best-effort refresh when this happens.
            try:
                if "List control" in str(e):
                    uia.refresh_surface(force=True)
            except Exception:
                pass
            if verbose:
                print(
                    f"[warn] message extract attempt {attempt}/{attempts} for {chat_title!r} failed: {e}",
                    file=sys.stderr,
                )
            if attempt < attempts:
                time.sleep(sleep_seconds * attempt)
            continue
        if msgs:
            if verbose and attempt > 1:
                print(
                    f"[info] message extract attempt {attempt}/{attempts} for {chat_title!r} succeeded with {len(msgs)} msgs",
                )
            return msgs
        if verbose:
            print(
                f"[warn] no messages extracted for {chat_title!r} on attempt {attempt}/{attempts}",
                file=sys.stderr,
            )
        if attempt < attempts:
            time.sleep(sleep_seconds * attempt)
    if last_err is not None:
        if raise_on_error:
            raise last_err
        return []
    if verbose:
        print(f"[warn] no messages extracted for {chat_title!r} after retries", file=sys.stderr)
    return []


def run_loop(cfg: Config, *, once: bool = False) -> int:
    return _run_loop_with_flags(cfg, once=once, scan_allowlisted=False, verbose=False)


def process_unread_once(cfg: Config, *, verbose: bool = True) -> int:
    """
    One-shot verification helper:
    - detect unread badges (UIA + local OCR)
    - open each unread chat
    - append the tail window to per-chat logs

    This is useful after the user cleans `logs/` and wants to verify the pipeline end-to-end.
    """
    _ensure_dirs(cfg)
    uia = LineUIA(
        title_re=cfg.line_window_title_regex,
        debug=cfg.debug,
        debug_dir=cfg.debug_dump_dir,
        single_window_mode=cfg.single_window_mode,
    )
    uia.connect()
    try:
        uia.window.set_focus()
    except Exception:
        pass

    badge_ocr: BadgeOCR | None = None
    if cfg.use_badge_ocr:
        tcmd = cfg.tesseract_cmd.strip() or None
        badge_ocr = BadgeOCR(
            BadgeOCRConfig(
                tesseract_cmd=tcmd,
                debug=bool(cfg.badge_ocr_debug),
                debug_dir=cfg.debug_dump_dir,
            )
        )

    persistor = Persistor.from_env()
    persistor.ensure_ready()
    vision = _vision_from_env(verbose=verbose)
    db = StateDB(cfg.state_db_path)
    try:
        prev_hdr = ""
        try:
            prev_hdr = uia.get_header_chat_name()
        except Exception:
            prev_hdr = ""

        entries = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
        unread_entries = []
        for ent in entries:
            unread = ent.unread_count
            if badge_ocr is not None:
                try:
                    r = ent.wrapper.rectangle()
                    n = badge_ocr.unread_count_for_row_rect(
                        left=int(r.left),
                        top=int(r.top),
                        right=int(r.right),
                        bottom=int(r.bottom),
                        debug_key="",
                    )
                    if n is not None:
                        unread = n
                except Exception:
                    pass
            if (unread or 0) > 0:
                unread_entries.append((ent, int(unread or 0)))

        if verbose:
            print(f"[info] unread_rooms={len(unread_entries)}")
            for ent, n in unread_entries:
                print(f"[info] unread title={ent.title_guess!r} count={n}")

        opened = 0
        for ent, _n in unread_entries:
            try:
                img_marker = time.time() - 1.0
                _open_and_verify(uia, ent, title_hint=ent.title_guess, previous_header=prev_hdr)
            except Exception as e:
                print(f"[warn] failed to open unread chat {ent.title_guess!r}: {e}", file=sys.stderr)
                continue
            # Let WeChat paint the message list before reading UIA.
            time.sleep(0.15)
            try:
                raw_title = (uia.get_header_chat_name() or "").strip() or ent.title_guess.strip() or "unknown_chat"
            except Exception:
                raw_title = ent.title_guess.strip() or "unknown_chat"
            chat_title = _canonical_chat_title(raw_title) or "unknown_chat"

            try:
                msgs = _extract_messages_with_retries(
                    uia,
                    max_messages=cfg.max_messages_to_scan,
                    chat_title=chat_title,
                    attempts=5,
                    sleep_seconds=0.2,
                    verbose=verbose,
                    raise_on_error=True,
                )
            except Exception as e:
                print(f"[warn] failed to extract messages for {chat_title!r}: {e}", file=sys.stderr)
                continue

            tail_n = max(1, int(cfg.activation_tail_messages))
            candidates = msgs[-tail_n:] if len(msgs) > tail_n else msgs

            delta = _persist_messages_to_db(
                persistor,
                uia=uia,
                vision=vision,
                image_export_dir=cfg.image_export_dir,
                raw_room_name=raw_title,
                canonical_room_name=chat_title,
                messages=candidates,
                state_db=db,
                image_since_epoch=img_marker,
            )
            if delta.inserted_messages:
                print(f"[ok] {chat_title}: +{delta.inserted_messages}")
            opened += 1

        if cfg.restore_previous_chat and prev_hdr:
            try:
                # Best-effort restore.
                entries2 = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
                for e in entries2:
                    if e.title_guess == prev_hdr:
                        uia.open_chat(e, allow_click=uia.is_foreground())  # type: ignore[arg-type]
                        break
            except Exception:
                pass

        if verbose:
            print(f"[info] opened={opened}")
    finally:
        db.close()
        persistor.close()

    return 0


def main(argv: list[str]) -> int:
    _force_utf8_stdio()
    load_env()
    _apply_linemon_env_defaults()
    ap = argparse.ArgumentParser(
        description="Visible (UI Automation) LINE capture -> MySQL (no injection/tampering)."
    )
    ap.add_argument(
        "--config",
        default="linemon_config.json",
        help="Path to config JSON (default: linemon_config.json)",
    )
    ap.add_argument("--once", action="store_true", help="Run a single scan cycle and exit")
    ap.add_argument(
        "--list-chats",
        action="store_true",
        help="List detected chats (name + unread heuristic) and exit (no logging).",
    )
    ap.add_argument(
        "--show-unread",
        action="store_true",
        help="List only chats detected as unread and exit (no logging).",
    )
    ap.add_argument(
        "--scan-allowlisted",
        action="store_true",
        help="Force-open allowlisted chats every cycle (debug; disruptive).",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-cycle stats (helps confirm the script is alive).",
    )
    ap.add_argument(
        "--process-unread-once",
        action="store_true",
        help="One-shot: detect unread badges and append the tail window from each unread room (verification helper).",
    )
    ap.add_argument(
        "--process-chat-once",
        default="",
        help="One-shot: open a specific chat by name and persist the tail window once (useful for backfilling attachments).",
    )
    ap.add_argument(
        "--attach-latest-export",
        action="store_true",
        help="With --process-chat-once: upload the newest file in image_export_dir and attach it to the newest image message in the tail window (manual high-res export assist).",
    )
    ap.add_argument(
        "--send-chat",
        default="",
        help="One-shot: open a specific chat by name and send a text message.",
    )
    ap.add_argument(
        "--send-text",
        default="",
        help="Message text used with --send-chat.",
    )
    args = ap.parse_args(argv)

    # Only enforce single-instance for the long-running monitor loop.
    # Allow one-shot helpers to run even when the monitor is already running.
    is_one_shot = bool(
        args.process_unread_once
        or args.process_chat_once
        or args.send_chat
        or args.send_text
        or args.list_chats
        or args.show_unread
    )
    if not is_one_shot:
        inst = acquire_mutex("linemon.line_capture")
        if not inst.ok:
            # Exit quietly; a primary instance is already running.
            return 0

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        # Create a starter config to reduce friction.
        sample = Path("linemon_config.sample.json")
        if sample.exists():
            cfg_path.write_text(sample.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"Wrote {cfg_path} from {sample}. Edit group_allowlist then re-run.", file=sys.stderr)
            return 2
        print(f"Missing config file: {cfg_path}", file=sys.stderr)
        return 2

    cfg = Config.from_path(cfg_path)

    if args.process_unread_once:
        return process_unread_once(cfg, verbose=True)

    if args.send_chat or args.send_text:
        chat = (args.send_chat or "").strip()
        msg_text = str(args.send_text or "")
        if not chat:
            print("Missing --send-chat value", file=sys.stderr)
            return 2
        if not msg_text.strip():
            print("Missing --send-text value", file=sys.stderr)
            return 2

        chat_key = _normalize_chat_lookup(chat)
        uia = LineUIA(
            title_re=cfg.line_window_title_regex,
            debug=cfg.debug,
            debug_dir=cfg.debug_dump_dir,
            single_window_mode=cfg.single_window_mode,
        )
        try:
            uia.connect()
            try:
                uia.main_window.set_focus()
                time.sleep(0.35)
            except Exception:
                pass
            entries = uia.list_chat_list_entries(limit=max(80, cfg.chat_list_limit))
        except Exception as e:
            print(f"Failed to connect/read chat list: {e}", file=sys.stderr)
            return 2

        ent = None
        if chat_key:
            ent = next((e for e in entries if _normalize_chat_lookup(e.title_guess) == chat_key), None)
            if ent is None:
                ent = next((e for e in entries if _normalize_chat_lookup(e.row_text) == chat_key), None)
            if ent is None:
                ent = next((e for e in entries if _normalize_chat_lookup(e.title_guess).startswith(chat_key)), None)
            if ent is None:
                ent = next((e for e in entries if _normalize_chat_lookup(e.row_text).startswith(chat_key)), None)
        if ent is None:
            print(f"Chat not found: {chat!r}", file=sys.stderr)
            return 2

        prev_hdr = ""
        try:
            prev_hdr = uia.get_header_chat_name() or ""
        except Exception:
            prev_hdr = ""
        try:
            _open_and_verify(uia, ent, title_hint=(ent.title_guess or chat), previous_header=prev_hdr)
        except Exception as e:
            print(f"Failed to open chat {chat!r}: {e}", file=sys.stderr)
            return 2

        sent_ok = False
        try:
            sent_ok = bool(uia.send_text_message(msg_text))
        except Exception as e:
            print(f"Failed to send message to {chat!r}: {e}", file=sys.stderr)
            return 1
        if not sent_ok:
            print(f"[warn] message send could not be confirmed for {chat!r}", file=sys.stderr)
            return 1

        try:
            raw_title = (uia.get_header_chat_name() or "").strip() or ent.title_guess.strip() or chat
        except Exception:
            raw_title = ent.title_guess.strip() or chat
        chat_title = _canonical_chat_title(raw_title) or chat
        try:
            msgs = _extract_messages_with_retries(
                uia,
                max_messages=50,
                chat_title=chat_title,
                attempts=4,
                sleep_seconds=0.20,
                verbose=True,
                raise_on_error=False,
            )
        except Exception:
            msgs = []
        needle = (msg_text or "").strip()
        verified = False
        if needle:
            for m in reversed(msgs[-20:]):
                body = (getattr(m, "text", "") or "").strip()
                if body and (body == needle or needle in body):
                    verified = True
                    break

        # Persist newest visible messages immediately so the sent message appears in DB now.
        persistor = Persistor.from_env()
        db = StateDB(cfg.state_db_path)
        try:
            persistor.ensure_ready()
            tail_n = max(10, int(cfg.activation_tail_messages))
            cands = msgs[-tail_n:] if len(msgs) > tail_n else msgs
            delta = _persist_messages_to_db(
                persistor,
                uia=uia,
                vision=_vision_from_env(verbose=False),
                image_export_dir=cfg.image_export_dir,
                raw_room_name=raw_title,
                canonical_room_name=chat_title,
                messages=cands,
                state_db=db,
                image_since_epoch=time.time() - 2.0,
                force_attachments=False,
            )
        except Exception:
            delta = _PersistDelta(room_id=None, inserted_messages=0, message_ids=[], attachment_ids=[])
        finally:
            try:
                db.close()
            except Exception:
                pass
            try:
                persistor.close()
            except Exception:
                pass

        print(
            f"[ok] sent chat={chat_title!r} verified={verified} persisted_new={delta.inserted_messages}"
        )
        return 0 if verified else 1

    if args.process_chat_once:
        chat = (args.process_chat_once or "").strip()
        if not chat:
            print("Missing --process-chat-once value", file=sys.stderr)
            return 2

        chat_key = _normalize_chat_lookup(chat)

        uia = LineUIA(
            title_re=cfg.line_window_title_regex,
            debug=cfg.debug,
            debug_dir=cfg.debug_dump_dir,
            single_window_mode=cfg.single_window_mode,
        )
        ent = None
        try:
            uia.connect()
            try:
                uia.main_window.set_focus()
                time.sleep(0.6)
            except Exception:
                pass
            entries = uia.list_chat_list_entries(limit=max(60, cfg.chat_list_limit))
            if chat_key:
                ent = next((e for e in entries if _normalize_chat_lookup(e.title_guess) == chat_key), None)
                if ent is None:
                    ent = next((e for e in entries if _normalize_chat_lookup(e.row_text) == chat_key), None)
                if ent is None:
                    ent = next((e for e in entries if _normalize_chat_lookup(e.title_guess).startswith(chat_key)), None)
                if ent is None:
                    ent = next((e for e in entries if _normalize_chat_lookup(e.row_text).startswith(chat_key)), None)
        except Exception as e:
            # If the main chat list isn't accessible (some builds use separate chat windows or restrict UIA),
            # fall back to attaching an already-open chat window by title.
            try:
                uia.attach_chat_window(title_hint=chat, timeout_seconds=1.0)
            except Exception:
                pass
            if uia._chat_window is None:
                print(f"Failed to connect/read chat list: {e}", file=sys.stderr)
                print(f"[hint] Open the chat window '{chat}' in WeChat, then re-run --process-chat-once.", file=sys.stderr)
                return 2

        prev_hdr = ""
        try:
            prev_hdr = uia.get_header_chat_name() or ""
        except Exception:
            prev_hdr = ""

        persistor = Persistor.from_env()
        try:
            persistor.ensure_ready()
        except Exception as e:
            print(f"Failed to connect/init DB storage: {e}", file=sys.stderr)
            return 2
        vision = _vision_from_env(verbose=True)
        db = StateDB(cfg.state_db_path)
        try:
            img_marker = time.time() - 1.0
            raw_title = chat
            if ent is not None:
                title_hint = ent.title_guess or chat
                _open_and_verify(uia, ent, title_hint=title_hint, previous_header=prev_hdr)
                time.sleep(0.2)
                try:
                    raw_title = (uia.get_header_chat_name() or "").strip() or ent.title_guess.strip() or chat
                except Exception:
                    raw_title = ent.title_guess.strip() or chat
            else:
                # Using an already-open chat window.
                try:
                    raw_title = (uia.get_header_chat_name() or "").strip() or chat
                except Exception:
                    raw_title = chat
            chat_title = _canonical_chat_title(raw_title) or chat

            try:
                msgs = _extract_messages_with_retries(
                    uia,
                    max_messages=cfg.max_messages_to_scan,
                    chat_title=chat_title,
                    attempts=5,
                    sleep_seconds=0.2,
                    verbose=True,
                    raise_on_error=True,
                )
            except Exception as e:
                print(f"Failed to extract messages for {chat_title!r}: {e}", file=sys.stderr)
                return 2

            tail_n = max(1, int(cfg.activation_tail_messages))
            candidates = msgs[-tail_n:] if len(msgs) > tail_n else msgs
            delta = _persist_messages_to_db(
                persistor,
                uia=uia,
                vision=vision,
                image_export_dir=cfg.image_export_dir,
                raw_room_name=raw_title,
                canonical_room_name=chat_title,
                messages=candidates,
                state_db=db,
                image_since_epoch=img_marker,
                force_attachments=True,
            )

            if args.attach_latest_export:
                export_dir = (cfg.image_export_dir or "").strip()
                if delta.room_id and export_dir:
                    newest_img_mid = None
                    try:
                        newest_img_mid = persistor.latest_image_message_id(room_id=int(delta.room_id))
                    except Exception:
                        newest_img_mid = None
                else:
                    newest_img_mid = None

                if newest_img_mid and export_dir:
                    try:
                        pdir = Path(export_dir)
                        files = [p for p in pdir.iterdir() if p.is_file()]
                        # Prefer non-tool-generated files (user exports).
                        files = [
                            p
                            for p in files
                            if ".overlay." not in p.name.lower()
                            and ".viewer." not in p.name.lower()
                            and not p.name.lower().endswith(".tmp")
                        ]
                        files.sort(key=lambda p: getattr(p.stat(), "st_mtime", 0.0), reverse=True)
                        newest = files[0] if files else None
                    except Exception:
                        newest = None
                    if newest is None:
                        print("[warn] attach-latest-export: no files found in image_export_dir", file=sys.stderr)
                    else:
                        try:
                            aid2 = persistor.upload_and_record_file(
                                room_id=int(delta.room_id or 0),
                                message_id=int(newest_img_mid),
                                path=newest,
                                original_name=newest.name,
                            )
                            if aid2:
                                try:
                                    persistor.clear_other_attachments_for_message(
                                        message_id=int(newest_img_mid), keep_attachment_id=int(aid2)
                                    )
                                except Exception:
                                    pass
                                print(f"[ok] attached_export message_id={int(newest_img_mid)} file={newest.name}")
                        except Exception as e:
                            print(f"[warn] attach-latest-export failed: {e}", file=sys.stderr)

            print(f"[ok] {chat_title}: +{delta.inserted_messages} attachments={len(delta.attachment_ids)}")
            return 0
        finally:
            try:
                db.close()
            except Exception:
                pass
            try:
                persistor.close()
            except Exception:
                pass

    # One-shot listing helpers (useful when allowlist doesn't match exact UI names).
    if args.list_chats or args.show_unread:
        uia = LineUIA(
            title_re=cfg.line_window_title_regex,
            debug=cfg.debug,
            debug_dir=cfg.debug_dump_dir,
            single_window_mode=cfg.single_window_mode,
        )
        try:
            uia.connect()
            chats = uia.list_chats()
        except Exception as e:
            print(f"Failed to connect/list chats: {e}", file=sys.stderr)
            return 2

        for row in chats:
            if args.show_unread and not row.unread:
                continue
            suffix = ""
            if row.unread_count is not None:
                suffix = f" (unread={row.unread_count})"
            elif row.unread:
                suffix = " (unread=?)"
            print(f"{row.name}{suffix}")
        return 0

    try:
        # Run capture loop.
        return _run_loop_with_flags(
            cfg,
            once=args.once,
            scan_allowlisted=args.scan_allowlisted,
            verbose=args.verbose,
        )
    finally:
        release_mutex()


@dataclass
class _RowState:
    row_text: str
    unread_count: int | None
    index: int
    time_label: str


@dataclass(frozen=True)
class _Queued:
    key: str
    title_hint: str
    row_text: str
    unread_count: int | None
    time_label: str


@dataclass
class _WaitState:
    banner_visible: bool = False
    last_notice_at: float = 0.0


def _wait_for_wechat_ready(
    uia: LineUIA,
    *,
    once: bool,
    verbose: bool,
    banner: LoginBanner | None,
    state: _WaitState,
    sleep_seconds: float = 5.0,
) -> bool:
    """
    Block until WeChat is running and logged in.

    While waiting, show a top banner and keep logs throttled.
    """
    while True:
        ready = False
        reason = "not_running"
        try:
            uia.connect()
            if uia.is_logged_in():
                ready = True
            else:
                reason = "not_logged_in"
        except Exception:
            ready = False

        if ready:
            if state.banner_visible:
                if verbose:
                    print("[info] LINE login detected; resuming monitor.")
                if banner is not None:
                    try:
                        banner.hide()
                    except Exception:
                        pass
                state.banner_visible = False
            return True

        now = time.time()
        if (not state.banner_visible) or ((now - state.last_notice_at) >= 20.0):
            # Keep the text exact as requested.
            print("Login your LINE")
            if verbose:
                print(f"[info] waiting_for_line reason={reason}", file=sys.stderr)
            state.last_notice_at = now

        if banner is not None:
            try:
                banner.show("Login your LINE")
            except Exception:
                pass
        state.banner_visible = True

        if once:
            return False
        time.sleep(max(1.0, float(sleep_seconds)))


def _run_loop_with_flags(cfg: Config, *, once: bool, scan_allowlisted: bool, verbose: bool) -> int:
    _ensure_dirs(cfg)

    allow = set(cfg.group_allowlist)
    capture_all = not allow

    uia = LineUIA(
        title_re=cfg.line_window_title_regex,
        debug=cfg.debug,
        debug_dir=cfg.debug_dump_dir,
        single_window_mode=cfg.single_window_mode,
    )
    banner = LoginBanner(text="Login your LINE")
    wait_state = _WaitState()
    if not _wait_for_wechat_ready(
        uia,
        once=once,
        verbose=verbose,
        banner=banner,
        state=wait_state,
        sleep_seconds=5.0,
    ):
        try:
            banner.close()
        except Exception:
            pass
        return 2

    if cfg.debug:
        try:
            uia.dump_list_inventory(filename="list_inventory_startup.txt")
        except Exception:
            pass

    persistor = Persistor.from_env()
    try:
        persistor.ensure_ready()
    except Exception as e:
        print(f"Failed to connect/init DB storage: {e}", file=sys.stderr)
        try:
            banner.close()
        except Exception:
            pass
        return 2
    vision = _vision_from_env(verbose=verbose)

    db = StateDB(cfg.state_db_path)
    notifier = None
    try:
        notifier = Notifier.from_env(state_path=str(cfg.state_db_path))
    except Exception as e:
        if verbose:
            print(f"[warn] notify init failed: {e}", file=sys.stderr)
        notifier = None
    if verbose:
        if notifier is None:
            print("[info] notify=off (set NOTIFY_URL + NOTIFY_TOKEN in .env to enable)")
        else:
            # Don't print the token.
            print(f"[info] notify=on url={notifier.cfg.url!r} source={notifier.cfg.source!r}")
    last_attach_sweep = time.time()
    last_verbose = 0.0
    start = time.time()
    last_fg_warn_at = 0.0
    activated_keys: set[str] = set()
    activated_titles: set[str] = set()
    key_to_title: dict[str, str] = {}
    active_last_sig: dict[str, str] = {}
    row_state: dict[str, _RowState] = {}
    pending: dict[str, tuple[str, int]] = {}  # key -> (row_text, polls_seen)
    queue: deque[_Queued] = deque()
    in_queue: set[str] = set()
    last_opened_at: dict[str, float] = {}  # row key -> timestamp

    badge_ocr: BadgeOCR | None = None
    if cfg.use_badge_ocr:
        tcmd = cfg.tesseract_cmd.strip() or None
        badge_ocr = BadgeOCR(
            BadgeOCRConfig(
                tesseract_cmd=tcmd,
                debug=bool(cfg.badge_ocr_debug),
                debug_dir=cfg.debug_dump_dir,
            )
        )
    try:
        while True:
            cycle_room_ids: set[int] = set()
            cycle_message_ids: list[int] = []
            cycle_attachment_ids: list[int] = []
            cycle_changes: set[str] = set()
            if not _wait_for_wechat_ready(
                uia,
                once=once,
                verbose=verbose,
                banner=banner,
                state=wait_state,
                sleep_seconds=5.0,
            ):
                break

            # Run only within a configured time window (evaluated in a fixed timezone).
            # This avoids any UI automation outside the user's desired hours.
            try:
                in_win, sleep_s = _in_run_window(cfg)
            except Exception:
                in_win, sleep_s = True, 0.0
            if not in_win:
                if verbose:
                    tz = cfg.run_window_tz_offset
                    print(f"[info] outside window; sleeping {sleep_s:.0f}s (tz={tz} window={cfg.run_window_start}-{cfg.run_window_end})")
                if once:
                    break
                time.sleep(sleep_s)
                continue

            # Badge OCR is screen-based. Prefer WeChat foreground so captured pixels match
            # the UIA rectangles. If we can't bring it foreground, continue using UIA-only
            # heuristics (row_text/index changes) and avoid click_input when opening chats.
            fg_ok = False
            try:
                fg_ok = bool(uia.ensure_foreground(settle_seconds=1.0))
            except Exception:
                fg_ok = False
            if (not fg_ok) and verbose:
                # Throttle the warning so logs don't explode.
                if (time.time() - last_fg_warn_at) >= 60.0:
                    print("[warn] WeChat not foreground; badge OCR may fail (continuing with UIA-only detection)")
                    last_fg_warn_at = time.time()

            if cfg.idle_only:
                idle = get_idle_seconds()
                if idle < cfg.idle_seconds:
                    time.sleep(min(_next_poll_seconds(cfg), 1.0))
                    if once:
                        break
                    continue

            previous_chat = ""
            if cfg.restore_previous_chat:
                try:
                    previous_chat = uia.get_header_chat_name()
                except Exception:
                    previous_chat = ""

            try:
                entries = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
            except Exception as e:
                # WeChat's UI surface can change while running (main window closed, modal dialogs,
                # conversation pop-outs, etc.). Try a reconnect once so the monitor can recover
                # automatically when the main window reappears.
                if verbose:
                    print(f"[warn] failed to read chat list: {e} (reconnecting)", file=sys.stderr)
                try:
                    uia.connect()
                    entries = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
                except Exception as e2:
                    # If UIA goes stale/empty, a minimize/restore can bring the tree back.
                    try:
                        uia.refresh_surface(force=True)
                        uia.connect()
                        entries = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
                    except Exception as e3:
                        print(f"[warn] failed to read chat list: {e3}", file=sys.stderr)
                        _wait_for_wechat_ready(
                            uia,
                            once=once,
                            verbose=verbose,
                            banner=banner,
                            state=wait_state,
                            sleep_seconds=5.0,
                        )
                        time.sleep(_next_poll_seconds(cfg))
                        if once:
                            break
                        continue

            # Active-chat monitor: detect new messages in the currently opened room without switching.
            # We only start logging after the first observed change while this tool is running.
            try:
                active_title_raw = (uia.get_header_chat_name() or "").strip()
                if not active_title_raw:
                    try:
                        active_title_raw = (uia.get_selected_chat_row_text() or "").strip()
                    except Exception:
                        active_title_raw = ""
                active_title = _canonical_chat_title(active_title_raw)
            except Exception:
                active_title = ""
            if active_title:
                try:
                    msgs = uia.extract_recent_messages(max_messages=cfg.max_messages_to_scan)
                    last_sig = msgs[-1].signature if msgs else ""
                except Exception:
                    msgs = []
                    last_sig = ""
                prev_sig = active_last_sig.get(active_title)
                if prev_sig is None:
                    active_last_sig[active_title] = last_sig
                elif last_sig and last_sig != prev_sig:
                    # New message(s) observed in the active chat.
                    active_last_sig[active_title] = last_sig
                    # Append only new lines; on first activation, limit to tail.
                    candidates = msgs
                    if active_title not in activated_titles:
                        tail_n = max(1, int(cfg.activation_tail_messages))
                        candidates = msgs[-tail_n:] if len(msgs) > tail_n else msgs
                    # Persist to DB.
                    delta = _persist_messages_to_db(
                        persistor,
                        uia=uia,
                        vision=vision,
                        image_export_dir=cfg.image_export_dir,
                        raw_room_name=active_title_raw,
                        canonical_room_name=active_title,
                        messages=candidates,
                        state_db=db,
                        image_since_epoch=time.time() - 20.0,
                    )
                    if delta.inserted_messages:
                        activated_titles.add(active_title)
                        print(f"[ok] {active_title}: +{delta.inserted_messages}")
                    if delta.room_id and (delta.message_ids or delta.attachment_ids):
                        cycle_room_ids.add(int(delta.room_id))
                        if delta.message_ids:
                            cycle_changes.add("messages")
                            cycle_message_ids.extend(delta.message_ids)
                        if delta.attachment_ids:
                            cycle_changes.add("attachments")
                            cycle_attachment_ids.extend(delta.attachment_ids)

            # Detect "new activity" from the chat list only. We open a chat only when needed.
            seen = 0
            for ent in entries:
                if not capture_all:
                    if (
                        _match_allowlist(ent.title_guess, allow) is None
                        and _match_allowlist(ent.row_text, allow) is None
                    ):
                        continue
                seen += 1

                # If UIA doesn't expose unread count, attempt local badge OCR.
                unread_count = ent.unread_count
                used_badge_ocr = False
                if unread_count is None and badge_ocr is not None:
                    try:
                        r = ent.wrapper.rectangle()
                        unread_count = badge_ocr.unread_count_for_row_rect(
                            left=int(r.left),
                            top=int(r.top),
                            right=int(r.right),
                            bottom=int(r.bottom),
                            debug_key=sanitize_filename(ent.title_guess)[:40],
                        )
                        used_badge_ocr = True
                    except Exception:
                        unread_count = None

                prev = row_state.get(ent.key)
                # If OCR was used, treat "no badge found" as 0 to reduce flapping between None/1.
                if used_badge_ocr and unread_count is None:
                    unread_count = 0
                cur = _RowState(
                    row_text=ent.row_text,
                    unread_count=unread_count,
                    index=ent.index,
                    time_label=ent.time_label,
                )
                if prev is None:
                    # First time we've seen this row key in this run.
                    # If it is already unread and the user wants to process startup unreads,
                    # queue it once (we will still only log a tail window).
                    row_state[ent.key] = cur
                    if cfg.process_unreads_on_startup and (unread_count or 0) > 0:
                        # Reuse the standard pending/debounce path so startup behavior matches
                        # non-startup triggers.
                        pending[ent.key] = (ent.row_text, 1)
                        if pending[ent.key][1] >= max(1, cfg.debounce_polls):
                            if ent.key not in in_queue:
                                if verbose:
                                    print(
                                        f"[info] trigger=startup_unread title={ent.title_guess!r} unread={unread_count} idx={ent.index}"
                                    )
                                queue.append(
                                    _Queued(
                                        key=ent.key,
                                        title_hint=ent.title_guess,
                                        row_text=ent.row_text,
                                        unread_count=unread_count,
                                        time_label=ent.time_label,
                                    )
                                )
                                in_queue.add(ent.key)
                    continue

                # Trigger opening a chat only when we have evidence of new activity:
                # - unread badge appears (0 -> >0)
                # - or chat preview/time changes while it remains unread (multiple messages within same minute)
                # - or chat moves up the list while unread (list reordering is a strong signal in WeChat)
                triggered = False
                trigger_reason = ""
                badge_now = (unread_count or 0) > 0
                badge_prev = (prev.unread_count or 0) > 0
                if (unread_count is not None) and (prev.unread_count is not None):
                    if unread_count > prev.unread_count:
                        triggered = True
                        trigger_reason = "count_increased"
                if badge_now and not badge_prev:
                    triggered = True
                    trigger_reason = trigger_reason or "badge_appeared"
                elif badge_now:
                    if cur.row_text != prev.row_text:
                        triggered = True
                        trigger_reason = trigger_reason or "row_changed"
                    elif cur.index < prev.index:
                        triggered = True
                        trigger_reason = trigger_reason or "moved_up"
                else:
                    # When unread badge signals aren't available (UIA doesn't expose it / OCR can't be used),
                    # fall back to WeChat's list behavior: new activity updates preview/time and often moves
                    # the conversation to the top.
                    if cur.row_text and prev.row_text and cur.row_text != prev.row_text:
                        triggered = True
                        trigger_reason = trigger_reason or "row_changed_no_badge"
                    elif cur.index < prev.index:
                        triggered = True
                        trigger_reason = trigger_reason or "moved_up_no_badge"
                if (not triggered) and unread_count is None and ent.index <= 1:
                    # If the list metadata is too sparse (e.g. no unread/count row text),
                    # periodically sample the top rows so we still handle rows that receive
                    # messages while keeping the same preview signature.
                    rescan_interval = float(cfg.unread_rescan_seconds)
                    if rescan_interval <= 0:
                        rescan_interval = 60.0
                    else:
                        # Keep fallback rescan bounded so new messages aren't deferred too long.
                        rescan_interval = min(rescan_interval, 60.0)
                    last_t = last_opened_at.get(ent.key, 0.0)
                    if not last_t or (time.time() - last_t) >= rescan_interval:
                        triggered = True
                        trigger_reason = "top_row_no_unread_rescan"

                if scan_allowlisted:
                    triggered = True
                    trigger_reason = "scan_mode"

                # Do not auto-open unreads present at startup; we only open when we observe a change.
                # If configured, we also open rows that remain unread but were never processed in this run.
                if (
                    (not triggered)
                    and cfg.process_unreads_on_startup
                    and badge_now
                    and (ent.key not in activated_keys)
                ):
                    triggered = True
                    trigger_reason = "unread_unprocessed"
                # If a room stays unread for a long time, re-open it periodically to avoid
                # missing messages when OCR/count signals are noisy.
                if (not triggered) and badge_now and cfg.unread_rescan_seconds > 0:
                    last_t = last_opened_at.get(ent.key, 0.0)
                    if last_t and (time.time() - last_t) >= cfg.unread_rescan_seconds:
                        triggered = True
                        trigger_reason = "unread_rescan"

                if triggered:
                    p = pending.get(ent.key)
                    if p is None or p[0] != ent.row_text:
                        pending[ent.key] = (ent.row_text, 1)
                    else:
                        pending[ent.key] = (p[0], p[1] + 1)
                    if pending[ent.key][1] >= max(1, cfg.debounce_polls):
                        if ent.key not in in_queue:
                            if verbose:
                                print(
                                    f"[info] trigger={trigger_reason} title={ent.title_guess!r} unread={unread_count} idx={ent.index}"
                                )
                            queue.append(
                                _Queued(
                                    key=ent.key,
                                    title_hint=ent.title_guess,
                                    row_text=ent.row_text,
                                    unread_count=unread_count,
                                    time_label=ent.time_label,
                                )
                            )
                            in_queue.add(ent.key)
                else:
                    pending.pop(ent.key, None)

                row_state[ent.key] = cur

            opened = 0
            while queue and opened < cfg.max_chats_per_cycle:
                q = queue.popleft()
                key = q.key
                in_queue.discard(key)

                ent = next((e for e in entries if e.key == key), None)
                if ent is None:
                    try:
                        entries = uia.list_chat_list_entries(limit=cfg.chat_list_limit)
                    except Exception:
                        continue
                    ent = next((e for e in entries if e.key == key), None)
                if ent is None:
                    # Fallback matching: some builds reuse UIA elements; match by last seen row_text/title hint.
                    ent = next((e for e in entries if e.row_text == q.row_text), None)
                if ent is None and q.title_hint:
                    ent = next((e for e in entries if e.row_text.startswith(q.title_hint)), None)
                if ent is None:
                    continue

                # Optional throttling delay (reduces UI "thrash").
                lo = max(0.0, cfg.open_delay_seconds_min)
                hi = max(lo, cfg.open_delay_seconds_max)
                if hi > 0:
                    time.sleep(random.uniform(lo, hi))

                img_marker = time.time() - 1.0
                try:
                    _open_and_verify(
                        uia,
                        ent,
                        title_hint=q.title_hint or ent.title_guess,
                        previous_header=previous_chat,
                    )
                except Exception as e:
                    print(f"[warn] failed to open chat row {ent.title_guess!r}: {e}", file=sys.stderr)
                    continue

                try:
                    raw_title = (
                        uia.get_header_chat_name().strip()
                        or ent.title_guess.strip()
                        or key_to_title.get(key, "")
                        or "unknown_chat"
                    )
                    chat_title = _canonical_chat_title(raw_title) or "unknown_chat"
                except Exception:
                    raw_title = ent.title_guess.strip() or key_to_title.get(key, "") or "unknown_chat"
                    chat_title = _canonical_chat_title(raw_title) or "unknown_chat"
                key_to_title[key] = chat_title
                activated_titles.add(chat_title)

                now = time.time()
                last_t = last_opened_at.get(key, 0.0)
                if (now - last_t) < max(0.0, cfg.per_chat_cooldown_seconds):
                    opened += 1
                    continue
                last_opened_at[key] = now

                if cfg.debug:
                    try:
                        uia.dump_list_inventory(
                            filename=f"list_inventory_after_open_{sanitize_filename(chat_title)}.txt"
                        )
                    except Exception:
                        pass

                try:
                    msgs = _extract_messages_with_retries(
                        uia,
                        max_messages=cfg.max_messages_to_scan,
                        chat_title=chat_title,
                        attempts=5,
                        sleep_seconds=0.2,
                        verbose=verbose,
                        raise_on_error=False,
                    )
                except Exception as e:
                    print(f"[warn] failed to extract messages for {chat_title!r}: {e}", file=sys.stderr)
                    opened += 1
                    continue
                if not msgs:
                    if verbose:
                        print(f"[warn] no messages extracted for {chat_title!r} after retries", file=sys.stderr)

                # Start logging a chat only after it first receives activity while we're running.
                # On first activation, limit to a tail window to avoid dumping history.
                candidates = msgs
                if key not in activated_keys:
                    tail_n = max(1, int(cfg.activation_tail_messages))
                    candidates = msgs[-tail_n:] if len(msgs) > tail_n else msgs

                # If the chat-list preview suggests an image (e.g. "[Photo]"), force attachment capture.
                # Also force attachment capture for any unread-triggered open: image bubbles can have
                # unstable/virtualized UIA signatures, so we need the sha-based synthetic fallback.
                preview_is_image = _row_suggests_image_preview(q.row_text) or _row_suggests_image_preview(q.title_hint)
                has_unread = int(q.unread_count or 0) > 0
                force_att = bool(preview_is_image or has_unread)
                try:
                    if preview_is_image or has_unread:
                        # LINE extraction is vision-backed and each extraction call is expensive.
                        # The WeChat wheel-scroll heuristic can stall a cycle for a long time here,
                        # so skip forced scrolling and do a direct re-read instead.
                        msgs2 = _extract_messages_with_retries(
                            uia,
                            max_messages=cfg.max_messages_to_scan,
                            chat_title=chat_title,
                            attempts=2,
                            sleep_seconds=0.18,
                            verbose=verbose,
                            raise_on_error=False,
                        )
                        if msgs2:
                            msgs = msgs2
                            candidates = msgs
                            if key not in activated_keys:
                                tail_n = max(1, int(cfg.activation_tail_messages))
                                candidates = msgs[-tail_n:] if len(msgs) > tail_n else msgs
                except Exception:
                    pass

                delta = _persist_messages_to_db(
                    persistor,
                    uia=uia,
                    vision=vision,
                    image_export_dir=cfg.image_export_dir,
                    raw_room_name=raw_title,
                    canonical_room_name=chat_title,
                    messages=candidates,
                    state_db=db,
                    image_since_epoch=img_marker,
                    force_attachments=bool(force_att),
                )
                if delta.inserted_messages:
                    print(f"[ok] {chat_title}: +{delta.inserted_messages}")
                if key not in activated_keys:
                    activated_keys.add(key)
                if delta.room_id and (delta.message_ids or delta.attachment_ids):
                    cycle_room_ids.add(int(delta.room_id))
                    if delta.message_ids:
                        cycle_changes.add("messages")
                        cycle_message_ids.extend(delta.message_ids)
                    if delta.attachment_ids:
                        cycle_changes.add("attachments")
                        cycle_attachment_ids.extend(delta.attachment_ids)

                opened += 1

            if verbose:
                now = time.time()
                if now - last_verbose >= 5.0:
                    up = now - start
                    at = active_title if active_title else ""
                    ls = active_last_sig.get(active_title, "")[:10] if active_title else ""
                    print(
                        f"[info] up={up:.0f}s seen={seen} queued={len(queue)} opened={opened} scan_mode={scan_allowlisted} active={at!r} last={ls}"
                    )
                    last_verbose = now

            if cfg.restore_previous_chat and previous_chat:
                try:
                    pc = _canonical_chat_title(previous_chat).strip()
                    for ent in entries:
                        tg = _canonical_chat_title(ent.title_guess).strip()
                        if not tg:
                            continue
                        # title_guess can include extra preview text; allow prefix matching.
                        if tg == pc or tg.startswith(pc) or pc.startswith(tg.rstrip("...")):
                            uia.open_chat(ent, allow_click=uia.is_foreground())
                            break
                except Exception:
                    pass

            if once:
                break

            # Opportunistic attachment sweep (docs + decoded images) so we don't miss
            # files even if UIA parsing doesn't surface the file bubble cleanly.
            try:
                # Avoid sweeping too often; align with our randomized polling.
                if (time.time() - last_attach_sweep) >= max(10.0, float(cfg.poll_interval_seconds_min)):
                    last_attach_sweep, aids = _sweep_recent_attachments(
                        persistor,
                        since_epoch=last_attach_sweep,
                    )
                    if aids:
                        cycle_changes.add("attachments")
                        cycle_attachment_ids.extend(aids)
            except Exception:
                pass

            # Notify the frontend (best-effort). This is a hint; UI re-queries DB.
            if notifier is not None and cycle_changes:
                try:
                    ok = notifier.notify_delta(
                        changes=sorted(cycle_changes),
                        room_ids=sorted(cycle_room_ids),
                        message_ids=cycle_message_ids,
                        attachment_ids=cycle_attachment_ids,
                    )
                    if verbose:
                        meta = ""
                        try:
                            rid = notifier.last_response_json.get("request_id") if notifier.last_response_json else None
                            delivered = notifier.last_response_json.get("delivered") if notifier.last_response_json else None
                            dup = notifier.last_response_json.get("duplicate") if notifier.last_response_json else None
                            ooo = notifier.last_response_json.get("out_of_order") if notifier.last_response_json else None
                            meta = f" request_id={rid!r} delivered={delivered!r} duplicate={dup!r} out_of_order={ooo!r}"
                        except Exception:
                            meta = ""
                        print(
                            f"[info] notify ok={ok} seq={notifier.last_seq} status={notifier.last_status_code} changes={sorted(cycle_changes)} rooms={len(cycle_room_ids)} msgs={len(cycle_message_ids)} atts={len(cycle_attachment_ids)}{meta}"
                        )
                    if (not ok) and verbose:
                        err = notifier.last_error
                        if err:
                            print(f"[warn] notify failed: {err}", file=sys.stderr)
                        else:
                            print("[warn] notify failed", file=sys.stderr)
                except Exception:
                    pass
            time.sleep(_next_poll_seconds(cfg))
    finally:
        db.close()
        if notifier is not None:
            try:
                notifier.close()
            except Exception:
                pass
        try:
            banner.close()
        except Exception:
            pass
        persistor.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
