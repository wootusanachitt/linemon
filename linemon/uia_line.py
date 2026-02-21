from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from linemon.screen_capture import Rect, ScreenGrabber
from linemon.uia_wechat import (
    ChatListEntry,
    ChatRow,
    ExtractedMessage,
    WeChatUIA,
    sha1_hex,
)
from linemon.vision_client import VisionClient, VisionConfig


def _now() -> float:
    return time.time()


def _norm(s: str) -> str:
    return (s or "").replace("\r", "").replace("\n", " ").strip()


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


_TRAILING_BADGE_RE = re.compile(r"\s*[\(\[]\s*\d[\d,]*\s*[\)\]]\s*$")


def _canonical_room_title(title: str) -> str:
    t = _norm(title)
    if not t:
        return ""
    # Strip trailing unread/count suffixes such as "Room Name (1,847)".
    t = _TRAILING_BADGE_RE.sub("", t).strip()
    return t


def _is_unknown_sender(sender: str) -> bool:
    s = _norm(sender).lower()
    if not s:
        return True
    return s in {"(unknown)", "unknown", "n/a", "na", "none", "null", "-", "?"}


def _image_hash(img_bgr: np.ndarray) -> str:
    # Downsample before hashing so tiny render noise does not churn cache keys.
    try:
        import cv2  # type: ignore

        h, w = img_bgr.shape[:2]
        sw = 240
        sh = max(40, int(h * (sw / max(1, w))))
        small = cv2.resize(img_bgr, (sw, sh), interpolation=cv2.INTER_AREA)
        return hashlib.sha1(small.tobytes()).hexdigest()
    except Exception:
        return hashlib.sha1(img_bgr.tobytes()).hexdigest()


@dataclass(frozen=True)
class _VisionChatRow:
    row_index: int
    chat_title: str
    unread_count: int
    preview: str
    time_label: str


@dataclass(frozen=True)
class _VisionMessage:
    row_index: int
    sender: str
    text: str
    is_image: bool
    msg_type: str
    attachment_name: str | None
    direction: str  # "incoming" | "outgoing" | "unknown"


def _load_default_vision_client(timeout_seconds: float = 35.0) -> VisionClient:
    base_url = (os.environ.get("VISION_BASE_URL") or "").strip()
    model = (os.environ.get("VISION_MODEL") or "").strip()
    if not base_url or not model:
        raise RuntimeError("Missing VISION_BASE_URL/VISION_MODEL for LINE monitor vision parsing.")
    cfg = VisionConfig(
        kind="openai_compatible",
        base_url=base_url,
        model=model,
        api_key_env="VISION_API_KEY",
        require_api_key=True,
        timeout_seconds=float(timeout_seconds),
    )
    return VisionClient(cfg)


def _prompt_chat_list() -> str:
    return (
        "You are reading a screenshot of the LINE desktop chat list panel. "
        "Return ONLY a JSON array, ordered from top to bottom.\n"
        "Each object must have: "
        "row_index (int, 0 = top visible row), "
        "chat_title (string), "
        "unread_count (int, 0 when no unread badge), "
        "preview (string, empty allowed), "
        "time_label (string, empty allowed).\n"
        "Rules: include only real chat rows; ignore sidebars/buttons/search box. "
        "If badge exists but number is unclear, use unread_count=1. "
        "No markdown, no explanation, JSON only."
    )


def _prompt_messages() -> str:
    return (
        "You are reading a screenshot of the LINE desktop message list area for one conversation. "
        "Return ONLY a JSON array, ordered from top to bottom.\n"
        "Each object must have: row_index (int), sender (string), text (string), is_image (boolean), "
        "msg_type (one of: text,image,file,video,sticker,audio,system), attachment_name (string or empty), "
        "direction (incoming|outgoing|unknown), is_outgoing (boolean).\n"
        "Rules:\n"
        "- Include only chat message rows (no compose box/sidebar).\n"
        "- For image/sticker/video rows, set is_image=true. Use text='[Image]' if no readable caption.\n"
        "- For file rows, set msg_type='file' and put the filename into attachment_name when visible.\n"
        "- In 1:1 chats LINE may hide sender labels; use bubble side to infer direction and set sender empty if unknown.\n"
        "- Ignore system separators/date headers as much as possible; if unavoidable set msg_type='system'.\n"
        "No markdown, no explanation, JSON only."
    )


class LineUIA(WeChatUIA):
    """
    LINE desktop UI adapter.

    LINE's Qt accessibility tree usually exposes row geometry but not reliable text,
    so this class combines UIA rectangles with vision parsing for row content.
    """

    def __init__(
        self,
        *,
        title_re: str,
        debug: bool = False,
        debug_dir: str | Path = "debug",
        single_window_mode: bool = True,
        vision_client: VisionClient | None = None,
        vision_timeout_seconds: float = 35.0,
    ) -> None:
        super().__init__(
            title_re=title_re,
            debug=debug,
            debug_dir=debug_dir,
            single_window_mode=single_window_mode,
        )
        self.grabber = ScreenGrabber()
        self.vision = vision_client or _load_default_vision_client(timeout_seconds=vision_timeout_seconds)
        self._active_chat_title: str = ""
        self._chat_cache_hash: str = ""
        self._chat_cache_at: float = 0.0
        self._chat_cache_rows: list[ChatListEntry] = []
        self._msg_cache_hash: str = ""
        self._msg_cache_at: float = 0.0
        self._msg_cache_rows: list[ExtractedMessage] = []
        self._last_vision_warn_at: float = 0.0

    def _warn_vision(self, label: str, err: Exception) -> None:
        now = _now()
        if (now - self._last_vision_warn_at) < 8.0:
            return
        self._last_vision_warn_at = now
        try:
            print(f"[warn] LINE vision {label} failed: {err}", file=sys.stderr)
        except Exception:
            pass

    def connect(self):
        if sys.platform != "win32":
            raise RuntimeError("LINE monitor must run with Windows Python (UI Automation required).")

        from linemon.dpi import make_dpi_aware

        make_dpi_aware()

        from pywinauto import Desktop  # type: ignore

        desk = Desktop(backend="uia")
        wins = []
        try:
            wins = desk.windows(control_type="Window", top_level_only=True, visible_only=False)
        except Exception:
            wins = []

        pat = re.compile(self.title_re, re.IGNORECASE)
        best = None
        best_score = None
        for w in wins:
            try:
                wo = w.wrapper_object()
            except Exception:
                wo = w
            try:
                title = (wo.element_info.name or "").strip()
            except Exception:
                title = ""
            try:
                cls = (wo.element_info.class_name or "").strip()
            except Exception:
                cls = ""
            title_low = title.lower()

            match = False
            base_score = 0
            if cls == "AllInOneWindow":
                match = True
                base_score = 300
            elif title and pat.search(title):
                match = True
                base_score = 220
            elif "line" in title_low:
                match = True
                base_score = 120
            if not match:
                continue

            try:
                r = wo.rectangle()
                area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
            except Exception:
                area = 0

            # Prefer windows that actually contain list controls.
            has_list = 0
            try:
                lists = wo.descendants(control_type="List")
                has_list = 1 if lists else 0
            except Exception:
                has_list = 0

            score = (base_score + (50 * has_list), area)
            if best is None or (best_score is not None and score > best_score):
                best = wo
                best_score = score

        if best is None:
            raise RuntimeError(f"Could not find LINE window. title_re={self.title_re!r}")

        self._main_window = best
        return self._main_window

    @property
    def active_window(self):
        # LINE is single-window in this adapter.
        return self.main_window

    def has_session_list(self) -> bool:
        try:
            _ = self.find_chat_list()
            return True
        except Exception:
            return False

    def is_logged_in(self) -> bool:
        return self.has_session_list()

    def attach_chat_window(self, *, title_hint: str | None = None, timeout_seconds: float = 1.2):
        # LINE monitor runs in single-window mode.
        self._chat_window = None
        return None

    def _line_lists(self) -> list[object]:
        win = self.main_window
        try:
            lists = list(win.descendants(control_type="List"))
        except Exception:
            lists = []
        out: list[object] = []
        for lst in lists:
            try:
                r = lst.rectangle()
                if int(r.right - r.left) <= 40 or int(r.bottom - r.top) <= 60:
                    continue
            except Exception:
                continue
            out.append(lst)
        return out

    def _sorted_items(self, lst) -> list[object]:
        try:
            items = list(lst.descendants(control_type="ListItem"))
        except Exception:
            items = []
        try:
            items.sort(key=lambda it: int((it.rectangle().top + it.rectangle().bottom) // 2))
        except Exception:
            pass
        return items

    def _selected_chat_index(self) -> int | None:
        """
        Return selected chat-list row index from UIA SelectionItem state.
        """
        try:
            chat_list = self.find_chat_list()
            items = self._sorted_items(chat_list)
        except Exception:
            return None
        for idx, it in enumerate(items):
            try:
                if bool(it.iface_selection_item.CurrentIsSelected):  # type: ignore[attr-defined]
                    return idx
            except Exception:
                continue
        return None

    def find_chat_list(self):
        lists = self._line_lists()
        if not lists:
            raise RuntimeError("LINE chat list not found.")

        best = None
        best_score = None
        for lst in lists:
            try:
                r = lst.rectangle()
                left = int(r.left)
                h = max(1, int(r.bottom - r.top))
                n = len(self._sorted_items(lst))
            except Exception:
                continue
            # Chat list is usually the left list, with multiple rows.
            score = (-left, n * 1000 + h)
            if best is None or (best_score is not None and score > best_score):
                best = lst
                best_score = score

        if best is None:
            raise RuntimeError("LINE chat list selection failed.")
        return best

    def _find_message_list(self):
        lists = self._line_lists()
        if not lists:
            raise RuntimeError("LINE message list not found.")

        best = None
        best_score = None
        for lst in lists:
            try:
                r = lst.rectangle()
                left = int(r.left)
                h = max(1, int(r.bottom - r.top))
                n = len(self._sorted_items(lst))
            except Exception:
                continue
            # Message list is usually the right list.
            score = (left, n * 1000 + h)
            if best is None or (best_score is not None and score > best_score):
                best = lst
                best_score = score

        if best is None:
            raise RuntimeError("LINE message list selection failed.")
        return best

    def _capture_bgr(self, wrapper) -> np.ndarray:
        r = wrapper.rectangle()
        left = int(r.left)
        top = int(r.top)
        right = int(r.right)
        bottom = int(r.bottom)

        # Prefer HWND capture so we can read LINE even when another window overlaps it.
        hwnd = 0
        try:
            hwnd = int(getattr(self.main_window, "handle", 0) or 0)
        except Exception:
            hwnd = 0
        if hwnd:
            try:
                import win32gui  # type: ignore

                wl, wt, wr, wb = win32gui.GetWindowRect(int(hwnd))
                whole = self.grabber.grab_hwnd_bgr(int(hwnd))
                if whole is not None:
                    x0 = max(0, left - int(wl))
                    y0 = max(0, top - int(wt))
                    x1 = min(int(whole.shape[1]), right - int(wl))
                    y1 = min(int(whole.shape[0]), bottom - int(wt))
                    if x1 > x0 and y1 > y0:
                        return whole[y0:y1, x0:x1].copy()
            except Exception:
                pass

        rect = Rect(left=left, top=top, right=right, bottom=bottom)
        return self.grabber.grab_bgr(rect)

    def _vision_chat_rows(self, img_bgr: np.ndarray) -> list[_VisionChatRow]:
        try:
            obj = self.vision.describe_image_json(
                img_bgr=img_bgr,
                prompt=_prompt_chat_list(),
                max_tokens=1100,
            )
        except Exception as e:
            self._warn_vision("chat-list", e)
            return []

        if not isinstance(obj, list):
            return []

        out: list[_VisionChatRow] = []
        for i, it in enumerate(obj):
            if not isinstance(it, dict):
                continue
            title = _norm(str(it.get("chat_title", "") or it.get("title", "") or ""))
            if not title:
                continue
            row_index = _safe_int(it.get("row_index", i), i)
            unread = max(0, _safe_int(it.get("unread_count", it.get("unread", 0)), 0))
            preview = _norm(str(it.get("preview", "") or it.get("excerpt", "") or ""))
            time_label = _norm(str(it.get("time_label", "") or it.get("time", "") or ""))
            out.append(
                _VisionChatRow(
                    row_index=row_index,
                    chat_title=title,
                    unread_count=unread,
                    preview=preview,
                    time_label=time_label,
                )
            )
        out.sort(key=lambda x: x.row_index)
        return out

    def list_chat_list_entries(self, *, limit: int = 40) -> list[ChatListEntry]:
        try:
            self.ensure_foreground(settle_seconds=0.08)
        except Exception:
            pass
        chat_list = self.find_chat_list()
        items = self._sorted_items(chat_list)
        if limit and len(items) > limit:
            items = items[:limit]

        if not items:
            return []

        try:
            img = self._capture_bgr(chat_list)
            img_hash = _image_hash(img)
        except Exception:
            img = None
            img_hash = ""

        now = _now()
        if (
            img_hash
            and self._chat_cache_rows
            and self._chat_cache_hash == img_hash
            and (now - self._chat_cache_at) <= 1.5
        ):
            return self._chat_cache_rows[:limit] if limit else list(self._chat_cache_rows)

        parsed: list[_VisionChatRow] = []
        if img is not None:
            parsed = self._vision_chat_rows(img)

        out: list[ChatListEntry] = []
        used_idx: set[int] = set()

        # Primary mapping: vision row_index -> UIA item index.
        for row in parsed:
            idx = int(row.row_index)
            if idx < 0 or idx >= len(items):
                continue
            wrapper = items[idx]
            used_idx.add(idx)
            title = _canonical_room_title(row.chat_title) or f"chat_{idx + 1}"
            row_text = " ".join(x for x in [title, row.preview, row.time_label] if x).strip()
            key = f"title:{sha1_hex(title.lower())[:16]}"
            out.append(
                ChatListEntry(
                    key=key,
                    index=idx,
                    title_guess=title,
                    row_text=row_text or title,
                    unread_count=int(row.unread_count),
                    time_label=row.time_label,
                    wrapper=wrapper,
                )
            )

        # Fallback rows when vision misses some items.
        if len(out) < len(items):
            for idx, it in enumerate(items):
                if idx in used_idx:
                    continue
                title = f"chat_{idx + 1}"
                key = f"idx:{idx}:{sha1_hex(title)[:10]}"
                out.append(
                    ChatListEntry(
                        key=key,
                        index=idx,
                        title_guess=title,
                        row_text=title,
                        unread_count=None,
                        time_label="",
                        wrapper=it,
                    )
                )

        out.sort(key=lambda e: e.index)
        if limit and len(out) > limit:
            out = out[:limit]

        # Keep active-chat title in sync even when the monitor didn't explicitly open a row.
        sel_idx = self._selected_chat_index()
        if sel_idx is not None:
            for e in out:
                if int(e.index) == int(sel_idx):
                    if e.title_guess:
                        self._active_chat_title = _canonical_room_title(e.title_guess)
                    break

        self._chat_cache_hash = img_hash
        self._chat_cache_at = now
        self._chat_cache_rows = list(out)
        return out

    def list_chats(self) -> list[ChatRow]:
        entries = self.list_chat_list_entries(limit=80)
        rows: list[ChatRow] = []
        for e in entries:
            unread_count = e.unread_count
            unread = bool((unread_count or 0) > 0)
            rows.append(
                ChatRow(
                    name=e.title_guess,
                    unread=unread,
                    unread_count=unread_count,
                    wrapper=e.wrapper,
                )
            )
        return rows

    def open_chat(self, row: ChatRow, *, allow_click: bool = True) -> None:
        try:
            self.ensure_foreground(settle_seconds=0.12)
        except Exception:
            pass

        wrapper = getattr(row, "wrapper", None)
        if wrapper is None:
            raise RuntimeError("LINE row wrapper is missing.")

        title = (
            _norm(str(getattr(row, "name", "") or ""))
            or _norm(str(getattr(row, "title_guess", "") or ""))
            or self._active_chat_title
        )
        title = _canonical_room_title(title)

        try:
            wrapper.scroll_into_view()
            time.sleep(0.05)
        except Exception:
            pass

        acted = False
        for fn in ("select", "invoke", "click"):
            try:
                getattr(wrapper, fn)()
                acted = True
                time.sleep(0.05)
                break
            except Exception:
                continue

        if allow_click:
            try:
                wrapper.click_input()
                acted = True
            except Exception:
                try:
                    wrapper.double_click_input()
                    acted = True
                except Exception:
                    pass

        if not acted:
            raise RuntimeError("Failed to open LINE chat row.")

        if title:
            self._active_chat_title = title

    def _relative_click_point(self, *, abs_x: int, abs_y: int) -> tuple[int, int]:
        wr = self.main_window.rectangle()
        return (max(5, int(abs_x - wr.left)), max(5, int(abs_y - wr.top)))

    def _composer_text_click_point_abs(self) -> tuple[int, int]:
        wr = self.main_window.rectangle()
        try:
            ml = self._find_message_list().rectangle()
        except Exception:
            ml = None
        if ml is None:
            x = int(wr.left + max(80, (wr.right - wr.left) * 0.55))
            y = int(wr.top + max(80, (wr.bottom - wr.top) * 0.92))
            return x, y
        gap = max(40, int(wr.bottom - ml.bottom))
        y = int(min(wr.bottom - 24, ml.bottom + max(18, int(gap * 0.45))))
        x = int(min(wr.right - 120, ml.left + 36))
        return x, y

    def _composer_send_button_click_point_abs(self) -> tuple[int, int]:
        wr = self.main_window.rectangle()
        try:
            ml = self._find_message_list().rectangle()
        except Exception:
            ml = None
        if ml is None:
            x = int(wr.right - 34)
            y = int(wr.top + max(80, (wr.bottom - wr.top) * 0.92))
            return x, y
        gap = max(40, int(wr.bottom - ml.bottom))
        y = int(min(wr.bottom - 22, ml.bottom + max(16, int(gap * 0.42))))
        x = int(wr.right - 28)
        return x, y

    def _message_visible_in_tail(self, text: str, *, timeout_seconds: float = 2.4) -> bool:
        needle = _norm(text)
        if not needle:
            return False
        end = _now() + max(0.6, float(timeout_seconds))
        while _now() < end:
            try:
                msgs = self.extract_recent_messages(max_messages=20)
            except Exception:
                msgs = []
            for m in reversed(msgs):
                body = _norm(getattr(m, "text", "") or "")
                if not body:
                    continue
                if needle == body or needle in body:
                    return True
            time.sleep(0.28)
        return False

    def send_text_message(self, text: str) -> bool:
        """
        Send a message to the currently opened chat.

        LINE doesn't expose a stable Edit control via UIA on this host, so we focus
        the composer by click and send keys, with a send-button click fallback.
        """
        msg = (text or "").strip()
        if not msg:
            return False
        try:
            self.ensure_foreground(settle_seconds=0.12)
        except Exception:
            pass
        try:
            from pywinauto.keyboard import send_keys  # type: ignore
        except Exception:
            return False

        # Focus composer text area.
        try:
            tx, ty = self._composer_text_click_point_abs()
            self.main_window.click_input(coords=self._relative_click_point(abs_x=tx, abs_y=ty))
        except Exception:
            try:
                self.main_window.set_focus()
            except Exception:
                pass
        time.sleep(0.10)

        # Type and attempt Enter send first.
        try:
            send_keys(msg, with_spaces=True, pause=0.01)
            time.sleep(0.05)
            send_keys("{ENTER}")
        except Exception:
            return False

        if self._message_visible_in_tail(msg, timeout_seconds=1.8):
            return True

        # Fallback: click send button (handles setups where Enter inserts newline).
        try:
            sx, sy = self._composer_send_button_click_point_abs()
            self.main_window.click_input(coords=self._relative_click_point(abs_x=sx, abs_y=sy))
        except Exception:
            pass
        return self._message_visible_in_tail(msg, timeout_seconds=2.8)

    def get_selected_chat_row_text(self) -> str:
        idx = self._selected_chat_index()
        if idx is None:
            return self._active_chat_title

        rows = list(self._chat_cache_rows)
        if not rows or idx >= len(rows):
            try:
                rows = self.list_chat_list_entries(limit=max(40, idx + 1))
            except Exception:
                rows = []
        for e in rows:
            if int(e.index) == int(idx):
                if e.title_guess:
                    self._active_chat_title = _canonical_room_title(e.title_guess)
                return _canonical_room_title(e.title_guess)
        return self._active_chat_title

    def get_header_chat_name(self) -> str:
        s = self.get_selected_chat_row_text()
        if s:
            return s
        return self._active_chat_title

    def _vision_messages(self, img_bgr: np.ndarray) -> list[_VisionMessage]:
        try:
            obj = self.vision.describe_image_json(
                img_bgr=img_bgr,
                prompt=_prompt_messages(),
                max_tokens=1500,
            )
        except Exception as e:
            self._warn_vision("messages", e)
            return []

        if not isinstance(obj, list):
            return []

        out: list[_VisionMessage] = []
        for i, it in enumerate(obj):
            if not isinstance(it, dict):
                continue
            row_index = _safe_int(it.get("row_index", i), i)
            sender = _norm(str(it.get("sender", "") or "")) or "(unknown)"
            text = _norm(str(it.get("text", "") or ""))
            attachment_name = _norm(str(it.get("attachment_name", "") or "")) or None
            msg_type = _norm(str(it.get("msg_type", "") or "")).lower()
            if msg_type not in {"text", "image", "file", "video", "sticker", "audio", "system"}:
                msg_type = "text"
            is_image = bool(it.get("is_image", False))
            if msg_type in {"image", "video", "sticker"}:
                is_image = True
            if is_image and not text:
                text = "[Image]"
            if msg_type == "file" and not text and attachment_name:
                text = f"[File] {attachment_name}"
            if msg_type == "system":
                continue
            if not text and not is_image and msg_type != "file":
                continue
            direction = _norm(str(it.get("direction", "") or "")).lower()
            is_outgoing = bool(it.get("is_outgoing", False))
            if direction not in {"incoming", "outgoing", "unknown"}:
                if is_outgoing:
                    direction = "outgoing"
                elif bool(it.get("is_incoming", False)):
                    direction = "incoming"
                else:
                    direction = "unknown"
            out.append(
                _VisionMessage(
                    row_index=row_index,
                    sender=sender,
                    text=text,
                    is_image=is_image,
                    msg_type=msg_type,
                    attachment_name=attachment_name,
                    direction=direction,
                )
            )
        out.sort(key=lambda x: x.row_index)
        return out

    def extract_recent_messages(self, *, max_messages: int = 120) -> list[ExtractedMessage]:
        try:
            self.ensure_foreground(settle_seconds=0.08)
        except Exception:
            pass
        msg_list = self._find_message_list()
        items = self._sorted_items(msg_list)
        if max_messages and len(items) > max_messages:
            items = items[-max_messages:]

        if not items:
            return []

        try:
            img = self._capture_bgr(msg_list)
            img_hash = _image_hash(img)
        except Exception:
            img = None
            img_hash = ""

        now = _now()
        if (
            img_hash
            and self._msg_cache_rows
            and self._msg_cache_hash == img_hash
            and (now - self._msg_cache_at) <= 1.3
        ):
            return self._msg_cache_rows[-max_messages:] if max_messages else list(self._msg_cache_rows)

        parsed: list[_VisionMessage] = []
        if img is not None:
            parsed = self._vision_messages(img)

        if max_messages and len(parsed) > max_messages:
            parsed = parsed[-max_messages:]

        active_room = _canonical_room_title(self.get_header_chat_name() or self._active_chat_title)
        if active_room:
            self._active_chat_title = active_room

        explicit_senders: set[str] = set()
        for pm in parsed:
            s = _norm(pm.sender)
            if _is_unknown_sender(s):
                continue
            low = s.lower()
            if low in {"you", "me", "myself", "(me)"}:
                continue
            explicit_senders.add(low)

        # Map bottom-most parsed rows to bottom-most UIA rows.
        mapped_items: list[object | None] = [None] * len(parsed)
        if items and parsed:
            if len(items) >= len(parsed):
                tail = items[-len(parsed):]
                mapped_items = list(tail)
            else:
                mapped_items = [None] * (len(parsed) - len(items)) + list(items)

        out: list[ExtractedMessage] = []
        for i, m in enumerate(parsed):
            it = mapped_items[i] if i < len(mapped_items) else None
            rect_t: tuple[int, int, int, int] | None = None
            if it is not None:
                try:
                    r = it.rectangle()
                    rect_t = (int(r.left), int(r.top), int(r.right), int(r.bottom))
                except Exception:
                    rect_t = None

            msg_type = m.msg_type
            if msg_type in {"video", "sticker"}:
                msg_type = "image"
            if msg_type not in {"text", "image", "file"}:
                msg_type = "image" if m.is_image else "text"

            sender = _norm(m.sender)
            low_sender = sender.lower()
            if low_sender in {"you", "me", "myself"}:
                sender = "(me)"

            if _is_unknown_sender(sender):
                if m.direction == "outgoing":
                    sender = "(me)"
                else:
                    # 1:1 LINE chats often omit sender labels; use room title for incoming side.
                    if active_room and (not explicit_senders or explicit_senders == {active_room.lower()}):
                        sender = active_room
                    else:
                        sender = "(unknown)"

            rect_sig = ""
            if rect_t is not None:
                rect_sig = f"{rect_t[1]}:{rect_t[3]}"
            sig = sha1_hex(
                f"{self._active_chat_title}\n{sender}\n{m.text}\n{msg_type}\n{int(m.is_image)}\n{i}\n{rect_sig}"
            )
            legacy_sig = sha1_hex(f"{sender}\n{m.text}\n{int(m.is_image)}\n{i}")

            out.append(
                ExtractedMessage(
                    sender=sender or "(unknown)",
                    text=m.text,
                    is_image=bool(m.is_image),
                    signature=sig,
                    legacy_signature=legacy_sig,
                    rect=rect_t,
                    wrapper=it,
                    kind="message",
                    msg_type=msg_type,
                    attachment_name=m.attachment_name,
                )
            )

        self._msg_cache_hash = img_hash
        self._msg_cache_at = now
        self._msg_cache_rows = list(out)
        return out

    def open_image_viewer_from_message_item(
        self,
        item_wrapper,
        *,
        timeout_seconds: float = 2.0,
        maximize_window: bool = False,
    ) -> object | None:
        # LINE image viewer automation is not stable via UIA; bubble capture fallback is used.
        return None

    def viewer_best_capture_rect(self, viewer_window) -> tuple[int, int, int, int] | None:
        return None

    def close_image_viewer(self, viewer_window=None) -> None:
        return None

    def resolve_sender_from_message_item_avatar(
        self,
        item_wrapper,
        *,
        timeout_seconds: float = 1.2,
    ) -> str | None:
        # Sender is resolved from vision output for LINE.
        return None
