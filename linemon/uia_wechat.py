from __future__ import annotations

import ctypes
import hashlib
import os
import re
import shutil
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional


def _is_windows() -> bool:
    return sys.platform == "win32"


def get_idle_seconds() -> int:
    if not _is_windows():
        return 0

    class LASTINPUTINFO(ctypes.Structure):
        _fields_ = [("cbSize", ctypes.c_uint), ("dwTime", ctypes.c_uint)]

    lii = LASTINPUTINFO()
    lii.cbSize = ctypes.sizeof(LASTINPUTINFO)
    if not ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii)):
        return 0
    millis = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
    return int(millis / 1000)


def sanitize_filename(name: str) -> str:
    # Windows file name constraints; keep it readable.
    name = name.strip().strip(".")
    name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
    name = re.sub(r"\\s+", " ", name).strip()
    return name or "unknown_chat"


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "replace")).hexdigest()


TIMEISH_RE = re.compile(
    r"(^\d{1,2}:\d{2}$)|(^\d{4}[-/]\d{1,2}[-/]\d{1,2}$)|(^\d{1,2}\s*(AM|PM)$)",
    re.IGNORECASE,
)


def _norm_token(s: str) -> str:
    # UIA strings sometimes include "format" characters; strip them for matching/parsing.
    s = (s or "").replace("\r\n", "\n").strip()
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    return s.strip()


def _is_image_marker_text(s: str) -> bool:
    """
    Detect compact UIA placeholders that represent image messages.
    Keep this strict to avoid classifying normal chat text as an image.
    """
    t = _norm_token(s)
    if not t:
        return False

    wrappers = "[]【】()（）<>《》"

    # Direct token markers (EN/CN).
    low = t.lower().strip(wrappers)
    zh = t.strip(wrappers)
    if low in {"image", "picture", "photo", "pic"}:
        return True
    if zh in {"图片", "照片", "相片"}:
        return True

    # Common bracket forms, e.g. "[Image]" / "[图片]".
    if re.search(r"(?i)[\[\(【（]\s*(image|picture|photo|pic)\s*[\]\)】）]", t):
        return True
    if re.search(r"[\[\(【（]\s*(图片|照片|相片)\s*[\]\)】）]", t):
        return True

    # Tokens sometimes include sender prefix separated by newline/tab/colon.
    parts = [t]
    for sep in ("\n", "\t", ":", "："):
        nxt: list[str] = []
        for p in parts:
            nxt.extend(p.split(sep))
        parts = nxt
    for p in parts:
        p_low = p.strip().lower().strip(wrappers)
        p_zh = p.strip().strip(wrappers)
        if p_low in {"image", "picture", "photo", "pic"}:
            return True
        if p_zh in {"图片", "照片", "相片"}:
            return True
    return False


@dataclass(frozen=True)
class ChatRow:
    name: str
    unread: bool
    unread_count: Optional[int]
    # pywinauto wrapper object (UIAWrapper)
    wrapper: object


@dataclass(frozen=True)
class ChatListEntry:
    """
    Lightweight chat-list row data for "new activity" detection.

    WeChat often exposes most of this in ListItem.Name even when it doesn't expose
    separate Text children for name/preview/time.
    """

    key: str
    index: int
    title_guess: str
    row_text: str
    unread_count: Optional[int]
    time_label: str
    wrapper: object


@dataclass(frozen=True)
class ExtractedMessage:
    sender: str
    text: str
    is_image: bool
    signature: str  # stable-ish de-dupe key input
    legacy_signature: str | None = None  # previous signature scheme (best-effort)
    # Screen-space rectangle of the UIA list item (for pixel capture).
    # Tuple is (left, top, right, bottom) in screen pixels.
    rect: tuple[int, int, int, int] | None = None
    # pywinauto wrapper object (ListItem) for optional interactions (e.g. export image).
    wrapper: object | None = None
    kind: str = "message"  # "message" | "sender_only"
    msg_type: str = "text"  # "text" | "image" | "file"
    attachment_name: str | None = None


class WeChatUIA:
    def __init__(
        self,
        *,
        title_re: str,
        debug: bool = False,
        debug_dir: str | Path = "debug",
        single_window_mode: bool = False,
    ) -> None:
        self.title_re = title_re
        self.debug = debug
        self.debug_dir = Path(debug_dir)
        # Main WeChat window (chat list, navigation).
        self._main_window = None
        # Active chat/conversation window (some WeChat builds open chats in a separate top-level window).
        self._chat_window = None
        self.single_window_mode = single_window_mode
        self._last_surface_refresh_at = 0.0

    def connect(self):
        if not _is_windows():
            raise RuntimeError(
                "This capture script must be run with a Windows Python interpreter "
                "(pywinauto uses Windows UI Automation)."
            )

        # Ensure UIA rectangles are in screen pixels (important for screen capture / OCR).
        from linemon.dpi import make_dpi_aware

        make_dpi_aware()

        from pywinauto import Desktop  # type: ignore

        desk = Desktop(backend="uia")

        def pick_window(title_re: str):
            try:
                wins = desk.windows(title_re=title_re, visible_only=False)
            except Exception:
                wins = []
            scored: list[tuple[int, object]] = []
            for w in wins:
                try:
                    wo = w.wrapper_object()
                except Exception:
                    wo = w
                try:
                    r = wo.rectangle()
                    area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                except Exception:
                    area = 0
                scored.append((area, wo))
            if not scored:
                return None
            scored.sort(reverse=True, key=lambda x: x[0])
            # Sometimes pywinauto can't read a window rect until focus/restore, producing area=0.
            # Return the best candidate anyway; recovery steps can nudge it alive.
            return scored[0][1]

        def has_session_list(wo) -> bool:
            try:
                for lst in wo.descendants(control_type="List"):
                    try:
                        if (lst.element_info.automation_id or "") == "session_list":
                            return True
                    except Exception:
                        continue
            except Exception:
                return False
            return False

        title_win = None
        win = pick_window(self.title_re)
        if win is None:
            for alt in [
                r".*WeChat.*",
                r".*Weixin.*",
                ".*\u5fae\u4fe1.*",
            ]:
                win = pick_window(alt)
                if win is not None:
                    break

        title_win = win
        # If the title-based pick landed on a non-main WeChat window (e.g. a hidden helper window),
        # prefer the real main window that contains the session list.
        if win is not None and not has_session_list(win):
            win = None

        # Fallback: some builds use non-matching titles; scan all top-level windows and
        # pick the one that contains WeChat's known session list automation id.
        if win is None:
            candidates = []
            try:
                wins = desk.windows(control_type="Window", top_level_only=True, visible_only=False)
            except Exception:
                wins = []
            for w in wins:
                try:
                    wo = w.wrapper_object()
                except Exception:
                    wo = w
                try:
                    r = wo.rectangle()
                    area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                except Exception:
                    area = 0
                if area <= 0:
                    continue
                try:
                    lists = wo.descendants(control_type="List")
                except Exception:
                    lists = []
                found = False
                for lst in lists:
                    try:
                        if (lst.element_info.automation_id or "") == "session_list":
                            found = True
                            break
                    except Exception:
                        continue
                if found:
                    candidates.append((area, wo))
            candidates.sort(reverse=True, key=lambda x: x[0])
            if candidates:
                win = candidates[0][1]

        # Last-chance: WeChat can be "minimized to tray" where the main window exists but is hidden.
        # Try to restore a hidden top-level window titled "WeChat/Weixin/微信", then re-scan.
        if win is None:
            try:
                import win32con  # type: ignore
                import win32gui  # type: ignore

                hidden: list[tuple[int, int]] = []  # (area, hwnd)

                def _cb(hwnd, _):
                    try:
                        if win32gui.IsWindowVisible(hwnd):
                            return
                    except Exception:
                        return
                    try:
                        title = (win32gui.GetWindowText(hwnd) or "").strip()
                    except Exception:
                        title = ""
                    if not title:
                        return
                    if not re.search("(wechat|weixin|\u5fae\u4fe1)", title, re.IGNORECASE):
                        return
                    try:
                        l, t, r, b = win32gui.GetWindowRect(hwnd)
                        area = max(0, int(r - l)) * max(0, int(b - t))
                    except Exception:
                        area = 0
                    # Ignore tiny message windows; prefer a real main window rect.
                    if area < 20000:
                        return
                    hidden.append((area, int(hwnd)))

                win32gui.EnumWindows(_cb, None)
                hidden.sort(reverse=True, key=lambda x: x[0])
                if hidden:
                    hwnd = hidden[0][1]
                    try:
                        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                        win32gui.ShowWindow(hwnd, win32con.SW_SHOW)
                        win32gui.SetForegroundWindow(hwnd)
                    except Exception:
                        pass
                    time.sleep(0.35)
            except Exception:
                pass

            # Retry the previous heuristics once after attempting restore.
            win = pick_window(self.title_re)
            if win is None:
                for alt in [
                    r".*WeChat.*",
                    r".*Weixin.*",
                    ".*\u5fae\u4fe1.*",
                ]:
                    win = pick_window(alt)
                    if win is not None:
                        break
            title_win = win
            # Re-apply the "main window" sanity check after restore attempts too.
            if win is not None and not has_session_list(win):
                win = None
            if win is None:
                candidates = []
                try:
                    wins = desk.windows(control_type="Window", top_level_only=True, visible_only=False)
                except Exception:
                    wins = []
                for w in wins:
                    try:
                        wo = w.wrapper_object()
                    except Exception:
                        wo = w
                    try:
                        r = wo.rectangle()
                        area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                    except Exception:
                        area = 0
                    if area <= 0:
                        continue
                    try:
                        lists = wo.descendants(control_type="List")
                    except Exception:
                        lists = []
                    found = False
                    for lst in lists:
                        try:
                            if (lst.element_info.automation_id or "") == "session_list":
                                found = True
                                break
                        except Exception:
                            continue
                    if found:
                        candidates.append((area, wo))
                candidates.sort(reverse=True, key=lambda x: x[0])
                if candidates:
                    win = candidates[0][1]

        if win is None:
            # We couldn't locate a proper session list; fall back to the best title match.
            win = title_win

        if win is None:
            raise RuntimeError(f"Could not find a WeChat window. title_re={self.title_re!r}")

        self._main_window = win
        return self._main_window

    @property
    def main_window(self):
        if self._main_window is None:
            return self.connect()
        return self._main_window

    @property
    def active_window(self):
        # Prefer the dedicated chat window when available.
        if self.single_window_mode:
            return self.main_window
        if self._chat_window is not None:
            try:
                # Touch a property to ensure it's still alive.
                _ = self._chat_window.element_info.name  # type: ignore[attr-defined]
                return self._chat_window
            except Exception:
                self._chat_window = None
        return self.main_window

    def has_session_list(self) -> bool:
        """
        Return True when the main WeChat UI (session list) is present.

        Login/QR screens do not expose this list.
        """
        win = self.main_window
        try:
            for lst in win.descendants(control_type="List"):
                try:
                    if (lst.element_info.automation_id or "") == "session_list":
                        return True
                except Exception:
                    continue
        except Exception:
            return False
        return False

    def is_logged_in(self) -> bool:
        """
        Best-effort check for a usable, logged-in WeChat UI.
        """
        if self.has_session_list():
            return True
        try:
            _ = self.find_chat_list()
            return True
        except Exception:
            return False

    def attach_chat_window(self, *, title_hint: str | None = None, timeout_seconds: float = 1.2):
        """
        Best-effort: find and attach a top-level chat window (separate from the main 'WeChat' window).

        Some WeChat PC builds open each conversation in its own top-level window titled with the chat name.
        When found, message extraction/export should operate on that window instead of the main window.
        """
        if self.single_window_mode:
            self._chat_window = None
            return None
        try:
            from pywinauto import Desktop  # type: ignore
        except Exception:
            return None

        hint = (title_hint or "").strip()
        deadline = time.time() + max(0.2, float(timeout_seconds))
        main = self.main_window
        try:
            pid = main.element_info.process_id  # type: ignore[attr-defined]
        except Exception:
            pid = None
        try:
            main_title = (main.element_info.name or "").strip()  # type: ignore[attr-defined]
        except Exception:
            main_title = ""

        def is_chat_window(w) -> bool:
            try:
                for lst in w.descendants(control_type="List"):
                    try:
                        if (lst.element_info.automation_id or "") == "chat_message_list":
                            return True
                    except Exception:
                        continue
            except Exception:
                return False
            return False

        while time.time() < deadline:
            desk = Desktop(backend="uia")
            try:
                wins = desk.windows(control_type="Window", top_level_only=True)
            except Exception:
                wins = []
            candidates = []
            for w in wins:
                try:
                    wo = w.wrapper_object()
                except Exception:
                    wo = w
                try:
                    if pid is not None and getattr(wo.element_info, "process_id", None) != pid:
                        continue
                except Exception:
                    pass
                try:
                    title = (wo.element_info.name or "").strip()
                except Exception:
                    title = ""
                if not title:
                    continue
                if main_title and title == main_title:
                    continue
                if title.lower() == "wechat":
                    continue
                if hint:
                    tl = title.lower()
                    hl = hint.lower()
                    if not (
                        tl == hl
                        or tl.startswith(hl)
                        or hl.startswith(tl)
                        or (hl in tl)
                        or (tl in hl)
                    ):
                        continue
                if not is_chat_window(wo):
                    continue
                candidates.append(wo)
            if candidates:
                # Prefer the largest chat window (usually the actual conversation window).
                best = None
                best_area = -1
                for c in candidates:
                    try:
                        r = c.rectangle()
                        area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                    except Exception:
                        area = 0
                    if area > best_area:
                        best_area = area
                        best = c
                self._chat_window = best or candidates[0]
                return self._chat_window
            time.sleep(0.08)
        return None

    def export_image_from_message_item(
        self,
        item_wrapper,
        *,
        export_dir: str | Path,
        base_name: str,
        timeout_seconds: float = 10.0,
        maximize_window: bool = False,
    ) -> Path | None:
        """
        Best-effort export of the original image by opening the viewer.

        Strategies (in order):
        - Try WeChat viewer Save As (Ctrl+S) into export_dir.
        - If Save As is not detected, watch WeChat's ImageTemp for a freshly decoded file and copy it.

        Returns the saved file path, or None on failure.
        """
        export_path = Path(export_dir)
        export_path.mkdir(parents=True, exist_ok=True)

        base = (base_name or "wcmon_image").strip()
        base = re.sub(r"[<>:\"/\\\\|?*]+", "_", base).strip().strip(".") or "wcmon_image"

        before = set()
        try:
            before = {p.name for p in export_path.iterdir() if p.is_file()}
        except Exception:
            before = set()

        # Prepare ImageTemp watcher snapshot (best-effort).
        try:
            from linemon.wechat_files import WeChatFiles

            wf = WeChatFiles()
            imgtemp_before = wf.snapshot_image_temp_names() if wf.available() else set()
            temp_before = wf.snapshot_temp_names() if wf.available() else set()
        except Exception:
            wf = None
            imgtemp_before = set()
            temp_before = set()
        imgtemp_start = time.time()

        def find_save_as_dialog():
            """
            Find a native file dialog (Save As / Download) in a language-agnostic way.

            Many systems localize button text ("Save"), so don't key off titles.
            Prefer the common filename edit control (auto_id=1001) and avoid broad
            fallbacks that can accidentally target unrelated app windows.
            """
            try:
                from pywinauto import Desktop  # type: ignore
            except Exception:
                return None
            desk2 = Desktop(backend="uia")
            try:
                pid0 = int(self.main_window.element_info.process_id)  # type: ignore[attr-defined]
            except Exception:
                pid0 = None
            try:
                wins = desk2.windows(control_type="Window", top_level_only=True)
            except Exception:
                wins = []
            for w in wins:
                try:
                    wo = w.wrapper_object()
                except Exception:
                    wo = w
                try:
                    if pid0 is not None and int(getattr(wo.element_info, "process_id", -1)) != pid0:
                        continue
                except Exception:
                    pass
                try:
                    e = wo.child_window(auto_id="1001", control_type="Edit")
                    if e.exists(timeout=0.05):
                        return wo
                except Exception:
                    pass
            return None

        # Snapshot existing windows so we can detect the viewer window.
        try:
            from pywinauto import Desktop  # type: ignore

            desk0 = Desktop(backend="uia")
            before_titles: set[tuple[int | None, str]] = set()
            try:
                for w in desk0.windows(control_type="Window", top_level_only=True):
                    try:
                        wo = w.wrapper_object()
                    except Exception:
                        wo = w
                    try:
                        pid = getattr(wo.element_info, "process_id", None)
                    except Exception:
                        pid = None
                    try:
                        title = (wo.element_info.name or "").strip()
                    except Exception:
                        title = ""
                    before_titles.add((pid, title))
            except Exception:
                before_titles = set()
        except Exception:
            before_titles = set()

        def best_bubble_click_target():
            # Prefer clicking a large clickable descendant (often the image bubble) instead of the whole ListItem.
            try:
                ir = item_wrapper.rectangle()
                item_area = max(1, int(ir.right - ir.left)) * max(1, int(ir.bottom - ir.top))
            except Exception:
                item_area = 0
            best = None
            best_area = 0
            try:
                desc = item_wrapper.descendants()
            except Exception:
                desc = []
            for d in desc:
                try:
                    ctype = d.element_info.control_type
                except Exception:
                    continue
                if ctype not in {"Button", "Pane", "Custom", "Image", "Group"}:
                    continue
                try:
                    r = d.rectangle()
                    w = int(r.right - r.left)
                    h = int(r.bottom - r.top)
                    area = max(0, w) * max(0, h)
                except Exception:
                    continue
                if area <= 0 or w < 30 or h < 30:
                    continue
                # Exclude elements that are basically the whole list item.
                if item_area and area >= int(item_area * 0.95):
                    continue
                if area > best_area:
                    best_area = area
                    best = d
            return best

        # Open the image viewer.
        #
        # Different WeChat builds react to different actions (click vs double-click vs Invoke),
        # so try a few strategies before giving up.
        target = best_bubble_click_target() or item_wrapper
        acted = False
        fns = []
        # When the ListItem has no useful descendants, clicking its center often misses the
        # left/right-aligned image bubble. Try a few points across the row first.
        try:
            ir = item_wrapper.rectangle()
            w = max(1, int(ir.right - ir.left))
            h = max(1, int(ir.bottom - ir.top))
            y = max(5, min(h - 5, int(h * 0.50)))
            for xf in (0.15, 0.85, 0.35, 0.65, 0.50):
                fns.append(lambda xf=xf: item_wrapper.click_input(coords=(int(w * xf), y)))
        except Exception:
            pass

        fns.extend(
            [
                lambda: target.invoke(),
                lambda: target.click_input(),
                lambda: target.double_click_input(),
            ]
        )

        for fn in fns:
            try:
                fn()
                acted = True
                # Give WeChat a moment to open the viewer/overlay before the next action.
                time.sleep(0.08)
            except Exception:
                continue
        if not acted:
            return None

        def find_viewer_window(*, include_existing: bool = False):
            try:
                from pywinauto import Desktop  # type: ignore

                desk = Desktop(backend="uia")
            except Exception:
                return None
            # Prefer windows from the same process as WeChat main window.
            try:
                pid0 = self.main_window.element_info.process_id  # type: ignore[attr-defined]
            except Exception:
                pid0 = None
            try:
                main_title = (self.main_window.element_info.name or "").strip()  # type: ignore[attr-defined]
            except Exception:
                main_title = ""
            candidates = []
            try:
                wins = desk.windows(control_type="Window", top_level_only=True)
            except Exception:
                wins = []
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
                    pid = getattr(wo.element_info, "process_id", None)
                except Exception:
                    pid = None
                if pid0 is not None and pid != pid0:
                    continue
                if main_title and title == main_title:
                    continue
                if title.lower() == "wechat":
                    continue
                if (not include_existing) and before_titles and (pid, title) in before_titles:
                    continue
                # Heuristic: viewer window is usually sizeable.
                try:
                    r = wo.rectangle()
                    area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                except Exception:
                    area = 0
                if area <= 0:
                    continue
                candidates.append((area, wo))
            candidates.sort(reverse=True, key=lambda x: x[0])
            return candidates[0][1] if candidates else None

        viewer = None
        try:
            # Give the viewer time to appear.
            deadline = time.time() + 1.2
            while time.time() < deadline and viewer is None:
                viewer = find_viewer_window()
                if viewer is None:
                    time.sleep(0.08)
        except Exception:
            viewer = None
        if viewer is None:
            # Viewer may already be open from a previous run; allow selecting an existing window.
            try:
                viewer = find_viewer_window(include_existing=True)
            except Exception:
                viewer = None

        if maximize_window:
            try:
                # Maximize the window hosting the viewer/overlay to improve capture resolution.
                if viewer is not None:
                    viewer.maximize()
                else:
                    self.active_window.maximize()
                time.sleep(0.15)
            except Exception:
                pass

        # Ensure WeChat is focused so Ctrl+S goes to the viewer.
        try:
            if viewer is not None:
                viewer.set_focus()
            else:
                self.active_window.set_focus()
            time.sleep(0.05)
        except Exception:
            pass

        try:
            from pywinauto import Desktop  # type: ignore
            from pywinauto.keyboard import send_keys  # type: ignore
        except Exception:
            return None

        def wait_for_save_as_dialog(timeout_s: float = 1.2) -> object | None:
            deadline = time.time() + max(0.3, float(timeout_s))
            while time.time() < deadline:
                try:
                    dlgw = find_save_as_dialog()
                except Exception:
                    dlgw = None
                if dlgw is not None:
                    return dlgw
                time.sleep(0.08)
            return None

        def trigger_save_as() -> object | None:
            # Strategy 1: Ctrl+S (common shortcut)
            try:
                send_keys("^s")
            except Exception:
                pass
            dlg2 = wait_for_save_as_dialog(1.2)
            if dlg2 is not None:
                return dlg2

            # Strategy 2: right-click and choose a "save" option (best-effort; may be localized).
            # Prefer right-clicking the bubble target rather than the whole list item.
            try:
                (best_bubble_click_target() or item_wrapper).click_input(button="right")
                time.sleep(0.15)
                desk2 = Desktop(backend="uia")
                menu = desk2.window(control_type="Menu")
                if menu.exists(timeout=0.6):
                    try:
                        items = menu.descendants(control_type="MenuItem") or []
                    except Exception:
                        items = []
                    # Click the first menu item that contains a known "save" token.
                    # Keep this conservative; if we can't confidently identify it, bail.
                    # Keep tokens ASCII-only; use unicode escapes for CJK to avoid encoding issues on Windows shells.
                    save_tokens = [
                        "save",
                        "save as",
                        "download",
                        "\u4fdd\u5b58",  # 保存
                        "\u53e6\u5b58",  # 另存
                        "\u53e6\u5b58\u4e3a",  # 另存为
                    ]
                    preferred = None
                    for mi in items:
                        try:
                            nm = (mi.window_text() or mi.element_info.name or "").strip()
                        except Exception:
                            nm = ""
                        low = nm.lower()
                        if any(t in low for t in save_tokens) or any(t in nm for t in save_tokens):
                            preferred = mi
                            break
                    if preferred is not None:
                        preferred.click_input()
            except Exception:
                pass
            return wait_for_save_as_dialog(1.2)

        def try_click_viewer_save_button() -> bool:
            """
            Some viewer builds have a Save/Download icon button instead of Ctrl+S.
            Click it if we can find it.
            """
            if viewer is None:
                return False
            try:
                viewer.set_focus()
            except Exception:
                pass
            # Try common labeled buttons first.
            try:
                btns = viewer.descendants(control_type="Button") or []
            except Exception:
                btns = []
            for name_re in [
                r"(?i)^save$",
                r"(?i)save as",
                r"(?i)download",
                r"(?i)save picture",
                r"(?i)save image",
            ]:
                for b in btns:
                    try:
                        nm = (b.window_text() or b.element_info.name or "").strip()
                    except Exception:
                        nm = ""
                    if not nm:
                        continue
                    try:
                        if re.search(name_re, nm):
                            b.click_input()
                            return True
                    except Exception:
                        continue

            # Fallback: click the top-right-most small button in the viewer.
            try:
                btns = viewer.descendants(control_type="Button")
            except Exception:
                btns = []
            cand = None
            cand_score = None
            for b in btns:
                try:
                    r = b.rectangle()
                    w = r.right - r.left
                    h = r.bottom - r.top
                    if w <= 0 or h <= 0:
                        continue
                    if w > 80 or h > 80:
                        continue
                    # Prefer near top-right: minimize top, maximize left.
                    score = (int(r.top), -int(r.left))
                except Exception:
                    continue
                if cand is None or (cand_score is not None and score < cand_score):
                    cand = b
                    cand_score = score
            if cand is not None:
                try:
                    cand.click_input()
                    return True
                except Exception:
                    return False
            return False

        def _pick_filename_edit(dlgw) -> object | None:
            # Common dialog uses auto_id=1001 for filename in many builds.
            try:
                e = dlgw.child_window(auto_id="1001", control_type="Edit")
                if e.exists():
                    return e
            except Exception:
                pass
            try:
                edits = dlgw.descendants(control_type="Edit")
            except Exception:
                edits = []
            file_edit = None
            for e in reversed(edits):
                try:
                    r = e.rectangle()
                    if (r.right - r.left) >= 180:
                        file_edit = e
                        break
                except Exception:
                    continue
            if file_edit is None and edits:
                file_edit = edits[-1]
            return file_edit

        def _set_filename(editw, text: str) -> bool:
            try:
                editw.set_edit_text(text)
                return True
            except Exception:
                pass
            try:
                editw.click_input()
                time.sleep(0.05)
                send_keys("^a{BACKSPACE}")
                time.sleep(0.03)
                # send_keys treats {} as special; paths should not contain them.
                send_keys(text, with_spaces=True, pause=0.01)
                return True
            except Exception:
                return False

        def _confirm_dialog(dlgw) -> bool:
            # Language-agnostic: press Enter to activate the default button ("Save"/"OK").
            try:
                dlgw.set_focus()
            except Exception:
                pass
            try:
                send_keys("{ENTER}")
                return True
            except Exception:
                return False

        def _accept_overwrite_prompt() -> None:
            # Best-effort: if we accidentally hit a collision, press Enter to accept default.
            # Collisions should be rare because our filenames include timestamps/signature prefixes.
            try:
                send_keys("{ENTER}")
            except Exception:
                pass

        def _wait_new_file() -> Path | None:
            deadline = time.time() + max(2.0, float(timeout_seconds))
            while time.time() < deadline:
                try:
                    after = [p for p in export_path.iterdir() if p.is_file()]
                except Exception:
                    after = []
                new = [p for p in after if p.name not in before and p.name.startswith(base)]
                if not new:
                    new = [p for p in after if p.name not in before]
                if new:
                    new.sort(key=lambda p: getattr(p.stat(), "st_mtime", 0.0), reverse=True)
                    saved = new[0]
                    try:
                        s1 = saved.stat().st_size
                        time.sleep(0.2)
                        s2 = saved.stat().st_size
                        if s2 == s1 and s2 > 0:
                            return saved
                    except Exception:
                        return saved
                time.sleep(0.15)
            return None

        def _close_viewer() -> None:
            # Close viewer/overlay (best-effort). ESC is commonly used.
            try:
                if viewer is not None:
                    viewer.set_focus()
                else:
                    self.active_window.set_focus()
                time.sleep(0.03)
            except Exception:
                pass
            try:
                send_keys("{ESC}")
            except Exception:
                pass

        saved: Path | None = None
        dlg = trigger_save_as()
        if dlg is None:
            # Try to click a Save button inside the viewer, then look again for the Save As dialog.
            try:
                if try_click_viewer_save_button():
                    dlg = wait_for_save_as_dialog(1.6)
            except Exception:
                pass
        if dlg is not None:
            file_edit = _pick_filename_edit(dlg)
            if file_edit is not None:
                # Use a full path so the file goes into export_dir even if the dialog
                # is currently pointed somewhere else.
                if _set_filename(file_edit, str(export_path / base)):
                    if _confirm_dialog(dlg):
                        time.sleep(0.15)
                        _accept_overwrite_prompt()
                        saved = _wait_new_file()
                        if saved is not None:
                            _close_viewer()
                            return saved
        # Close lingering Save dialog only (best-effort).
        if dlg is not None:
            try:
                dlg.set_focus()
                time.sleep(0.03)
                send_keys("{ESC}")
                time.sleep(0.03)
            except Exception:
                pass

        def _try_clipboard_save() -> Path | None:
            """
            Fallback: copy the image to clipboard and save to export_dir.

            This avoids full-window screenshots and can work even when Save As is blocked/localized.
            Note: this overwrites the user's clipboard contents.
            """
            try:
                from PIL import ImageGrab  # type: ignore
            except Exception:
                return None
            try:
                if viewer is not None:
                    viewer.set_focus()
                else:
                    self.active_window.set_focus()
                time.sleep(0.05)
            except Exception:
                pass
            try:
                send_keys("^c")
            except Exception:
                return None
            time.sleep(0.25)

            try:
                clip = ImageGrab.grabclipboard()
            except Exception:
                clip = None
            if clip is None:
                return None

            # Some apps place a temp file path list on the clipboard.
            if isinstance(clip, list):
                for fp in clip:
                    try:
                        src = Path(str(fp))
                    except Exception:
                        continue
                    try:
                        if not src.exists() or not src.is_file():
                            continue
                    except Exception:
                        continue
                    try:
                        ext = src.suffix.lower().lstrip(".") or "png"
                        dest = export_path / f"{base}.{ext}"
                        if dest.exists():
                            for i in range(1, 50):
                                cand = export_path / f"{base}_{i}.{ext}"
                                if not cand.exists():
                                    dest = cand
                                    break
                        shutil.copyfile(src, dest)
                        return dest
                    except Exception:
                        continue
                return None

            # Otherwise, Pillow returns a PIL.Image.Image object.
            try:
                dest = export_path / f"{base}.png"
                if dest.exists():
                    for i in range(1, 50):
                        cand = export_path / f"{base}_{i}.png"
                        if not cand.exists():
                            dest = cand
                            break
                clip.save(dest)  # type: ignore[union-attr]
                return dest
            except Exception:
                return None

        # Fallback: try clipboard copy before scanning temp dirs.
        copied = _try_clipboard_save()
        if copied is not None:
            _close_viewer()
            return copied

        # Fallback: if Save As could not be automated, try to grab the freshly decoded file from ImageTemp.
        if wf is not None:
            try:
                src = wf.wait_for_new_image_temp_file(
                    since_epoch=imgtemp_start,
                    exclude_names=imgtemp_before,
                    timeout_seconds=float(timeout_seconds),
                    min_bytes=8 * 1024,
                )
            except Exception:
                src = None
            if src is None:
                # Broader scan: temp/* (in case the build doesn't use ImageTemp consistently).
                try:
                    src = wf.wait_for_new_temp_image_file(
                        since_epoch=imgtemp_start,
                        exclude_names=temp_before,
                        timeout_seconds=float(timeout_seconds),
                        min_bytes=8 * 1024,
                    )
                except Exception:
                    src = None
            if src is not None:
                try:
                    ext = (src.suffix or "").lower().lstrip(".")
                    if not ext:
                        ext = wf.sniff_image_ext(src) or ""
                    if not ext:
                        _close_viewer()
                        return None
                    # Avoid clobbering if multiple exports share a base.
                    dest = export_path / f"{base}.{ext}"
                    if dest.exists():
                        for i in range(1, 50):
                            cand = export_path / f"{base}_{i}.{ext}"
                            if not cand.exists():
                                dest = cand
                                break
                    shutil.copyfile(src, dest)
                    _close_viewer()
                    return dest
                except Exception:
                    pass

        _close_viewer()
        return None

    def open_image_viewer_from_message_item(
        self,
        item_wrapper,
        *,
        timeout_seconds: float = 2.0,
        maximize_window: bool = False,
    ) -> object | None:
        """
        Open the image viewer for a given chat message list item and return the viewer window wrapper.

        This intentionally does NOT attempt to use WeChat's "Save" feature. The intended use is to
        open the viewer and then screen-capture the viewer window (or the best image-area rect inside it).
        """
        if item_wrapper is None:
            return None

        # This flow relies on screen capture, so bring WeChat to the foreground before any
        # physical click_input calls. If we can't, avoid click_input to reduce misclick risk.
        fg_ok = False
        try:
            fg_ok = bool(self.ensure_foreground(settle_seconds=0.25))
        except Exception:
            fg_ok = False

        # Snapshot existing windows so we can detect a new viewer window.
        before_titles: set[tuple[int | None, str]] = set()
        try:
            from pywinauto import Desktop  # type: ignore

            desk0 = Desktop(backend="uia")
            try:
                for w in desk0.windows(control_type="Window", top_level_only=True):
                    try:
                        wo = w.wrapper_object()
                    except Exception:
                        wo = w
                    try:
                        pid = getattr(wo.element_info, "process_id", None)
                    except Exception:
                        pid = None
                    try:
                        title = (wo.element_info.name or "").strip()
                    except Exception:
                        title = ""
                    before_titles.add((pid, title))
            except Exception:
                before_titles = set()
        except Exception:
            before_titles = set()

        def best_bubble_click_target():
            # Prefer clicking a large clickable descendant (often the image bubble) instead of the whole ListItem.
            try:
                ir = item_wrapper.rectangle()
                item_area = max(1, int(ir.right - ir.left)) * max(1, int(ir.bottom - ir.top))
            except Exception:
                item_area = 0
            best = None
            best_area = 0
            try:
                desc = item_wrapper.descendants()
            except Exception:
                desc = []
            for d in desc:
                try:
                    ctype = d.element_info.control_type
                except Exception:
                    continue
                if ctype not in {"Button", "Pane", "Custom", "Image", "Group"}:
                    continue
                try:
                    r = d.rectangle()
                    w = int(r.right - r.left)
                    h = int(r.bottom - r.top)
                    area = max(0, w) * max(0, h)
                except Exception:
                    continue
                if area <= 0 or w < 30 or h < 30:
                    continue
                # Exclude elements that are basically the whole list item.
                if item_area and area >= int(item_area * 0.95):
                    continue
                if area > best_area:
                    best_area = area
                    best = d
            return best

        # Open the image viewer.
        target = best_bubble_click_target() or item_wrapper
        acted = False
        fns = []
        fns.extend(
            [
                lambda: target.invoke(),
                lambda: target.click(),
            ]
        )
        if fg_ok:
            # Clicking the center of the ListItem can miss the left/right aligned bubble.
            # Try a few points across the row to reliably open the viewer.
            try:
                ir = item_wrapper.rectangle()
                w = max(1, int(ir.right - ir.left))
                h = max(1, int(ir.bottom - ir.top))
                y = max(5, min(h - 5, int(h * 0.50)))
                for xf in (0.15, 0.85, 0.35, 0.65, 0.50):
                    fns.append(lambda xf=xf: item_wrapper.click_input(coords=(int(w * xf), y)))
            except Exception:
                pass
            fns.extend(
                [
                    lambda: target.click_input(),
                    lambda: target.double_click_input(),
                ]
            )
        for fn in fns:
            try:
                fn()
                acted = True
                time.sleep(0.08)
            except Exception:
                continue
        if not acted:
            return None

        def find_viewer_window(*, include_existing: bool = False):
            try:
                from pywinauto import Desktop  # type: ignore

                desk = Desktop(backend="uia")
            except Exception:
                return None
            try:
                pid0 = self.main_window.element_info.process_id  # type: ignore[attr-defined]
            except Exception:
                pid0 = None
            try:
                main_title = (self.main_window.element_info.name or "").strip()  # type: ignore[attr-defined]
            except Exception:
                main_title = ""
            candidates = []
            try:
                wins = desk.windows(control_type="Window", top_level_only=True)
            except Exception:
                wins = []
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
                    pid = getattr(wo.element_info, "process_id", None)
                except Exception:
                    pid = None
                if pid0 is not None and pid != pid0:
                    continue
                if main_title and title == main_title:
                    continue
                if title.lower() == "wechat":
                    continue
                if (not include_existing) and before_titles and (pid, title) in before_titles:
                    continue
                try:
                    r = wo.rectangle()
                    area = max(0, int(r.right - r.left)) * max(0, int(r.bottom - r.top))
                except Exception:
                    area = 0
                if area <= 0:
                    continue
                candidates.append((area, wo))
            candidates.sort(reverse=True, key=lambda x: x[0])
            return candidates[0][1] if candidates else None

        viewer = None
        deadline = time.time() + max(0.4, float(timeout_seconds))
        while time.time() < deadline and viewer is None:
            try:
                viewer = find_viewer_window()
            except Exception:
                viewer = None
            if viewer is None:
                time.sleep(0.08)
        if viewer is None:
            # Viewer may already be open from a previous run; allow selecting an existing window.
            try:
                viewer = find_viewer_window(include_existing=True)
            except Exception:
                viewer = None

        if viewer is None:
            return None

        if maximize_window:
            try:
                viewer.maximize()
                time.sleep(0.15)
            except Exception:
                pass

        try:
            viewer.set_focus()
            time.sleep(0.05)
        except Exception:
            pass
        return viewer

    def viewer_best_capture_rect(self, viewer_window) -> tuple[int, int, int, int] | None:
        """
        Return the best screen-rect to capture for an image viewer window.

        Prefer the largest Image-like descendant (usually the actual photo), falling back to the
        entire viewer window rect.
        """
        if viewer_window is None:
            return None
        try:
            wr = viewer_window.rectangle()
            wrect = (int(wr.left), int(wr.top), int(wr.right), int(wr.bottom))
        except Exception:
            wrect = None

        try:
            if wrect is None:
                return None
            wl, wt, wr0, wb = wrect
            win_w = max(1, int(wr0 - wl))
            win_h = max(1, int(wb - wt))
            win_area = win_w * win_h
            cx0 = (wl + wr0) / 2.0
            cy0 = (wt + wb) / 2.0
        except Exception:
            win_area = 0
            cx0 = 0.0
            cy0 = 0.0

        candidates: list[tuple[int, float, tuple[int, int, int, int]]] = []
        try:
            desc = viewer_window.descendants()
        except Exception:
            desc = []
        for d in desc:
            try:
                ctype = d.element_info.control_type
            except Exception:
                continue
            if ctype not in {"Image", "Pane", "Custom", "Group", "Document"}:
                continue
            try:
                rr = d.rectangle()
                l, t, r, b = int(rr.left), int(rr.top), int(rr.right), int(rr.bottom)
                w = r - l
                h = b - t
                area = max(0, w) * max(0, h)
            except Exception:
                continue
            if area <= 0 or w < 120 or h < 120:
                continue
            if wrect is not None:
                wl, wt, wr0, wb = wrect
                if l < wl or t < wt or r > wr0 or b > wb:
                    continue
                # Exclude elements that basically cover the whole window (often background panes).
                if win_area and area >= int(win_area * 0.985):
                    continue
            prio = 0 if ctype == "Image" else 1
            cx = (l + r) / 2.0
            cy = (t + b) / 2.0
            dist = abs(cx - cx0) + abs(cy - cy0)
            score = float(area) - (dist * 45.0)
            candidates.append((prio, score, (l, t, r, b)))

        if candidates:
            candidates.sort(key=lambda x: (x[0], -x[1]))
            return candidates[0][2]
        return wrect

    def close_image_viewer(self, viewer_window=None) -> None:
        # Close viewer/overlay (best-effort). ESC is commonly used.
        try:
            from pywinauto.keyboard import send_keys  # type: ignore
        except Exception:
            return
        try:
            if viewer_window is not None:
                viewer_window.set_focus()
            else:
                self.active_window.set_focus()
            time.sleep(0.03)
        except Exception:
            pass
        try:
            send_keys("{ESC}")
            time.sleep(0.05)
        except Exception:
            pass
        # Try to restore focus to WeChat so subsequent UIA interactions are stable.
        try:
            self.active_window.set_focus()
            time.sleep(0.03)
        except Exception:
            pass

    def _profile_popup_windows(self) -> list[object]:
        """
        Return currently visible profile popup windows (best-effort).

        WeChat (Qt) opens an avatar/contact card as a separate top-level window.
        We use this to resolve message sender names without OCR.
        """
        if not _is_windows():
            return []

        # Fast path: when the profile popup opens, it becomes the foreground window.
        try:
            fg = int(ctypes.windll.user32.GetForegroundWindow())
        except Exception:
            fg = 0
        if not fg:
            return []
        try:
            from pywinauto import Desktop  # type: ignore
        except Exception:
            return []
        try:
            desk = Desktop(backend="uia")
            w = desk.window(handle=int(fg))
            try:
                wo = w.wrapper_object()
            except Exception:
                wo = w
        except Exception:
            return []

        try:
            cls = (wo.element_info.class_name or "").strip()  # type: ignore[attr-defined]
        except Exception:
            cls = ""
        if cls != "mmui::ProfileUniquePop":
            return []

        # Ensure it's from the same process as the main WeChat window.
        try:
            pid0 = int(self.main_window.element_info.process_id)  # type: ignore[attr-defined]
        except Exception:
            pid0 = 0
        if pid0:
            try:
                pid = int(getattr(wo.element_info, "process_id", 0) or 0)  # type: ignore[attr-defined]
            except Exception:
                pid = 0
            if pid and pid != pid0:
                return []

        return [wo]

    def _profile_popup_display_name(self, popup_window) -> str:
        if popup_window is None:
            return ""

        def norm(s: str) -> str:
            return (s or "").replace("\r\n", "\n").strip()

        # Prefer the explicit display-name automation id when available.
        try:
            for t in popup_window.descendants(control_type="Text"):
                try:
                    aid = (t.element_info.automation_id or "").lower()
                except Exception:
                    aid = ""
                if "display_name_text" not in aid:
                    continue
                try:
                    s = norm((t.window_text() or "") or (t.element_info.name or ""))
                except Exception:
                    s = ""
                if s:
                    return s
        except Exception:
            pass

        # Fallback: the avatar/button at top-left often uses the contact name as its label.
        try:
            for b in popup_window.descendants(control_type="Button"):
                try:
                    aid = (b.element_info.automation_id or "").lower()
                except Exception:
                    aid = ""
                if not aid:
                    continue
                if "head_view" not in aid and "head_image" not in aid:
                    continue
                try:
                    s = norm((b.window_text() or "") or (b.element_info.name or ""))
                except Exception:
                    s = ""
                if s:
                    return s
        except Exception:
            pass

        # Best-effort: pick the top-most short text that isn't a label.
        candidates: list[tuple[int, int, int, str]] = []
        try:
            pr = popup_window.rectangle()
            top_band_bottom = int(pr.top + 110)
        except Exception:
            top_band_bottom = None
        try:
            texts = popup_window.descendants(control_type="Text")
        except Exception:
            texts = []
        for t in texts:
            try:
                s = norm((t.window_text() or "") or (t.element_info.name or ""))
            except Exception:
                s = ""
            if not s:
                continue
            if len(s) > 80:
                continue
            # Filter out labels like "Region:" and common action buttons.
            if s.endswith(":") or s.endswith("："):
                continue
            if s.strip().lower() in {"region", "moments", "channels", "add to contacts", "send message"}:
                continue
            try:
                r = t.rectangle()
                top = int(r.top)
                left = int(r.left)
            except Exception:
                top = 10**9
                left = 10**9
            if top_band_bottom is not None and top > top_band_bottom:
                continue
            candidates.append((top, left, -len(s), s))
        candidates.sort()
        return candidates[0][3] if candidates else ""

    def _close_profile_popup(self, popup_window=None) -> None:
        try:
            from pywinauto.keyboard import send_keys  # type: ignore
        except Exception:
            return
        try:
            if popup_window is not None:
                popup_window.set_focus()
            else:
                wins = self._profile_popup_windows()
                if wins:
                    try:
                        wins[0].set_focus()
                    except Exception:
                        pass
            time.sleep(0.03)
        except Exception:
            pass
        try:
            send_keys("{ESC}")
            time.sleep(0.06)
        except Exception:
            pass
        try:
            self.active_window.set_focus()
            time.sleep(0.03)
        except Exception:
            pass

    def resolve_sender_from_message_item_avatar(
        self,
        item_wrapper,
        *,
        timeout_seconds: float = 1.2,
    ) -> str | None:
        """
        Resolve the sender name for a chat message by clicking the avatar and reading
        the profile popup window text (no OCR).

        Returns None if it can't be resolved safely.
        """
        if item_wrapper is None:
            return None

        # Avatar click is a physical click; never do it if WeChat isn't foreground.
        try:
            fg_ok = bool(self.ensure_foreground(settle_seconds=0.10))
        except Exception:
            fg_ok = False
        if not fg_ok:
            return None

        try:
            item_wrapper.scroll_into_view()
            time.sleep(0.04)
        except Exception:
            pass

        try:
            ir = item_wrapper.rectangle()
            w = max(1, int(ir.right - ir.left))
            h = max(1, int(ir.bottom - ir.top))
        except Exception:
            return None

        # Avatar is usually near the top of the row; for tall bubbles (quotes / long text),
        # clicking the middle can miss. Try a few y positions.
        # Most rows work with mid-height. For tall rows (quotes / long text), try
        # a top-band click as a fallback.
        y_candidates = [
            int(h * 0.25),
            int(h * 0.50),
            min(h - 6, 42),
            int(h * 0.35),
            int(h * 0.70),
        ]
        ys: list[int] = []
        for y in y_candidates:
            yy = max(6, min(h - 6, int(y)))
            if yy not in ys:
                ys.append(yy)
        # Keep the attempt space small; we only need to hit the avatar area.
        left_xs = [32, 44, 56]
        right_xs = [w - 32, w - 44, w - 56]

        base_left = int(getattr(ir, "left", 0) or 0)
        base_top = int(getattr(ir, "top", 0) or 0)

        def click_rel(x: int, y: int) -> bool:
            # Use absolute mouse clicks here; ListItem.click_input(coords=...)
            # is intermittently unreliable on some Qt builds.
            try:
                from pywinauto import mouse  # type: ignore
            except Exception:
                return False
            try:
                mouse.click(button="left", coords=(int(base_left + int(x)), int(base_top + int(y))))
                return True
            except Exception:
                return False

        def wait_popup(deadline: float) -> object | None:
            while time.time() < deadline:
                wins = self._profile_popup_windows()
                if wins:
                    return wins[0]
                time.sleep(0.05)
            return None

        # If a popup is already open from a prior attempt, close it first to avoid
        # reading stale data.
        try:
            if self._profile_popup_windows():
                self._close_profile_popup()
        except Exception:
            pass

        deadline_total = time.time() + max(0.25, float(timeout_seconds))

        # Try left avatar first (incoming). If nothing opens, try right (outgoing/self).
        for xs in (left_xs, right_xs):
            for y in ys:
                for x in xs:
                    remaining = deadline_total - time.time()
                    if remaining <= 0:
                        break
                    # Keep each attempt short; most popups appear quickly.
                    per_wait = min(0.25, remaining)

                    # Ensure no stale popup.
                    try:
                        if self._profile_popup_windows():
                            self._close_profile_popup()
                    except Exception:
                        pass

                    if not click_rel(int(x), int(y)):
                        continue
                    pop = wait_popup(time.time() + per_wait)
                    if pop is None:
                        continue
                    name = (self._profile_popup_display_name(pop) or "").strip()
                    self._close_profile_popup(pop)
                    if name:
                        return name

        return None

    @property
    def window(self):
        # Backward-compatible alias: the main WeChat window.
        return self.main_window

    def _hwnd(self) -> int | None:
        w = self.main_window
        for attr in ("handle",):
            try:
                h = getattr(w, attr)
                if isinstance(h, int) and h:
                    return h
            except Exception:
                pass
        try:
            h = getattr(getattr(w, "element_info", None), "handle", None)
            if isinstance(h, int) and h:
                return h
        except Exception:
            pass
        return None

    def is_foreground(self) -> bool:
        if not _is_windows():
            return True
        hwnd = self._hwnd()
        if not hwnd:
            return False
        try:
            fg = int(ctypes.windll.user32.GetForegroundWindow())
        except Exception:
            return False
        return fg == int(hwnd)

    def ensure_foreground(self, *, settle_seconds: float = 0.35) -> bool:
        """
        Best-effort bring WeChat to the foreground (visible + focused).

        Returns True if we believe the WeChat main window is foreground afterwards.
        This is important for screen-based OCR; without foreground, OCR sees whatever is on screen.
        """
        if not _is_windows():
            return True
        win = self.main_window
        hwnd = self._hwnd()

        # Quick path.
        try:
            if hwnd and self.is_foreground():
                return True
        except Exception:
            pass

        # Restore if minimized and ask pywinauto to focus.
        try:
            if getattr(win, "is_minimized", None) and win.is_minimized():
                win.restore()
        except Exception:
            pass
        try:
            win.set_focus()
        except Exception:
            pass
        if settle_seconds > 0:
            time.sleep(float(settle_seconds))
        if hwnd and self.is_foreground():
            return True

        # Harder attempt via Win32 APIs (some machines block focus stealing unless nudged).
        if hwnd:
            try:
                import win32con  # type: ignore
                import win32gui  # type: ignore
                import win32process  # type: ignore

                try:
                    win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                except Exception:
                    pass

                # Nudge: sending an ALT keystroke sometimes relaxes foreground restrictions.
                try:
                    ctypes.windll.user32.keybd_event(0x12, 0, 0, 0)  # VK_MENU down
                    ctypes.windll.user32.keybd_event(0x12, 0, 2, 0)  # VK_MENU up
                except Exception:
                    pass

                try:
                    win32gui.BringWindowToTop(hwnd)
                except Exception:
                    pass

                try:
                    fg = win32gui.GetForegroundWindow()
                    if fg and fg != hwnd:
                        cur_tid = win32process.GetCurrentThreadId()
                        fg_tid, _ = win32process.GetWindowThreadProcessId(fg)
                        ctypes.windll.user32.AttachThreadInput(fg_tid, cur_tid, True)
                        try:
                            win32gui.SetForegroundWindow(hwnd)
                        finally:
                            ctypes.windll.user32.AttachThreadInput(fg_tid, cur_tid, False)
                    else:
                        win32gui.SetForegroundWindow(hwnd)
                except Exception:
                    # Ignore; we'll still try to continue with UIA-only operations.
                    pass
            except Exception:
                pass

        if settle_seconds > 0:
            time.sleep(float(settle_seconds))
        return bool(hwnd and self.is_foreground())

    def refresh_surface(
        self,
        *,
        force: bool = False,
        min_interval_seconds: float = 45.0,
        settle_seconds: float = 0.9,
    ) -> bool:
        """
        Best-effort recover a broken UIA surface.

        WeChat occasionally stops exposing its UI Automation tree (e.g. descendants()==0)
        until the window is minimized/restored. This is disruptive, so it is throttled.
        Returns True if the surface appears alive afterwards.
        """
        if not _is_windows():
            return False
        now = time.time()
        try:
            last = float(getattr(self, "_last_surface_refresh_at", 0.0) or 0.0)
        except Exception:
            last = 0.0
        if (now - last) < float(min_interval_seconds):
            return False
        self._last_surface_refresh_at = now

        try:
            win = self.main_window
        except Exception:
            return False

        # If the surface already looks alive, avoid flicker unless explicitly forced.
        if not force:
            try:
                if len(win.descendants()) > 0:
                    return True
            except Exception:
                pass

        try:
            win.minimize()
            time.sleep(0.25)
        except Exception:
            pass
        try:
            win.restore()
            time.sleep(float(settle_seconds))
        except Exception:
            pass
        try:
            win.set_focus()
            time.sleep(0.10)
        except Exception:
            pass

        # Re-bind wrapper object (some builds rebuild the tree on restore).
        try:
            self._main_window = None
            win2 = self.connect()
        except Exception:
            win2 = win

        try:
            return len(win2.descendants()) > 0
        except Exception:
            return False

    def dump_tree(self, *, filename: str) -> None:
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        path = self.debug_dir / filename
        with path.open("w", encoding="utf-8") as f:
            self._dump_elem(self.window, f, depth=0, max_depth=8)

    def dump_elem_tree(self, elem, *, filename: str, max_depth: int = 6) -> None:
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        path = self.debug_dir / filename
        with path.open("w", encoding="utf-8") as f:
            self._dump_elem(elem, f, depth=0, max_depth=max_depth)

    def dump_message_list_tree(self, *, filename: str) -> None:
        msg_list = self._find_message_list()
        self.dump_elem_tree(msg_list, filename=filename, max_depth=7)

    def dump_list_inventory(self, *, filename: str) -> None:
        """
        Debug helper: enumerate all UIA List controls and their ListItem counts.
        """
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        path = self.debug_dir / filename
        win = self.active_window
        try:
            wrect = win.rectangle()
            wrect_s = f"({wrect.left},{wrect.top})-({wrect.right},{wrect.bottom})"
        except Exception:
            wrect_s = "(?)"
        lists = win.descendants(control_type="List")
        with path.open("w", encoding="utf-8") as f:
            f.write(f"window_rect={wrect_s}\n")
            for i, lst in enumerate(lists):
                try:
                    r = lst.rectangle()
                    rect_s = f"({r.left},{r.top})-({r.right},{r.bottom})"
                except Exception:
                    rect_s = "(?)"
                try:
                    n = len(lst.descendants(control_type="ListItem"))
                except Exception:
                    n = -1
                try:
                    name = lst.element_info.name or ""
                except Exception:
                    name = ""
                try:
                    aid = lst.element_info.automation_id or ""
                except Exception:
                    aid = ""
                f.write(f"[{i}] items={n} rect={rect_s} name={name!r} automation_id={aid!r}\n")

    def _dump_elem(self, elem, f, *, depth: int, max_depth: int) -> None:
        if depth > max_depth:
            return
        try:
            rect = elem.rectangle()
            rect_s = f"({rect.left},{rect.top})-({rect.right},{rect.bottom})"
        except Exception:
            rect_s = "(?)"
        try:
            name = elem.element_info.name or ""
        except Exception:
            name = ""
        try:
            ctype = elem.element_info.control_type or ""
        except Exception:
            ctype = ""
        try:
            aid = elem.element_info.automation_id or ""
        except Exception:
            aid = ""
        f.write(
            f"{'  '*depth}{ctype} name={name!r} automation_id={aid!r} rect={rect_s}\n"
        )
        try:
            kids = elem.children()
        except Exception:
            kids = []
        for k in kids[:120]:
            self._dump_elem(k, f, depth=depth + 1, max_depth=max_depth)

    def _pick_best_list(
        self, lists: Iterable[object], *, prefer_left: bool, min_items: int = 3
    ) -> object:
        best = None
        best_score = None
        for lst in lists:
            try:
                items = lst.descendants(control_type="ListItem")
                n = len(items)
                if n < min_items:
                    continue
                r = lst.rectangle()
                # Prefer lists that are tall and have many items. Also bias left/right.
                height = max(1, r.bottom - r.top)
                # WeChat often has multiple List controls; message history can dwarf chat count.
                # When we want the chat list, aggressively penalize controls further to the right.
                # When we want message history, do the opposite.
                side_weight = 50
                side_score = (-r.left * side_weight) if prefer_left else (r.left * side_weight)
                score = (n * 1000) + (height * 2) + side_score
            except Exception:
                continue
            if best is None or (best_score is not None and score > best_score):
                best = lst
                best_score = score
        if best is None:
            raise RuntimeError("Could not locate a suitable List control in the WeChat UI.")
        return best

    def find_chat_list(self):
        win = self.main_window

        def ensure_chats_tab() -> None:
            # Click the "WeChat" navigation button to ensure the chats list is visible.
            try:
                win.set_focus()
            except Exception:
                pass
            try:
                btn = win.child_window(title="WeChat", control_type="Button")
                if btn.exists(timeout=0.2):
                    btn.click_input()
                    time.sleep(0.15)
                    return
            except Exception:
                pass
            # Fallback: toolbar id used by many builds.
            try:
                bar = win.child_window(auto_id="main_tabbar", control_type="ToolBar")
                if bar.exists(timeout=0.2):
                    b2 = bar.child_window(title="WeChat", control_type="Button")
                    if b2.exists(timeout=0.2):
                        b2.click_input()
                        time.sleep(0.15)
            except Exception:
                pass

        def try_find_session_list():
            # Prefer known automation id when present (more stable than geometry heuristics).
            for lst in win.descendants(control_type="List"):
                try:
                    if (lst.element_info.automation_id or "") == "session_list":
                        return lst
                except Exception:
                    continue
            return None

        lst0 = try_find_session_list()
        if lst0 is not None:
            return lst0
        # If we're not on the chats tab, switch and retry once.
        ensure_chats_tab()
        lst1 = try_find_session_list()
        if lst1 is not None:
            return lst1
        try:
            w = win.rectangle()
            width = max(1, w.right - w.left)
            left_cutoff = w.left + int(width * 0.55)
        except Exception:
            left_cutoff = None

        lists = []
        for lst in win.descendants(control_type="List"):
            if left_cutoff is None:
                lists.append(lst)
                continue
            try:
                r = lst.rectangle()
            except Exception:
                continue
            # Chat list sits on the left pane.
            if r.left <= left_cutoff:
                lists.append(lst)
        return self._pick_best_list(lists, prefer_left=True, min_items=3)

    def list_chats(self) -> list[ChatRow]:
        chat_list = self.find_chat_list()
        rows: list[ChatRow] = []
        for item in chat_list.descendants(control_type="ListItem"):
            name = self._extract_chat_name(item)
            if not name:
                continue
            unread, unread_count = self._extract_unread(item)
            rows.append(ChatRow(name=name, unread=unread, unread_count=unread_count, wrapper=item))
        return rows

    def list_chat_list_entries(self, *, limit: int = 40) -> list[ChatListEntry]:
        """
        Return chat list entries for activity detection without opening chats.

        Prefer using ListItem.Name (accessibility label) which frequently contains:
          - chat title
          - preview excerpt
          - unread count (sometimes)
          - last activity time label
        """
        chat_list = self.find_chat_list()
        items = chat_list.descendants(control_type="ListItem")
        if limit and len(items) > limit:
            items = items[:limit]

        out: list[ChatListEntry] = []
        for idx, item in enumerate(items):
            try:
                li_name = _norm_token(item.element_info.name or "")
            except Exception:
                li_name = ""

            title_guess = self._extract_chat_name(item) or ""
            unread_count = self._parse_unread_count(li_name)
            time_label = self._parse_time_label(li_name)
            if unread_count is None or not time_label:
                # Some builds emit unread/time metadata in child Text nodes.
                try:
                    for t in item.descendants(control_type="Text"):
                        try:
                            t_txt = _norm_token(t.window_text() or "")
                        except Exception:
                            t_txt = ""
                        if not t_txt:
                            continue
                        if unread_count is None:
                            uc = self._parse_unread_count(t_txt)
                            if uc is not None:
                                unread_count = uc
                        if not time_label:
                            tl = self._parse_time_label(t_txt)
                            if tl:
                                time_label = tl
                        if unread_count is not None and time_label:
                            break
                except Exception:
                    pass
            key = self._row_key(item, idx=idx, title_guess=title_guess, li_name=li_name)

            # In some WeChat builds, ListItem has no children and Name contains everything
            # (title + unread + preview + time). Extract a cleaner title for matching/verification.
            parsed_title = self._parse_title(li_name)
            if parsed_title:
                # If we already extracted a plausible title from UI geometry, don't replace it
                # with a noisier parsed string that includes preview text.
                if not title_guess:
                    title_guess = parsed_title
                else:
                    tg = (title_guess or "").strip()
                    pt = (parsed_title or "").strip()
                    if pt and (pt == tg or pt.startswith(tg) or tg.startswith(pt)):
                        title_guess = tg if len(tg) <= len(pt) else pt
                    else:
                        title_guess = parsed_title

            # row_text is what we use for change detection; it should include preview+time when possible.
            row_text = li_name or title_guess
            out.append(
                ChatListEntry(
                    key=key,
                    index=idx,
                    title_guess=title_guess or li_name,
                    row_text=row_text,
                    unread_count=unread_count,
                    time_label=time_label,
                    wrapper=item,
                )
            )

        return out

    def _row_key(self, item, *, idx: int, title_guess: str, li_name: str) -> str:
        # Prefer automation id (stable across reordering in many WeChat builds).
        try:
            aid = item.element_info.automation_id or ""
        except Exception:
            aid = ""
        if aid:
            return "aid:" + aid

        # Prefer runtime_id when available; it's stable during a session.
        try:
            rid = item.element_info.runtime_id  # type: ignore[attr-defined]
        except Exception:
            rid = None
        if rid:
            try:
                return "rid:" + ",".join(str(x) for x in rid)
            except Exception:
                pass
        # Fallback: title guess + index.
        base = title_guess or li_name or "<unknown>"
        return f"idx:{idx}:{sha1_hex(base)[:12]}"

    def _parse_unread_count(self, li_name: str) -> Optional[int]:
        if not li_name:
            return None
        s = _norm_token(li_name)
        lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
        # Some builds put the unread badge on its own line, e.g. "Chat\n2\n10:44".
        if len(lines) >= 2:
            for i, seg in enumerate(lines):
                if re.fullmatch(r"\d{1,3}", seg):
                    if 0 < i < len(lines) - 1:
                        try:
                            return int(seg)
                        except Exception:
                            return None
        # Square-bracket badges appear in many WeChat UIA row labels, e.g. "Chat\n[3]\n...".
        # Parse those before the older English/Chinese heuristics.
        m = re.search(r"\[\s*(\d+)\s*\]", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        m = re.search(r"【\s*(\d+)\s*】", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        m = re.search(r"\(\s*(\d+)\s*\)", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        m = re.search(r"（\s*(\d+)\s*）", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None

        # Example: "BMM 2 unread message(s) ... 10:44"
        m = re.search(r"\b(\d+)\s+unread\s+message\(s\)", li_name, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        m = re.search(r"\b(\d+)\s+unread\s+message\b", li_name, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        m = re.search(r"\b(\d+)\s+unread\s+messages\b", li_name, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        # Chinese-ish variants (best-effort; may not appear in consumer WeChat).
        m = re.search(r"(\d+)\s*条\s*未读", li_name)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        return None

    def _parse_time_label(self, li_name: str) -> str:
        if not li_name:
            return ""
        # Often ends with "HH:MM". Use the last time-ish token.
        normalized = _norm_token(li_name).replace("\u00a0", " ").replace("\u2007", " ").replace("\u202f", " ")
        tokens = [t for t in re.split(r"\s+", normalized) if t]
        for t in reversed(tokens):
            if TIMEISH_RE.fullmatch(t):
                return t
        return ""

    def _parse_title(self, li_name: str) -> str:
        """
        Best-effort extraction of chat title from a ListItem.Name string.
        Typical shape (via UIA): "Title N unread message(s) Sender: Preview 10:44".
        """
        s = (li_name or "").strip()
        if not s:
            return ""

        # With unread count.
        m = re.match(r"^(.*?)\s+\d+\s+unread\s+message\(s\)\s+", s, re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
        m = re.match(r"^(.*?)\s+\d+\s+unread\s+messages\s+", s, re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()

        # Without unread count: strip trailing time and take the left-most chunk.
        t = self._parse_time_label(s)
        if t and s.endswith(t):
            s = s[: -len(t)].rstrip()
        # If there's a sender separator like "Name:" assume title is before that only if there's no spaces?
        # This is heuristic; return the full string if we're unsure.
        if "  " in s:
            return s.split("  ", 1)[0].strip()
        return s

    def _extract_chat_name(self, item) -> str:
        # Many WeChat builds don't set ListItem.Name reliably.
        # Prefer the *top* line text within the row, which tends to be the chat name.
        def is_noise(s: str) -> bool:
            if s.isdigit():
                return True
            if TIMEISH_RE.search(s):
                return True
            if s.lower() in {"pinned", "draft"}:
                return True
            if s in {"置顶", "草稿"}:
                return True
            return False

        try:
            item_r = item.rectangle()
            item_h = max(1, item_r.bottom - item_r.top)
            name_band_bottom = item_r.top + int(item_h * 0.55)
        except Exception:
            item_r = None
            name_band_bottom = None

        candidates: list[tuple[int, int, int, int, str]] = []
        try:
            for t in item.descendants(control_type="Text"):
                s = (t.window_text() or "").strip()
                if not s or is_noise(s):
                    continue
                try:
                    r = t.rectangle()
                    band = 0 if (name_band_bottom is not None and r.top <= name_band_bottom) else 1
                    # Prefer top-most, then left-most, then shorter (chat names are often shorter than previews).
                    candidates.append((band, r.top, r.left, len(s), s))
                except Exception:
                    # If we can't get geometry, keep it as a fallback.
                    candidates.append((1, 10**9, 10**9, len(s), s))
        except Exception:
            candidates = []

        if candidates:
            candidates.sort()
            return candidates[0][4]

        try:
            s = (item.window_text() or "").strip()
            if s and not is_noise(s):
                return s
        except Exception:
            pass
        try:
            return (item.element_info.name or "").strip()
        except Exception:
            return ""

    def _extract_unread(self, item) -> tuple[bool, Optional[int]]:
        # Heuristics: unread badge often appears as a small Text control with a digit.
        # Some builds might use a dot; we treat any "new" marker as unread=True.
        try:
            li_name = _norm_token(item.element_info.name or "")
            li_count = self._parse_unread_count(li_name)
            if li_count is not None:
                return True, li_count
        except Exception:
            pass
        markers: list[str] = []
        try:
            for t in item.descendants(control_type="Text"):
                s = (t.window_text() or "").strip()
                if s:
                    markers.append(s)
        except Exception:
            markers = []

        for s in markers:
            if re.fullmatch(r"\\d+", s):
                try:
                    return True, int(s)
                except Exception:
                    return True, None
            if any(x in s.lower() for x in ["new", "unread"]):
                return True, None
            if any(x in s for x in ["未读", "新", "条"]):
                # "x条" is common, but we don't rely on parsing it.
                return True, None
        return False, None

    def open_chat(self, row: ChatRow, *, allow_click: bool = True) -> None:
        # Best-effort foreground. If it fails, avoid click_input to prevent clicking other apps.
        fg_ok = True
        try:
            fg_ok = self.ensure_foreground(settle_seconds=0.10)
        except Exception:
            fg_ok = False
        # Ensure the row is actually visible (UIA can return off-screen virtualized items).
        try:
            row.wrapper.scroll_into_view()
            time.sleep(0.05)
        except Exception:
            pass
        # Prefer UIA patterns over mouse clicks (less brittle, avoids desktop focus issues).
        try:
            row.wrapper.select()
            time.sleep(0.08)
        except Exception:
            pass
        if not self.single_window_mode:
            # In some builds, Invoke can pop-out a conversation into a new top-level window.
            # When the user prefers single-window behavior, avoid Invoke and rely on select/click.
            try:
                row.wrapper.invoke()
                time.sleep(0.08)
            except Exception:
                pass

        # Physical clicks are a last resort. Only do this if WeChat is foreground,
        # otherwise click_input can land on the wrong app.
        if allow_click and fg_ok:
            clicked = False
            try:
                row.wrapper.click_input()
                clicked = True
            except Exception:
                try:
                    row.wrapper.click()
                    clicked = True
                except Exception as e:
                    # Keep single-window behavior intact: no hard failure on click here.
                    # We still rely on Select/Invoke and caller-side verification.
                    if not clicked and not self.single_window_mode:
                        raise RuntimeError(f"Failed to open chat row: {e}") from e
            # Double-click is kept only for non-single-window mode where pop-out
            # behavior is already enabled.
            if clicked and not self.single_window_mode:
                try:
                    time.sleep(0.05)
                    row.wrapper.double_click_input()
                except Exception:
                    pass
        time.sleep(0.15)
        # If the chat opens in a separate top-level window, attach it for message extraction.
        if not self.single_window_mode:
            try:
                li_name = _norm_token(row.wrapper.element_info.name or "")
            except Exception:
                li_name = ""
            hint = self._extract_chat_name(row.wrapper) or self._parse_title(li_name) or li_name
            hint = (hint or "").strip()
            if hint:
                try:
                    self.attach_chat_window(title_hint=hint, timeout_seconds=1.0)
                except Exception:
                    pass

    def _message_source_windows(self) -> list[object]:
        """
        Candidate windows for the conversation message list.

        In ideal single-window mode this is the main window. Some WeChat builds
        still surface messages in a secondary chat window; in that case add it too.
        """
        wins: list[object] = []

        try:
            mw = self.main_window
        except Exception:
            mw = None

        seen: set[int] = set()

        def _add(win):
            if win is None:
                return
            try:
                key = id(win)
            except Exception:
                key = 0
            if key in seen:
                return
            seen.add(key)
            wins.append(win)

        _add(mw)

        # If we already detected a secondary chat window, search there too.
        _add(self._chat_window)

        # Opportunistic attach for cases where the active conversation moved to a
        # separate popup while we're still in single-window mode.
        if self.single_window_mode and self._chat_window is None:
            try:
                self.attach_chat_window(title_hint="", timeout_seconds=0.8)
            except Exception:
                pass
            _add(self._chat_window)

        return wins

    def get_selected_chat_row_text(self) -> str:
        """
        Return the UIA Name of the currently selected chat list row (best-effort).
        """
        chat_list = self.find_chat_list()
        try:
            items = chat_list.descendants(control_type="ListItem")
        except Exception:
            items = []
        for it in items:
            try:
                # UIA SelectionItem pattern
                if it.iface_selection_item.CurrentIsSelected:  # type: ignore[attr-defined]
                    return _norm_token(it.element_info.name or "")
            except Exception:
                continue
        return ""

    def get_header_chat_name(self) -> str:
        # When chats open in a dedicated window, the window title is a good header.
        if not self.single_window_mode and self._chat_window is not None:
            try:
                t = (self._chat_window.element_info.name or "").strip()  # type: ignore[attr-defined]
                if t:
                    return t
            except Exception:
                pass
        # Prefer stable automation ids when present (much faster and more reliable than heuristics).
        try:
            for t in self.active_window.descendants(control_type="Text"):
                try:
                    aid = t.element_info.automation_id or ""
                except Exception:
                    aid = ""
                if not aid:
                    continue
                if aid.endswith("current_chat_name_label") or "current_chat_name_label" in aid:
                    s = (t.window_text() or "").strip()
                    if s:
                        return s
        except Exception:
            pass

        # Best-effort fallback: header is often a Text control near top of the right pane.
        try:
            rect = self.window.rectangle()
        except Exception:
            rect = None
        candidates: list[tuple[int, str]] = []
        for t in self.active_window.descendants(control_type="Text"):
            try:
                s = (t.window_text() or "").strip()
                if not s:
                    continue
                r = t.rectangle()
                if rect is not None:
                    # right side, near top
                    if r.top > rect.top + 220:
                        continue
                    if r.left < rect.left + int((rect.right - rect.left) * 0.35):
                        continue
                # Prefer bigger/centered header-ish text: use width.
                score = (r.right - r.left) - abs(r.top - (rect.top if rect else 0))
                candidates.append((score, s))
            except Exception:
                continue
        candidates.sort(reverse=True)
        return candidates[0][1] if candidates else ""

    def extract_recent_messages(self, *, max_messages: int = 120) -> list[ExtractedMessage]:
        """
        Extract a best-effort list of recent visible messages from the currently opened chat.

        This is intentionally heuristic: WeChat's UIA surface varies by version/language.
        """
        msg_list = self._find_message_list()
        items = []
        try:
            items = msg_list.descendants(control_type="ListItem")
        except Exception:
            items = []
        # UIA enumeration order is not stable across WeChat builds.
        # Normalize to visual top->bottom order before applying recency slicing.
        try:
            items.sort(key=lambda it: int((it.rectangle().top + it.rectangle().bottom) // 2))
        except Exception:
            pass
        # Take the last N visible items (UIA order is usually visual/top->bottom).
        if len(items) > max_messages:
            items = items[-max_messages:]

        out: list[ExtractedMessage] = []
        current_sender: Optional[str] = None
        for it in items:
            em = self._extract_message_from_item(it)
            if em is None:
                continue
            if em.kind == "sender_only":
                current_sender = em.sender
                continue
            if (not em.sender or em.sender == "(unknown)") and current_sender:
                em = ExtractedMessage(
                    sender=current_sender,
                    text=em.text,
                    is_image=em.is_image,
                    signature=em.signature,
                    legacy_signature=em.legacy_signature,
                    rect=em.rect,
                    wrapper=em.wrapper,
                    kind=em.kind,
                    msg_type=em.msg_type,
                    attachment_name=em.attachment_name,
                )
            out.append(em)
        return out

    def _find_message_list(self):
        # Prefer known automation id when present.
        for win in self._message_source_windows():
            for lst in win.descendants(control_type="List"):
                try:
                    if (lst.element_info.automation_id or "") == "chat_message_list":
                        return lst
                except Exception:
                    continue

            # Best signal: the message list typically sits immediately to the right of the chat list.
            try:
                chat_list = self.find_chat_list()
                chat_r = chat_list.rectangle()
                min_left = chat_r.right - 5
            except Exception:
                min_left = None

            candidates = []
            for lst in win.descendants(control_type="List"):
                try:
                    r = lst.rectangle()
                except Exception:
                    continue
                if min_left is not None and r.left < min_left:
                    continue
                candidates.append(lst)

            if candidates:
                return self._pick_best_list(candidates, prefer_left=False, min_items=1)

        # Fallback: choose a list that's not on the far left.
        try:
            w = self.main_window.rectangle()
            width = max(1, w.right - w.left)
            right_min = w.left + int(width * 0.20)
        except Exception:
            right_min = None

        lists = []
        for win in self._message_source_windows():
            for lst in win.descendants(control_type="List"):
                if right_min is None:
                    lists.append(lst)
                    continue
                try:
                    r = lst.rectangle()
                except Exception:
                    continue
                if r.left >= right_min:
                    lists.append(lst)
        return self._pick_best_list(lists, prefer_left=False, min_items=1)

    def _extract_message_from_item(self, item) -> Optional[ExtractedMessage]:
        rect_t: tuple[int, int, int, int] | None = None
        try:
            r = item.rectangle()
            rect_t = (int(r.left), int(r.top), int(r.right), int(r.bottom))
        except Exception:
            rect_t = None

        # UIA surface varies a lot by WeChat version; don't assume only Text controls.
        texts: list[str] = []
        try:
            item_name = (item.element_info.name or "").strip()
        except Exception:
            item_name = ""
        if item_name:
            texts.append(item_name)
        try:
            for t in item.descendants():
                try:
                    ctype = t.element_info.control_type
                except Exception:
                    continue
                if ctype not in {"Text", "Edit", "Document"}:
                    continue
                try:
                    s = (t.window_text() or "").strip()
                except Exception:
                    s = ""
                if not s:
                    try:
                        s = (t.element_info.name or "").strip()
                    except Exception:
                        s = ""
                if s:
                    texts.append(s)
        except Exception:
            texts = []

        # De-dupe adjacent identical text nodes.
        deduped: list[str] = []
        for s in texts:
            if not deduped or deduped[-1] != s:
                deduped.append(s)
        texts = deduped

        item0 = texts[0] if texts else ""
        item0_norm = _norm_token(item0)
        if item0_norm and TIMEISH_RE.fullmatch(item0_norm):
            return None
        # Some builds add duplicates; if *all* tokens look like time separators, skip.
        if texts and all(TIMEISH_RE.fullmatch(_norm_token(t) or "") for t in texts):
            return None

        # File messages: keep a marker and capture the displayed filename if available.
        attachment_name: str | None = None
        msg_type = "text"
        if item0_norm.lower().startswith("file\n") or item0_norm.startswith("文件\n"):
            msg_type = "file"
            parts = [p.strip() for p in item0_norm.split("\n") if p.strip()]
            # Common layout: "File" / "文件", then filename, then size.
            if len(parts) >= 2:
                attachment_name = parts[1]

        has_image_marker = any(_is_image_marker_text(t) for t in texts)
        is_image = False
        if msg_type != "file":
            is_image = has_image_marker or self._looks_like_image_bubble(item, texts)
        if is_image:
            msg_type = "image"
            # Prefer the image bubble rectangle (avoids virtualization returning identical ListItem rects).
            try:
                item_area = 0
                try:
                    rr0 = item.rectangle()
                    item_area = max(1, int(rr0.right - rr0.left)) * max(1, int(rr0.bottom - rr0.top))
                except Exception:
                    item_area = 0

                best = None
                best_area = 0
                for d in item.descendants():
                    try:
                        ctype = d.element_info.control_type
                    except Exception:
                        continue
                    if ctype not in {"Button", "Pane", "Custom", "Image", "Group"}:
                        continue
                    try:
                        rr = d.rectangle()
                        w = int(rr.right - rr.left)
                        h = int(rr.bottom - rr.top)
                        area = max(0, w) * max(0, h)
                    except Exception:
                        continue
                    if area <= 0 or w < 30 or h < 30:
                        continue
                    if item_area and area >= int(item_area * 0.95):
                        continue
                    if area > best_area:
                        best_area = area
                        best = rr
                if best is not None:
                    rect_t = (int(best.left), int(best.top), int(best.right), int(best.bottom))
            except Exception:
                pass

        sender = ""
        content = ""

        # Prefer parsing from the list-item name; many WeChat builds expose most content there.
        if _is_image_marker_text(item0_norm):
            is_image = True
            msg_type = "image"
            content = "[Image]"
        elif item0_norm and "\u2005" in item0_norm:
            sender, content = item0_norm.split("\u2005", 1)
            if not content.strip():
                raw = item0_norm
                signature = sha1_hex(f"{sender}\n(sender_only)\n{raw}")
                return ExtractedMessage(
                    sender=sender.strip() or "(unknown)",
                    text="",
                    is_image=False,
                    signature=signature,
                    kind="sender_only",
                    rect=rect_t,
                    wrapper=item,
                )
        elif item0_norm and "\t" in item0_norm:
            sender, content = item0_norm.split("\t", 1)
        elif item0_norm and "\n" in item0_norm:
            parts = [p.strip() for p in item0_norm.split("\n") if p.strip()]
            parts = [p for p in parts if not TIMEISH_RE.fullmatch(p)]
            if parts and parts[0].startswith("@") and len(parts) >= 2:
                sender = parts[0]
                content = " ".join(parts[1:])
            else:
                content = " ".join(parts)
        else:
            # Filter out time-ish nodes and use a heuristic fallback.
            if texts:
                msg_texts = [t for t in texts if not TIMEISH_RE.search(t)]
                if msg_texts:
                    if len(msg_texts) >= 2:
                        sender = msg_texts[0]
                        content = max(msg_texts[1:], key=len)
                    else:
                        content = msg_texts[0]
        if is_image and _is_image_marker_text(content):
            content = "[Image]"
        if is_image and not content:
            content = "[Image]"

        # If we still can't find anything meaningful, skip.
        if not content and not is_image:
            return None
        raw = "|".join(texts) if texts else ("[Image]" if is_image else "")

        # Image rows often expose little text (often just "[Image]"), so include the
        # runtime id when available to avoid collapsing distinct images into one.
        # For text messages we keep the old stable text-only signature.
        signature_basis = f"{sender}\n{msg_type}\n{content}\n{raw}"
        signature_extra = ""

        # Legacy signature (used by older builds of this tool) for best-effort compatibility
        # with existing local state/db de-dupe after upgrades.
        rid_s = ""
        try:
            rid = item.element_info.runtime_id  # type: ignore[attr-defined]
        except Exception:
            rid = None
        if rid:
            try:
                rid_s = ",".join(str(x) for x in rid)
            except Exception:
                rid_s = ""
        # Keep a compatibility signature without runtime id for older DB/state rows.
        legacy_signature = sha1_hex(f"{sender}\n{msg_type}\n{content}\n{raw}")

        if rid_s and msg_type == "image":
            signature_extra = f"\n{rid_s}"

        signature = sha1_hex(f"{signature_basis}{signature_extra}")

        return ExtractedMessage(
            sender=sender.strip() or "(unknown)",
            text=content.strip(),
            is_image=is_image,
            signature=signature,
            legacy_signature=legacy_signature,
            kind="message",
            msg_type=msg_type,
            attachment_name=attachment_name,
            rect=rect_t,
            wrapper=item,
        )

    def _looks_like_image_bubble(self, item, texts: list[str]) -> bool:
        # If there is non-time text and no explicit image marker, don't infer image from controls
        # because avatar icons can appear in normal text rows.
        norm_texts = [_norm_token(t) for t in texts if _norm_token(t)]
        non_time_texts = [t for t in norm_texts if not TIMEISH_RE.search(t)]
        if any(t.lower().startswith("file\n") or t.startswith("文件\n") for t in non_time_texts):
            return False
        has_marker = any(_is_image_marker_text(t) for t in non_time_texts)
        if non_time_texts and not has_marker:
            return False

        try:
            rr0 = item.rectangle()
            item_area = max(1, int(rr0.right - rr0.left)) * max(1, int(rr0.bottom - rr0.top))
        except Exception:
            item_area = 0

        try:
            for c in item.descendants():
                try:
                    ctype = c.element_info.control_type
                    name = (c.element_info.name or "").lower()
                    aid = (c.element_info.automation_id or "").lower()
                    r = c.rectangle()
                    w = int(r.right - r.left)
                    h = int(r.bottom - r.top)
                    area = max(0, w) * max(0, h)
                except Exception:
                    continue
                if area <= 0:
                    continue
                if ctype in {"Image"}:
                    if not non_time_texts:
                        return True
                    if (w >= 48 and h >= 48) or (item_area and area >= int(item_area * 0.12)):
                        return True
                if ctype in {"Button"} and any(k in name for k in ["image", "picture", "photo", "pic"]):
                    return True
                if any(k in aid for k in ["image", "picture", "photo", "pic"]):
                    return True
        except Exception:
            return False
        return False


def format_log_line(chat: str, msg: ExtractedMessage) -> str:
    # Use capture time; UIA doesn't reliably provide per-message timestamps.
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sender = msg.sender or "(unknown)"
    text = msg.text or ""
    return f"[{ts}] {sender}: {text}"
