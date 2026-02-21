from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass(frozen=True)
class Rect:
    left: int
    top: int
    right: int
    bottom: int

    @property
    def width(self) -> int:
        return max(1, int(self.right - self.left))

    @property
    def height(self) -> int:
        return max(1, int(self.bottom - self.top))


class ScreenGrabber:
    """
    Thin wrapper over `mss` to grab screen pixels as numpy arrays.

    Returns BGR arrays to match OpenCV conventions.
    """

    def __init__(self) -> None:
        self._sct = None

    def _get(self):
        if self._sct is None:
            import mss

            self._sct = mss.mss()
        return self._sct

    def grab_bgr(self, rect: Rect) -> np.ndarray:
        sct = self._get()
        shot = sct.grab(
            {"left": int(rect.left), "top": int(rect.top), "width": rect.width, "height": rect.height}
        )
        img = np.asarray(shot)  # BGRA
        return img[:, :, :3].copy()  # BGR

    def grab_hwnd_bgr(self, hwnd: int) -> Optional[np.ndarray]:
        """
        Capture a window by HWND using PrintWindow (best-effort).

        Unlike screen-rect capture, this can work even if the window is partially covered.
        Some GPU-accelerated/UWP windows may return black or fail; caller must fall back.
        """
        if not hwnd:
            return None
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:
            ctypes = None  # type: ignore[assignment]
            wintypes = None  # type: ignore[assignment]
        try:
            import win32con  # type: ignore
            import win32gui  # type: ignore
            import win32ui  # type: ignore
        except Exception:
            return None

        try:
            left, top, right, bottom = win32gui.GetWindowRect(int(hwnd))
            width = max(1, int(right - left))
            height = max(1, int(bottom - top))
        except Exception:
            return None

        hwindc = None
        srcdc = None
        memdc = None
        bmp = None
        try:
            hwindc = win32gui.GetWindowDC(int(hwnd))
            srcdc = win32ui.CreateDCFromHandle(hwindc)
            memdc = srcdc.CreateCompatibleDC()
            bmp = win32ui.CreateBitmap()
            bmp.CreateCompatibleBitmap(srcdc, width, height)
            memdc.SelectObject(bmp)

            ok = 0
            try:
                if ctypes is not None and wintypes is not None:
                    user32 = ctypes.windll.user32
                    user32.PrintWindow.argtypes = [wintypes.HWND, wintypes.HDC, wintypes.UINT]
                    user32.PrintWindow.restype = wintypes.BOOL
                    # Try PW_RENDERFULLCONTENT (2) first; fall back to 0.
                    ok = int(user32.PrintWindow(int(hwnd), int(memdc.GetSafeHdc()), 2))
                    if not ok:
                        ok = int(user32.PrintWindow(int(hwnd), int(memdc.GetSafeHdc()), 0))
            except Exception:
                ok = 0
            if ok != 1:
                return None

            # GetBitmapBits yields BGRA bytes, bottom-up.
            raw = bmp.GetBitmapBits(True)
            img = np.frombuffer(raw, dtype=np.uint8)
            if img.size < (width * height * 4):
                return None
            img = img.reshape((height, width, 4))
            # Note: some windows return top-down pixel buffers via PrintWindow on modern Windows.
            # Don't flip here; if a specific app renders upside-down, handle it at the call site.
            return img[:, :, :3].copy()  # BGR
        except Exception:
            return None
        finally:
            try:
                if bmp is not None:
                    win32gui.DeleteObject(bmp.GetHandle())
            except Exception:
                pass
            try:
                if memdc is not None:
                    memdc.DeleteDC()
            except Exception:
                pass
            try:
                if srcdc is not None:
                    srcdc.DeleteDC()
            except Exception:
                pass
            try:
                if hwindc is not None:
                    win32gui.ReleaseDC(int(hwnd), hwindc)
            except Exception:
                pass
