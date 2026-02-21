from __future__ import annotations

import ctypes
import sys


def make_dpi_aware() -> None:
    """
    Make the current process DPI aware so UI Automation coordinates match screen pixels.
    Best-effort; safe to call multiple times.
    """
    if sys.platform != "win32":
        return

    # Prefer Per-Monitor V2 when available (Win10+).
    try:
        # -4 == DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2
        ctypes.windll.user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4))
        return
    except Exception:
        pass

    # Fallbacks.
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)  # PROCESS_PER_MONITOR_DPI_AWARE
        return
    except Exception:
        pass

    try:
        ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass

