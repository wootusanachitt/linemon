from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class SingleInstance:
    ok: bool
    reason: str = ""


_mutex_handle = None
_lock_file = None


def acquire_mutex(name: str) -> SingleInstance:
    """
    Prevent accidental double-starts.

    We have seen some Windows environments spawn a duplicate python.exe for UIA/COM
    dependencies; a named mutex makes the secondary instance exit immediately.
    """
    global _mutex_handle

    if sys.platform != "win32":
        return SingleInstance(ok=True)

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        # CreateMutexW returns a handle even if it already exists.
        #
        # Important: unprefixed names are per-session ("Local\\"). Some Windows setups can
        # spawn duplicate python.exe instances in different sessions, so prefer a global
        # mutex when possible and fall back to a session-local one.

        raw = str(name or "").strip()
        if not raw:
            return SingleInstance(ok=True)

        candidates: list[str] = []
        if raw.startswith("Global\\") or raw.startswith("Local\\"):
            candidates = [raw]
        else:
            candidates = [f"Global\\{raw}", raw]

        created = False
        for nm in candidates:
            h = kernel32.CreateMutexW(None, False, str(nm))
            if not h:
                continue
            _mutex_handle = h
            # ERROR_ALREADY_EXISTS = 183
            if kernel32.GetLastError() == 183:
                return SingleInstance(ok=False, reason="already running")
            created = True
            break

        # Extra guard: file lock works across sessions even when Global\\ mutex creation is blocked.
        try:
            import msvcrt  # type: ignore
            import re

            raw2 = raw
            # Keep the lock name filesystem-safe and short.
            safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw2)[:80] or "wcmon"
            # Put the lock file next to the repo to ensure it is shared even across
            # sandboxed/Python-distribution temp directories.
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            lock_path = os.path.join(base_dir, f".{safe}.lock")
            f = open(lock_path, "a+", encoding="utf-8")
            try:
                # Lock 1 byte; non-blocking.
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                try:
                    f.close()
                except Exception:
                    pass
                # Release mutex if we created it in this process.
                if created and _mutex_handle:
                    try:
                        kernel32.CloseHandle(int(_mutex_handle))
                    except Exception:
                        pass
                    # Avoid double-close in release_mutex.
                    _mutex_handle = None
                return SingleInstance(ok=False, reason="already running")
            global _lock_file
            _lock_file = f
        except Exception:
            # If file locking fails for any reason, don't block the app.
            pass

        if created:
            return SingleInstance(ok=True)

        # If we couldn't create any mutex, don't block the app.
        return SingleInstance(ok=True)
    except Exception:
        # If mutex fails for any reason, don't block the app.
        return SingleInstance(ok=True)


def release_mutex() -> None:
    global _mutex_handle, _lock_file
    lf = _lock_file
    _lock_file = None
    if lf is not None:
        try:
            import msvcrt  # type: ignore

            try:
                msvcrt.locking(lf.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
        except Exception:
            pass
        try:
            lf.close()
        except Exception:
            pass
    if sys.platform != "win32":
        return
    h = _mutex_handle
    _mutex_handle = None
    if not h:
        return
    try:
        import ctypes

        ctypes.windll.kernel32.CloseHandle(int(h))
    except Exception:
        pass
