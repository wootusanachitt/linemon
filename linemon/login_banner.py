from __future__ import annotations

import queue
import threading


class LoginBanner:
    """
    Small always-on-top banner used while waiting for WeChat login.

    This is best-effort and should never crash the monitor when UI toolkits
    are unavailable.
    """

    def __init__(self, *, text: str = "Login your wechat", enabled: bool = True) -> None:
        self._default_text = str(text or "Login your wechat")
        self._enabled = bool(enabled)
        self._started = False
        self._thread: threading.Thread | None = None
        self._queue: queue.Queue[tuple[str, str | None]] = queue.Queue()

    def start(self) -> None:
        if not self._enabled or self._started:
            return
        self._started = True
        t = threading.Thread(target=self._run, name="wcmon-login-banner", daemon=True)
        self._thread = t
        t.start()

    def show(self, text: str | None = None) -> None:
        if not self._enabled:
            return
        self.start()
        self._queue.put(("show", text or self._default_text))

    def hide(self) -> None:
        if not self._enabled or not self._started:
            return
        self._queue.put(("hide", None))

    def close(self) -> None:
        if not self._enabled or not self._started:
            return
        self._queue.put(("close", None))
        t = self._thread
        if t is not None:
            t.join(timeout=1.5)

    def _run(self) -> None:
        try:
            import tkinter as tk
        except Exception:
            return

        try:
            root = tk.Tk()
            root.withdraw()
            root.overrideredirect(True)
            root.attributes("-topmost", True)
            try:
                root.attributes("-alpha", 0.95)
            except Exception:
                pass

            frame = tk.Frame(root, bg="#FFF4D4", bd=1, relief="solid", padx=14, pady=8)
            frame.pack(fill="both", expand=True)
            label = tk.Label(
                frame,
                text=self._default_text,
                bg="#FFF4D4",
                fg="#2E2E2E",
                font=("Segoe UI", 11, "bold"),
            )
            label.pack(fill="both", expand=True)

            def _place_window() -> None:
                root.update_idletasks()
                width = max(root.winfo_reqwidth(), 220)
                height = max(root.winfo_reqheight(), 44)
                x = max(12, int(root.winfo_screenwidth() - width - 24))
                y = 24
                root.geometry(f"{width}x{height}+{x}+{y}")

            def _pump() -> None:
                try:
                    while True:
                        cmd, payload = self._queue.get_nowait()
                        if cmd == "show":
                            label.configure(text=(payload or self._default_text))
                            _place_window()
                            root.deiconify()
                            try:
                                root.lift()
                            except Exception:
                                pass
                        elif cmd == "hide":
                            root.withdraw()
                        elif cmd == "close":
                            root.withdraw()
                            root.destroy()
                            return
                except queue.Empty:
                    pass
                root.after(140, _pump)

            root.after(0, _pump)
            root.mainloop()
        except Exception:
            return
