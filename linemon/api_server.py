from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _tail_text(text: str, *, max_chars: int = 4000) -> str:
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _resolve_path(raw_path: str, *, root: Path) -> Path:
    p = Path(raw_path)
    if not p.is_absolute():
        p = root / p
    return p.resolve()


@dataclass(frozen=True)
class RuntimeSettings:
    bind_host: str
    bind_port: int
    api_token: str
    python_exe: Path
    line_capture_script: Path
    linemon_config: Path
    command_timeout_sec: float
    config_file: Path


def _load_json_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"API config must be a JSON object: {path}")
    return parsed


def load_runtime_settings(
    *,
    config_file: str | Path | None = None,
    host_override: str | None = None,
    port_override: int | None = None,
) -> RuntimeSettings:
    root = _project_root()
    cfg_file = _resolve_path(str(config_file), root=root) if config_file else (root / "linemon_api_config.json")
    cfg_json = _load_json_config(cfg_file)

    default_python = root / ".venv" / "Scripts" / "python.exe"
    if not default_python.exists():
        default_python = Path(sys.executable)

    bind_host = (host_override or cfg_json.get("bind_host") or "0.0.0.0").strip()
    bind_port = port_override if port_override is not None else _to_int(cfg_json.get("bind_port"), default=8788)
    api_token = str(cfg_json.get("api_token") or "").strip()
    python_raw = str(cfg_json.get("python_exe") or str(default_python))
    capture_raw = str(cfg_json.get("line_capture_script") or "line_capture.py")
    linemon_cfg_raw = str(cfg_json.get("linemon_config") or "linemon_config.json")
    timeout_sec = _to_float(cfg_json.get("command_timeout_sec"), default=120.0)

    python_exe = _resolve_path(python_raw, root=root)
    capture_script = _resolve_path(capture_raw, root=root)
    linemon_config = _resolve_path(linemon_cfg_raw, root=root)

    if not python_exe.exists():
        raise RuntimeError(f"python_exe not found: {python_exe}")
    if not capture_script.exists():
        raise RuntimeError(f"line_capture_script not found: {capture_script}")
    if bind_port <= 0 or bind_port > 65535:
        raise RuntimeError(f"bind_port out of range: {bind_port}")
    if timeout_sec <= 0:
        raise RuntimeError(f"command_timeout_sec must be > 0: {timeout_sec}")

    return RuntimeSettings(
        bind_host=bind_host,
        bind_port=bind_port,
        api_token=api_token,
        python_exe=python_exe,
        line_capture_script=capture_script,
        linemon_config=linemon_config,
        command_timeout_sec=timeout_sec,
        config_file=cfg_file,
    )


class ChatSender:
    def __init__(self, runtime: RuntimeSettings) -> None:
        self.runtime = runtime
        self._lock = threading.Lock()

    def send(self, *, chat: str, text: str) -> dict[str, Any]:
        chat = chat.strip()
        msg = str(text)
        if not chat:
            raise ValueError("chat must not be empty")
        if not msg.strip():
            raise ValueError("text must not be empty")

        cmd = [
            str(self.runtime.python_exe),
            "-u",
            str(self.runtime.line_capture_script),
            "--config",
            str(self.runtime.linemon_config),
            "--send-chat",
            chat,
            "--send-text",
            msg,
        ]

        started = time.monotonic()
        with self._lock:
            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=self.runtime.command_timeout_sec,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                return {
                    "ok": False,
                    "timeout": True,
                    "exit_code": None,
                    "duration_ms": elapsed_ms,
                    "stdout": _tail_text((exc.stdout or "").strip()),
                    "stderr": _tail_text((exc.stderr or "").strip()),
                }

        elapsed_ms = int((time.monotonic() - started) * 1000)
        stdout_txt = (proc.stdout or "").strip()
        stderr_txt = (proc.stderr or "").strip()

        return {
            "ok": proc.returncode == 0,
            "timeout": False,
            "exit_code": proc.returncode,
            "duration_ms": elapsed_ms,
            "stdout": _tail_text(stdout_txt),
            "stderr": _tail_text(stderr_txt),
        }


class LinemonApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        runtime: RuntimeSettings,
    ) -> None:
        super().__init__(server_address, LinemonApiHandler)
        self.runtime = runtime
        self.sender = ChatSender(runtime)


class LinemonApiHandler(BaseHTTPRequestHandler):
    server_version = "LinemonApi/1.0"

    @property
    def _api_server(self) -> LinemonApiServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep HTTP logging concise and stdout-safe for service usage.
        msg = fmt % args
        print(f"[api] {self.address_string()} {self.command} {self.path} {msg}")

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        cl_header = self.headers.get("Content-Length") or "0"
        try:
            length = int(cl_header)
        except ValueError as e:
            raise ValueError("invalid Content-Length") from e
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError("body must be valid JSON") from e
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        return payload

    def _authorized(self) -> bool:
        expected = self._api_server.runtime.api_token
        if not expected:
            return True
        actual = (self.headers.get("Authorization") or "").strip()
        return actual == f"Bearer {expected}"

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/health":
            self._write_json(
                200,
                {
                    "ok": True,
                    "service": "linemon-api",
                    "bind": f"{self._api_server.runtime.bind_host}:{self._api_server.runtime.bind_port}",
                },
            )
            return
        self._write_json(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in {"/api/send-chat", "/v1/chat/send"}:
            self._write_json(404, {"ok": False, "error": "not found"})
            return

        if not self._authorized():
            self._write_json(401, {"ok": False, "error": "unauthorized"})
            return

        try:
            payload = self._read_json_body()
            chat = str(payload.get("chat") or "").strip()
            text = str(payload.get("text") or "")
            result = self._api_server.sender.send(chat=chat, text=text)
        except ValueError as e:
            self._write_json(400, {"ok": False, "error": str(e)})
            return
        except Exception as e:
            self._write_json(500, {"ok": False, "error": str(e)})
            return

        status = 200 if result.get("ok") else (504 if result.get("timeout") else 500)
        self._write_json(
            status,
            {
                "ok": bool(result.get("ok")),
                "chat": chat,
                "result": result,
            },
        )


def build_server(
    *,
    config_file: str | Path | None = None,
    host_override: str | None = None,
    port_override: int | None = None,
) -> tuple[LinemonApiServer, RuntimeSettings]:
    runtime = load_runtime_settings(
        config_file=config_file,
        host_override=host_override,
        port_override=port_override,
    )
    server = LinemonApiServer((runtime.bind_host, runtime.bind_port), runtime)
    return server, runtime


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Linemon HTTP API wrapper for sending chat messages.")
    ap.add_argument(
        "--config-file",
        default="",
        help="Path to API config JSON (default: linemon_api_config.json).",
    )
    ap.add_argument("--host", default="", help="Optional bind host override.")
    ap.add_argument("--port", type=int, default=0, help="Optional bind port override.")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    cfg_file = args.config_file.strip() or None
    host = args.host.strip() or None
    port = args.port if args.port > 0 else None

    server, runtime = build_server(config_file=cfg_file, host_override=host, port_override=port)
    print(
        f"[api] listening on {runtime.bind_host}:{runtime.bind_port} "
        f"(config={runtime.config_file}, target={runtime.line_capture_script})"
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
