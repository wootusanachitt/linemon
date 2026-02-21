from __future__ import annotations

import socket

try:
    import servicemanager
    import win32service
    import win32serviceutil
except ImportError as exc:
    raise SystemExit("pywin32 is required to run/install the Windows service.") from exc

from linemon.api_server import build_server


class LinemonApiService(win32serviceutil.ServiceFramework):
    _svc_name_ = "LinemonApiService"
    _svc_display_name_ = "Linemon Chat API Service"
    _svc_description_ = "HTTP API wrapper for linemon chat message sending."

    def __init__(self, args):
        super().__init__(args)
        socket.setdefaulttimeout(60)
        self._server = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass

    def SvcDoRun(self):
        try:
            self._server, runtime = build_server()
            servicemanager.LogInfoMsg(
                f"{self._svc_name_} listening on {runtime.bind_host}:{runtime.bind_port}"
            )
            self._server.serve_forever(poll_interval=0.5)
        except Exception as exc:
            servicemanager.LogErrorMsg(f"{self._svc_name_} failed: {exc}")
            raise
        finally:
            if self._server is not None:
                try:
                    self._server.server_close()
                except Exception:
                    pass


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(LinemonApiService)
