from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests

from linemon.env import env
from linemon.kv_state import KVState


def _rfc3339_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class NotifyConfig:
    url: str
    token: str
    source: str = "wechat-monitor"
    timeout_seconds: float = 8.0

    @staticmethod
    def from_env() -> Optional["NotifyConfig"]:
        url = (env("NOTIFY_URL") or "").strip()
        tok = (env("NOTIFY_TOKEN") or "").strip()
        if not url or not tok:
            return None
        source = (env("NOTIFY_SOURCE") or "wechat-monitor").strip() or "wechat-monitor"
        try:
            timeout = float((env("NOTIFY_TIMEOUT_SECONDS") or "8").strip())
        except Exception:
            timeout = 8.0
        return NotifyConfig(url=url, token=tok, source=source, timeout_seconds=max(1.0, timeout))


class Notifier:
    """
    Best-effort notify client.

    Notifications are hints only; the frontend re-queries MySQL by cursor/timestamp.
    """

    def __init__(self, cfg: NotifyConfig, *, state_path: str) -> None:
        self.cfg = cfg
        self._state = KVState(state_path)
        self._sess = requests.Session()
        # Last-call debug (best-effort; do not rely on this for correctness).
        self.last_event_id: str | None = None
        self.last_seq: int | None = None
        self.last_status_code: int | None = None
        self.last_response_json: dict | None = None
        self.last_error: str | None = None

    @staticmethod
    def from_env(*, state_path: str) -> Optional["Notifier"]:
        cfg = NotifyConfig.from_env()
        if cfg is None:
            return None
        return Notifier(cfg, state_path=state_path)

    def close(self) -> None:
        try:
            self._state.close()
        except Exception:
            pass
        try:
            self._sess.close()
        except Exception:
            pass

    def notify_delta(
        self,
        *,
        changes: Iterable[str],
        room_ids: Iterable[int] | None = None,
        message_ids: Iterable[int] | None = None,
        attachment_ids: Iterable[int] | None = None,
        captured_at_max: datetime | None = None,
        cursor: str | None = None,
        max_ids: int = 80,
    ) -> bool:
        # reset last-call debug
        self.last_event_id = None
        self.last_seq = None
        self.last_status_code = None
        self.last_response_json = None
        self.last_error = None

        ch = sorted({str(c).strip() for c in changes if str(c).strip()})
        if not ch:
            return True

        now = datetime.now(timezone.utc)
        cap = captured_at_max or now
        cap_s = _rfc3339_z(cap)
        sent_s = _rfc3339_z(now)

        rid = []
        if room_ids is not None:
            # Keep payload small and stable.
            rid = sorted({int(x) for x in room_ids if int(x) > 0})[: max(1, int(max_ids))]

        mids = []
        if message_ids is not None:
            mids = list(dict.fromkeys(int(x) for x in message_ids if int(x) > 0))[: max(1, int(max_ids))]

        aids = []
        if attachment_ids is not None:
            aids = list(dict.fromkeys(int(x) for x in attachment_ids if int(x) > 0))[: max(1, int(max_ids))]

        seq = self._state.incr_int("notify_seq", 1)
        ev_id = str(uuid.uuid4())
        self.last_seq = int(seq)
        self.last_event_id = ev_id

        payload: dict = {
            "version": 1,
            "source": self.cfg.source,
            "event_id": ev_id,
            "seq": seq,
            "event": "delta",
            "changes": ch,
            "captured_at_max": cap_s,
            "sent_at": sent_s,
        }
        if rid:
            payload["room_ids"] = rid
        if mids:
            payload["message_ids"] = mids
        if aids:
            payload["attachment_ids"] = aids
        if cursor:
            payload["cursor"] = str(cursor)
        else:
            payload["cursor"] = f"captured_at<={cap_s}"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.cfg.token}",
        }

        # Two quick attempts; never crash capture on notify failures.
        for attempt in range(2):
            try:
                resp = self._sess.post(
                    self.cfg.url,
                    json=payload,
                    headers=headers,
                    timeout=self.cfg.timeout_seconds,
                )
                self.last_status_code = int(resp.status_code)
                try:
                    js = resp.json()
                    if isinstance(js, dict):
                        self.last_response_json = js
                except Exception:
                    self.last_response_json = None
                if 200 <= resp.status_code < 300:
                    return True
                # 4xx: don't retry except 429.
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    return False
            except Exception as e:
                self.last_error = str(e)
                pass
            # brief backoff before retry
            if attempt == 0:
                try:
                    import time as _time

                    _time.sleep(0.6)
                except Exception:
                    pass

        return False
