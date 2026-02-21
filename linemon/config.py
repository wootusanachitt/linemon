from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any
@dataclass(frozen=True)
class Config:
    line_window_title_regex: str
    wechat_window_title_regex: str
    single_window_mode: bool
    group_allowlist: list[str]
    poll_interval_seconds: float
    poll_interval_seconds_min: float
    poll_interval_seconds_max: float
    output_dir: str
    state_db_path: str
    max_chats_per_cycle: int
    max_messages_to_scan: int
    restore_previous_chat: bool
    idle_only: bool
    idle_seconds: int
    chat_list_limit: int
    debounce_polls: int
    per_chat_cooldown_seconds: float
    open_delay_seconds_min: float
    open_delay_seconds_max: float
    activation_tail_messages: int
    use_badge_ocr: bool
    tesseract_cmd: str
    badge_ocr_debug: bool
    process_unreads_on_startup: bool
    unread_rescan_seconds: float
    image_export_dir: str
    run_window_tz_offset: str
    run_window_start: str
    run_window_end: str
    debug: bool
    debug_dump_dir: str

    @staticmethod
    def from_path(path: str | Path) -> "Config":
        p = Path(path)
        data = json.loads(p.read_text(encoding="utf-8-sig"))

        def get(name: str, default: Any) -> Any:
            return data.get(name, default)

        poll = float(get("poll_interval_seconds", 2))
        poll_min = float(get("poll_interval_seconds_min", poll))
        poll_max = float(get("poll_interval_seconds_max", poll))
        if poll_min <= 0:
            poll_min = poll
        if poll_max < poll_min:
            poll_max = poll_min

        return Config(
            line_window_title_regex=str(
                get("line_window_title_regex", get("wechat_window_title_regex", r".*(LINE|Line|line).*"))
            ),
            wechat_window_title_regex=str(
                get("wechat_window_title_regex", r".*(WeChat|\u5fae\u4fe1).*")
            ),
            single_window_mode=bool(get("single_window_mode", True)),
            group_allowlist=[str(x) for x in get("group_allowlist", [])],
            poll_interval_seconds=poll,
            poll_interval_seconds_min=poll_min,
            poll_interval_seconds_max=poll_max,
            output_dir=str(get("output_dir", "logs")),
            state_db_path=str(get("state_db_path", "state.sqlite")),
            max_chats_per_cycle=int(get("max_chats_per_cycle", 50)),
            max_messages_to_scan=int(get("max_messages_to_scan", 120)),
            restore_previous_chat=bool(get("restore_previous_chat", True)),
            idle_only=bool(get("idle_only", False)),
            idle_seconds=int(get("idle_seconds", 8)),
            chat_list_limit=int(get("chat_list_limit", 40)),
            debounce_polls=int(get("debounce_polls", 2)),
            per_chat_cooldown_seconds=float(get("per_chat_cooldown_seconds", 3)),
            open_delay_seconds_min=float(get("open_delay_seconds_min", 0.0)),
            open_delay_seconds_max=float(get("open_delay_seconds_max", 0.0)),
            activation_tail_messages=int(get("activation_tail_messages", 20)),
            use_badge_ocr=bool(get("use_badge_ocr", True)),
            tesseract_cmd=str(get("tesseract_cmd", "")),
            badge_ocr_debug=bool(get("badge_ocr_debug", False)),
            process_unreads_on_startup=bool(get("process_unreads_on_startup", True)),
            unread_rescan_seconds=float(get("unread_rescan_seconds", 180.0)),
            image_export_dir=str(get("image_export_dir", r"C:\\Users\\Home\\Documents\\wcmon_exports")),
            run_window_tz_offset=str(get("run_window_tz_offset", "+07:00")),
            run_window_start=str(get("run_window_start", "06:00")),
            run_window_end=str(get("run_window_end", "23:00")),
            debug=bool(get("debug", False)),
            debug_dump_dir=str(get("debug_dump_dir", "debug")),
        )
