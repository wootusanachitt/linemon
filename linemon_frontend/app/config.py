from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return default


def _env_int(*names: str, default: int) -> int:
    raw = _env(*names, default=str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(*names: str, default: float) -> float:
    raw = _env(*names, default=str(default))
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    app_name: str
    host: str
    port: int

    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

    send_api_base_url: str
    send_api_token: str
    request_timeout_seconds: float

    r2_public_base_url: str

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            app_name=_env("APP_NAME", default="Linemon Frontend"),
            host=_env("HOST", default="0.0.0.0"),
            port=_env_int("PORT", default=8080),
            db_host=_env("DB_HOST", "LINEMON_DB_HOST"),
            db_port=_env_int("DB_PORT", "LINEMON_DB_PORT", default=3306),
            db_user=_env("DB_USER", "LINEMON_DB_USER"),
            db_password=_env("DB_PASSWORD", "LINEMON_DB_PASSWORD"),
            db_name=_env("DB_NAME", "LINEMON_DB_NAME"),
            send_api_base_url=_env("SEND_API_BASE_URL", default="http://wootust.ddns.net:8788"),
            send_api_token=_env("SEND_API_TOKEN"),
            request_timeout_seconds=_env_float("REQUEST_TIMEOUT_SECONDS", default=12.0),
            r2_public_base_url=_env("R2_PUBLIC_BASE_URL"),
        )


settings = Settings.from_env()
