from __future__ import annotations

import os


def load_env() -> None:
    """
    Load `.env` if python-dotenv is installed.

    We keep this optional so the capture script still runs if the user doesn't
    need DB/R2 features.
    """
    # Don't override explicitly-set environment variables (dotenv default).
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    # Resolve relative to current working directory by default.
    load_dotenv()


def env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return default
    return v

