from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np


def _bgr_to_png_bytes(img_bgr: np.ndarray) -> bytes:
    import cv2

    ok, buf = cv2.imencode(".png", img_bgr)
    if not ok:
        raise RuntimeError("Failed to encode image to PNG")
    return bytes(buf)


def _data_url_png(png: bytes) -> str:
    b64 = base64.b64encode(png).decode("ascii")
    return "data:image/png;base64," + b64


@dataclass(frozen=True)
class VisionConfig:
    # "openai_compatible" is an HTTP API that accepts a chat-completions style payload.
    kind: str = "openai_compatible"
    base_url: str = "http://localhost:8000"
    model: str = "gpt-4o-mini"
    api_key_env: str = "VISION_API_KEY"
    require_api_key: bool = True
    timeout_seconds: float = 30.0


class VisionClient:
    def __init__(self, cfg: VisionConfig) -> None:
        self.cfg = cfg

    def _api_key(self) -> str | None:
        env = (self.cfg.api_key_env or "").strip()
        if not env:
            return None
        k = os.environ.get(env, "").strip()
        if not k and self.cfg.require_api_key:
            raise RuntimeError(f"Missing API key in env var {env!r}. Set it before running.")
        return k or None

    def describe_image_json(self, *, img_bgr: np.ndarray, prompt: str, max_tokens: int = 900) -> Any:
        """
        Send an image + prompt and return parsed JSON.

        This deliberately asks the model to return ONLY JSON so the result can be parsed.
        """
        txt = self.describe_image_text(img_bgr=img_bgr, prompt=prompt, max_tokens=max_tokens)
        txt = (txt or "").strip()
        # Allow the model to wrap JSON in ```json fences; strip them.
        if txt.startswith("```"):
            # Typical form:
            # ```json
            # {...}
            # ```
            lines = txt.splitlines()
            if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
                body = "\n".join(lines[1:-1]).lstrip()
                if body.lower().startswith("json"):
                    body = body[4:].lstrip()
                txt = body.strip()
        try:
            return json.loads(txt)
        except Exception as e:
            raise RuntimeError(f"Vision API did not return valid JSON. Raw={txt[:400]!r}") from e

    def describe_image_text(self, *, img_bgr: np.ndarray, prompt: str, max_tokens: int = 900) -> str:
        kind = (self.cfg.kind or "").strip().lower()
        if kind != "openai_compatible":
            raise RuntimeError(f"Unsupported vision API kind: {self.cfg.kind!r}")
        return self._openai_compatible(img_bgr=img_bgr, prompt=prompt, max_tokens=max_tokens)

    def _openai_compatible(self, *, img_bgr: np.ndarray, prompt: str, max_tokens: int) -> str:
        import requests

        png = _bgr_to_png_bytes(img_bgr)
        data_url = _data_url_png(png)
        base = self.cfg.base_url.rstrip("/")
        # Allow base_url to be either:
        # - https://host (we append /v1/chat/completions)
        # - https://host/v1 (we append /chat/completions)
        url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")
        headers = {"Content-Type": "application/json"}
        k = self._api_key()
        if k:
            headers["Authorization"] = "Bearer " + k
        payload = {
            "model": self.cfg.model,
            "temperature": 0,
            "max_tokens": int(max_tokens),
            "messages": [
                {
                    "role": "system",
                    "content": "Return ONLY valid JSON. Do not include any commentary.",
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
        }
        # Remote vision providers occasionally fail with transient TLS/network issues.
        # Retry a few times before giving up so capture loops don't miss incoming messages.
        last_err: Exception | None = None
        data: Any = None
        for attempt in range(1, 4):
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=self.cfg.timeout_seconds)
                if resp.status_code >= 500:
                    raise RuntimeError(f"Vision API HTTP {resp.status_code}: {resp.text[:400]}")
                if resp.status_code >= 400:
                    # 4xx is usually non-transient; don't keep retrying.
                    raise RuntimeError(f"Vision API HTTP {resp.status_code}: {resp.text[:400]}")
                data = resp.json()
                break
            except Exception as e:
                last_err = e
                if attempt >= 3:
                    raise RuntimeError(f"Vision API request failed after retries: {e}") from e
                # Short linear backoff.
                time.sleep(0.45 * attempt)
                continue
        if data is None:
            if last_err is not None:
                raise RuntimeError(f"Vision API request failed: {last_err}") from last_err
            raise RuntimeError("Vision API request failed: empty response")
        # Common shape: {choices:[{message:{content:"..."}}]}
        try:
            return (data["choices"][0]["message"]["content"] or "").strip()
        except Exception:
            # Some providers return "output_text" or similar; best-effort.
            if isinstance(data, dict):
                for k in ["output_text", "text", "content"]:
                    if k in data and isinstance(data[k], str):
                        return data[k].strip()
            raise RuntimeError(f"Unexpected vision API response shape: keys={list(data)[:20]}")
