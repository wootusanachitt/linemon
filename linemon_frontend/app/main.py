from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .config import settings
from .db import list_messages, list_rooms


APP_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(APP_DIR / "templates"))

app = FastAPI(title=settings.app_name)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")


class SendChatRequest(BaseModel):
    chat: str = Field(min_length=1, max_length=255)
    text: str = Field(min_length=1)


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse(
        "index.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "linemon-frontend"}


@app.get("/api/rooms")
def api_rooms(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        rooms = list_rooms(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "rooms": rooms}


@app.get("/api/rooms/{room_id}/messages")
def api_room_messages(
    room_id: int,
    limit: int = Query(default=150, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=1),
    before_id: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    try:
        messages = list_messages(room_id=room_id, limit=limit, after_id=after_id, before_id=before_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "messages": messages}


@app.get("/api/upstream-health")
async def api_upstream_health() -> JSONResponse:
    url = settings.send_api_base_url.rstrip("/") + "/health"
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            payload = resp.json()
            return JSONResponse(status_code=resp.status_code, content=payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"upstream unreachable: {exc}") from exc


@app.post("/api/send-chat")
async def api_send_chat(body: SendChatRequest) -> JSONResponse:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if settings.send_api_token:
        headers["Authorization"] = f"Bearer {settings.send_api_token}"

    timeout = httpx.Timeout(settings.request_timeout_seconds)
    url = settings.send_api_base_url.rstrip("/") + "/api/send-chat"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json={"chat": body.chat.strip(), "text": body.text}, headers=headers)
            content_type = (resp.headers.get("content-type") or "").lower()
            if "application/json" in content_type:
                payload = resp.json()
            else:
                payload = {"ok": False, "error": resp.text}
            return JSONResponse(status_code=resp.status_code, content=payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"send-chat failed: {exc}") from exc
