# Linemon Frontend

Single-user FastAPI web UI for:

- listing chat rooms from Linemon MySQL
- viewing full room conversations
- near real-time updates (polling)
- sending chat messages through the upstream Linemon send API

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Open `http://127.0.0.1:8080`.

## Docker run

```bash
cp .env.example .env
docker compose up -d --build
curl -s http://127.0.0.1:18080/health
```

## Required environment variables

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- `SEND_API_BASE_URL` (default: `http://wootust.ddns.net:8788`)
- `SEND_API_TOKEN` (optional)

`LINEMON_DB_*` aliases are also supported for DB config.

