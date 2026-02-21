# linemon

Standalone LINE monitor app extracted from `wcmon`.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
copy .env.example .env
copy linemon_config.sample.json linemon_config.json
```

## Run

```powershell
.\.venv\Scripts\python line_capture.py --config linemon_config.json --verbose
```

## Helper script

```powershell
.\scripts\run_linemon.ps1
```

## Chat Send API

Default API config is `linemon_api_config.json`:

```json
{
  "bind_host": "0.0.0.0",
  "bind_port": 8788
}
```

Run API manually:

```powershell
.\.venv\Scripts\python -m linemon.api_server --config-file .\linemon_api_config.json
```

Or with helper script:

```powershell
.\scripts\run_linemon_api.ps1
```

Health check:

```powershell
curl http://127.0.0.1:8788/health
```

Send chat message:

```powershell
curl -X POST http://127.0.0.1:8788/api/send-chat `
  -H "Content-Type: application/json" `
  -d '{"chat":"My Chat Name","text":"hello from api"}'
```

## Windows Service (Auto Start)

Install and start:

```powershell
.\scripts\install_linemon_api_service.ps1
```

Run the installer in an elevated PowerShell window (Administrator).

Uninstall:

```powershell
.\scripts\uninstall_linemon_api_service.ps1
```

The service name is `LinemonApiService`, configured as automatic startup.

Note: LINE message send uses Windows UI automation. If your machine blocks UI automation from services,
run `.\scripts\run_linemon_api.ps1` from Task Scheduler at user logon instead.
