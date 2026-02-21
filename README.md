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
