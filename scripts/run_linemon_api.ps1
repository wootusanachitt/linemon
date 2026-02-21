[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ApiModule = "linemon.api_server"
$ApiConfig = Join-Path $ProjectRoot "linemon_api_config.json"
$LogsDir = Join-Path $ProjectRoot "logs_api"
$StdoutLog = Join-Path $LogsDir "linemon_api_stdout.log"
$StderrLog = Join-Path $LogsDir "linemon_api_stderr.log"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $ApiConfig)) {
    throw "API config file not found: $ApiConfig"
}

New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
Set-Location -LiteralPath $ProjectRoot

cmd.exe /d /c "`"$PythonExe`" -u -m $ApiModule --config-file `"$ApiConfig`" 1>>`"$StdoutLog`" 2>>`"$StderrLog`""
exit $LASTEXITCODE
