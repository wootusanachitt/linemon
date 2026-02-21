[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$CaptureScript = Join-Path $ProjectRoot "line_capture.py"
$ConfigPath = Join-Path $ProjectRoot "linemon_config.json"
$LogsDir = Join-Path $ProjectRoot "logs"
$StdoutLog = Join-Path $LogsDir "line_capture_stdout.log"
$StderrLog = Join-Path $LogsDir "line_capture_stderr.log"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $CaptureScript)) {
    throw "Capture script not found: $CaptureScript"
}
if (-not (Test-Path -LiteralPath $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
Set-Location -LiteralPath $ProjectRoot

cmd.exe /d /c "`"$PythonExe`" -u `"$CaptureScript`" --config `"$ConfigPath`" --verbose 1>>`"$StdoutLog`" 2>>`"$StderrLog`""
exit $LASTEXITCODE
