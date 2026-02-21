[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Run this script as Administrator."
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ApiConfig = Join-Path $ProjectRoot "linemon_api_config.json"
$ApiConfigSample = Join-Path $ProjectRoot "linemon_api_config.sample.json"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $ApiConfig) -and (Test-Path -LiteralPath $ApiConfigSample)) {
    Copy-Item -LiteralPath $ApiConfigSample -Destination $ApiConfig
}

Set-Location -LiteralPath $ProjectRoot

$svc = Get-Service -Name "LinemonApiService" -ErrorAction SilentlyContinue
if ($null -eq $svc) {
    & $PythonExe -m linemon.windows_service --startup auto install
    if ($LASTEXITCODE -ne 0) {
        throw "Service install failed with exit code $LASTEXITCODE"
    }
}

Set-Service -Name "LinemonApiService" -StartupType Automatic
$svcNow = Get-Service -Name "LinemonApiService" -ErrorAction Stop
if ($svcNow.Status -ne "Running") {
    Start-Service -Name "LinemonApiService"
}
Write-Host "LinemonApiService installed and started."
