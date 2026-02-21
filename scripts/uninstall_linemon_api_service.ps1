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

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python not found: $PythonExe"
}

$svc = Get-Service -Name "LinemonApiService" -ErrorAction SilentlyContinue
if ($null -eq $svc) {
    Write-Host "LinemonApiService is not installed."
    exit 0
}

Set-Location -LiteralPath $ProjectRoot

try {
    Stop-Service -Name "LinemonApiService" -ErrorAction Stop
} catch {
    # Service may already be stopped.
}

& $PythonExe -m linemon.windows_service remove
if ($LASTEXITCODE -ne 0) {
    throw "Service remove failed with exit code $LASTEXITCODE"
}

Write-Host "LinemonApiService removed."
