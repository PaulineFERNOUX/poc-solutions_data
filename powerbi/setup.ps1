# Environnement Python pour Power BI Desktop (Windows uniquement).
# Usage : PowerShell, depuis le dossier powerbi\
#   cd C:\Users\<vous>\formation_projet12\powerbi
#   .\setup.ps1

$ErrorActionPreference = "Stop"

$venv = Join-Path $PSScriptRoot ".venv"
$requirements = Join-Path $PSScriptRoot "requirements.txt"
$pip = Join-Path $venv "Scripts\pip.exe"
$python = Join-Path $venv "Scripts\python.exe"

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    Write-Error "Python introuvable. Installez Python 3 depuis https://www.python.org/downloads/ (cochez 'Add to PATH')."
}

Write-Host "Creation du venv : $venv"
py -3 -m venv $venv

Write-Host "Installation des packages..."
& $pip install -r $requirements

Write-Host "Verification..."
& $python -c "import boto3, pandas, pyarrow, matplotlib; print('OK')"

Write-Host ""
Write-Host "=== Power BI Desktop ==="
Write-Host "Fichier > Options > Scripting Python > Repertoire de base (Autre) :"
Write-Host "  $venv"
Write-Host ""
Write-Host "Ne pas mettre python.exe : uniquement le dossier ci-dessus."
