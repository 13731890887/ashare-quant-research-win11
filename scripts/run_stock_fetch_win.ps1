$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..
$env:PYTHONPATH = "src"
python .\scripts\fetch_full_sina.py
