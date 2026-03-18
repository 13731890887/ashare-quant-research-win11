$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..
$env:PYTHONPATH = "src"
python -m ashare_quant.pipeline.stage11_targeted_repair
