$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..
$env:PYTHONPATH = "src"
python -m ashare_quant.pipeline.stage16_ml_multifactor_fund_longhold
