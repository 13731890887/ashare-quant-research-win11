$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot\..
$projectRoot = (Get-Location).Path
$env:PYTHONPATH = "src"

$dataUrl = if ($env:ASHARE_DATA_URL) { $env:ASHARE_DATA_URL } else { "https://seqiwang.cn/uploads/ashare-quant-data-20260320.tar.gz" }
$archivePath = if ($env:ASHARE_DATA_ARCHIVE) { $env:ASHARE_DATA_ARCHIVE } else { Join-Path $projectRoot ".cache\ashare-quant-data-20260320.tar.gz" }
$runMode = if ($env:ASHARE_RUN_MODE) { $env:ASHARE_RUN_MODE } else { "strategy" }

New-Item -ItemType Directory -Force -Path (Split-Path $archivePath -Parent) | Out-Null

if (-not (Test-Path .env)) {
  Copy-Item .env.example .env
}

if (-not (Test-Path $archivePath)) {
  Write-Host "[1/4] Downloading data package..."
  try {
    Invoke-WebRequest -Uri $dataUrl -OutFile $archivePath
  } catch {
    if (Test-Path $archivePath) {
      Remove-Item $archivePath -Force
    }
    Write-Host "Download failed once, retrying..."
    Invoke-WebRequest -Uri $dataUrl -OutFile $archivePath
  }
} else {
  Write-Host "[1/4] Reusing existing archive: $archivePath"
}

Write-Host "[2/4] Extracting data package..."
tar -xzf $archivePath -C $projectRoot

Write-Host "[3/4] Installing dependencies..."
uv sync

switch ($runMode) {
  "strategy" {
    Write-Host "[4/4] Running strategy pipeline..."
    uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
    Write-Host "Done. Check reports/ for outputs."
  }
  "app" {
    Write-Host "[4/4] Starting decision app..."
    bash ./scripts/run_decision_app.sh
  }
  "both" {
    Write-Host "[4/4] Running strategy pipeline first..."
    uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
    Write-Host "Starting decision app..."
    bash ./scripts/run_decision_app.sh
  }
  default {
    throw "Unsupported ASHARE_RUN_MODE: $runMode. Use: strategy | app | both"
  }
}
