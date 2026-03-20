#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

export PATH="$HOME/.local/bin:$PATH"
export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"

DATA_URL="${ASHARE_DATA_URL:-https://seqiwang.cn/uploads/ashare-quant-data-20260320.tar.gz}"
ARCHIVE_PATH="${ASHARE_DATA_ARCHIVE:-$PROJECT_ROOT/.cache/ashare-quant-data-20260320.tar.gz}"
RUN_MODE="${ASHARE_RUN_MODE:-strategy}"

mkdir -p "$(dirname "$ARCHIVE_PATH")"

if [ ! -f .env ]; then
  cp .env.example .env
fi

if [ ! -f "$ARCHIVE_PATH" ]; then
  echo "[1/4] Downloading data package..."
  if command -v curl >/dev/null 2>&1; then
    curl -L --fail --retry 5 --retry-delay 2 --retry-all-errors --progress-bar "$DATA_URL" -o "$ARCHIVE_PATH" \
      || rm -f "$ARCHIVE_PATH"
  fi
  if [ ! -f "$ARCHIVE_PATH" ] && command -v wget >/dev/null 2>&1; then
    wget -O "$ARCHIVE_PATH" "$DATA_URL" || rm -f "$ARCHIVE_PATH"
  fi
  if [ ! -f "$ARCHIVE_PATH" ]; then
    echo "Failed to download data package from: $DATA_URL"
    exit 1
  fi
else
  echo "[1/4] Reusing existing archive: $ARCHIVE_PATH"
fi

echo "[2/4] Extracting data package..."
tar -xzf "$ARCHIVE_PATH" -C "$PROJECT_ROOT"

echo "[3/4] Installing dependencies..."
uv sync

case "$RUN_MODE" in
  strategy)
    echo "[4/4] Running strategy pipeline..."
    uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
    echo
    echo "Done. Check reports/ for outputs."
    ;;
  app)
    echo "[4/4] Starting decision app..."
    exec "$SCRIPT_DIR/run_decision_app.sh"
    ;;
  both)
    echo "[4/4] Running strategy pipeline first..."
    uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
    echo "Starting decision app..."
    exec "$SCRIPT_DIR/run_decision_app.sh"
    ;;
  *)
    echo "Unsupported ASHARE_RUN_MODE: $RUN_MODE"
    echo "Use one of: strategy | app | both"
    exit 1
    ;;
esac
