#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
export PATH="$HOME/.local/bin:$PATH"
export PYTHONPATH="$PWD/src:$PYTHONPATH"
exec "$HOME/.local/bin/uv" run streamlit run app/decision_app.py --server.port 8512 --server.address 127.0.0.1 --server.headless true
