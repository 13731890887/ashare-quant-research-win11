#!/usr/bin/env bash
set -e
cd ~/Desktop/ashare-quant-research
export PATH="$HOME/.local/bin:$PATH"
export PYTHONPATH="$PWD/src:$PYTHONPATH"
exec "$HOME/.local/bin/uv" run streamlit run app/decision_app.py --server.port 8512 --server.address 127.0.0.1 --server.headless true
