#!/usr/bin/env bash
set -e
cd ~/Desktop/ashare-quant-research
export PATH="$HOME/.local/bin:$PATH"
exec "$HOME/.local/bin/uv" run streamlit run app/00_Home.py --server.port 8501 --server.address 127.0.0.1
