@echo off
title Market Dashboard (Local)
cd /d "%~dp0.."
start "" http://localhost:4006
uv run python -m streamlit run local/local_app.py --server.port 4006
