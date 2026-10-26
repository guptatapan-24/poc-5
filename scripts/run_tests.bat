@echo off
echo ========================================================
echo Running Full Test Suite (Unit + Channels + Kafka Integration)
echo ========================================================
cd /d "%~dp0..\backend"
uv run pytest -v
