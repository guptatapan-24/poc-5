@echo off
echo ========================================================
echo Starting Django ASGI Daphne Server on http://127.0.0.1:8000
echo ========================================================
cd /d "%~dp0..\backend"
uv run daphne -b 127.0.0.1 -p 8000 --ping-interval 15 --ping-timeout 30 config.asgi:application
