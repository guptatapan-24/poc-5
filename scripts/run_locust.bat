@echo off
echo ========================================================
echo Starting Locust Load Testing UI on http://localhost:8089
echo Target host: http://127.0.0.1:8000 (Daphne ASGI Server)
echo ========================================================
cd /d "%~dp0..\backend"
start http://localhost:8089
uv run locust -f locustfile.py --host http://127.0.0.1:8000
