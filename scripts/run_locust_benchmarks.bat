@echo off
echo ========================================================
echo Executing Automated Locust Benchmark Scenarios (H1, W1, E1)
echo ========================================================
cd /d "%~dp0..\backend"
uv run python run_locust_tests.py --users 10 --duration 15s --scenario all
pause
