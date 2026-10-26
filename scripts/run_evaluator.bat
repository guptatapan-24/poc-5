@echo off
echo ========================================================
echo Starting Threshold Evaluator Engine
echo Consuming 'stock-prices' -^> Publishing to 'stock-events'
echo ========================================================
cd /d "%~dp0..\backend"
uv run python manage.py run_threshold_evaluator
