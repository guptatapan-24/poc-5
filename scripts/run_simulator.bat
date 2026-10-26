@echo off
echo ========================================================
echo Starting Price Simulator (Deterministic Sequence)
echo Publishing to Kafka 'stock-prices'
echo ========================================================
cd /d "%~dp0..\backend"
uv run python manage.py run_price_simulator --mode deterministic --interval 1.0
