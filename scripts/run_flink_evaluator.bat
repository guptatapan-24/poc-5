@echo off
echo ========================================================
echo Starting Apache Flink and Flink CEP Stream Evaluator
echo Keyed State - Event Time - Watermarks - Checkpoints
echo ========================================================
cd /d "%~dp0..\backend"
set DJANGO_SETTINGS_MODULE=config.settings
uv run python manage.py run_flink_evaluator
