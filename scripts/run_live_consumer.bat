@echo off
echo ========================================================
echo Starting Live Kafka Event Consumer
echo Consuming 'stock-events' -^> Forwarding to Django Channels / WebSockets
echo ========================================================
cd /d "%~dp0..\backend"
uv run python manage.py run_live_event_consumer
