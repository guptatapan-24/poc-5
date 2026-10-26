@echo off
echo ========================================================
echo Starting All Real-Time Stock Event System Services
echo ========================================================
echo.

set ROOT=%~dp0

echo [1/6] Starting Redis Server on port 6379...
start "1 - Local Redis (Port 6379)" cmd /k "%ROOT%scripts\start_redis.bat"
timeout /t 3 /nobreak >nul

echo [2/6] Starting Apache Kafka KRaft on port 9092...
start "2 - Apache Kafka (Port 9092)" cmd /k "%ROOT%scripts\start_kafka.bat"
timeout /t 5 /nobreak >nul

echo [3/6] Starting Django ASGI Daphne Server on port 8000...
start "3 - Django ASGI Daphne (Port 8000)" cmd /k "%ROOT%scripts\run_backend.bat"
timeout /t 3 /nobreak >nul

echo [4/6] Starting Apache Flink and Flink CEP Stream Evaluator...
start "4 - Apache Flink Evaluator" cmd /k "%ROOT%scripts\run_flink_evaluator.bat"
timeout /t 2 /nobreak >nul

echo [5/6] Starting Live Event Consumer...
start "5 - Live Kafka Consumer" cmd /k "%ROOT%scripts\run_live_consumer.bat"
timeout /t 2 /nobreak >nul

echo [6/6] Starting Next.js Dashboard Frontend on port 3000...
start "6 - Next.js Frontend (Port 3000)" cmd /k "%ROOT%scripts\run_frontend.bat"
timeout /t 3 /nobreak >nul

echo.
echo ========================================================
echo All 6 services launched in dedicated terminal windows!
echo Opening Dashboard at http://localhost:3000...
echo To stop all services later, run: .\stop_all.bat
echo ========================================================
start http://localhost:3000
