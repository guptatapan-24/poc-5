@echo off
echo ========================================================
echo Stopping All Real-Time Stock Event System Services
echo ========================================================

echo Stopping WSL background processes (Kafka KRaft, Redis)...
wsl bash -c "killall -9 java redis-server 2>/dev/null || true"

echo Stopping Windows processes (Daphne, Python, Node)...
taskkill /F /IM python.exe 2>nul
taskkill /F /IM node.exe 2>nul
taskkill /F /IM daphne.exe 2>nul

echo.
echo ========================================================
echo All services have been stopped.
echo ========================================================
