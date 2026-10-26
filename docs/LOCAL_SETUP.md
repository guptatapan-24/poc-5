# Local Infrastructure & Setup Guide (Non-Docker)

This guide provides step-by-step instructions to run the entire system locally on a Windows development environment without Docker or cloud services.

---

## 1. Prerequisites Check

Verify your installed runtimes:
```powershell
python --version    # Requires Python 3.12+ (Python 3.13 / 3.14 tested)
uv --version        # Fast Python package & environment manager
node --version      # Node.js 18+ (Node 22 tested)
npm --version       # npm 10+
java -version       # Java 17+ (Kafka runs on the JVM)
```

---

## 2. Windows-Specific Architecture Note

### Why Kafka Uses WSL2 for Local Execution on Windows
Under native Windows NTFS file systems, Apache Kafka's log segment management encounters known JVM memory-mapped file locking issues (`java.nio.file.FileSystemException: The process cannot access the file because it is being used by another process`). In fact, Apache Kafka officially removed/deprecated the native `.bat` scripts in Kafka 3.x/4.x.

To provide a 100% reproducible, non-Docker local experience:
- **Kafka 4.4.0 (KRaft Mode)** runs locally within the lightweight local subsystem (`/opt/kafka`).
- **Redis 7.0** runs locally in the background on standard port `6379`.
- **WSL automatically bridges ports** so that Windows Python processes (`uv run`) and React connect directly to `127.0.0.1:9092` and `127.0.0.1:6379`.

---

## 3. Starting Supporting Services

### Step 1: Start Redis Server
```powershell
# In a dedicated terminal or background:
.\scripts\start_redis.bat

# Verify Redis is listening:
Test-NetConnection -ComputerName 127.0.0.1 -Port 6379
```

### Step 2: Start Apache Kafka (KRaft Mode)
```powershell
# In a dedicated terminal:
.\scripts\start_kafka.bat

# Verify Kafka is listening on port 9092:
Test-NetConnection -ComputerName 127.0.0.1 -Port 9092
```

### Step 3: Create Kafka Topics
```powershell
.\scripts\create_topics.bat
```
This initializes `stock-prices` and `stock-events` with 2 partitions and 7-day retention.

---

## 4. Starting Application Processes

To run the full end-to-end stack, launch the following processes in separate terminals:

### Terminal 1: Django ASGI Server (Daphne)
```powershell
.\scripts\run_backend.bat
# Runs Daphne on http://127.0.0.1:8000
```
Verify:
Open `http://127.0.0.1:8000/api/health/` in your browser. Both Kafka and Redis will show `reachable: true`.

### Terminal 2: Threshold Evaluator Engine
```powershell
.\scripts\run_evaluator.bat
# Consumes 'stock-prices', evaluates thresholds, publishes to 'stock-events'
```

### Terminal 3: Live Event Consumer
```powershell
.\scripts\run_live_consumer.bat
# Consumes 'stock-events' and pushes to Django Channels over WebSockets
```

### Terminal 4: React Dashboard Frontend
```powershell
.\scripts\run_frontend.bat
# Starts Vite dev server on http://localhost:5173
```
Open `http://localhost:5173` in your browser.

### Terminal 5 (Optional): Standalone Price Simulator CLI
```powershell
.\scripts\run_simulator.bat
# Alternatively, you can start/stop the simulator directly using the UI buttons on the dashboard!
```

---

## 5. Clean Process Shutdown

To stop the system cleanly:
- Press `Ctrl + C` in each application terminal.
- To terminate background Kafka and Redis instances:
```powershell
wsl -u root bash -c "pkill -f kafka; pkill -f redis-server"
```
