# Real-Time Stock Event Capture and Replay System

A distributed, local event-driven architecture demonstrating real-time stock price streaming, stateful threshold crossing detection, durable event capture in **Apache Kafka**, asynchronous push delivery over **Django Channels & WebSockets**, and **isolated historical event replay**.

---

## 1. Project Objective & Features

- **Continuous Price Simulation**: Generates realistic and deterministic stock price updates published to Kafka topic `stock-prices`.
- **Durable Event Capture**: Detected price-threshold crossing events are published to Kafka topic `stock-events` with a 7-day retention policy.
- **Stateful Threshold Evaluator**: Evaluates price crossings with hysteresis tracking to prevent duplicate repeated alerts.
- **Real-Time Push Delivery**: Events are consumed from Kafka and pushed to connected React dashboard clients via **Django Channels**, **ASGI Daphne**, and a **Redis channel layer**.
- **Historical Event Replay**: Clients can request a replay of previously captured events directly from Kafka. Replay seeks historical log offsets without modifying the live consumer group's offsets or republishing events.
- **Zero Cloud / Zero Docker**: Runs 100% locally on your machine using locally installed services.

---

## 2. Technology Stack
 
- **Backend**: Python 3.13+, Django 6.1+, Django Channels 4.3+, Daphne (ASGI Server), Redis 7.0+ (`channels_redis`), Pydantic v2
- **Event Streaming**: Apache Kafka 4.4.0 (KRaft Mode, single-broker local cluster), `kafka-python-ng`
- **Stream Processing & CEP**: Apache Flink 1.20+ / Flink CEP (Java DataStream + shaded fat JAR), FlinkStreamEvaluator engine with Keyed State, Event Time, Watermarking, and Checkpointing
- **Load Testing**: Locust 2.47.0 (HTTP REST API scenario H1 + WebSocket Concurrency & Event Delivery Latency scenarios W1 & E1)
- **Frontend**: Next.js 16 (App Router), React 19, Lucide Icons, Vanilla CSS Design System, Native browser WebSocket API
- **Tooling**: `uv` for Python environment & dependency management, `npm` for Node frontend, `mvn` (Apache Maven 3.8+) for Java Flink builds
- **Testing**: `pytest`, `pytest-django`, `pytest-asyncio`, Channels `WebsocketCommunicator`, `locust`
 
---

## 3. High-Level Architecture

```
                    PRICE SIMULATOR
                          |
                          v
                    Kafka Producer (key: Symbol)
                          |
                          v
                   Kafka Topic: stock-prices (2 partitions)
                          |
                          v
                   Threshold Evaluator (Crossing Detection & Hysteresis)
                          |
                Threshold crossed? (e.g. AAPL $180.40 -> $179.80 <= $180)
                          |
                          v
                    Kafka Producer (key: Symbol)
                          |
                          v
                   Kafka Topic: stock-events (Durable Log, 7d Retention)
                          |
              +-----------+-----------+
              |                       |
              v                       v
       Live Event Consumer      Replay Consumer (Isolated seek)
              |                       |
              +-----------+-----------+
                          |
                          v
                Django Channels Layer
                          |
                          v
                    Redis Backend (channels_redis)
                          |
                          v
                    WebSocket (ws://localhost:8000/ws/events/)
                          |
                          v
                    Next.js Dashboard (http://localhost:3000)
```

---

## 4. Repository Structure

```
PoC/
|-- backend/
|   |-- manage.py
|   |-- pyproject.toml
|   |-- .env.example
|   |-- .env
|   |-- config/
|   |   |-- settings.py        # Django settings (Channels, Redis, Kafka)
|   |   |-- asgi.py            # ASGI application with WebSocket URLRouter
|   |   |-- urls.py            # Root URL routing
|   |   |-- kafka_utils.py     # Cross-platform Kafka bootstrap resolution
|   |   |-- middleware.py      # CORS middleware for local frontend integration
|   |-- events/
|   |   |-- schemas.py         # Pydantic models (PriceUpdate, ThresholdCrossingEvent)
|   |   |-- state.py           # Thread-safe in-memory threshold & replay tracker
|   |   |-- consumers.py       # StockEventsConsumer (live broadcast + private replay)
|   |   |-- routing.py         # WebSocket URL routes (ws/events/)
|   |   |-- views.py           # REST endpoints (/api/health, /api/thresholds, /api/replay)
|   |   |-- urls.py            # API URL mapping
|   |   |-- management/
|   |       |-- commands/
|   |           |-- run_price_simulator.py
|   |           |-- run_threshold_evaluator.py
|   |           |-- run_live_event_consumer.py
|   |           |-- create_kafka_topics.py
|   |-- event_pipeline/
|   |   |-- kafka_producer.py      # Thread-safe StockEventProducer wrapper
|   |   |-- price_simulator.py     # Deterministic & stochastic price generator
|   |   |-- threshold_evaluator.py # Hysteresis threshold evaluator
|   |   |-- live_consumer.py       # Background consumer pushing to Channels
|   |   |-- replay_consumer.py     # Isolated historical replay service
|   |   |-- event_service.py       # Facade for health checks and topic inspection
|   |-- tests/                     # 28 passing unit, channels, and Kafka integration tests
|
|-- frontend/
|   |-- package.json
|   |-- next.config.mjs
|   |-- src/
|       |-- app/
|           |-- layout.jsx     # Root layout with metadata and styles
|           |-- page.jsx       # Dashboard root page (App Router)
|       |-- App.jsx
|       |-- index.css          # Custom dark FinTech CSS design system
|       |-- hooks/useWebSocket.js  # Native WebSocket hook with bounded backoff
|       |-- services/api.js        # REST client
|       |-- components/
|           |-- Header.jsx
|           |-- StockPriceDisplay.jsx
|           |-- ThresholdConfig.jsx
|           |-- SimulatorControls.jsx
|           |-- ReplayControls.jsx
|           |-- EventFeed.jsx
|           |-- ArchitectureModal.jsx
|           |-- SimulatedTradeModal.jsx
|
|-- scripts/
|   |-- start_kafka.bat / .sh
|   |-- start_redis.bat / .sh
|   |-- create_topics.bat / .sh
|   |-- run_backend.bat
|   |-- run_evaluator.bat
|   |-- run_live_consumer.bat
|   |-- run_simulator.bat
|   |-- run_frontend.bat
|   |-- run_tests.bat
|
|-- docs/
|   |-- ARCHITECTURE.md
|   |-- EVENT_FLOW.md
|   |-- KAFKA_AND_REPLAY.md
|   |-- LOCAL_SETUP.md
|   |-- TESTING.md
|-- README.md
|-- .gitignore
```

---

## 5. Prerequisites & Quick Start

### 1. Prerequisites Check
Ensure the following tools are available on your system:
- **Python 3.12+** with `uv` (`uv --version`)
- **Node.js 18+** with `npm` (`npm --version`)
- **Java 17+** (`java -version`)

---

## 6. Launch Sequence (Step-by-Step)

Launch the processes in separate terminals in the following order:

### Step 1: Start Redis Server
```powershell
.\scripts\start_redis.bat
```
*Verifies on `127.0.0.1:6379`.*

### Step 2: Start Apache Kafka (Local KRaft Mode)
```powershell
.\scripts\start_kafka.bat
```
*Verifies on `127.0.0.1:9092`.*

### Step 3: Create Kafka Topics
```powershell
.\scripts\create_topics.bat
```
*Creates `stock-prices` and `stock-events` with 2 partitions each and 7-day retention.*

### Step 4: Start Django ASGI Server (Daphne)
```powershell
.\scripts\run_backend.bat
```
*Listens on `http://127.0.0.1:8000` for HTTP and `ws://127.0.0.1:8000/ws/events/` for WebSockets.*

### Step 5: Start Threshold Evaluator
```powershell
.\scripts\run_evaluator.bat
```
*Consumes price updates from `stock-prices` and publishes crossing events to `stock-events`.*

### Step 6: Start Live Event Consumer
```powershell
.\scripts\run_live_consumer.bat
```
*Consumes events from `stock-events` and forwards them to Django Channels.*

### Step 7: Start Next.js Frontend
```powershell
.\scripts\run_frontend.bat
```
*Opens on `http://localhost:3000`.*

---

## 7. End-to-End Demonstration Scenario

Follow this repeatable scenario to demonstrate the full application:

1. **Open Dashboard**:
   Navigate to **`http://localhost:3000`**.
   Verify that the header badge shows **`CONNECTED`** with a green pulse and that both Kafka and Redis report **`Online`**.

2. **Verify / Configure Threshold**:
   By default, the system pre-populates an alert:
   - **Symbol**: `AAPL`
   - **Threshold**: `$180.00`
   - **Condition**: Falls to or below (≤)

3. **Start Price Simulator**:
   In the **Price Simulator** card on the dashboard, ensure mode is set to:
   `Deterministic Demo ($185.00 → $180.40 → $179.80 Crossing)`
   Click **"Start Simulator"**.

4. **Observe Price Sequence & Detection**:
   The simulator publishes the canonical sequence to `stock-prices`:
   - `$185.00`
   - `$183.50`
   - `$181.00`
   - `$180.40`
   - `$179.80` **<-- CROSSING DETECTED!**

5. **Observe Real-Time Event Capture & WebSocket Delivery**:
   The threshold evaluator detects the crossing from `$180.40` to `$179.80`, publishes event `evt-xxxxx` to Kafka `stock-events`, and within milliseconds the event appears in the dashboard with a vibrant green **`LIVE`** badge!

6. **Demonstrate Hysteresis**:
   As the price continues to `$179.20`, no duplicate alert is generated because the price remains below the threshold.
   When the price bounces back to `$180.50` and crosses back down to `$179.50`, a second event is captured.

7. **Demonstrate Historical Event Replay**:
   Click the purple **"Replay Events from Kafka"** button in the dashboard.
   - The `ReplayService` opens an ephemeral consumer.
   - It assigns partitions and seeks to the beginning of `stock-events`.
   - It retrieves previously captured events and streams them over WebSockets to your browser.
   - The events appear with a purple **`REPLAY`** badge, **strictly preserving original event IDs and timestamps**.
   - Notice that live event processing is completely unblocked and live consumer offsets remain unchanged!

8. **Filter and Clear View**:
   - Filter by **"Live Alerts"** or **"Historical Replays"** using the filter pills.
   - Click **"Clear View"** to clear the frontend display.
   - Click **"Replay Events"** again to verify that all events were durably retained in Kafka.

---

---

## 8. Apache Flink & Complex Event Processing (CEP)

As detailed in the **Technical Research and Implementation Proposal**, the system incorporates **Apache Flink** and **Flink CEP** for stateful stream processing and multi-event temporal pattern matching:

1. **Stateful Threshold Processing**:
   - Implemented via a `KeyedProcessFunction` scoped by `symbol`.
   - Manages per-rule states (`WAITING`, `ARMED`, `TRIGGERED`) with hysteresis to prevent alert storms.
   - Emits `PRICE_THRESHOLD_CROSSED` events with deterministic IDs.

2. **Flink CEP Downtrend Pattern (`PRICE_DOWNTREND_DETECTED`)**:
   - Detects sequences of **3 consecutive price decreases within 10 seconds** for the same symbol:
     - 4 events: `e0, e1, e2, e3` where `e1 < e0`, `e2 < e1`, and `e3 < e2`.
     - **Strict Contiguity**: An intervening flat or rising price cancels and resets the candidate sequence.
     - **Event Time & Watermarks**: Driven by `observed_at` event timestamps with bounded out-of-orderness and partition idleness handling.
     - **Overlap Strategy**: `skipPastLastEvent` ensures a 5-drop sequence generates exactly 1 alert instead of 2 overlapping alerts.

3. **Fault Tolerance & Checkpointing**:
   - Checkpoints state every 10 seconds to disk (`backend/checkpoints/`).
   - Restores partition offsets and candidate buffers automatically on restart.

### Running Flink:
- **Python Flink CEP Stream Evaluator**:
  ```powershell
  .\scripts\run_flink_evaluator.bat
  ```
- **Java Apache Flink Shaded JAR** (built with Maven in WSL2):
  ```bash
  cd flink-evaluator
  mvn clean package -DskipTests
  # Generates target/flink-evaluator-1.0.0.jar ready for submission to any Apache Flink cluster:
  flink run -c com.stockstream.flink.StockStreamFlinkJob target/flink-evaluator-1.0.0.jar
  ```

---

## 9. Locust Load & Performance Testing

The system includes an automated **Locust** load-testing suite implementing the four scenarios specified in the proposal (Section 13):

- **Scenario H1 (HTTP REST API)**: Tests `/api/events/recent/`, `/api/thresholds/`, `/api/health/`, and `/api/simulator/status/` at 10, 50, and 100 concurrent users. Measures RPS, failure rate, and p50/p95/p99 latency.
- **Scenario W1 (WebSocket Concurrency)**: Tests 10 to 100 concurrent WebSocket connections to Daphne (`ws://127.0.0.1:8000/ws/events/`), heartbeat ping stability (every 10s), and drop rates.
- **Scenario E1 (End-to-End Event Delivery Latency)**: Measures time from a price tick's `observed_at` to client receipt over WebSockets (`delivery_latency = client_receive_time - event_timestamp`). Separately reports percentiles for `THRESHOLD_CROSSED` and `PRICE_DOWNTREND_DETECTED`.
- **Scenario S1 (Sustained Load)**: 50 WebSocket users + 10 HTTP users running concurrently.

### Running Locust:
- **Interactive Web UI (`http://localhost:8089`)**:
  ```powershell
  .\scripts\run_locust.bat
  ```
- **Automated Headless Benchmarks (Generates HTML & CSV Reports)**:
  ```powershell
  .\scripts\run_locust_benchmarks.bat
  ```
  Reports are archived to `backend/locust_reports/`.

---

## 10. Running Tests

Run the complete test suite:
```powershell
cd backend
uv run pytest -v
```
Or execute:
```powershell
.\scripts\run_tests.bat
```

### Test Suite Coverage:
- `tests/test_schemas.py`: Schema validation, models, and union parsing
- `tests/test_threshold_evaluator.py`: Canonical scenario hysteresis logic
- `tests/test_flink_evaluator.py`: Flink threshold crossing, CEP 3-drop sequence, contiguity reset, 10s timeout, skipPastLastEvent, and checkpoint restore
- `tests/test_api.py`: Django REST API endpoints
- `tests/test_channels.py`: WebSocket handshake, ping-pong, and live group broadcasting
- `tests/test_replay.py`: Historical partition seek and offset isolation
- `tests/test_kafka_integration.py`: End-to-end Kafka publish, capture, and delivery

---

## 11. Inspecting Kafka Records from CLI

You can directly inspect the retained Kafka topic contents using Kafka's built-in CLI:

```bash
# View captured threshold crossing and CEP downtrend events:
wsl /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stock-events \
  --from-beginning \
  --property print.key=true

# View consumer group offset lag:
wsl /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-event-live-group
```

---

## 12. Known Limitations & Next Steps

- **Multi-Broker Clustering**: The local configuration uses a single Kafka broker (`replication.factor=1`). Production setups should use at least 3 brokers across availability zones with `min.insync.replicas=2`.
- **Authentication**: Authentication is excluded per specification. Production deployments should enforce JWT/OAuth tokens over TLS for WebSocket connections and SASL/SCRAM for Kafka.
- **Safety Disclaimer**: Simulated prices only; educational system. Threshold crossings and CEP downtrend alerts are notifications, not financial trading advice.

