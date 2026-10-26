# Testing Strategy & Test Verification Report

## 1. Test Architecture

The testing suite covers all layers of the event-driven stack:

```
backend/tests/
|-- test_schemas.py               # Pydantic schema validation, edge cases, negative prices
|-- test_threshold_evaluator.py   # Pure hysteresis evaluation, canonical scenario, symbol isolation
|-- test_api.py                   # REST endpoints, CRUD thresholds, simulator controls, replay limits
|-- test_channels.py              # Async WebsocketCommunicator, handshake, ping-pong, channel groups
|-- test_replay.py                # Replay lifecycle, metadata preservation, original timestamp preservation
|-- test_kafka_integration.py     # Live Kafka producer publishing, consumer reading, offset isolation
```

---

## 2. Running Backend Tests

Run all 28 tests using `uv`:
```powershell
cd backend
uv run pytest -v
```

Or execute via the convenience script from repository root:
```powershell
.\scripts\run_tests.bat
```

### Running Specific Test Modules:
```powershell
# Run hysteresis & threshold crossing logic tests only
uv run pytest tests/test_threshold_evaluator.py -v

# Run Django Channels WebSocket consumer tests only
uv run pytest tests/test_channels.py -v

# Run Live Kafka integration tests only
uv run pytest tests/test_kafka_integration.py -v
```

---

## 3. Test Verification Checklist

| Test Name | Suite | Scope Verified |
| :--- | :--- | :--- |
| `test_price_update_valid` | Schemas | Ensures symbols normalize to uppercase, prices round to 4 decimals, unique IDs generated. |
| `test_price_update_invalid_price` | Schemas | Rejects non-positive prices (`<= 0`) with validation error. |
| `test_threshold_crossing_event_schema` | Schemas | Validates complete event payload schema and default processing mode. |
| `test_initial_price_does_not_produce_crossing` | Evaluator | Confirms first observation records state without inventing a false crossing. |
| `test_downward_threshold_crossing_detected` | Evaluator | Detects `180.40 -> 179.80` crossing for downward rule `<= $180.00`. |
| `test_remaining_below_threshold_does_not_repeat` | Evaluator | Confirms `179.80 -> 179.20 -> 178.50` does NOT emit duplicate alerts. |
| `test_rearming_after_returning_above_threshold` | Evaluator | Confirms bouncing back above `$180` (`179.80 -> 180.50`) re-arms the trigger for subsequent crossing (`180.50 -> 179.50`). |
| `test_canonical_sequence_scenario` | Evaluator | Validates the complete 8-step project scenario sequence, confirming exactly 2 crossing events emitted at indices 4 and 7. |
| `test_symbol_independence` | Evaluator | Confirms AAPL and MSFT maintain isolated previous-price and trigger states. |
| `test_websocket_connection_and_handshake` | Channels | Verifies WebSocket connection establishment, client ID assignment, and welcome handshake. |
| `test_channel_layer_event_broadcast` | Channels | Confirms `channel_layer.group_send("live_stock_events")` pushes events to browser socket. |
| `test_replay_preserves_event_id_and_timestamp` | Replay | Confirms replayed events preserve original `event_id` and `event_timestamp` while setting `delivery_mode="REPLAY"`. |
| `test_kafka_publish_and_consume_price_update` | Kafka | Publishes real record to Kafka broker `stock-prices` and verifies consumer reads it. |
| `test_kafka_publish_and_consume_threshold_event` | Kafka | Captures real event in Kafka `stock-events` and verifies independent consumer read. |
| `test_kafka_replay_isolation_does_not_alter_live_offsets` | Kafka | **Crucial verification**: Proves that replay consumer partition seeking leaves live consumer group committed offsets untouched. |

---

## 4. Frontend Production Build Check

Verify that the React frontend compiles cleanly with Vite:
```powershell
cd frontend
npm run build
```

Expected output:
```
vite building client environment for production...
✓ 1943 modules transformed.
dist/index.html                   0.68 kB
dist/assets/index-XXXX.css        8.98 kB
dist/assets/index-XXXX.js       259.32 kB
✓ built in ~500ms
```
