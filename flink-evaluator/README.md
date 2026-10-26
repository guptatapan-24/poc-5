# Apache Flink CEP Stock Stream Evaluator

This module provides the Java DataStream and Flink CEP implementation as specified in the Technical Research & Implementation Proposal.

## Architecture

- **KafkaSource**: Consumes raw price updates from `stock-prices` with group `flink-stock-processor`.
- **Event-Time Watermarks**: Assigns event timestamps from `observed_at` with bounded out-of-orderness and partition idleness handling.
- **Keyed Stream**: Partitions stream by `symbol`.
- **Operator 1 (KeyedProcessFunction)**: Tracks stateful hysteresis (`WAITING`, `ARMED`, `TRIGGERED`) for price threshold crossings.
- **Operator 2 (Flink CEP)**: Detects 3-consecutive price decreases within 10 seconds (`DOWN_3_WITHIN_10S`) with strict contiguity and `skipPastLastEvent` overlap strategy.
- **KafkaSink**: Emits validated `THRESHOLD_CROSSED` and `PRICE_DOWNTREND_DETECTED` events to `stock-events`.
- **Checkpoints**: Periodic state snapshots for fault tolerance.

## Building the JAR

Run inside WSL2:
```bash
cd /mnt/c/Users/gupta/Desktop/PoC/flink-evaluator
mvn clean package -DskipTests
```
The shaded executable JAR will be generated at `target/flink-evaluator-1.0.0.jar`.

## Submitting to Apache Flink Cluster

```bash
flink run -c com.stockstream.flink.StockStreamFlinkJob target/flink-evaluator-1.0.0.jar
```
Web UI can be monitored at `http://localhost:8081`.
