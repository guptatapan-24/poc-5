# Kafka Architecture, Offsets, and Historical Replay

## 1. Topic Topology and Configuration

The system uses two primary Kafka topics created with explicit partition counts and retention policies:

```
+---------------------------------------------------------------------------------+
| TOPIC: stock-prices                                                             |
| Partitions: 2 | Replication Factor: 1 | Retention: 7 days (604,800,000 ms)      |
| Purpose: Ingress log for raw, continuous synthetic stock price updates.        |
+---------------------------------------------------------------------------------+

+---------------------------------------------------------------------------------+
| TOPIC: stock-events                                                             |
| Partitions: 2 | Replication Factor: 1 | Retention: 7 days (604,800,000 ms)      |
| Purpose: Retained log for captured business events (threshold crossings).       |
+---------------------------------------------------------------------------------+
```

### Partitioning Strategy and Ordering Guarantees
- In Apache Kafka, **total ordering is guaranteed only within a single partition**, not across partitions.
- Every message published to `stock-prices` and `stock-events` uses the **stock symbol** (e.g., `"AAPL"`, `"MSFT"`) as the **Kafka message key**.
- Kafka's default murmur2 partitioner computes:
  $$\text{partition} = \text{hash}(\text{key}) \pmod{\text{num\_partitions}}$$
- Because all price ticks and crossing events for `"AAPL"` have the same key, they are guaranteed to land on the **same partition**, preserving strict chronological sequence for each stock symbol.

---

## 2. Consumer Groups & Offset Management

In Kafka, an offset is a sequential integer ID assigned to each record as it is written to a partition.

The system employs separate consumer groups to decouple functional responsibilities:

```
[stock-prices]
      |
      +---> Consumer Group: threshold-evaluator-group (tracks processed price updates)

[stock-events]
      |
      +---> Consumer Group: stock-event-live-group (tracks pushed live notifications)
      |
      +---> Ephemeral Replay Worker (NO group ID, manual partition assignment)
```

### Why Replay Does Not Alter Live Offsets
When a traditional Kafka consumer runs under a group ID (such as `stock-event-live-group`), Kafka maintains its current committed offset in the internal `__consumer_offsets` topic.

If a replay service were to join `stock-event-live-group` and seek backward to offset 0, it would corrupt the live stream: the live consumer would re-process and re-broadcast old messages to everyone.

To prevent this:
1. The **`ReplayService` never uses `stock-event-live-group`**.
2. It uses **Manual Partition Assignment** via `consumer.assign([TopicPartition('stock-events', 0), TopicPartition('stock-events', 1)])`.
3. It disables offset commits (`enable_auto_commit=False`).
4. When `consumer.seek_to_beginning()` is called, it adjusts **only the private, in-memory fetch pointer** of that specific replay thread.
5. Once the requested batch limit (e.g., 20 events) is retrieved, the ephemeral consumer is closed.
6. The live consumer continues reading newly arriving events without interruption.

---

## 3. Event Replay vs. Input Replay

| Dimension | Event Replay (Implemented) | Input Replay (Alternative) |
| :--- | :--- | :--- |
| **Source Stream** | `stock-events` | `stock-prices` |
| **Logic Execution** | None (reads already evaluated events) | Re-runs `ThresholdEvaluator` logic |
| **Determinism** | **100% deterministic**; returns the exact historical event ID and timestamp captured at the time | Depends on current threshold configuration (which may have changed since the original event) |
| **Overhead** | Very lightweight disk seek | Intensive re-computation of every historical price update |
| **Side Effects** | Zero side-effects; streamed with `delivery_mode="REPLAY"` | Risk of re-publishing duplicate events into Kafka |

This system implements **Event Replay**, reading durable historical business events directly from `stock-events` and streaming them with clear delivery metadata.

---

## 4. Topic Inspection Commands

You can inspect the retained Kafka records directly from the terminal using Kafka's built-in console tools:

### Inspecting `stock-prices` Topic:
```bash
wsl /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --max-messages 10
```

### Inspecting `stock-events` Topic:
```bash
wsl /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stock-events \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --max-messages 10
```

### Checking Consumer Group Lag:
```bash
wsl /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-event-live-group
```
