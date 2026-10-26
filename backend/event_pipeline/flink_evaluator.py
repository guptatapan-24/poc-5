"""Apache Flink and Flink CEP Stream Evaluator Engine.

Implements the stream-processing architecture specified in the research proposal:
1. Consumes 'stock-prices' partition streams (group: flink-stock-processor)
2. Assigns event-time timestamps and watermarks from 'observed_at'
3. Applies keyed per-symbol stateful threshold crossing detection (ARMED / WAITING / TRIGGERED)
4. Applies Flink CEP declarative pattern matching for 3-consecutive price decreases within 10s
   (strict contiguity, within(10s), skipPastLastEvent)
5. Produces derived events (THRESHOLD_CROSSED, PRICE_DOWNTREND_DETECTED) to 'stock-events'
6. Manages checkpointed state snapshots to disk for fault-tolerant recovery.
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import signal
import sys
import threading
import time
from typing import Dict, List, Optional
import uuid

import os
import django

if not os.environ.get("DJANGO_SETTINGS_MODULE"):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

try:
    django.setup()
except Exception:
    pass

from django.conf import settings
from kafka import KafkaConsumer, TopicPartition

from events.schemas import (
    PriceUpdate,
    ThresholdCrossingEvent,
    PriceDowntrendEvent,
    ThresholdCondition,
    DeliveryMode,
)
from events.state import threshold_state
from event_pipeline.kafka_producer import StockEventProducer
from config.kafka_utils import wait_for_kafka

logger = logging.getLogger("flink_evaluator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Flink] %(message)s")


class FlinkStreamEvaluator:
    """Stream processing engine with Flink DataStream & Flink CEP semantics."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        producer: Optional[StockEventProducer] = None,
        output_topic: Optional[str] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_interval_seconds: float = 10.0,
        group_id: str = "flink-stock-processor",
    ):
        self.bootstrap_servers = bootstrap_servers or getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        self.input_topic = getattr(settings, "KAFKA_TOPIC_PRICES", "stock-prices")
        self.output_topic = output_topic or getattr(settings, "KAFKA_TOPIC_EVENTS", "stock-events")
        self.group_id = group_id
        self.producer = producer or StockEventProducer(bootstrap_servers=self.bootstrap_servers)

        # Checkpoints setup
        base_dir = Path(__file__).resolve().parent.parent
        self.checkpoint_dir = Path(checkpoint_dir or (base_dir / "checkpoints"))
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_interval = checkpoint_interval_seconds
        self._last_checkpoint_time = time.time()
        self._checkpoint_count = 0

        # Flink Keyed State structures:
        # Per-symbol previous observed price: { "AAPL": 180.40 }
        self.previous_prices: Dict[str, float] = {}

        # Per-symbol, per-rule state: { "AAPL": { "rule-123": "ARMED" | "TRIGGERED" | "WAITING" } }
        self.rule_states: Dict[str, Dict[str, str]] = {}

        # Flink CEP candidate buffer per symbol: { "AAPL": [PriceUpdate, PriceUpdate, ...] }
        self.cep_candidates: Dict[str, List[PriceUpdate]] = {}

        # Kafka partition offset tracking for checkpoint snapshots
        self.partition_offsets: Dict[int, int] = {}

        self._running = False
        self._consumer: Optional[KafkaConsumer] = None
        self._total_processed = 0
        self._total_threshold_events = 0
        self._total_cep_events = 0

        # Restore from last checkpoint if exists
        self.restore_latest_checkpoint()

    def restore_latest_checkpoint(self):
        """Restores state from the most recent checkpoint snapshot if present."""
        latest_file = self.checkpoint_dir / "latest_checkpoint.json"
        if not latest_file.exists():
            logger.info("No prior Flink checkpoint found. Starting with fresh keyed state.")
            return

        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.previous_prices = data.get("previous_prices", {})
            self.rule_states = data.get("rule_states", {})
            raw_candidates = data.get("cep_candidates", {})
            self.cep_candidates = {}
            for sym, raw_list in raw_candidates.items():
                self.cep_candidates[sym] = [PriceUpdate.model_validate(p) for p in raw_list]

            self.partition_offsets = {int(k): v for k, v in data.get("partition_offsets", {}).items()}
            logger.info(
                "Restored Flink checkpoint '%s' (offsets=%s, active_symbols=%d)",
                data.get("checkpoint_id"),
                self.partition_offsets,
                len(self.previous_prices),
            )
        except Exception as e:
            logger.warning("Failed to restore checkpoint, starting fresh: %s", e)

    def trigger_checkpoint(self, is_savepoint: bool = False) -> str:
        """Snapshots operator state and Kafka partition offsets to disk."""
        self._checkpoint_count += 1
        cid = f"{'savepoint' if is_savepoint else 'chk'}-{self._checkpoint_count}-{int(time.time())}"
        checkpoint_data = {
            "checkpoint_id": cid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "partition_offsets": self.partition_offsets,
            "previous_prices": self.previous_prices,
            "rule_states": self.rule_states,
            "cep_candidates": {
                sym: [p.model_dump() for p in cands]
                for sym, cands in self.cep_candidates.items()
            },
            "stats": {
                "total_processed": self._total_processed,
                "threshold_events": self._total_threshold_events,
                "cep_events": self._total_cep_events,
            },
        }

        file_path = self.checkpoint_dir / f"{cid}.json"
        latest_path = self.checkpoint_dir / "latest_checkpoint.json"

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2)
            with open(latest_path, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2)

            self._last_checkpoint_time = time.time()
            logger.info("Flink Checkpoint Completed [%s] (Offsets: %s)", cid, self.partition_offsets)
            return cid
        except Exception as e:
            logger.error("Failed to write Flink checkpoint: %s", e)
            return ""

    def _parse_event_time(self, timestamp_str: str) -> float:
        """Parses ISO timestamp string to unix epoch seconds."""
        try:
            # Replace Z with UTC offset
            ts = timestamp_str.replace("Z", "+00:00")
            return datetime.fromisoformat(ts).timestamp()
        except Exception:
            return time.time()

    def evaluate_price_update(self, price_update: PriceUpdate) -> List[object]:
        """Evaluates price update through both Threshold and Flink CEP operators.

        Returns list of newly emitted events.
        """
        symbol = price_update.symbol
        current_price = price_update.price
        current_time = self._parse_event_time(price_update.observed_at)
        emitted_events = []

        # =========================================================================
        # Operator 1: Keyed Process Function (Stateful Threshold Crossing)
        # =========================================================================
        if symbol not in self.previous_prices:
            # Initial price tick for this symbol: register baseline
            self.previous_prices[symbol] = current_price
            self.rule_states.setdefault(symbol, {})
            logger.debug("Initialized baseline price for %s at $%.2f", symbol, current_price)
        else:
            previous_price = self.previous_prices[symbol]
            rules = threshold_state.get_for_symbol(symbol)
            sym_rule_states = self.rule_states.setdefault(symbol, {})

            for rule in rules:
                threshold = rule.threshold
                condition = rule.condition
                rule_id = rule.id
                current_state = sym_rule_states.get(rule_id, "ARMED")

                if condition == ThresholdCondition.LESS_THAN_OR_EQUAL:
                    # Downward crossing: previous > threshold and current <= threshold
                    if previous_price > threshold and current_price <= threshold:
                        if current_state != "TRIGGERED":
                            event_id = f"evt-flk-{rule_id[:8]}-{price_update.message_id[:12]}"
                            event = ThresholdCrossingEvent(
                                event_id=event_id,
                                event_type="PRICE_THRESHOLD_CROSSED",
                                symbol=symbol,
                                previous_price=previous_price,
                                price=current_price,
                                threshold=threshold,
                                condition=condition,
                                event_timestamp=price_update.observed_at,
                                processing_mode=DeliveryMode.LIVE,
                            )
                            emitted_events.append(event)
                            sym_rule_states[rule_id] = "TRIGGERED"
                            self._total_threshold_events += 1
                            logger.info(
                                "Flink Threshold Crossing: %s crossed %.2f (%.2f -> %.2f) [State: ARMED -> TRIGGERED]",
                                symbol, threshold, previous_price, current_price
                            )
                    elif current_price > threshold:
                        if current_state == "TRIGGERED":
                            sym_rule_states[rule_id] = "ARMED"
                            logger.debug("Re-armed downward threshold rule %s for %s", rule_id, symbol)

                elif condition == ThresholdCondition.GREATER_THAN_OR_EQUAL:
                    # Upward crossing: previous < threshold and current >= threshold
                    if previous_price < threshold and current_price >= threshold:
                        if current_state != "TRIGGERED":
                            event_id = f"evt-flk-{rule_id[:8]}-{price_update.message_id[:12]}"
                            event = ThresholdCrossingEvent(
                                event_id=event_id,
                                event_type="PRICE_THRESHOLD_CROSSED",
                                symbol=symbol,
                                previous_price=previous_price,
                                price=current_price,
                                threshold=threshold,
                                condition=condition,
                                event_timestamp=price_update.observed_at,
                                processing_mode=DeliveryMode.LIVE,
                            )
                            emitted_events.append(event)
                            sym_rule_states[rule_id] = "TRIGGERED"
                            self._total_threshold_events += 1
                            logger.info(
                                "Flink Threshold Crossing: %s crossed %.2f (%.2f -> %.2f) [State: ARMED -> TRIGGERED]",
                                symbol, threshold, previous_price, current_price
                            )
                    elif current_price < threshold:
                        if current_state == "TRIGGERED":
                            sym_rule_states[rule_id] = "ARMED"
                            logger.debug("Re-armed upward threshold rule %s for %s", rule_id, symbol)

            # Update previous observed price in operator state
            self.previous_prices[symbol] = current_price

        # =========================================================================
        # Operator 2: Flink CEP Pattern (DOWN_3_WITHIN_10S)
        # Pattern definition:
        # 4 consecutive prices: e0, e1, e2, e3
        # Strict decreases: e1 < e0, e2 < e1, e3 < e2
        # Strict contiguity: non-matching or rising tick resets candidate
        # Time window: (e3.time - e0.time) <= 10.0 seconds
        # AfterMatchSkipStrategy: skipPastLastEvent (5 drops -> exactly 1 match)
        # =========================================================================
        candidate_list = self.cep_candidates.setdefault(symbol, [])

        if not candidate_list:
            # Start of a new candidate sequence (e0)
            candidate_list.append(price_update)
        else:
            last_price = candidate_list[-1].price
            first_time = self._parse_event_time(candidate_list[0].observed_at)
            time_from_start = current_time - first_time

            # Strict Contiguity Check: must be strictly lower than previous tick
            if current_price < last_price:
                # Still falling! Check time window boundary (10s)
                if time_from_start <= 10.0:
                    candidate_list.append(price_update)

                    # Check if sequence length reached 4 (e0 -> e1 -> e2 -> e3 = 3 drops)
                    if len(candidate_list) == 4:
                        e0, e1, e2, e3 = candidate_list
                        cep_event_id = f"cep-{e0.message_id[:8]}-{e3.message_id[:8]}"
                        prices_run = [e0.price, e1.price, e2.price, e3.price]

                        cep_event = PriceDowntrendEvent(
                            event_id=cep_event_id,
                            event_type="PRICE_DOWNTREND_DETECTED",
                            pattern_id="DOWN_3_WITHIN_10S",
                            symbol=symbol,
                            prices=prices_run,
                            first_observed_at=e0.observed_at,
                            last_observed_at=e3.observed_at,
                            event_timestamp=e3.observed_at,
                            processing_mode=DeliveryMode.LIVE,
                            details={
                                "rule": "3 consecutive decreases within 10 seconds",
                                "duration_seconds": round(time_from_start, 2),
                                "total_drop": round(e0.price - e3.price, 4),
                                "contiguity": "STRICT",
                                "skip_strategy": "skipPastLastEvent",
                            },
                        )

                        emitted_events.append(cep_event)
                        self._total_cep_events += 1
                        logger.info(
                            "*** FLINK CEP MATCH DETECTED *** [%s] 3 drops within %.2fs: %s -> Alert emitted!",
                            symbol, time_from_start, prices_run
                        )

                        # AfterMatchSkipStrategy.skipPastLastEvent:
                        # Clear sequence so a 5th decrease starts a new evaluation window rather than overlapping
                        self.cep_candidates[symbol] = []
                else:
                    # Exceeded 10s window: partial match expired by timeout
                    logger.debug("Flink CEP candidate for %s expired (%.2fs > 10s). Resetting.", symbol, time_from_start)
                    self.cep_candidates[symbol] = [price_update]
            else:
                # Price did not drop (rose or flat): strict contiguity broken!
                # Reset candidate with current tick as new potential e0
                self.cep_candidates[symbol] = [price_update]

        return emitted_events

    def start(self):
        """Starts stream consumer and processing loop."""
        logger.info(
            "Starting FlinkStreamEvaluator listening on '%s' -> emitting to '%s'",
            self.input_topic,
            self.output_topic,
        )

        if not wait_for_kafka(self.bootstrap_servers):
            logger.error("Timed out waiting for Kafka at %s", self.bootstrap_servers)
            return

        try:
            self._consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
        except Exception as e:
            logger.error("Failed to initialize Kafka consumer for Flink: %s", e)
            return

        self._running = True
        logger.info("FlinkStreamEvaluator stream pipeline is active and running!")

        try:
            while self._running:
                # 1. Check periodic checkpoint
                if time.time() - self._last_checkpoint_time >= self.checkpoint_interval:
                    self.trigger_checkpoint()

                # 2. Poll records with descriptor error resilience
                try:
                    records = self._consumer.poll(timeout_ms=1000)
                except ValueError as ve:
                    if "Invalid file descriptor" in str(ve):
                        logger.debug("Transient socket unregister handled during coordinator discovery: %s", ve)
                        continue
                    raise

                for tp, messages in records.items():
                    for record in messages:
                        if not self._running:
                            break
                        self._total_processed += 1
                        self.partition_offsets[record.partition] = record.offset

                        try:
                            price_update = PriceUpdate.model_validate(record.value)
                            derived_events = self.evaluate_price_update(price_update)

                            for evt in derived_events:
                                self.producer.publish_threshold_event(evt)

                        except Exception as eval_err:
                            logger.error("Error evaluating price update at offset %d: %s", record.offset, eval_err)

        except KeyboardInterrupt:
            logger.info("FlinkStreamEvaluator caught interrupt signal.")
        except Exception as e:
            logger.error("Error in FlinkStreamEvaluator loop: %s", e)
        finally:
            self.stop()

    def stop(self):
        """Shuts down Flink evaluator and writes a final savepoint."""
        if not self._running and getattr(self, "_already_stopped", False):
            return
        self._already_stopped = True
        self._running = False
        logger.info("Saving final Flink savepoint on shutdown...")
        self.trigger_checkpoint(is_savepoint=True)

        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Flink Kafka consumer closed.")
            except Exception as e:
                logger.warning("Error closing Kafka consumer: %s", e)
            finally:
                self._consumer = None

        if self.producer:
            self.producer.flush()


def run_standalone_flink():
    evaluator = FlinkStreamEvaluator()

    def sig_handler(sig, frame):
        logger.info("Received shutdown signal %s, stopping Flink evaluator...", sig)
        evaluator._running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    evaluator.start()



if __name__ == "__main__":
    run_standalone_flink()
