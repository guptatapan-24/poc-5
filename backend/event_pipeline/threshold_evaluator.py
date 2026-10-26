"""Threshold evaluator engine: consumes price updates from Kafka, evaluates crossing logic,
and publishes detected crossing events to 'stock-events' Kafka topic.
"""

import json
import logging
import signal
import sys
from typing import Dict, List, Optional
from django.conf import settings
from kafka import KafkaConsumer
from events.schemas import (
    PriceUpdate,
    ThresholdCrossingEvent,
    ThresholdCondition,
    ThresholdRule,
    DeliveryMode,
)
from events.state import threshold_state
from event_pipeline.kafka_producer import StockEventProducer
from config.kafka_utils import wait_for_kafka

logger = logging.getLogger(__name__)


class ThresholdEvaluator:
    """Evaluates stock price updates against configured thresholds with hysteresis tracking."""

    def __init__(
        self,
        producer: Optional[StockEventProducer] = None,
        bootstrap_servers: Optional[str] = None,
    ):
        self.producer = producer or StockEventProducer()
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self._running = False
        self._consumer: Optional[KafkaConsumer] = None

        # Tracks previous observed price per symbol: { "AAPL": 180.40 }
        self.previous_prices: Dict[str, float] = {}

        # Tracks whether a rule has already fired while staying on the triggered side:
        # { rule_id: True } means rule has fired and needs to reset across the boundary before firing again.
        self._rule_triggered_state: Dict[str, bool] = {}

    def evaluate_price_update(
        self,
        price_update: PriceUpdate,
        active_rules: Optional[List[ThresholdRule]] = None,
    ) -> List[ThresholdCrossingEvent]:
        """Pure evaluation logic given a price update and rules.

        Returns list of newly triggered ThresholdCrossingEvents.
        """
        symbol = price_update.symbol
        current_price = price_update.price
        detected_events: List[ThresholdCrossingEvent] = []

        if symbol not in self.previous_prices:
            # First price observation for this symbol.
            # Record it without inventing a crossing event.
            self.previous_prices[symbol] = current_price
            logger.info("Recorded initial price for %s: $%.2f (no crossing evaluated)", symbol, current_price)
            return detected_events

        previous_price = self.previous_prices[symbol]
        rules = active_rules if active_rules is not None else threshold_state.get_for_symbol(symbol)

        for rule in rules:
            threshold = rule.threshold
            condition = rule.condition
            rule_id = rule.id
            is_currently_triggered = self._rule_triggered_state.get(rule_id, False)

            if condition == ThresholdCondition.LESS_THAN_OR_EQUAL:
                # Downward crossing: previous was strictly above threshold, current is at or below
                if previous_price > threshold and current_price <= threshold:
                    if not is_currently_triggered:
                        event = ThresholdCrossingEvent(
                            symbol=symbol,
                            previous_price=previous_price,
                            price=current_price,
                            threshold=threshold,
                            condition=condition,
                            processing_mode=DeliveryMode.LIVE,
                        )
                        detected_events.append(event)
                        self._rule_triggered_state[rule_id] = True
                        logger.info(
                            "DETECTED DOWNWARD CROSSING for %s: $%.2f -> $%.2f (Threshold: $%.2f)",
                            symbol,
                            previous_price,
                            current_price,
                            threshold,
                        )
                elif current_price > threshold:
                    # Price bounced back above threshold: re-arm trigger
                    if is_currently_triggered:
                        self._rule_triggered_state[rule_id] = False
                        logger.debug("Re-armed downward threshold rule %s for %s", rule_id, symbol)

            elif condition == ThresholdCondition.GREATER_THAN_OR_EQUAL:
                # Upward crossing: previous was strictly below threshold, current is at or above
                if previous_price < threshold and current_price >= threshold:
                    if not is_currently_triggered:
                        event = ThresholdCrossingEvent(
                            symbol=symbol,
                            previous_price=previous_price,
                            price=current_price,
                            threshold=threshold,
                            condition=condition,
                            processing_mode=DeliveryMode.LIVE,
                        )
                        detected_events.append(event)
                        self._rule_triggered_state[rule_id] = True
                        logger.info(
                            "DETECTED UPWARD CROSSING for %s: $%.2f -> $%.2f (Threshold: $%.2f)",
                            symbol,
                            previous_price,
                            current_price,
                            threshold,
                        )
                elif current_price < threshold:
                    # Price dipped back below threshold: re-arm trigger
                    if is_currently_triggered:
                        self._rule_triggered_state[rule_id] = False
                        logger.debug("Re-armed upward threshold rule %s for %s", rule_id, symbol)

        # Update previous price for the symbol
        self.previous_prices[symbol] = current_price
        return detected_events

    def start_consuming(self):
        """Starts Kafka consumer polling loop on 'stock-prices'."""
        topic = settings.KAFKA_TOPIC_PRICES
        group_id = "threshold-evaluator-group"

        logger.info(
            "Connecting ThresholdEvaluator KafkaConsumer to %s [topic=%s, group=%s]",
            self.bootstrap_servers,
            topic,
            group_id,
        )

        if not wait_for_kafka(self.bootstrap_servers):
            logger.error("Timed out waiting for Kafka broker at %s", self.bootstrap_servers)
            return

        try:
            self._consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,  # Unblocks poll periodically for clean shutdown check
            )
        except Exception as e:
            logger.error("Failed to initialize KafkaConsumer: %s", e)
            return

        self._running = True
        logger.info("ThresholdEvaluator started listening for price updates...")

        try:
            while self._running:
                # Poll batch of records
                records = self._consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for record in messages:
                        if not self._running:
                            break
                        try:
                            # Validate price update
                            data = record.value
                            price_update = PriceUpdate.model_validate(data)
                            logger.debug(
                                "Evaluator received: %s = $%.2f [part=%d, offset=%d]",
                                price_update.symbol,
                                price_update.price,
                                record.partition,
                                record.offset,
                            )

                            # Evaluate thresholds
                            events = self.evaluate_price_update(price_update)

                            # Publish each crossing event to 'stock-events'
                            for evt in events:
                                published = self.producer.publish_threshold_event(evt)
                                if not published:
                                    logger.error("Failed to capture event %s to Kafka!", evt.event_id)

                        except Exception as msg_err:
                            logger.error("Error evaluating message at offset %d: %s", record.offset, msg_err)

        except KeyboardInterrupt:
            logger.info("ThresholdEvaluator caught interrupt signal.")
        finally:
            self.stop()

    def stop(self):
        """Stops consuming and closes resources cleanly."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("ThresholdEvaluator consumer closed.")
            except Exception as e:
                logger.warning("Error closing Kafka consumer: %s", e)
            finally:
                self._consumer = None

        if self.producer:
            self.producer.flush()


def run_standalone_evaluator():
    """CLI / Management command runner for ThresholdEvaluator."""
    evaluator = ThresholdEvaluator()

    def sig_handler(sig, frame):
        logger.info("Caught signal %s, stopping ThresholdEvaluator...", sig)
        evaluator.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    evaluator.start_consuming()
