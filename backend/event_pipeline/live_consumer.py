"""Live Kafka event consumer: consumes captured threshold events from 'stock-events'
and forwards them to Django Channels layer via Redis for real-time WebSocket delivery.
"""

import json
import logging
import signal
import sys
from typing import Optional
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from kafka import KafkaConsumer
from events.schemas import (
    ThresholdCrossingEvent,
    PriceDowntrendEvent,
    parse_stock_event,
    WebSocketEventMessage,
    DeliveryMode,
)
from config.kafka_utils import wait_for_kafka
from events.state import recent_events_state

logger = logging.getLogger(__name__)


class LiveEventConsumer:
    """Dedicated background consumer for 'stock-events'.

    Forwards captured Kafka events asynchronously to connected WebSocket clients via Redis channel layer.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = settings.KAFKA_TOPIC_EVENTS
        self.group_id = settings.KAFKA_CONSUMER_GROUP_LIVE
        self._running = False
        self._consumer: Optional[KafkaConsumer] = None
        self.channel_layer = get_channel_layer()
        self._processed_event_ids = set()
        self._processed_event_order = []
        self._max_history = 5000

    def start(self):
        """Starts Kafka consumer polling loop."""
        logger.info(
            "Connecting LiveEventConsumer to Kafka at %s [topic=%s, group=%s]",
            self.bootstrap_servers,
            self.topic,
            self.group_id,
        )

        if not wait_for_kafka(self.bootstrap_servers):
            logger.error("Timed out waiting for Kafka broker at %s", self.bootstrap_servers)
            return

        try:
            self._consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,  # Periodic unblocking for clean shutdown check
            )
        except Exception as e:
            logger.error("Failed to initialize LiveEventConsumer: %s", e)
            return

        self._running = True
        logger.info("LiveEventConsumer is running and forwarding to Channels group 'live_stock_events'...")

        try:
            while self._running:
                records = self._consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for record in messages:
                        if not self._running:
                            break
                        self._handle_record(record)

        except KeyboardInterrupt:
            logger.info("LiveEventConsumer received keyboard interrupt.")
        finally:
            self.stop()

    def _handle_record(self, record):
        """Parses and validates captured Kafka event, then dispatches to Channels layer."""
        try:
            raw_data = record.value
            event = parse_stock_event(raw_data)

            # Idempotency check: prevent duplicate forwarding
            if event.event_id in self._processed_event_ids:
                logger.debug("Skipping already processed event %s", event.event_id)
                return

            self._processed_event_ids.add(event.event_id)
            self._processed_event_order.append(event.event_id)
            if len(self._processed_event_order) > self._max_history:
                oldest = self._processed_event_order.pop(0)
                self._processed_event_ids.discard(oldest)

            # Ensure delivery mode is LIVE
            event.processing_mode = DeliveryMode.LIVE

            envelope = WebSocketEventMessage(
                message_type="stock_event",
                delivery_mode=DeliveryMode.LIVE,
                event=event.model_dump(),
                details={
                    "kafka_partition": record.partition,
                    "kafka_offset": record.offset,
                    "kafka_timestamp": record.timestamp,
                    "event_type": event.event_type,
                },
            )

            # Send to all connected WebSocket clients subscribed to 'live_stock_events'
            payload = envelope.model_dump()
            recent_events_state.record_event(payload)

            async_to_sync(self.channel_layer.group_send)(
                "live_stock_events",
                {
                    "type": "stock.event",
                    "payload": payload,
                },
            )

            if event.event_type == "PRICE_DOWNTREND_DETECTED":
                logger.info(
                    "Delivered LIVE Flink CEP downtrend event %s (%s: %s) to WebSocket group [offset=%d]",
                    event.event_id,
                    event.symbol,
                    event.prices,
                    record.offset,
                )
            else:
                logger.info(
                    "Delivered LIVE event %s (%s at $%.2f) to WebSocket group [offset=%d]",
                    event.event_id,
                    event.symbol,
                    getattr(event, "price", 0.0),
                    record.offset,
                )

        except Exception as e:
            logger.error("Error processing event record at offset %d: %s", record.offset, e)

    def stop(self):
        """Stops the live consumer cleanly."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("LiveEventConsumer closed cleanly.")
            except Exception as e:
                logger.warning("Error during LiveEventConsumer close: %s", e)
            finally:
                self._consumer = None


def run_standalone_live_consumer():
    """CLI / Management command runner for LiveEventConsumer."""
    consumer = LiveEventConsumer()

    def sig_handler(sig, frame):
        logger.info("Caught signal %s, stopping LiveEventConsumer...", sig)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    consumer.start()
