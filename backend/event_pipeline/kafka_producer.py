"""Kafka producer client wrapper for publishing price updates and threshold crossing events."""

import json
import logging
from typing import Optional
from django.conf import settings
from kafka import KafkaProducer
from events.schemas import PriceUpdate, ThresholdCrossingEvent

logger = logging.getLogger(__name__)


class StockEventProducer:
    """Thread-safe Kafka producer for stock price updates and detected threshold events."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self._producer: Optional[KafkaProducer] = None
        self._connect()

    def _connect(self):
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,  # Preserve per-partition order
                request_timeout_ms=10000,
            )
            logger.info("StockEventProducer successfully connected to Kafka at %s", self.bootstrap_servers)
        except Exception as e:
            logger.error("Failed to initialize KafkaProducer at %s: %s", self.bootstrap_servers, e)
            self._producer = None

    def is_connected(self) -> bool:
        return self._producer is not None

    def publish_price_update(self, price_update: PriceUpdate) -> bool:
        """Publishes a raw stock price update to 'stock-prices' topic keyed by symbol."""
        if not self._producer:
            self._connect()
            if not self._producer:
                logger.error("Cannot publish price update: Kafka producer not connected")
                return False

        topic = settings.KAFKA_TOPIC_PRICES
        key = price_update.symbol
        value = price_update.model_dump()

        try:
            future = self._producer.send(topic, key=key, value=value)
            # Flush or wait for confirmation
            record_metadata = future.get(timeout=5.0)
            logger.debug(
                "Published price update for %s ($%.2f) to topic %s [part=%d, offset=%d]",
                price_update.symbol,
                price_update.price,
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return True
        except Exception as e:
            logger.error("Error publishing price update to Kafka: %s", e)
            return False

    def publish_threshold_event(self, event) -> bool:
        """Publishes a detected threshold crossing or CEP pattern event to 'stock-events' topic keyed by symbol."""
        if not self._producer:
            self._connect()
            if not self._producer:
                logger.error("Cannot publish event: Kafka producer not connected")
                return False

        topic = getattr(settings, "KAFKA_TOPIC_EVENTS", "stock-events")
        key = getattr(event, "symbol", "AAPL")
        value = event.model_dump() if hasattr(event, "model_dump") else event

        try:
            future = self._producer.send(topic, key=key, value=value)
            record_metadata = future.get(timeout=5.0)
            event_type = getattr(event, "event_type", value.get("event_type", "EVENT"))
            logger.info(
                "Captured %s %s for %s in topic %s [part=%d, offset=%d]",
                event_type,
                getattr(event, "event_id", value.get("event_id")),
                key,
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return True
        except Exception as e:
            logger.error("Error publishing event to Kafka: %s", e)
            return False

    def flush(self):
        if self._producer:
            self._producer.flush()

    def close(self):
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info("StockEventProducer closed cleanly")
            except Exception as e:
                logger.warning("Error during producer close: %s", e)
            finally:
                self._producer = None
