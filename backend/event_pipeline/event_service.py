"""Coordinator facade for checking system health, inspecting Kafka topics, and managing background simulation."""

import json
import logging
from typing import Dict, Any, List, Optional
from django.conf import settings
import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from events.state import simulator_state
from event_pipeline.price_simulator import PriceSimulator
from event_pipeline.replay_consumer import replay_service
import threading

logger = logging.getLogger(__name__)


class EventService:
    """Unified service interface for system management, health diagnostics, and topic inspection."""

    def __init__(self):
        self._simulator_instance: Optional[PriceSimulator] = None
        self._simulator_thread: Optional[threading.Thread] = None

    def check_health(self) -> Dict[str, Any]:
        """Performs live health checks on Redis and Kafka broker."""
        health = {
            "status": "HEALTHY",
            "redis": {"reachable": False, "details": None},
            "kafka": {"reachable": False, "details": None},
            "simulator": simulator_state.get_status(),
        }

        # Test Redis
        try:
            r = redis.from_url(settings.REDIS_URL, socket_timeout=2.0)
            ping = r.ping()
            health["redis"]["reachable"] = bool(ping)
            health["redis"]["details"] = f"Connected to {settings.REDIS_URL}"
        except Exception as e:
            health["redis"]["reachable"] = False
            health["redis"]["details"] = str(e)
            health["status"] = "DEGRADED"

        # Test Kafka
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                request_timeout_ms=3000,
            )
            topics = admin.list_topics()
            admin.close()
            health["kafka"]["reachable"] = True
            health["kafka"]["details"] = f"Broker active at {settings.KAFKA_BOOTSTRAP_SERVERS}. Found topics: {topics}"
        except Exception as e:
            health["kafka"]["reachable"] = False
            health["kafka"]["details"] = str(e)
            health["status"] = "DEGRADED"

        return health

    def inspect_events_topic(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Reads and inspects the most recent captured events from 'stock-events' topic."""
        records_out = []
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                enable_auto_commit=False,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=2000,
            )
            topic = settings.KAFKA_TOPIC_EVENTS
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.close()
                return []

            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(topic_partitions)
            consumer.seek_to_beginning(*topic_partitions)

            polled = consumer.poll(timeout_ms=1000)
            for tp, messages in polled.items():
                for record in messages:
                    records_out.append({
                        "partition": record.partition,
                        "offset": record.offset,
                        "key": record.key,
                        "timestamp": record.timestamp,
                        "event": record.value,
                    })
            consumer.close()
            # Return last N events
            return records_out[-limit:]
        except Exception as e:
            logger.error("Failed to inspect topic: %s", e)
            return []

    def start_background_simulator(self, mode: str = "deterministic", interval: float = 1.0, symbols: Optional[List[str]] = None) -> bool:
        """Starts the price simulator in a background thread."""
        if simulator_state.is_running():
            logger.warning("Simulator is already running.")
            return False

        symbols = symbols or ["AAPL"]
        self._simulator_instance = PriceSimulator(
            symbols=symbols,
            interval_seconds=interval,
            mode=mode,
        )
        self._simulator_thread = threading.Thread(
            target=self._simulator_instance.start,
            name="BackgroundPriceSimulator",
            daemon=True,
        )
        self._simulator_thread.start()
        logger.info("Background PriceSimulator thread launched.")
        return True

    def stop_background_simulator(self) -> bool:
        """Stops the active background simulator thread."""
        if self._simulator_instance:
            self._simulator_instance.stop()
            self._simulator_instance = None
            logger.info("Background PriceSimulator stopped.")
            return True
        return False


# Global singleton instance
event_service = EventService()
