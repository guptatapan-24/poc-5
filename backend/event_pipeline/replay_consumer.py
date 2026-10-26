"""Historical event replay service: reads retained records from Kafka 'stock-events' topic
and streams them to the requesting WebSocket client without modifying live offsets or republishing events.
"""

from datetime import datetime, timezone
import json
import logging
import threading
import time
from typing import Optional
import uuid
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from kafka import KafkaConsumer, TopicPartition
from events.schemas import (
    ThresholdCrossingEvent,
    WebSocketEventMessage,
    DeliveryMode,
    ReplayRequest,
)
from events.state import replay_state

logger = logging.getLogger(__name__)


class ReplayService:
    """Manages isolated historical event replays from Kafka."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = settings.KAFKA_TOPIC_EVENTS
        self.channel_layer = get_channel_layer()

    def start_replay(self, request: ReplayRequest) -> str:
        """Initiates an asynchronous replay task. Returns generated replay_id."""
        replay_id = f"replay-{uuid.uuid4().hex[:8]}"
        replay_state.register_replay(
            replay_id=replay_id,
            limit=request.limit,
            symbol=request.symbol,
            client_id=request.client_id,
        )

        # Launch replay worker in a background thread
        thread = threading.Thread(
            target=self._execute_replay,
            args=(replay_id, request),
            name=f"ReplayWorker-{replay_id}",
            daemon=True,
        )
        thread.start()
        return replay_id

    def _execute_replay(self, replay_id: str, request: ReplayRequest):
        """Worker executing isolated Kafka partition seek and replay dispatch."""
        logger.info(
            "Starting replay %s [limit=%d, symbol=%s, client=%s]",
            replay_id,
            request.limit,
            request.symbol,
            request.client_id,
        )

        consumer: Optional[KafkaConsumer] = None
        replayed_count = 0
        target_group = f"client_{request.client_id}" if request.client_id else "live_stock_events"

        try:
            # Create an isolated ephemeral consumer with NO group ID or ephemeral group
            # Using manual partition assignment (assign) completely avoids group coordinator overhead
            # and GUARANTEES the live consumer group's offsets are never touched.
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
                enable_auto_commit=False,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=3000,
            )

            # Discover all partitions for the topic
            partitions = consumer.partitions_for_topic(self.topic)
            if not partitions:
                logger.warning("Replay %s: Topic '%s' has no partitions or does not exist", replay_id, self.topic)
                self._send_status(target_group, replay_id, "COMPLETED", 0, "No partitions or events found")
                replay_state.finish_replay(replay_id, 0)
                return

            topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
            consumer.assign(topic_partitions)

            # Seek to beginning of each partition to read all retained records
            consumer.seek_to_beginning(*topic_partitions)

            # Check beginning and ending offsets to see how many records exist
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            end_offsets = consumer.end_offsets(topic_partitions)
            total_available = sum(end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions)

            logger.info("Replay %s: Total records currently retained in Kafka: %d", replay_id, total_available)

            # Notify client that replay has started
            self._send_status(
                target_group,
                replay_id,
                "STARTED",
                0,
                f"Reading historical events from Kafka (total retained: {total_available})",
            )

            collected_events = []
            seen_event_ids = set()
            empty_poll_streak = 0

            # Read bounded number of records
            while len(collected_events) < request.limit and empty_poll_streak < 3:
                records = consumer.poll(timeout_ms=1000)
                if not records:
                    empty_poll_streak += 1
                    continue

                empty_poll_streak = 0
                for tp, messages in records.items():
                    for record in messages:
                        try:
                            raw_val = record.value
                            from events.schemas import parse_stock_event
                            event = parse_stock_event(raw_val)

                            # Filter by symbol if requested
                            if request.symbol and event.symbol != request.symbol:
                                continue

                            # De-duplicate historical replays by event_id across partitions
                            if event.event_id in seen_event_ids:
                                continue
                            seen_event_ids.add(event.event_id)

                            collected_events.append((event, record.partition, record.offset, record.timestamp))

                            if len(collected_events) >= request.limit:
                                break
                        except Exception as parse_err:
                            logger.error("Replay %s: Failed to parse record at offset %d: %s", replay_id, record.offset, parse_err)

            # Sort collected events by original timestamp to ensure chronological replay
            collected_events.sort(key=lambda item: item[0].event_timestamp)

            # Stream collected events over Django Channels to the target client
            for event, part, offset, ts in collected_events:
                # Mark delivery mode as REPLAY
                event.processing_mode = DeliveryMode.REPLAY

                envelope = WebSocketEventMessage(
                    message_type="stock_event",
                    delivery_mode=DeliveryMode.REPLAY,
                    replay_id=replay_id,
                    event=event.model_dump(),
                    details={
                        "original_partition": part,
                        "original_offset": offset,
                        "original_timestamp": ts,
                        "replayed_at": datetime.now(timezone.utc).isoformat(),
                        "event_type": event.event_type,
                    },
                )

                async_to_sync(self.channel_layer.group_send)(
                    target_group,
                    {
                        "type": "stock.event",
                        "payload": envelope.model_dump(),
                    },
                )
                replayed_count += 1
                replay_state.update_progress(replay_id, replayed_count)
                # Paced delivery (50ms) for smooth dashboard visualization
                time.sleep(0.05)

            # Replay finished successfully
            logger.info("Replay %s finished successfully. Dispatched %d historical events.", replay_id, replayed_count)
            self._send_status(
                target_group,
                replay_id,
                "COMPLETED",
                replayed_count,
                f"Successfully replayed {replayed_count} historical events from Kafka",
            )
            replay_state.finish_replay(replay_id, replayed_count)

        except Exception as e:
            logger.error("Replay %s failed: %s", replay_id, e)
            self._send_status(target_group, replay_id, "FAILED", replayed_count, str(e))
            replay_state.finish_replay(replay_id, replayed_count, error=str(e))

        finally:
            if consumer:
                try:
                    consumer.close()
                    logger.debug("Closed replay KafkaConsumer cleanly for %s", replay_id)
                except Exception as close_err:
                    logger.warning("Error closing replay consumer: %s", close_err)

    def _send_status(self, group: str, replay_id: str, status: str, count: int, message: str):
        """Helper to send replay lifecycle notifications over Channels."""
        msg = WebSocketEventMessage(
            message_type="replay_status",
            delivery_mode=DeliveryMode.REPLAY,
            replay_id=replay_id,
            details={
                "status": status,
                "replayed_count": count,
                "message": message,
            },
        )
        async_to_sync(self.channel_layer.group_send)(
            group,
            {
                "type": "stock.event",
                "payload": msg.model_dump(),
            },
        )


# Global singleton instance
replay_service = ReplayService()
