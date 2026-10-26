"""Live integration tests against local Apache Kafka broker."""

import json
import uuid
import pytest
from django.conf import settings
from kafka import KafkaConsumer, TopicPartition
from events.schemas import (
    PriceUpdate,
    ThresholdCrossingEvent,
    ThresholdCondition,
    DeliveryMode,
    ReplayRequest,
)
from event_pipeline.kafka_producer import StockEventProducer
from event_pipeline.replay_consumer import ReplayService


@pytest.fixture(scope="module")
def producer():
    p = StockEventProducer()
    yield p
    p.close()


def test_kafka_publish_and_consume_price_update(producer):
    """Publishes a real price update to 'stock-prices' and consumes it with an ephemeral consumer."""
    test_symbol = f"T{uuid.uuid4().hex[:4].upper()}"
    update = PriceUpdate(symbol=test_symbol, price=123.45)

    published = producer.publish_price_update(update)
    assert published is True, "Failed to publish price update to Kafka"

    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC_PRICES,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    found = False
    for message in consumer:
        val = message.value
        if val.get("symbol") == test_symbol and val.get("price") == 123.45:
            found = True
            break
    consumer.close()

    assert found is True, f"Could not find published price update for symbol {test_symbol} in Kafka"


def test_kafka_publish_and_consume_threshold_event(producer):
    """Publishes a real threshold crossing event to 'stock-events' and verifies independent consumer read."""
    event_id = f"evt-test-{uuid.uuid4().hex[:6]}"
    symbol = "AAPL"
    event = ThresholdCrossingEvent(
        event_id=event_id,
        symbol=symbol,
        previous_price=180.40,
        price=179.80,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
        processing_mode=DeliveryMode.LIVE,
    )

    published = producer.publish_threshold_event(event)
    assert published is True, "Failed to capture event to Kafka 'stock-events'"

    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC_EVENTS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    found = False
    for message in consumer:
        val = message.value
        if val.get("event_id") == event_id:
            found = True
            assert val.get("symbol") == symbol
            assert val.get("price") == 179.80
            assert val.get("threshold") == 180.00
            break
    consumer.close()

    assert found is True, f"Could not find captured event {event_id} in Kafka 'stock-events'"


def test_kafka_replay_isolation_does_not_alter_live_offsets(producer):
    """Verifies that an isolated replay consumer reading 'stock-events' does not touch

    or modify the committed offsets of the live consumer group.
    """
    topic = settings.KAFKA_TOPIC_EVENTS
    live_group = settings.KAFKA_CONSUMER_GROUP_LIVE

    # Create a simulated live consumer to record commit offset
    live_consumer = KafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        api_version=getattr(settings, "KAFKA_API_VERSION", (3, 8, 0)),
        group_id=live_group,
        enable_auto_commit=True,
        consumer_timeout_ms=2000,
    )
    assigned_partitions = [TopicPartition(topic, 0), TopicPartition(topic, 1)]
    live_consumer.assign(assigned_partitions)

    initial_live_offsets = {}
    for tp in assigned_partitions:
        initial_live_offsets[tp] = live_consumer.committed(tp)

    # Publish an event
    evt = ThresholdCrossingEvent(
        symbol="AAPL",
        previous_price=181.00,
        price=179.50,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
    )
    producer.publish_threshold_event(evt)

    # Run replay via ReplayService
    replay_svc = ReplayService()
    req = ReplayRequest(limit=10, symbol="AAPL", client_id="test-isolation-client")
    replay_id = replay_svc.start_replay(req)

    # Wait briefly for replay thread
    import time
    time.sleep(1.0)

    # Check live consumer offsets again
    for tp in assigned_partitions:
        current_committed = live_consumer.committed(tp)
        assert current_committed == initial_live_offsets[tp], (
            f"Live group committed offset for {tp} changed from {initial_live_offsets[tp]} to {current_committed} during replay!"
        )

    live_consumer.close()
