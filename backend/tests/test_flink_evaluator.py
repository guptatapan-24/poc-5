"""Unit tests for FlinkStreamEvaluator and Flink CEP pattern detection."""

from datetime import datetime, timezone, timedelta
import shutil
import tempfile
from pathlib import Path
import pytest

from events.schemas import PriceUpdate, ThresholdRule, ThresholdCondition
from events.state import threshold_state
from event_pipeline.flink_evaluator import FlinkStreamEvaluator


class DummyProducer:
    def __init__(self):
        self.published = []

    def publish_threshold_event(self, event):
        self.published.append(event)
        return True

    def flush(self):
        pass


@pytest.fixture
def temp_checkpoint_dir():
    temp_dir = tempfile.mkdtemp(prefix="flink_test_chk_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def flink_evaluator(temp_checkpoint_dir):
    producer = DummyProducer()
    evaluator = FlinkStreamEvaluator(
        producer=producer,
        checkpoint_dir=temp_checkpoint_dir,
    )
    # Configure baseline AAPL threshold at $180.00
    threshold_state.clear_all()
    rule = ThresholdRule(
        id="rule-test-aapl-180",
        symbol="AAPL",
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
    )
    threshold_state.add_or_update(rule)
    return evaluator


def test_flink_threshold_crossing_canonical(flink_evaluator):
    """Verifies that the Flink evaluator reproduces the exact canonical 8-step threshold crossing."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    prices = [
        185.00,
        183.50,
        181.00,
        180.40,
        179.80,  # Crossing 1: ARMED -> TRIGGERED (Alert 1)
        179.20,  # Suppressed by hysteresis
        180.50,  # Re-arms: TRIGGERED -> ARMED
        179.50,  # Crossing 2: ARMED -> TRIGGERED (Alert 2)
    ]

    emitted_threshold_events = []
    for i, price in enumerate(prices):
        t_str = (base_time + timedelta(seconds=i)).isoformat()
        update = PriceUpdate(message_id=f"msg-{i}", symbol="AAPL", price=price, observed_at=t_str)
        events = flink_evaluator.evaluate_price_update(update)
        for e in events:
            if getattr(e, "event_type", None) == "PRICE_THRESHOLD_CROSSED":
                emitted_threshold_events.append(e)

    assert len(emitted_threshold_events) == 2
    assert emitted_threshold_events[0].price == 179.80
    assert emitted_threshold_events[0].previous_price == 180.40
    assert emitted_threshold_events[1].price == 179.50
    assert emitted_threshold_events[1].previous_price == 180.50


def test_flink_cep_downtrend_detection_success(flink_evaluator):
    """Verifies that 4 strictly decreasing prices in <= 10s trigger a PRICE_DOWNTREND_DETECTED event."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    prices = [185.00, 183.00, 181.00, 179.00]  # 3 drops within 6 seconds

    cep_events = []
    for i, price in enumerate(prices):
        t_str = (base_time + timedelta(seconds=i * 2)).isoformat()
        update = PriceUpdate(message_id=f"cep-msg-{i}", symbol="AAPL", price=price, observed_at=t_str)
        events = flink_evaluator.evaluate_price_update(update)
        for e in events:
            if getattr(e, "event_type", None) == "PRICE_DOWNTREND_DETECTED":
                cep_events.append(e)

    assert len(cep_events) == 1
    event = cep_events[0]
    assert event.symbol == "AAPL"
    assert event.pattern_id == "DOWN_3_WITHIN_10S"
    assert event.prices == [185.00, 183.00, 181.00, 179.00]
    assert event.details["contiguity"] == "STRICT"


def test_flink_cep_strict_contiguity_reset(flink_evaluator):
    """Verifies that an intervening flat or rising price breaks strict contiguity and resets candidate."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    # Price rises at step 2 (183.50 > 183.00), breaking contiguity
    prices = [185.00, 183.00, 183.50, 181.00, 179.00]

    cep_events = []
    for i, price in enumerate(prices):
        t_str = (base_time + timedelta(seconds=i)).isoformat()
        update = PriceUpdate(message_id=f"msg-flat-{i}", symbol="AAPL", price=price, observed_at=t_str)
        events = flink_evaluator.evaluate_price_update(update)
        for e in events:
            if getattr(e, "event_type", None) == "PRICE_DOWNTREND_DETECTED":
                cep_events.append(e)

    # Broken contiguity means only 2 consecutive drops (183.50 -> 181.00 -> 179.00), which is < 3 drops!
    assert len(cep_events) == 0


def test_flink_cep_time_window_timeout(flink_evaluator):
    """Verifies that an incomplete candidate exceeding 10s times out and does not fire."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    # e3 arrives at t = 14s (> 10s from e0 at t = 0s)
    timestamps = [0, 2, 4, 14]
    prices = [185.00, 183.00, 181.00, 179.00]

    cep_events = []
    for i, (offset_s, price) in enumerate(zip(timestamps, prices)):
        t_str = (base_time + timedelta(seconds=offset_s)).isoformat()
        update = PriceUpdate(message_id=f"msg-time-{i}", symbol="AAPL", price=price, observed_at=t_str)
        events = flink_evaluator.evaluate_price_update(update)
        for e in events:
            if getattr(e, "event_type", None) == "PRICE_DOWNTREND_DETECTED":
                cep_events.append(e)

    assert len(cep_events) == 0


def test_flink_cep_skip_past_last_event(flink_evaluator):
    """Verifies skipPastLastEvent: a run of 5 consecutive falls produces exactly 1 match, not 2."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    prices = [185.00, 183.00, 181.00, 179.00, 177.00]  # 5 strict drops in 5 seconds

    cep_events = []
    for i, price in enumerate(prices):
        t_str = (base_time + timedelta(seconds=i)).isoformat()
        update = PriceUpdate(message_id=f"msg-skip-{i}", symbol="AAPL", price=price, observed_at=t_str)
        events = flink_evaluator.evaluate_price_update(update)
        for e in events:
            if getattr(e, "event_type", None) == "PRICE_DOWNTREND_DETECTED":
                cep_events.append(e)

    assert len(cep_events) == 1
    assert cep_events[0].prices == [185.00, 183.00, 181.00, 179.00]


def test_flink_checkpoint_save_and_restore(flink_evaluator, temp_checkpoint_dir):
    """Verifies that Flink evaluator state snapshots to checkpoint and restores correctly."""
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    # Feed 2 drops
    flink_evaluator.evaluate_price_update(
        PriceUpdate(message_id="chk-1", symbol="AAPL", price=185.00, observed_at=base_time.isoformat())
    )
    flink_evaluator.evaluate_price_update(
        PriceUpdate(message_id="chk-2", symbol="AAPL", price=183.00, observed_at=(base_time + timedelta(seconds=1)).isoformat())
    )
    flink_evaluator.partition_offsets[0] = 42

    cid = flink_evaluator.trigger_checkpoint()
    assert cid != ""
    assert Path(temp_checkpoint_dir, f"{cid}.json").exists()

    # Create new evaluator pointing to same checkpoint directory
    restored_evaluator = FlinkStreamEvaluator(
        producer=DummyProducer(),
        checkpoint_dir=temp_checkpoint_dir,
    )

    assert restored_evaluator.previous_prices["AAPL"] == 183.00
    assert restored_evaluator.partition_offsets[0] == 42
    assert len(restored_evaluator.cep_candidates["AAPL"]) == 2
