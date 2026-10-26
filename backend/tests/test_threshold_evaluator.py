"""Unit tests for the ThresholdEvaluator core evaluation logic and hysteresis state."""

import pytest
from events.schemas import PriceUpdate, ThresholdRule, ThresholdCondition
from event_pipeline.threshold_evaluator import ThresholdEvaluator


class DummyProducer:
    """Mock producer capturing published threshold crossing events in memory."""
    def __init__(self):
        self.published_events = []

    def publish_threshold_event(self, event):
        self.published_events.append(event)
        return True

    def flush(self):
        pass


@pytest.fixture
def evaluator():
    producer = DummyProducer()
    return ThresholdEvaluator(producer=producer)


def test_initial_price_does_not_produce_crossing(evaluator):
    """Initial observation must record state without generating an artificial event."""
    rules = [
        ThresholdRule(symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)
    ]
    events = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=175.0), active_rules=rules)
    assert len(events) == 0
    assert evaluator.previous_prices["AAPL"] == 175.0


def test_downward_threshold_crossing_detected(evaluator):
    """Previous price > threshold and current <= threshold must trigger one event."""
    rules = [
        ThresholdRule(id="rule-down", symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)
    ]
    # First tick at 180.40
    evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=180.40), active_rules=rules)

    # Second tick crosses down to 179.80
    events = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=179.80), active_rules=rules)
    assert len(events) == 1
    evt = events[0]
    assert evt.symbol == "AAPL"
    assert evt.previous_price == 180.40
    assert evt.price == 179.80
    assert evt.threshold == 180.00
    assert evt.condition == ThresholdCondition.LESS_THAN_OR_EQUAL


def test_remaining_below_threshold_does_not_repeat(evaluator):
    """Continuing below threshold must NOT emit additional events."""
    rules = [
        ThresholdRule(id="rule-down", symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)
    ]
    evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=180.40), active_rules=rules)
    first_crossing = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=179.80), active_rules=rules)
    assert len(first_crossing) == 1

    # Price falls further to 179.20 (should NOT emit)
    second_tick = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=179.20), active_rules=rules)
    assert len(second_tick) == 0

    # Price falls further to 178.50 (should NOT emit)
    third_tick = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=178.50), active_rules=rules)
    assert len(third_tick) == 0


def test_rearming_after_returning_above_threshold(evaluator):
    """Returning above threshold re-arms the trigger for subsequent crossing."""
    rules = [
        ThresholdRule(id="rule-down", symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)
    ]
    evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=180.40), active_rules=rules)
    # 1st crossing: 180.40 -> 179.80
    e1 = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=179.80), active_rules=rules)
    assert len(e1) == 1

    # Bounce back above threshold: 179.80 -> 180.50
    evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=180.50), active_rules=rules)

    # 2nd crossing: 180.50 -> 179.50
    e2 = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=179.50), active_rules=rules)
    assert len(e2) == 1
    assert e2[0].previous_price == 180.50
    assert e2[0].price == 179.50


def test_upward_threshold_crossing(evaluator):
    """Previous price < threshold and current >= threshold triggers upward crossing."""
    rules = [
        ThresholdRule(id="rule-up", symbol="MSFT", threshold=420.0, condition=ThresholdCondition.GREATER_THAN_OR_EQUAL)
    ]
    evaluator.evaluate_price_update(PriceUpdate(symbol="MSFT", price=418.0), active_rules=rules)

    # Crosses upward to 420.50
    events = evaluator.evaluate_price_update(PriceUpdate(symbol="MSFT", price=420.50), active_rules=rules)
    assert len(events) == 1
    assert events[0].symbol == "MSFT"
    assert events[0].previous_price == 418.0
    assert events[0].price == 420.50
    assert events[0].condition == ThresholdCondition.GREATER_THAN_OR_EQUAL

    # Price stays above: 421.00 (no duplicate)
    next_tick = evaluator.evaluate_price_update(PriceUpdate(symbol="MSFT", price=421.00), active_rules=rules)
    assert len(next_tick) == 0


def test_symbol_independence(evaluator):
    """AAPL and MSFT prices and rules must maintain strictly isolated state."""
    rules_aapl = [ThresholdRule(id="rule-aapl", symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)]
    rules_msft = [ThresholdRule(id="rule-msft", symbol="MSFT", threshold=400.0, condition=ThresholdCondition.GREATER_THAN_OR_EQUAL)]
    all_rules = rules_aapl + rules_msft

    # Establish baseline
    evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=182.0), active_rules=all_rules)
    evaluator.evaluate_price_update(PriceUpdate(symbol="MSFT", price=395.0), active_rules=all_rules)

    # Trigger AAPL crossing
    e_aapl = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=178.0), active_rules=all_rules)
    assert len(e_aapl) == 1
    assert e_aapl[0].symbol == "AAPL"

    # MSFT has not crossed yet
    assert evaluator.previous_prices["MSFT"] == 395.0

    # Trigger MSFT crossing
    e_msft = evaluator.evaluate_price_update(PriceUpdate(symbol="MSFT", price=405.0), active_rules=all_rules)
    assert len(e_msft) == 1
    assert e_msft[0].symbol == "MSFT"


def test_canonical_sequence_scenario(evaluator):
    """Validates the canonical project scenario sequence:
    185.00 -> 183.50 -> 181.00 -> 180.40 -> 179.80 -> 179.20 -> 180.50 -> 179.50
    With threshold $180 (downward).
    Expects exactly two crossing events at indices 4 (180.40 -> 179.80) and 7 (180.50 -> 179.50).
    """
    rule = ThresholdRule(id="scenario-rule", symbol="AAPL", threshold=180.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)
    sequence = [185.00, 183.50, 181.00, 180.40, 179.80, 179.20, 180.50, 179.50]

    all_emitted = []
    for price in sequence:
        emitted = evaluator.evaluate_price_update(PriceUpdate(symbol="AAPL", price=price), active_rules=[rule])
        all_emitted.extend(emitted)

    assert len(all_emitted) == 2

    # First crossing: 180.40 -> 179.80
    assert all_emitted[0].previous_price == 180.40
    assert all_emitted[0].price == 179.80

    # Second crossing: 180.50 -> 179.50
    assert all_emitted[1].previous_price == 180.50
    assert all_emitted[1].price == 179.50
