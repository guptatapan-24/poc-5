"""Unit tests for Pydantic data schemas and validation rules."""

import pytest
from pydantic import ValidationError
from events.schemas import (
    PriceUpdate,
    ThresholdCrossingEvent,
    ThresholdCondition,
    ThresholdRule,
    DeliveryMode,
    WebSocketEventMessage,
    ReplayRequest,
)


def test_price_update_valid():
    pu = PriceUpdate(symbol="aapl", price=180.40)
    assert pu.symbol == "AAPL"  # Normalized to uppercase
    assert pu.price == 180.40
    assert pu.message_id.startswith("price-")
    assert pu.observed_at is not None


def test_price_update_invalid_price():
    with pytest.raises(ValidationError):
        PriceUpdate(symbol="AAPL", price=-10.0)

    with pytest.raises(ValidationError):
        PriceUpdate(symbol="AAPL", price=0.0)


def test_price_update_empty_symbol():
    with pytest.raises(ValidationError):
        PriceUpdate(symbol="   ", price=100.0)


def test_threshold_crossing_event_schema():
    event = ThresholdCrossingEvent(
        symbol="AAPL",
        previous_price=180.40,
        price=179.80,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
        processing_mode=DeliveryMode.LIVE,
    )
    assert event.event_id.startswith("evt-")
    assert event.symbol == "AAPL"
    assert event.previous_price == 180.40
    assert event.price == 179.80
    assert event.threshold == 180.00
    assert event.condition == ThresholdCondition.LESS_THAN_OR_EQUAL
    assert event.processing_mode == DeliveryMode.LIVE
    assert event.event_type == "PRICE_THRESHOLD_CROSSED"


def test_threshold_rule_validation():
    rule = ThresholdRule(
        symbol="msft",
        threshold=420.50,
        condition=ThresholdCondition.GREATER_THAN_OR_EQUAL,
    )
    assert rule.symbol == "MSFT"
    assert rule.threshold == 420.50
    assert rule.id.startswith("rule-")

    with pytest.raises(ValidationError):
        ThresholdRule(symbol="MSFT", threshold=-5.0, condition=ThresholdCondition.LESS_THAN_OR_EQUAL)


def test_websocket_envelope():
    event = ThresholdCrossingEvent(
        symbol="AAPL",
        previous_price=180.40,
        price=179.80,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
    )
    msg = WebSocketEventMessage(
        message_type="stock_event",
        delivery_mode=DeliveryMode.LIVE,
        event=event,
    )
    dumped = msg.model_dump()
    assert dumped["message_type"] == "stock_event"
    assert dumped["delivery_mode"] == "LIVE"
    assert dumped["event"]["symbol"] == "AAPL"


def test_replay_request_limits():
    req = ReplayRequest(limit=25, symbol="AAPL")
    assert req.limit == 25
    assert req.symbol == "AAPL"

    # Limit must be between 1 and 1000
    with pytest.raises(ValidationError):
        ReplayRequest(limit=0)

    with pytest.raises(ValidationError):
        ReplayRequest(limit=5000)

    # ALL or empty symbol sanitizes to None
    req_all = ReplayRequest(limit=10, symbol="ALL")
    assert req_all.symbol is None
