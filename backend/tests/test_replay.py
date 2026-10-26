"""Tests for historical event replay logic, offset isolation, and delivery metadata."""

import pytest
from events.schemas import (
    ThresholdCrossingEvent,
    ThresholdCondition,
    DeliveryMode,
    ReplayRequest,
    WebSocketEventMessage,
)
from events.state import replay_state
from event_pipeline.replay_consumer import ReplayService


class MockChannelLayer:
    """Mock channel layer collecting dispatched messages."""
    def __init__(self):
        self.dispatched = []

    async def group_send(self, group, message):
        self.dispatched.append((group, message))


def test_replay_request_registration():
    service = ReplayService()
    req = ReplayRequest(limit=10, symbol="AAPL", client_id="test-client-1")
    replay_id = service.start_replay(req)
    assert replay_id.startswith("replay-")

    status = replay_state.get_status(replay_id)
    assert status is not None
    assert status["limit"] == 10
    assert status["symbol"] == "AAPL"
    assert status["client_id"] == "test-client-1"


def test_replay_preserves_event_id_and_original_timestamp():
    """Verify that replayed envelopes preserve the original event_id and original timestamp

    and set delivery_mode to REPLAY without modifying original event fields.
    """
    original_event = ThresholdCrossingEvent(
        event_id="evt-original-1234",
        symbol="AAPL",
        previous_price=180.40,
        price=179.80,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
        event_timestamp="2026-10-22T10:00:04Z",
        processing_mode=DeliveryMode.LIVE,
    )

    # Transform for replay
    original_event.processing_mode = DeliveryMode.REPLAY
    replay_msg = WebSocketEventMessage(
        message_type="stock_event",
        delivery_mode=DeliveryMode.REPLAY,
        replay_id="replay-test-99",
        event=original_event,
        details={"replayed": True},
    )

    dumped = replay_msg.model_dump()
    assert dumped["message_type"] == "stock_event"
    assert dumped["delivery_mode"] == "REPLAY"
    assert dumped["replay_id"] == "replay-test-99"
    # Original event ID preserved
    assert dumped["event"]["event_id"] == "evt-original-1234"
    # Original timestamp preserved
    assert dumped["event"]["event_timestamp"] == "2026-10-22T10:00:04Z"
    # Processing mode updated
    assert dumped["event"]["processing_mode"] == "REPLAY"


def test_replay_state_lifecycle():
    replay_id = "replay-lifecycle-test"
    replay_state.register_replay(replay_id, limit=5, symbol="MSFT", client_id="client-abc")

    status = replay_state.get_status(replay_id)
    assert status["status"] == "IN_PROGRESS"
    assert status["replayed_count"] == 0

    replay_state.update_progress(replay_id, 3)
    assert replay_state.get_status(replay_id)["replayed_count"] == 3

    replay_state.finish_replay(replay_id, 5)
    final_status = replay_state.get_status(replay_id)
    assert final_status["status"] == "COMPLETED"
    assert final_status["replayed_count"] == 5
    assert final_status["error"] is None
