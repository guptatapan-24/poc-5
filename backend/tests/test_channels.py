"""Tests for Django Channels WebSocket consumer and channel layer routing."""

import json
import pytest
from channels.testing import WebsocketCommunicator
from channels.layers import get_channel_layer
from events.consumers import StockEventsConsumer
from events.schemas import (
    ThresholdCrossingEvent,
    ThresholdCondition,
    DeliveryMode,
    WebSocketEventMessage,
)


@pytest.mark.asyncio
async def test_websocket_connection_and_handshake():
    """Validates that a WebSocket client connects and receives welcome status."""
    communicator = WebsocketCommunicator(
        StockEventsConsumer.as_asgi(),
        "/ws/events/?client_id=test-client-123",
    )
    connected, _ = await communicator.connect()
    assert connected

    # Receive welcome message
    response = await communicator.receive_json_from()
    assert response["message_type"] == "system_status"
    assert response["details"]["status"] == "CONNECTED"
    assert response["details"]["client_id"] == "test-client-123"

    await communicator.disconnect()


@pytest.mark.asyncio
async def test_websocket_ping_pong():
    """Validates client ping response over WebSocket."""
    communicator = WebsocketCommunicator(
        StockEventsConsumer.as_asgi(),
        "/ws/events/?client_id=test-client-ping",
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.receive_json_from()  # Consume handshake

    # Send ping
    await communicator.send_json_to({"action": "ping"})
    response = await communicator.receive_json_from()
    assert response["message_type"] == "system_status"
    assert response["details"]["action"] == "pong"

    await communicator.disconnect()


@pytest.mark.asyncio
async def test_channel_layer_event_broadcast():
    """Validates that messages sent to 'live_stock_events' group reach connected WebSocket clients."""
    channel_layer = get_channel_layer()
    communicator = WebsocketCommunicator(
        StockEventsConsumer.as_asgi(),
        "/ws/events/?client_id=test-client-broadcast",
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.receive_json_from()  # Handshake

    # Dispatch a test live event to the channel group
    test_event = ThresholdCrossingEvent(
        symbol="AAPL",
        previous_price=180.40,
        price=179.80,
        threshold=180.00,
        condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
        processing_mode=DeliveryMode.LIVE,
    )
    envelope = WebSocketEventMessage(
        message_type="stock_event",
        delivery_mode=DeliveryMode.LIVE,
        event=test_event.model_dump(),
    )

    await channel_layer.group_send(
        "live_stock_events",
        {
            "type": "stock.event",
            "payload": envelope.model_dump(),
        },
    )

    received = await communicator.receive_json_from()
    assert received["message_type"] == "stock_event"
    assert received["delivery_mode"] == "LIVE"
    assert received["event"]["symbol"] == "AAPL"
    assert received["event"]["price"] == 179.80

    await communicator.disconnect()
