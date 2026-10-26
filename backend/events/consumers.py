"""Django Channels WebSocket consumer for real-time stock events and isolated historical replays."""

import json
import logging
import uuid
from channels.generic.websocket import AsyncWebsocketConsumer
from urllib.parse import parse_qs
from events.schemas import ReplayRequest
from event_pipeline.replay_consumer import replay_service
from events.telemetry import telemetry_tracker

logger = logging.getLogger(__name__)


class StockEventsConsumer(AsyncWebsocketConsumer):
    """Handles WebSocket connections from React frontend dashboard and Locust load test clients.

    Subscribes to:
    - 'live_stock_events': shared group for real-time broadcasts
    - 'client_<client_id>': private channel group for directed historical replays
    """

    async def connect(self):
        # Extract or generate a client ID for private routing
        query_string = self.scope.get("query_string", b"").decode("utf-8")
        params = parse_qs(query_string)
        client_id_param = params.get("client_id", [None])[0]

        self.client_id = client_id_param or f"client-{uuid.uuid4().hex[:8]}"
        self.live_group = "live_stock_events"
        self.private_group = f"client_{self.client_id}"

        # Join the shared live events group
        await self.channel_layer.group_add(self.live_group, self.channel_name)

        # Join the private group for isolated replay delivery
        await self.channel_layer.group_add(self.private_group, self.channel_name)

        await self.accept()

        # Register connection in telemetry tracker
        try:
            telemetry_tracker.register_connection(self.client_id)
            metrics_snapshot = telemetry_tracker.get_telemetry_snapshot()
            active_cnt = metrics_snapshot.get("active_connections", 1)
            logger.info(
                "WebSocket client connected: %s (private group: %s, active: %d)",
                self.channel_name,
                self.private_group,
                active_cnt,
            )
        except Exception as e:
            logger.warning("Telemetry registration warning: %s", e)
            metrics_snapshot = {}

        # Send welcome handshake message with client_id and current telemetry snapshot
        try:
            await self.send(
                text_data=json.dumps({
                    "message_type": "system_status",
                    "delivery_mode": "LIVE",
                    "details": {
                        "status": "CONNECTED",
                        "client_id": self.client_id,
                        "subscribed_groups": [self.live_group, self.private_group],
                        "message": "Connected to Real-Time Stock Event Stream",
                    },
                    "metrics": metrics_snapshot,
                })
            )
        except Exception as e:
            logger.warning("Error sending welcome message: %s", e)

    async def disconnect(self, close_code):
        # Unregister connection from telemetry tracker
        try:
            telemetry_tracker.unregister_connection(self.client_id)
        except Exception:
            pass

        # Leave groups cleanly
        try:
            await self.channel_layer.group_discard(self.live_group, self.channel_name)
            await self.channel_layer.group_discard(self.private_group, self.channel_name)
        except Exception:
            pass
        logger.info("WebSocket client disconnected: %s [code=%s]", self.channel_name, close_code)

    async def receive(self, text_data=None, bytes_data=None):
        """Handles incoming messages from the WebSocket client."""
        if not text_data:
            return

        telemetry_tracker.record_request()
        telemetry_tracker.touch_connection(self.client_id)

        try:
            data = json.loads(text_data)
            action = data.get("action")

            if action == "ping":
                await self.send(text_data=json.dumps({
                    "message_type": "system_status",
                    "details": {"action": "pong"},
                    "metrics": telemetry_tracker.get_telemetry_snapshot(),
                }))

            elif action == "get_metrics":
                await self.send(text_data=json.dumps({
                    "message_type": "system_metrics",
                    "metrics": telemetry_tracker.get_telemetry_snapshot(),
                }))

            elif action == "replay":
                limit = int(data.get("limit", 50))
                symbol = data.get("symbol")
                req = ReplayRequest(
                    limit=limit,
                    symbol=symbol,
                    client_id=self.client_id,
                )
                replay_id = replay_service.start_replay(req)
                await self.send(text_data=json.dumps({
                    "message_type": "replay_status",
                    "replay_id": replay_id,
                    "details": {
                        "status": "QUEUED",
                        "message": f"Replay {replay_id} requested (limit={limit}, symbol={symbol})",
                    },
                }))

            else:
                logger.warning("Unrecognized WebSocket action received: %s", action)

        except Exception as e:
            logger.error("Error processing incoming WebSocket payload: %s", e)
            await self.send(text_data=json.dumps({
                "message_type": "system_status",
                "details": {"error": f"Invalid payload: {str(e)}"},
            }))

    async def stock_event(self, event):
        """Channel layer handler for 'stock.event' type messages."""
        payload = event.get("payload")
        if payload:
            await self.send(text_data=json.dumps(payload))


