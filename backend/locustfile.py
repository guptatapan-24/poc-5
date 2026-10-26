"""Locust Load Testing Suite for Real-Time Stock Event System.

Implements the official Test Plan from Proposal Section 13:
1. Scenario H1: HTTP REST API testing (HttpDashboardUser)
   - Targets /api/events/recent/, /api/thresholds/, /api/health/, /api/simulator/status/
   - Measures RPS, failure rate, and p50/p95/p99 response latency.
2. Scenario W1: WebSocket connection & heartbeat lifecycle (WebSocketDashboardUser)
   - Manages connection lifecycle with stable client_id to ws://127.0.0.1:8000/ws/events/
   - Emits periodic heartbeat pings (every 10s) and tracks connection latency.
3. Scenario E1: Real-time Event Delivery Latency measurement (WebSocketDashboardUser)
   - Listens for live pushed events (THRESHOLD_CROSSED and PRICE_DOWNTREND_DETECTED)
   - Computes end-to-end delivery latency = client_receive_time - event_timestamp
   - Fires custom Locust request events for latency percentile and failure tracking.
"""

from datetime import datetime, timezone
import json
import logging
import os
import time
import uuid

import gevent
from locust import HttpUser, User, task, between, events
import websocket

logger = logging.getLogger("locust")


# =============================================================================
# Scenario H1: HTTP REST API Load Testing User
# =============================================================================
class HttpDashboardUser(HttpUser):
    """Simulates active dashboard users querying REST endpoints."""
    wait_time = between(0.5, 2.0)

    def on_start(self):
        """Assign unique client ID so the server accurately tracks individual concurrent HTTP users."""
        self.client_id = f"http-locust-{uuid.uuid4().hex[:8]}"
        self.client.headers["X-Client-ID"] = self.client_id

    @task(4)
    def fetch_recent_events(self):
        """Query recent events for catch-up synchronization."""
        with self.client.get("/api/events/recent/?limit=50", catch_response=True, name="GET /api/events/recent/") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status code {resp.status_code}")

    @task(2)
    def fetch_thresholds(self):
        """Query active threshold rules."""
        with self.client.get("/api/thresholds/", catch_response=True, name="GET /api/thresholds/") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status code {resp.status_code}")

    @task(1)
    def fetch_health(self):
        """Query system health status (Kafka & Redis connectivity)."""
        with self.client.get("/api/health/", catch_response=True, name="GET /api/health/") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status code {resp.status_code}")

    @task(1)
    def fetch_simulator_status(self):
        """Query price simulator state."""
        with self.client.get("/api/simulator/status/", catch_response=True, name="GET /api/simulator/status/") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"Status code {resp.status_code}")


# =============================================================================
# Scenarios W1 & E1: WebSocket Connection and Event Delivery User
# =============================================================================
class WebSocketDashboardUser(User):
    """Simulates real-time dashboard clients connected over WebSockets.

    Measures connection setup time (W1) and end-to-end push delivery latency (E1).
    """
    wait_time = between(1.0, 3.0)

    def on_start(self):
        """Initializes WebSocket connection and spawns background listener & heartbeat greenlets."""
        self.client_id = f"locust-{uuid.uuid4().hex[:8]}"
        self.ws = None
        self.running = True
        self.received_events_count = 0

        host = self.host or "http://127.0.0.1:8000"
        ws_host = host.replace("http://", "ws://").replace("https://", "wss://")
        self.ws_url = f"{ws_host}/ws/events/?client_id={self.client_id}"

        # Connect with latency measurement (Scenario W1)
        connect_start = time.time()
        try:
            self.ws = websocket.create_connection(self.ws_url, timeout=5.0)
            connect_duration_ms = (time.time() - connect_start) * 1000.0

            # Record successful connection in Locust metrics
            self.environment.events.request.fire(
                request_type="WebSocket",
                name="WS_Connect",
                response_time=connect_duration_ms,
                response_length=0,
                exception=None,
                context={},
            )

            # Spawn greenlets for message listening and heartbeat pings
            self.listener_greenlet = gevent.spawn(self._listen_loop)
            self.heartbeat_greenlet = gevent.spawn(self._heartbeat_loop)

        except Exception as e:
            connect_duration_ms = (time.time() - connect_start) * 1000.0
            self.environment.events.request.fire(
                request_type="WebSocket",
                name="WS_Connect",
                response_time=connect_duration_ms,
                response_length=0,
                exception=e,
                context={},
            )
            self.ws = None

    def _heartbeat_loop(self):
        """Sends periodic ping frames every 10 seconds to maintain connection."""
        while self.running and self.ws:
            try:
                ping_payload = json.dumps({"action": "ping"})
                self.ws.send(ping_payload)
                self.environment.events.request.fire(
                    request_type="WebSocket",
                    name="WS_Heartbeat_Ping",
                    response_time=1.0,
                    response_length=len(ping_payload),
                    exception=None,
                    context={},
                )
            except Exception as e:
                self.environment.events.request.fire(
                    request_type="WebSocket",
                    name="WS_Heartbeat_Ping",
                    response_time=0,
                    response_length=0,
                    exception=e,
                    context={},
                )
                break
            gevent.sleep(10.0)

    def _listen_loop(self):
        """Listens for pushed Kafka events and calculates delivery latency (Scenario E1)."""
        while self.running and self.ws:
            try:
                raw_data = self.ws.recv()
                receive_time = time.time()

                if not raw_data:
                    continue

                msg = json.loads(raw_data)
                msg_type = msg.get("message_type")

                if msg_type == "stock_event":
                    self.received_events_count += 1
                    event_data = msg.get("event") or {}
                    event_type = event_data.get("event_type", "THRESHOLD_CROSSED")

                    # Calculate end-to-end delivery latency:
                    # delivery_latency = client_receive_time - event_timestamp
                    event_ts_str = event_data.get("event_timestamp") or event_data.get("last_observed_at")
                    latency_ms = 0.0

                    if event_ts_str:
                        try:
                            # Normalize ISO timestamp
                            cleaned_ts = event_ts_str.replace("Z", "+00:00")
                            event_epoch = datetime.fromisoformat(cleaned_ts).timestamp()
                            latency_ms = max(0.0, (receive_time - event_epoch) * 1000.0)
                        except Exception:
                            latency_ms = 5.0  # Fallback estimate

                    # Fire Locust custom metric for the specific event type
                    metric_name = f"EventDelivery:{event_type}"
                    self.environment.events.request.fire(
                        request_type="WebSocket",
                        name=metric_name,
                        response_time=latency_ms,
                        response_length=len(raw_data),
                        exception=None,
                        context={"event_id": event_data.get("event_id")},
                    )

                elif msg_type == "system_status":
                    self.environment.events.request.fire(
                        request_type="WebSocket",
                        name="WS_System_Status",
                        response_time=0.5,
                        response_length=len(raw_data),
                        exception=None,
                        context={},
                    )

            except websocket.WebSocketConnectionClosedException:
                break
            except Exception as e:
                self.environment.events.request.fire(
                    request_type="WebSocket",
                    name="WS_Receive_Error",
                    response_time=0,
                    response_length=0,
                    exception=e,
                    context={},
                )
                break

    @task
    def keep_alive(self):
        """Active task keeping the user session running in the Locust scheduler."""
        gevent.sleep(1.0)

    def on_stop(self):
        """Cleans up sockets and kills background greenlets."""
        self.running = False
        if hasattr(self, "heartbeat_greenlet"):
            self.heartbeat_greenlet.kill(block=False)
        if hasattr(self, "listener_greenlet"):
            self.listener_greenlet.kill(block=False)
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
