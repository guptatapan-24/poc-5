"""Real-time system telemetry tracker for CPU, memory, active connections, throughput, and Locust stats."""

from collections import deque
from datetime import datetime, timezone
import json
import logging
import os
import threading
import time
from typing import Dict, Set, Optional, List
import urllib.request
import psutil

logger = logging.getLogger(__name__)


class TelemetryTracker:
    """Thread-safe tracker for system load, active WebSocket & HTTP clients, requests/sec, and Locust metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._active_clients: Set[str] = set()
        self._client_last_seen: Dict[str, float] = {}
        self._http_clients: Dict[str, float] = {}
        self._endpoint_metrics: Dict[str, dict] = {}
        self._peak_active_clients: int = 0
        self._total_requests: int = 0
        self._request_timestamps: deque = deque(maxlen=5000)
        self._process = psutil.Process()

        # Prime initial CPU readings
        try:
            self._latest_sys_cpu = round(psutil.cpu_percent(interval=None), 1)
            self._latest_proc_cpu = round(self._process.cpu_percent(interval=None), 1)
        except Exception:
            self._latest_sys_cpu = 0.0
            self._latest_proc_cpu = 0.0

        self._broadcaster_started = False
        self._latest_locust_data: Optional[dict] = None

    def register_connection(self, client_id: str):
        """Registers a connected WebSocket client (in-memory, non-blocking)."""
        now = time.time()
        with self._lock:
            self._active_clients.add(client_id)
            self._client_last_seen[client_id] = now
            cur_conns = len(self._active_clients)
            if cur_conns > self._peak_active_clients:
                self._peak_active_clients = cur_conns

        self.record_request()
        self.ensure_broadcaster_started()

    def touch_connection(self, client_id: str):
        """Updates last seen timestamp for an active WebSocket client."""
        now = time.time()
        with self._lock:
            if client_id in self._active_clients:
                self._client_last_seen[client_id] = now

    def unregister_connection(self, client_id: str):
        """Unregisters a disconnected WebSocket client (in-memory, non-blocking)."""
        with self._lock:
            self._active_clients.discard(client_id)
            self._client_last_seen.pop(client_id, None)

    def record_request(self):
        """Records an incoming request tick for RPS calculation."""
        now = time.time()
        with self._lock:
            self._total_requests += 1
            self._request_timestamps.append(now)

    def record_http_request(
        self,
        client_id: Optional[str] = None,
        client_ip: Optional[str] = None,
        is_internal_poll: bool = False,
        endpoint: str = "",
    ):
        """Records an incoming HTTP request, distinguishing user traffic from internal telemetry polling."""
        now = time.time()
        with self._lock:
            # Internal metrics polling is excluded from request throughput so it doesn't inflate RPS
            if not is_internal_poll:
                self._total_requests += 1
                self._request_timestamps.append(now)

            # Track concurrent HTTP client session
            if client_id:
                self._http_clients[client_id] = now
            elif not is_internal_poll and client_ip:
                self._http_clients[f"http-{client_ip}"] = now

    def record_http_response(
        self,
        endpoint: str,
        duration_ms: float,
        is_error: bool = False,
        is_internal_poll: bool = False,
    ):
        """Records latency and status for endpoint breakdown statistics."""
        if is_internal_poll or not endpoint:
            return

        with self._lock:
            if endpoint not in self._endpoint_metrics:
                self._endpoint_metrics[endpoint] = {
                    "requests": 0,
                    "failures": 0,
                    "total_ms": 0.0,
                    "min_ms": duration_ms,
                    "max_ms": duration_ms,
                    "recent_timestamps": deque(maxlen=500),
                }

            stat = self._endpoint_metrics[endpoint]
            stat["requests"] += 1
            if is_error:
                stat["failures"] += 1
            stat["total_ms"] += duration_ms
            if duration_ms < stat["min_ms"]:
                stat["min_ms"] = duration_ms
            if duration_ms > stat["max_ms"]:
                stat["max_ms"] = duration_ms
            stat["recent_timestamps"].append(time.time())

    def _get_server_endpoint_stats(self) -> List[dict]:
        """Formats collected endpoint metrics into Locust-compatible breakdown rows."""
        now = time.time()
        results = []
        for name, m in self._endpoint_metrics.items():
            reqs = m.get("requests", 0)
            if reqs == 0:
                continue
            fails = m.get("failures", 0)
            avg_ms = round(m.get("total_ms", 0.0) / max(1, reqs), 1)

            # Clean recent timestamps older than 1.0s for current RPS
            recent = m.get("recent_timestamps", deque())
            while recent and (now - recent[0] > 1.0):
                recent.popleft()
            current_rps = round(float(len(recent)), 1)

            parts = name.split(" ", 1)
            method = parts[0] if len(parts) > 1 else "HTTP"
            endpoint_path = parts[1] if len(parts) > 1 else name

            results.append({
                "method": method,
                "name": endpoint_path,
                "num_requests": reqs,
                "num_failures": fails,
                "median_response_time": avg_ms,
                "avg_response_time": avg_ms,
                "min_response_time": round(m.get("min_ms", 0.0), 1),
                "max_response_time": round(m.get("max_ms", 0.0), 1),
                "current_rps": current_rps,
            })
        return results

    def _calculate_server_failure_rate(self) -> float:
        """Calculates percentage of failed HTTP requests."""
        total_reqs = sum(m.get("requests", 0) for m in self._endpoint_metrics.values())
        if total_reqs == 0:
            return 0.0
        total_fails = sum(m.get("failures", 0) for m in self._endpoint_metrics.values())
        return round((total_fails / total_reqs) * 100.0, 2)

    def _poll_locust_stats(self) -> Optional[dict]:
        """Attempts to poll live statistics from Locust web server if active."""
        for target_url in ("http://127.0.0.1:8089/stats/requests", "http://localhost:8089/stats/requests"):
            try:
                req = urllib.request.Request(
                    target_url,
                    headers={"User-Agent": "RealTimeStockTelemetry/1.0", "Accept": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=1.2) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        user_count = int(data.get("user_count", 0))
                        locust_state = data.get("state", "running")
                        return {
                            "running": (locust_state in ("running", "spawning") or user_count > 0),
                            "state": locust_state,
                            "user_count": user_count,
                            "total_rps": round(float(data.get("total_rps", 0.0)), 1),
                            "fail_ratio": round(float(data.get("fail_ratio", 0.0)), 4),
                            "total_fail_per_sec": round(float(data.get("total_fail_per_sec", 0.0)), 1),
                            "stats": data.get("stats", []),
                            "errors": data.get("errors", []),
                        }
            except Exception:
                continue
        return None

    def get_telemetry_snapshot(self) -> dict:
        """Returns instantaneous snapshot of CPU, RAM, active clients, RPS, and Locust metrics.

        Window for RPS calculation is strictly 1.0 second.
        """
        now = time.time()
        with self._lock:
            # Clean expired WebSocket clients only after 90 seconds of total silence (fail-safe)
            dead_clients = [cid for cid, seen in self._client_last_seen.items() if now - seen > 90.0]
            for cid in dead_clients:
                self._active_clients.discard(cid)
                self._client_last_seen.pop(cid, None)

            # Strictly 1.0s window: clean timestamps older than 1.0s
            while self._request_timestamps and (now - self._request_timestamps[0] > 1.0):
                self._request_timestamps.popleft()

            # Server-side RPS is exact count of requests in past 1.0 second
            server_rps = round(float(len(self._request_timestamps)), 1)

            # Active WebSocket connection count
            ws_conn_count = len(self._active_clients)

            # Active HTTP clients: seen within past 4.0 seconds (matches Locust 0.5-2.0s sleep interval)
            active_http = [cid for cid, seen in self._http_clients.items() if now - seen <= 4.0]
            server_http_count = len(active_http)

            # Prune stale HTTP clients older than 15.0s
            self._http_clients = {cid: seen for cid, seen in self._http_clients.items() if now - seen <= 15.0}

            total_reqs = self._total_requests
            locust_data = self._latest_locust_data

        # System and process metrics
        sys_cpu = self._latest_sys_cpu
        proc_cpu = self._latest_proc_cpu

        try:
            proc_mem_mb = round(self._process.memory_info().rss / (1024 * 1024), 1)
        except Exception:
            proc_mem_mb = 0.0

        try:
            sys_mem = psutil.virtual_memory()
            sys_mem_pct = round(sys_mem.percent, 1)
            sys_mem_used_mb = round(sys_mem.used / (1024 * 1024), 1)
            sys_mem_total_mb = round(sys_mem.total / (1024 * 1024), 1)
        except Exception:
            sys_mem_pct = 0.0
            sys_mem_used_mb = proc_mem_mb
            sys_mem_total_mb = 16384.0

        # Unify with Locust if running
        is_locust_active = locust_data is not None and locust_data.get("running", False)

        if is_locust_active:
            locust_users = locust_data.get("user_count", 0)
            total_users = max(locust_users, ws_conn_count + server_http_count)
            http_users = max(server_http_count, total_users - ws_conn_count)
            displayed_rps = max(locust_data.get("total_rps", 0.0), server_rps)
            fail_ratio = locust_data.get("fail_ratio", 0.0)
            fail_pct = round(fail_ratio * 100.0, 2)
            success_pct = round(max(0.0, 100.0 - fail_pct), 2)
            endpoint_stats = locust_data.get("stats", [])
            locust_errors = locust_data.get("errors", [])
            locust_state = locust_data.get("state", "RUNNING")
        else:
            total_users = ws_conn_count + server_http_count
            http_users = server_http_count
            displayed_rps = server_rps
            fail_pct = self._calculate_server_failure_rate()
            success_pct = round(max(0.0, 100.0 - fail_pct), 2)
            endpoint_stats = self._get_server_endpoint_stats()
            locust_errors = []
            locust_state = "RUNNING (SERVER TELEMETRY)" if server_http_count > 0 else "IDLE"

        peak_conns = max(self._peak_active_clients, total_users)

        return {
            "active_users": total_users,
            "ws_connections": ws_conn_count,
            "active_connections": ws_conn_count,
            "http_users": http_users,
            "peak_connections": peak_conns,
            "requests_per_second": displayed_rps,
            "server_rps": server_rps,
            "total_requests": total_reqs,
            "cpu_percent": sys_cpu,
            "process_cpu_percent": proc_cpu,
            "process_memory_mb": proc_mem_mb,
            "system_memory_percent": sys_mem_pct,
            "system_memory_used_mb": sys_mem_used_mb,
            "system_memory_total_mb": sys_mem_total_mb,
            "success_rate_percent": success_pct,
            "failure_rate_percent": fail_pct,
            "locust_active": is_locust_active,
            "locust_state": locust_state,
            "endpoint_stats": endpoint_stats,
            "locust_errors": locust_errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def ensure_broadcaster_started(self):
        """Ensures a background thread periodically samples CPU and pushes metrics to WebSocket channel."""
        if self._broadcaster_started:
            return

        with self._lock:
            if self._broadcaster_started:
                return
            self._broadcaster_started = True

        thread = threading.Thread(target=self._broadcast_loop, daemon=True, name="TelemetryBroadcaster")
        thread.start()
        logger.info("Real-time TelemetryBroadcaster daemon thread started (1.0s interval).")

    def _broadcast_loop(self):
        try:
            import django
            if not os.environ.get("DJANGO_SETTINGS_MODULE"):
                os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
            django.setup()
        except Exception:
            pass

        while True:
            try:
                # Sleep exactly 1.0 second between samples
                time.sleep(1.0)

                # Sample CPU over this 1.0s interval for true accuracy
                try:
                    self._latest_sys_cpu = round(psutil.cpu_percent(interval=None), 1)
                    self._latest_proc_cpu = round(self._process.cpu_percent(interval=None), 1)
                except Exception:
                    pass

                # Poll Locust stats if active
                locust_update = self._poll_locust_stats()
                with self._lock:
                    self._latest_locust_data = locust_update

            except Exception as e:
                logger.debug("Error in telemetry sampler loop: %s", e)


# Global singleton instance
telemetry_tracker = TelemetryTracker()
telemetry_tracker.ensure_broadcaster_started()
