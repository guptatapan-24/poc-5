"""Thread-safe in-memory state store for configured thresholds, simulator state, and replay tracking.

NOTE: As per design constraints, configuration is held in-memory behind a clean interface
for this local PoC. State resets upon process restart.
"""

import os
import json
import logging
import threading
import time
from typing import Dict, List, Optional
import redis
from events.schemas import ThresholdRule, ThresholdCondition

logger = logging.getLogger(__name__)


class ThresholdStateManager:
    """Manages active threshold rules with thread-safety and Redis cross-process sync."""

    def __init__(self):
        self._lock = threading.Lock()
        self._rules: Dict[str, ThresholdRule] = {}
        self._redis_client = None
        # Prepopulate with the scenario default: AAPL <= 180.00
        default_rule = ThresholdRule(
            id="rule-default-aapl",
            symbol="AAPL",
            threshold=180.00,
            condition=ThresholdCondition.LESS_THAN_OR_EQUAL,
        )
        self._rules[default_rule.id] = default_rule

    def _get_redis(self):
        now = time.time()
        if self._redis_client is None and (now - getattr(self, "_redis_last_check", 0) < 5.0):
            return None
        self._redis_last_check = now
        try:
            if self._redis_client is None:
                redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
                client = redis.Redis.from_url(redis_url, socket_timeout=0.2, decode_responses=True)
                client.ping()
                self._redis_client = client
            return self._redis_client
        except Exception:
            self._redis_client = None
            return None

    def add_or_update(self, rule: ThresholdRule) -> ThresholdRule:
        with self._lock:
            self._rules[rule.id] = rule
            try:
                r = self._get_redis()
                if r:
                    r.hset("stock_thresholds", rule.id, rule.model_dump_json())
            except Exception as e:
                logger.debug("Failed to sync rule to Redis: %s", e)
            return rule

    def list_all(self) -> List[ThresholdRule]:
        with self._lock:
            try:
                r = self._get_redis()
                if r:
                    items = r.hgetall("stock_thresholds")
                    if items:
                        parsed = []
                        for raw_json in items.values():
                            parsed.append(ThresholdRule.model_validate_json(raw_json))
                        self._rules = {rule.id: rule for rule in parsed}
                    elif self._rules:
                        # Seed Redis with default in-memory rules
                        for rule in self._rules.values():
                            r.hset("stock_thresholds", rule.id, rule.model_dump_json())
            except Exception as e:
                logger.debug("Redis read failed, fallback to in-memory: %s", e)
            return list(self._rules.values())

    def get_by_id(self, rule_id: str) -> Optional[ThresholdRule]:
        all_rules = self.list_all()
        for r in all_rules:
            if r.id == rule_id:
                return r
        return None

    def get_for_symbol(self, symbol: str) -> List[ThresholdRule]:
        clean_symbol = symbol.strip().upper()
        all_rules = self.list_all()
        return [r for r in all_rules if r.symbol == clean_symbol]

    def remove(self, rule_id: str) -> bool:
        with self._lock:
            removed = False
            if rule_id in self._rules:
                del self._rules[rule_id]
                removed = True
            try:
                r = self._get_redis()
                if r:
                    count = r.hdel("stock_thresholds", rule_id)
                    if count > 0:
                        removed = True
            except Exception as e:
                logger.debug("Failed to remove rule from Redis: %s", e)
            return removed

    def clear(self):
        with self._lock:
            self._rules.clear()
            try:
                r = self._get_redis()
                if r:
                    r.delete("stock_thresholds")
            except Exception:
                pass

    clear_all = clear



class SimulatorStateManager:
    """Tracks state and control signals for the price simulator across processes."""

    def __init__(self):
        self._lock = threading.Lock()
        self._running = False
        self._mode = "deterministic"  # "deterministic" or "random"
        self._interval = 1.0
        self._redis_client = None
        self._latest_prices: Dict[str, float] = {
            "AAPL": 185.00,
            "MSFT": 420.00,
            "GOOGL": 175.00,
        }

    def _get_redis(self):
        now = time.time()
        if self._redis_client is None and (now - getattr(self, "_redis_last_check", 0) < 5.0):
            return None
        self._redis_last_check = now
        try:
            if self._redis_client is None:
                redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
                client = redis.Redis.from_url(redis_url, socket_timeout=0.2, decode_responses=True)
                client.ping()
                self._redis_client = client
            return self._redis_client
        except Exception:
            self._redis_client = None
            return None

    def set_running(self, running: bool, mode: str = "deterministic", interval: float = 1.0):
        with self._lock:
            self._running = running
            self._mode = mode
            self._interval = interval

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def get_status(self) -> dict:
        with self._lock:
            prices = dict(self._latest_prices)
            try:
                r = self._get_redis()
                if r:
                    remote_prices = r.hgetall("stock_latest_prices")
                    if remote_prices:
                        for k, v in remote_prices.items():
                            prices[k] = float(v)
            except Exception:
                pass
            return {
                "running": self._running,
                "mode": self._mode,
                "interval": self._interval,
                "latest_prices": prices,
            }

    def update_price(self, symbol: str, price: float):
        clean_symbol = symbol.upper()
        rounded = round(float(price), 4)
        with self._lock:
            self._latest_prices[clean_symbol] = rounded
            try:
                r = self._get_redis()
                if r:
                    r.hset("stock_latest_prices", clean_symbol, str(rounded))
            except Exception:
                pass


class ReplayStateManager:
    """Tracks status of historical replay sessions."""

    def __init__(self):
        self._lock = threading.Lock()
        self._replays: Dict[str, dict] = {}

    def register_replay(self, replay_id: str, limit: int, symbol: Optional[str], client_id: Optional[str]):
        with self._lock:
            self._replays[replay_id] = {
                "replay_id": replay_id,
                "status": "IN_PROGRESS",
                "limit": limit,
                "symbol": symbol,
                "client_id": client_id,
                "replayed_count": 0,
                "error": None,
            }

    def update_progress(self, replay_id: str, count: int):
        with self._lock:
            if replay_id in self._replays:
                self._replays[replay_id]["replayed_count"] = count

    def finish_replay(self, replay_id: str, count: int, error: Optional[str] = None):
        with self._lock:
            if replay_id in self._replays:
                self._replays[replay_id]["replayed_count"] = count
                self._replays[replay_id]["status"] = "FAILED" if error else "COMPLETED"
                self._replays[replay_id]["error"] = error

    def get_status(self, replay_id: str) -> Optional[dict]:
        with self._lock:
            return self._replays.get(replay_id)


class RecentEventsManager:
    """Maintains a persistent sliding window of captured threshold events for client catch-up sync."""

    def __init__(self, max_items: int = 100):
        self._lock = threading.Lock()
        self._max_items = max_items
        self._events: List[dict] = []
        self._redis_client = None

    def _get_redis(self):
        now = time.time()
        if self._redis_client is None and (now - getattr(self, "_redis_last_check", 0) < 5.0):
            return None
        self._redis_last_check = now
        try:
            if self._redis_client is None:
                redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
                client = redis.Redis.from_url(redis_url, socket_timeout=0.2, decode_responses=True)
                client.ping()
                self._redis_client = client
            return self._redis_client
        except Exception:
            self._redis_client = None
            return None

    def record_event(self, event_data: dict):
        """Records a new event into in-memory buffer and Redis list."""
        with self._lock:
            eid = event_data.get("event_id")
            if not any(e.get("event_id") == eid for e in self._events):
                self._events.insert(0, event_data)
                if len(self._events) > self._max_items:
                    self._events.pop()

            try:
                r = self._get_redis()
                if r:
                    r.lpush("stock_recent_events", json.dumps(event_data))
                    r.ltrim("stock_recent_events", 0, self._max_items - 1)
            except Exception:
                pass

    def get_recent(self, limit: int = 50) -> List[dict]:
        """Returns the most recent captured events for catch-up syncing."""
        with self._lock:
            try:
                r = self._get_redis()
                if r:
                    raw_items = r.lrange("stock_recent_events", 0, limit - 1)
                    if raw_items:
                        return [json.loads(item) for item in raw_items]
            except Exception:
                pass
            return list(self._events[:limit])


# Global singleton managers
threshold_state = ThresholdStateManager()
simulator_state = SimulatorStateManager()
replay_state = ReplayStateManager()
recent_events_state = RecentEventsManager()
