"""Pydantic schemas and validation models for stock price events and threshold evaluations."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Literal, Union, Any
import uuid
from pydantic import BaseModel, Field, field_validator


class ThresholdCondition(str, Enum):
    LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"
    GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"


class DeliveryMode(str, Enum):
    LIVE = "LIVE"
    REPLAY = "REPLAY"


class PriceUpdate(BaseModel):
    """Schema for raw stock price updates published to 'stock-prices'."""
    message_id: str = Field(default_factory=lambda: f"price-{uuid.uuid4().hex[:8]}")
    symbol: str
    price: float
    observed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        s = v.strip().upper()
        if not s or len(s) > 10:
            raise ValueError("Symbol must be non-empty and up to 10 characters")
        return s

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Price must be strictly positive")
        return round(float(v), 4)


class ThresholdCrossingEvent(BaseModel):
    """Schema for detected threshold-crossing events published to 'stock-events'."""
    event_id: str = Field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:8]}")
    event_type: str = "PRICE_THRESHOLD_CROSSED"
    symbol: str
    previous_price: float
    price: float
    threshold: float
    condition: ThresholdCondition
    event_timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    processing_mode: DeliveryMode = DeliveryMode.LIVE

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("previous_price", "price", "threshold")
    @classmethod
    def validate_prices(cls, v: float) -> float:
        return round(float(v), 4)


class PriceDowntrendEvent(BaseModel):
    """Schema for Flink CEP detected 3-consecutive price drop pattern within 10s."""
    event_id: str = Field(default_factory=lambda: f"cep-{uuid.uuid4().hex[:8]}")
    event_type: str = "PRICE_DOWNTREND_DETECTED"
    pattern_id: str = "DOWN_3_WITHIN_10S"
    symbol: str
    prices: list[float]
    first_observed_at: str
    last_observed_at: str
    event_timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    processing_mode: DeliveryMode = DeliveryMode.LIVE
    details: Optional[dict] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("prices")
    @classmethod
    def validate_prices(cls, v: list[float]) -> list[float]:
        return [round(float(p), 4) for p in v]


def parse_stock_event(data: dict):
    """Parses raw dict into either ThresholdCrossingEvent or PriceDowntrendEvent."""
    event_type = data.get("event_type")
    if event_type == "PRICE_DOWNTREND_DETECTED":
        return PriceDowntrendEvent.model_validate(data)
    return ThresholdCrossingEvent.model_validate(data)


class ThresholdRule(BaseModel):
    """Schema for user-configured price alert thresholds."""
    id: str = Field(default_factory=lambda: f"rule-{uuid.uuid4().hex[:8]}")
    symbol: str
    threshold: float
    condition: ThresholdCondition

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        s = v.strip().upper()
        if not s:
            raise ValueError("Symbol is required")
        return s

    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Threshold must be greater than 0")
        return round(float(v), 2)


class WebSocketEventMessage(BaseModel):
    """Envelope schema for messages delivered to frontend clients over WebSocket."""
    message_type: Literal["stock_event", "system_status", "replay_status", "price_ticker"] = "stock_event"
    delivery_mode: DeliveryMode = DeliveryMode.LIVE
    replay_id: Optional[str] = None
    event: Optional[Union[dict, Any]] = None
    ticker: Optional[PriceUpdate] = None
    details: Optional[dict] = None

    @field_validator("event", mode="before")
    @classmethod
    def serialize_event(cls, v):
        if hasattr(v, "model_dump"):
            return v.model_dump()
        return v


class ReplayRequest(BaseModel):
    """Request payload for historical event replay from Kafka."""
    limit: int = Field(default=50, ge=1, le=1000)
    symbol: Optional[str] = None
    client_id: Optional[str] = None

    @field_validator("symbol")
    @classmethod
    def sanitize_symbol(cls, v: Optional[str]) -> Optional[str]:
        if v:
            clean = v.strip().upper()
            return clean if clean and clean != "ALL" else None
        return None
