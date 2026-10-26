"""WebSocket URL patterns for Django Channels routing."""

from django.urls import re_path
from events.consumers import StockEventsConsumer

websocket_urlpatterns = [
    re_path(r"^ws/events/$", StockEventsConsumer.as_asgi()),
]
