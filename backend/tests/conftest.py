import pytest
from django.conf import settings


@pytest.fixture(autouse=True)
def in_memory_channel_layer(settings):
    """Ensures unit tests run using fast in-memory channel layer without requiring Redis."""
    settings.CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels.layers.InMemoryChannelLayer",
        }
    }
