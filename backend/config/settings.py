"""Django settings for stock event replay system."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
load_dotenv(BASE_DIR / ".env")

# Quick-start development settings - unsuitable for production
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-secret-key-fallback-for-local-development")
DEBUG = os.getenv("DJANGO_DEBUG", "True").lower() in ("true", "1", "yes")
ALLOWED_HOSTS = [host.strip() for host in os.getenv("ALLOWED_HOSTS", "*").split(",")]

# Application definition
INSTALLED_APPS = [
    "daphne",
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "channels",
    "events",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "config.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
            ],
        },
    },
]

# ASGI and WSGI applications
ASGI_APPLICATION = "config.asgi.application"
WSGI_APPLICATION = "config.wsgi.application"

# Database - Lightweight SQLite for Django metadata
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

# Redis & Django Channels configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [
                {
                    "address": REDIS_URL,
                    "socket_timeout": 10.0,
                    "socket_connect_timeout": 5.0,
                    "health_check_interval": 15,
                    "retry_on_timeout": True,
                }
            ],
            "capacity": 1500,
            "expiry": 60,
        },
    },
}

# Kafka configuration
from config.kafka_utils import resolve_kafka_bootstrap_servers  # noqa: E402
KAFKA_BOOTSTRAP_SERVERS = resolve_kafka_bootstrap_servers()
KAFKA_API_VERSION = (3, 8, 0)
KAFKA_TOPIC_PRICES = os.getenv("KAFKA_TOPIC_PRICES", "stock-prices")
KAFKA_TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "stock-events")
KAFKA_CONSUMER_GROUP_LIVE = os.getenv("KAFKA_CONSUMER_GROUP_LIVE", "stock-event-live-group")
DEFAULT_REPLAY_LIMIT = int(os.getenv("DEFAULT_REPLAY_LIMIT", "50"))
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
