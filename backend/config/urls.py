"""URL configuration for stock event replay system."""

from django.urls import path, include

urlpatterns = [
    path("api/", include("events.urls")),
]
