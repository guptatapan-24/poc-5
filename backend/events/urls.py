"""URL routing for events REST API endpoints."""

from django.urls import path
from events import views

urlpatterns = [
    path("health/", views.health_view, name="api-health"),
    path("thresholds/", views.thresholds_view, name="api-thresholds"),
    path("thresholds/<str:rule_id>/", views.threshold_detail_view, name="api-threshold-detail"),
    path("simulator/status/", views.simulator_status_view, name="api-simulator-status"),
    path("simulator/start/", views.simulator_start_view, name="api-simulator-start"),
    path("simulator/stop/", views.simulator_stop_view, name="api-simulator-stop"),
    path("replay/", views.replay_view, name="api-replay"),
    path("replay/<str:replay_id>/", views.replay_status_view, name="api-replay-status"),
    path("events/inspect/", views.inspect_events_view, name="api-events-inspect"),
    path("events/recent/", views.recent_events_view, name="api-events-recent"),
    path("system/metrics/", views.system_metrics_view, name="api-system-metrics"),
]

