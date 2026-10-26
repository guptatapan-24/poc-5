"""HTTP REST API views for health checks, threshold configuration, simulator control, and event replay."""

import json
import logging
from django.http import JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseNotFound
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from events.schemas import ThresholdRule, ReplayRequest
from events.state import threshold_state, simulator_state, replay_state, recent_events_state
from event_pipeline.event_service import event_service
from event_pipeline.replay_consumer import replay_service
from config.kafka_utils import is_kafka_reachable

logger = logging.getLogger(__name__)


def health_view(request: HttpRequest):
    """GET /api/health/ - reports system health including Redis and Kafka connectivity."""
    data = event_service.check_health()
    status_code = 200 if data["status"] == "HEALTHY" else 503
    return JsonResponse(data, status=status_code)


@csrf_exempt
def thresholds_view(request: HttpRequest):
    """GET /api/thresholds/ - list configured thresholds.

    POST /api/thresholds/ - create or update a threshold.
    """
    if request.method == "GET":
        rules = [r.model_dump() for r in threshold_state.list_all()]
        return JsonResponse({"thresholds": rules})

    elif request.method == "POST":
        try:
            body = json.loads(request.body.decode("utf-8"))
            rule = ThresholdRule.model_validate(body)
            saved = threshold_state.add_or_update(rule)
            return JsonResponse({"status": "SUCCESS", "threshold": saved.model_dump()}, status=201)
        except Exception as e:
            return JsonResponse({"status": "ERROR", "message": str(e)}, status=400)

    return JsonResponse({"error": "Method not allowed"}, status=405)


@csrf_exempt
def threshold_detail_view(request: HttpRequest, rule_id: str):
    """DELETE /api/thresholds/<rule_id>/ - removes a configured threshold."""
    if request.method == "DELETE":
        deleted = threshold_state.remove(rule_id)
        if deleted:
            return JsonResponse({"status": "DELETED", "rule_id": rule_id})
        return HttpResponseNotFound(json.dumps({"error": f"Rule '{rule_id}' not found"}), content_type="application/json")

    return JsonResponse({"error": "Method not allowed"}, status=405)


def simulator_status_view(request: HttpRequest):
    """GET /api/simulator/status/ - returns current simulator status."""
    return JsonResponse(simulator_state.get_status())


@csrf_exempt
def simulator_start_view(request: HttpRequest):
    """POST /api/simulator/start/ - starts the price simulator."""
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    try:
        body = {}
        if request.body:
            body = json.loads(request.body.decode("utf-8"))

        mode = body.get("mode", "deterministic")
        interval = float(body.get("interval", 1.0))
        symbols = body.get("symbols", ["AAPL"])

        started = event_service.start_background_simulator(
            mode=mode,
            interval=interval,
            symbols=symbols,
        )

        return JsonResponse({
            "status": "STARTED" if started else "ALREADY_RUNNING",
            "simulator": simulator_state.get_status(),
        })
    except Exception as e:
        return JsonResponse({"status": "ERROR", "message": str(e)}, status=400)


@csrf_exempt
def simulator_stop_view(request: HttpRequest):
    """POST /api/simulator/stop/ - stops the price simulator."""
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    stopped = event_service.stop_background_simulator()
    return JsonResponse({
        "status": "STOPPED" if stopped else "NOT_RUNNING",
        "simulator": simulator_state.get_status(),
    })


@csrf_exempt
def replay_view(request: HttpRequest):
    """POST /api/replay/ - launches an isolated historical replay from Kafka."""
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    try:
        body = {}
        if request.body:
            body = json.loads(request.body.decode("utf-8"))

        replay_req = ReplayRequest.model_validate(body)
        replay_id = replay_service.start_replay(replay_req)

        return JsonResponse({
            "status": "QUEUED",
            "replay_id": replay_id,
            "limit": replay_req.limit,
            "symbol": replay_req.symbol,
            "client_id": replay_req.client_id,
        }, status=202)
    except Exception as e:
        return JsonResponse({"status": "ERROR", "message": str(e)}, status=400)


def replay_status_view(request: HttpRequest, replay_id: str):
    """GET /api/replay/<replay_id>/ - returns the status of a specific replay session."""
    status_info = replay_state.get_status(replay_id)
    if status_info:
        return JsonResponse(status_info)
    return HttpResponseNotFound(json.dumps({"error": f"Replay session '{replay_id}' not found"}), content_type="application/json")


def inspect_events_view(request: HttpRequest):
    """GET /api/events/inspect/ - inspects retained events directly from Kafka 'stock-events' topic."""
    limit = int(request.GET.get("limit", 20))
    records = event_service.inspect_events_topic(limit=limit)
    return JsonResponse({"retained_events": records, "count": len(records)})


def recent_events_view(request: HttpRequest):
    """GET /api/events/recent/ - returns recent live events for client catch-up sync."""
    limit = int(request.GET.get("limit", 50))
    events = recent_events_state.get_recent(limit=limit)
    if not events and is_kafka_reachable(getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"), timeout=0.3):
        try:
            records = event_service.inspect_events_topic(limit=limit)
            events = [
                {
                    **r,
                    "delivery_mode": "LIVE",
                }
                for r in records
            ]
        except Exception:
            events = []
    return JsonResponse({"events": events, "count": len(events)})


def system_metrics_view(request: HttpRequest):
    """GET /api/system/metrics/ - returns instantaneous CPU, memory, active connection count, and RPS."""
    from events.telemetry import telemetry_tracker
    return JsonResponse({
        "status": "SUCCESS",
        "metrics": telemetry_tracker.get_telemetry_snapshot(),
    })


