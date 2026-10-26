"""CORS and telemetry middleware for local development between React/Next.js and Django."""

import time
from django.http import HttpResponse
from events.telemetry import telemetry_tracker


class CorsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        client_id = request.headers.get("X-Client-ID") or request.headers.get("Client-ID")
        is_metrics_poll = request.path.startswith("/api/system/metrics")
        endpoint_name = f"{request.method} {request.path.split('?')[0]}"
        start_time = time.time()

        # Record incoming HTTP request in telemetry tracker
        telemetry_tracker.record_http_request(
            client_id=client_id,
            client_ip=request.META.get("REMOTE_ADDR"),
            is_internal_poll=is_metrics_poll,
            endpoint=endpoint_name,
        )

        if request.method == "OPTIONS":
            response = HttpResponse()
            self._add_cors_headers(response)
            return response

        response = self.get_response(request)
        self._add_cors_headers(response)

        # Track response latency & status
        duration_ms = (time.time() - start_time) * 1000.0
        telemetry_tracker.record_http_response(
            endpoint=endpoint_name,
            duration_ms=duration_ms,
            is_error=(response.status_code >= 400),
            is_internal_poll=is_metrics_poll,
        )

        return response

    def _add_cors_headers(self, response):
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With, client-id, x-client-id"
        response["Access-Control-Max-Age"] = "86400"
