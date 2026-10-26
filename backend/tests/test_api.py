"""API integration tests for Django REST views."""

import json
import pytest
from django.test import Client
from events.state import threshold_state, simulator_state, replay_state


@pytest.fixture
def client():
    return Client()


def test_health_endpoint(client):
    response = client.get("/api/health/")
    # Status code is either 200 or 503 depending on local broker availability
    assert response.status_code in (200, 503)
    data = response.json()
    assert "status" in data
    assert "redis" in data
    assert "kafka" in data
    assert "simulator" in data


def test_thresholds_crud(client):
    # GET list
    res_list = client.get("/api/thresholds/")
    assert res_list.status_code == 200
    assert "thresholds" in res_list.json()

    # POST new rule
    new_rule = {
        "symbol": "GOOGL",
        "threshold": 175.50,
        "condition": "LESS_THAN_OR_EQUAL",
    }
    res_create = client.post(
        "/api/thresholds/",
        data=json.dumps(new_rule),
        content_type="application/json",
    )
    assert res_create.status_code == 201
    created_id = res_create.json()["threshold"]["id"]

    # Verify present in state
    found = threshold_state.get_by_id(created_id)
    assert found is not None
    assert found.symbol == "GOOGL"

    # DELETE rule
    res_del = client.delete(f"/api/thresholds/{created_id}/")
    assert res_del.status_code == 200
    assert res_del.json()["status"] == "DELETED"

    # DELETE non-existent rule
    res_del_404 = client.delete("/api/thresholds/fake-rule-id/")
    assert res_del_404.status_code == 404


def test_threshold_validation_errors(client):
    # Invalid condition
    res_invalid_cond = client.post(
        "/api/thresholds/",
        data=json.dumps({"symbol": "AAPL", "threshold": 100.0, "condition": "INVALID_CONDITION"}),
        content_type="application/json",
    )
    assert res_invalid_cond.status_code == 400

    # Negative threshold
    res_neg = client.post(
        "/api/thresholds/",
        data=json.dumps({"symbol": "AAPL", "threshold": -50.0, "condition": "LESS_THAN_OR_EQUAL"}),
        content_type="application/json",
    )
    assert res_neg.status_code == 400


def test_simulator_endpoints(client):
    # Status
    res_status = client.get("/api/simulator/status/")
    assert res_status.status_code == 200
    assert "running" in res_status.json()

    # Stop when not running
    simulator_state.set_running(False)
    res_stop = client.post("/api/simulator/stop/")
    assert res_stop.status_code == 200
    assert res_stop.json()["status"] == "NOT_RUNNING"


def test_replay_request_validation(client):
    # Invalid limit (0)
    res_invalid = client.post(
        "/api/replay/",
        data=json.dumps({"limit": 0, "symbol": "AAPL"}),
        content_type="application/json",
    )
    assert res_invalid.status_code == 400

    # Unknown replay status
    res_status_404 = client.get("/api/replay/non-existent-replay-id/")
    assert res_status_404.status_code == 404


def test_recent_events_endpoint(client):
    res = client.get("/api/events/recent/?limit=10")
    assert res.status_code == 200
    data = res.json()
    assert "events" in data
    assert "count" in data

