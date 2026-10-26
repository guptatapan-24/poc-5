/**
 * REST API client for interacting with the Django backend.
 */

const API_BASE = "http://localhost:8000/api";

export async function fetchHealth() {
  const res = await fetch(`${API_BASE}/health/`);
  return res.json();
}

export async function fetchThresholds() {
  const res = await fetch(`${API_BASE}/thresholds/`);
  return res.json();
}

export async function createThreshold(thresholdData) {
  const res = await fetch(`${API_BASE}/thresholds/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(thresholdData),
  });
  return res.json();
}

export async function deleteThreshold(ruleId) {
  const res = await fetch(`${API_BASE}/thresholds/${ruleId}/`, {
    method: "DELETE",
  });
  return res.json();
}

export async function fetchSimulatorStatus() {
  const res = await fetch(`${API_BASE}/simulator/status/`);
  return res.json();
}

export async function startSimulator({ mode = "deterministic", interval = 1.0, symbols = ["AAPL"] } = {}) {
  const res = await fetch(`${API_BASE}/simulator/start/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode, interval, symbols }),
  });
  return res.json();
}

export async function stopSimulator() {
  const res = await fetch(`${API_BASE}/simulator/stop/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  return res.json();
}

export async function requestReplay({ limit = 50, symbol = null, client_id = null }) {
  const res = await fetch(`${API_BASE}/replay/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ limit, symbol, client_id }),
  });
  return res.json();
}

export async function inspectRetainedEvents(limit = 20) {
  const res = await fetch(`${API_BASE}/events/inspect/?limit=${limit}`);
  return res.json();
}

export async function fetchRecentEvents(limit = 50) {
  const res = await fetch(`${API_BASE}/events/recent/?limit=${limit}`);
  return res.json();
}

export async function fetchSystemMetrics() {
  const res = await fetch(`${API_BASE}/system/metrics/`);
  return res.json();
}


