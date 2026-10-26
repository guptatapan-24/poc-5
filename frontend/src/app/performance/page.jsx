"use client";

import React, { useState, useEffect, useRef } from "react";
import {
  Activity,
  ArrowLeft,
  Users,
  Zap,
  Cpu,
  HardDrive,
  CheckCircle2,
  AlertTriangle,
  RotateCcw,
  RefreshCw,
  Terminal,
  ExternalLink,
} from "lucide-react";
import { fetchSystemMetrics } from "../../services/api";

const WS_URL = "ws://localhost:8000/ws/events/";

export default function PerformancePage() {
  const [telemetry, setTelemetry] = useState({
    active_users: 1,
    ws_connections: 1,
    http_users: 0,
    peak_connections: 1,
    requests_per_second: 0.0,
    server_rps: 0.0,
    total_requests: 0,
    cpu_percent: 0.0,
    process_cpu_percent: 0.0,
    process_memory_mb: 0.0,
    system_memory_percent: 0.0,
    system_memory_used_mb: 0.0,
    system_memory_total_mb: 0.0,
    success_rate_percent: 100.0,
    failure_rate_percent: 0.0,
    locust_active: false,
    locust_state: "IDLE",
    endpoint_stats: [],
    locust_errors: [],
    timestamp: new Date().toISOString(),
  });

  const [wsStatus, setWsStatus] = useState("CONNECTING");
  // Rolling time-series history of up to 45 data points for all 6 charts
  const [history, setHistory] = useState([]);
  const wsRef = useRef(null);

  // Poll via HTTP every 1s as guaranteed reliable heartbeat
  useEffect(() => {
    let isMounted = true;
    const pollMetrics = async () => {
      try {
        const res = await fetchSystemMetrics();
        if (isMounted && res && res.metrics) {
          updateTelemetryAndHistory(res.metrics);
        }
      } catch (err) {
        // Ignored
      }
    };

    pollMetrics();
    const interval = setInterval(pollMetrics, 1000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  // Connect to WebSocket for instant live push
  useEffect(() => {
    let isMounted = true;
    let pingTimer = null;

    const connectWs = () => {
      if (wsRef.current) return;
      try {
        const ws = new WebSocket(`${WS_URL}?client_id=perf-monitor`);
        wsRef.current = ws;

        ws.onopen = () => {
          if (!isMounted) return;
          setWsStatus("CONNECTED");
          try {
            ws.send(JSON.stringify({ action: "ping" }));
          } catch {
            // ignore
          }
        };

        ws.onmessage = (event) => {
          if (!isMounted) return;
          try {
            const data = JSON.parse(event.data);
            if (data.metrics) {
              updateTelemetryAndHistory(data.metrics);
            }
          } catch (e) {
            // Ignored
          }
        };

        ws.onclose = () => {
          if (!isMounted) return;
          setWsStatus("DISCONNECTED");
          wsRef.current = null;
          setTimeout(connectWs, 2000);
        };
      } catch (err) {
        setWsStatus("DISCONNECTED");
      }
    };

    connectWs();

    // Heartbeat ping every 5 seconds to keep connection actively open and registered on server
    pingTimer = setInterval(() => {
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        try {
          wsRef.current.send(JSON.stringify({ action: "ping" }));
        } catch {
          // ignore
        }
      }
    }, 5000);

    return () => {
      isMounted = false;
      if (pingTimer) clearInterval(pingTimer);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, []);

  const updateTelemetryAndHistory = (metrics) => {
    setTelemetry(metrics);
    setHistory((prev) => {
      const nowTime = new Date(metrics.timestamp || Date.now()).toLocaleTimeString();
      const lastPoint = prev[prev.length - 1];
      if (lastPoint && lastPoint.rawTime === metrics.timestamp) {
        return prev;
      }

      const point = {
        time: nowTime,
        rawTime: metrics.timestamp,
        users: metrics.active_users || 0,
        cpu: metrics.cpu_percent || 0,
        rps: metrics.requests_per_second || 0,
        memory: metrics.process_memory_mb || 0,
        successRate: metrics.success_rate_percent || 100.0,
        failureRate: metrics.failure_rate_percent || 0.0,
      };

      const updated = [...prev, point];
      if (updated.length > 40) updated.shift();
      return updated;
    });
  };

  // Helper to render an SVG sparkline chart
  const renderChart = ({ dataKey, color, strokeColor, maxValOverride, unit = "", formatVal = (v) => v }) => {
    const chartWidth = 360;
    const chartHeight = 90;
    const vals = history.map((h) => h[dataKey]);
    const computedMax = Math.max(...vals, 1);
    const maxVal = maxValOverride !== undefined ? maxValOverride : Math.max(computedMax, 5);
    const minVal = 0;

    let pathD = "";
    if (history.length > 1) {
      pathD = history
        .map((pt, idx) => {
          const x = (idx / (history.length - 1)) * chartWidth;
          const normalized = Math.min(1, Math.max(0, (pt[dataKey] - minVal) / (maxVal - minVal || 1)));
          const y = chartHeight - normalized * (chartHeight - 16) - 8;
          return `${idx === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");
    }

    const currentVal = history.length > 0 ? history[history.length - 1][dataKey] : telemetry[dataKey] || 0;

    return (
      <div
        className="glass-panel"
        style={{
          padding: "16px",
          borderRadius: "12px",
          background: "rgba(15, 23, 42, 0.65)",
          border: "1px solid rgba(255, 255, 255, 0.08)",
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: "8px" }}>
          <span style={{ fontSize: "0.85rem", fontWeight: 700, color: "var(--text-secondary)" }}>
            {formatVal(currentVal)} {unit}
          </span>
          <span style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>
            Peak: {formatVal(computedMax)} {unit}
          </span>
        </div>

        <div style={{ width: "100%", height: `${chartHeight}px`, position: "relative" }}>
          <svg
            viewBox={`0 0 ${chartWidth} ${chartHeight}`}
            preserveAspectRatio="none"
            style={{ width: "100%", height: "100%", overflow: "visible" }}
          >
            {/* Guide grid lines */}
            <line x1="0" y1="12" x2={chartWidth} y2="12" stroke="rgba(255,255,255,0.06)" strokeDasharray="3 3" />
            <line x1="0" y1={chartHeight / 2} x2={chartWidth} y2={chartHeight / 2} stroke="rgba(255,255,255,0.06)" strokeDasharray="3 3" />
            <line x1="0" y1={chartHeight - 12} x2={chartWidth} y2={chartHeight - 12} stroke="rgba(255,255,255,0.06)" strokeDasharray="3 3" />

            {/* Area Fill */}
            {pathD && (
              <path
                d={`${pathD} L${chartWidth},${chartHeight} L0,${chartHeight} Z`}
                fill={color}
                opacity="0.18"
              />
            )}

            {/* Line Path */}
            {pathD && (
              <path
                d={pathD}
                fill="none"
                stroke={strokeColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                style={{ filter: `drop-shadow(0 0 5px ${strokeColor}66)` }}
              />
            )}
          </svg>
        </div>
      </div>
    );
  };

  const isLocust = telemetry.locust_active;

  return (
    <div className="app-container" style={{ maxWidth: "1400px", margin: "0 auto", padding: "20px 24px" }}>
      {/* Navigation Header */}
      <header
        className="glass-panel"
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          padding: "16px 24px",
          marginBottom: "24px",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <a
            href="/"
            className="btn btn-secondary btn-sm"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "6px",
              textDecoration: "none",
            }}
          >
            <ArrowLeft size={14} />
            Back to Live Trading
          </a>

          <div>
            <h1 style={{ fontSize: "1.25rem", fontWeight: 800, color: "#ffffff", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity size={22} color="var(--accent-cyan)" />
              Performance & Locust Load Monitor
            </h1>
            <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginTop: "2px" }}>
              Real-time server telemetry synchronized with Apache Flink, Daphne ASGI, and Locust (1.0s window)
            </p>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          {isLocust ? (
            <span
              className="badge"
              style={{
                background: "rgba(168, 85, 247, 0.18)",
                color: "#c084fc",
                border: "1px solid rgba(168, 85, 247, 0.35)",
                fontSize: "0.75rem",
                padding: "6px 12px",
              }}
            >
              <span className="status-dot status-dot-purple" style={{ background: "#c084fc", boxShadow: "0 0 8px #a855f7" }} />
              Locust Running: {telemetry.active_users} Users
            </span>
          ) : (
            <span
              className="badge badge-live"
              style={{ fontSize: "0.75rem", padding: "6px 12px" }}
            >
              <span className="status-dot status-dot-green" />
              Live Telemetry: 1.0s Rate
            </span>
          )}

          <a
            href="http://localhost:8089"
            target="_blank"
            rel="noreferrer"
            className="btn btn-secondary btn-sm"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "6px",
              textDecoration: "none",
              background: "rgba(168, 85, 247, 0.12)",
              color: "#c084fc",
              borderColor: "rgba(168, 85, 247, 0.3)",
            }}
          >
            <ExternalLink size={13} />
            Open Locust Web UI (:8089)
          </a>
        </div>
      </header>

      {/* Top 6 KPI Metric Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
          gap: "14px",
          marginBottom: "24px",
        }}
      >
        {/* Card 1: Users */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Active Users
            </span>
            <Users size={18} color="var(--accent-cyan)" />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span className="mono-text" style={{ fontSize: "2rem", fontWeight: 800, color: "#ffffff" }}>
              {telemetry.active_users}
            </span>
            <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>users</span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginTop: "6px" }}>
            {telemetry.ws_connections} WebSockets • {telemetry.http_users} HTTP Users
          </div>
        </div>

        {/* Card 2: Throughput RPS */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Throughput (RPS)
            </span>
            <Zap size={18} color="var(--accent-amber)" />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span className="mono-text" style={{ fontSize: "2rem", fontWeight: 800, color: "var(--accent-amber)" }}>
              {telemetry.requests_per_second.toFixed(1)}
            </span>
            <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>req/s</span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginTop: "6px" }}>
            Server RPS: {telemetry.server_rps.toFixed(1)} • {telemetry.total_requests.toLocaleString()} total reqs
          </div>
        </div>

        {/* Card 3: Server CPU */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Server CPU Load
            </span>
            <Cpu size={18} color="var(--accent-purple)" />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span className="mono-text" style={{ fontSize: "2rem", fontWeight: 800, color: "#c084fc" }}>
              {telemetry.cpu_percent.toFixed(1)}%
            </span>
            <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>system</span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginTop: "6px" }}>
            Daphne Process: {telemetry.process_cpu_percent.toFixed(1)}% CPU
          </div>
        </div>

        {/* Card 4: Memory */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Process Memory
            </span>
            <HardDrive size={18} color="var(--accent-emerald)" />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span className="mono-text" style={{ fontSize: "2rem", fontWeight: 800, color: "#ffffff" }}>
              {telemetry.process_memory_mb.toFixed(0)}
            </span>
            <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>MB RSS</span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginTop: "6px" }}>
            System RAM: {telemetry.system_memory_percent.toFixed(1)}% ({telemetry.system_memory_used_mb.toFixed(0)} MB)
          </div>
        </div>

        {/* Card 5: Success Rate */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Success Rate
            </span>
            <CheckCircle2 size={18} color="#10b981" />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span className="mono-text" style={{ fontSize: "2rem", fontWeight: 800, color: "#10b981" }}>
              {telemetry.success_rate_percent.toFixed(1)}%
            </span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "#10b981", marginTop: "6px" }}>
            Stable • 0 Crashes
          </div>
        </div>

        {/* Card 6: Failure Rate */}
        <div className="glass-panel" style={{ padding: "16px 18px", borderRadius: "10px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 700, textTransform: "uppercase" }}>
              Failure Rate
            </span>
            <AlertTriangle size={18} color={telemetry.failure_rate_percent > 0 ? "var(--accent-rose)" : "var(--text-muted)"} />
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
            <span
              className="mono-text"
              style={{
                fontSize: "2rem",
                fontWeight: 800,
                color: telemetry.failure_rate_percent > 0 ? "var(--accent-rose)" : "#f8fafc",
              }}
            >
              {telemetry.failure_rate_percent.toFixed(2)}%
            </span>
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginTop: "6px" }}>
            {telemetry.locust_errors.length} Registered Exceptions
          </div>
        </div>
      </div>

      {/* ALL 6 Real-Time Concurrent Charts Section */}
      <div style={{ marginBottom: "28px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
          <h2 style={{ fontSize: "1.1rem", fontWeight: 700, color: "#ffffff", display: "flex", alignItems: "center", gap: "8px" }}>
            <Zap size={18} color="var(--accent-amber)" />
            Real-Time Telemetry Performance Curves (Concurrent Live Streams)
          </h2>
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Rolling Window: 40s • Sample Interval: 1.0s
          </span>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(380px, 1fr))",
            gap: "18px",
          }}
        >
          {/* Chart 1: Active Users */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--accent-cyan)" }}>
                1. Active Users Over Time
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Target: 10 - 100 Users</span>
            </div>
            {renderChart({
              dataKey: "users",
              color: "rgba(6, 182, 212, 0.4)",
              strokeColor: "var(--accent-cyan)",
              unit: "Users",
              formatVal: (v) => v,
            })}
          </div>

          {/* Chart 2: Server CPU */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "#c084fc" }}>
                2. Server CPU Load (%)
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Scale: 0 - 100%</span>
            </div>
            {renderChart({
              dataKey: "cpu",
              color: "rgba(168, 85, 247, 0.4)",
              strokeColor: "#c084fc",
              maxValOverride: 100,
              unit: "%",
              formatVal: (v) => v.toFixed(1),
            })}
          </div>

          {/* Chart 3: Requests Per Second */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--accent-amber)" }}>
                3. Request Throughput (RPS)
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Measured over 1.0s</span>
            </div>
            {renderChart({
              dataKey: "rps",
              color: "rgba(245, 158, 11, 0.4)",
              strokeColor: "var(--accent-amber)",
              unit: "req/s",
              formatVal: (v) => v.toFixed(1),
            })}
          </div>

          {/* Chart 4: Process Memory */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--accent-emerald)" }}>
                4. Process Memory Footprint
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Daphne ASGI RSS</span>
            </div>
            {renderChart({
              dataKey: "memory",
              color: "rgba(16, 185, 129, 0.4)",
              strokeColor: "var(--accent-emerald)",
              unit: "MB",
              formatVal: (v) => v.toFixed(0),
            })}
          </div>

          {/* Chart 5: Success Rate */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "#10b981" }}>
                5. Success Rate (%)
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Scale: 0 - 100%</span>
            </div>
            {renderChart({
              dataKey: "successRate",
              color: "rgba(16, 185, 129, 0.3)",
              strokeColor: "#10b981",
              maxValOverride: 100,
              unit: "%",
              formatVal: (v) => v.toFixed(1),
            })}
          </div>

          {/* Chart 6: Failure Rate */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--accent-rose)" }}>
                6. Failure Rate (%)
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Target: 0.0%</span>
            </div>
            {renderChart({
              dataKey: "failureRate",
              color: "rgba(244, 63, 94, 0.3)",
              strokeColor: "var(--accent-rose)",
              maxValOverride: 10,
              unit: "%",
              formatVal: (v) => v.toFixed(2),
            })}
          </div>
        </div>
      </div>

      {/* Locust Endpoint Statistics Table */}
      <div className="glass-panel" style={{ padding: "20px 24px", borderRadius: "12px", marginBottom: "28px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
          <div>
            <h3 style={{ fontSize: "1rem", fontWeight: 700, color: "#ffffff" }}>
              Endpoint Statistics Breakdown
            </h3>
            <p style={{ fontSize: "0.75rem", color: "var(--text-muted)", marginTop: "2px" }}>
              {isLocust
                ? "Synchronized directly from running Locust engine (:8089)"
                : "Awaiting Locust run — start Locust below to see endpoint percentiles"}
            </p>
          </div>

          {isLocust && (
            <span className="badge badge-live" style={{ fontSize: "0.72rem" }}>
              Status: {telemetry.locust_state}
            </span>
          )}
        </div>

        <div className="event-table-container">
          <table className="event-table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Name / Endpoint</th>
                <th># Requests</th>
                <th># Fails</th>
                <th>Median (ms)</th>
                <th>95%ile (ms)</th>
                <th>Average (ms)</th>
                <th>Min (ms)</th>
                <th>Max (ms)</th>
                <th>Current RPS</th>
              </tr>
            </thead>
            <tbody>
              {telemetry.endpoint_stats && telemetry.endpoint_stats.length > 0 ? (
                telemetry.endpoint_stats.map((row, idx) => (
                  <tr
                    key={idx}
                    style={{
                      background: row.name === "Aggregated" ? "rgba(168, 85, 247, 0.12)" : "transparent",
                      fontWeight: row.name === "Aggregated" ? 700 : 400,
                    }}
                  >
                    <td>
                      <span className="badge" style={{ fontSize: "0.68rem" }}>
                        {row.method || "HTTP"}
                      </span>
                    </td>
                    <td className="mono-text" style={{ color: "#f8fafc" }}>
                      {row.name}
                    </td>
                    <td className="mono-text">{row.num_requests?.toLocaleString()}</td>
                    <td className="mono-text" style={{ color: row.num_failures > 0 ? "var(--accent-rose)" : "var(--text-muted)" }}>
                      {row.num_failures || 0}
                    </td>
                    <td className="mono-text">{row.median_response_time || 0} ms</td>
                    <td className="mono-text">{row.ninetynine_response_time || row["95%"] || 0} ms</td>
                    <td className="mono-text">{row.avg_response_time?.toFixed(1) || 0} ms</td>
                    <td className="mono-text">{row.min_response_time || 0} ms</td>
                    <td className="mono-text">{row.max_response_time || 0} ms</td>
                    <td className="mono-text" style={{ color: "var(--accent-amber)" }}>
                      {row.current_rps?.toFixed(1) || "0.0"}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={10} style={{ textAlign: "center", padding: "24px", color: "var(--text-muted)" }}>
                    No active Locust run detected on port 8089. Run Locust with the command below to stream live endpoint statistics.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Terminal Command Assistant */}
      <div
        className="glass-panel"
        style={{
          padding: "18px 24px",
          borderRadius: "10px",
          background: "rgba(10, 15, 29, 0.8)",
          border: "1px solid rgba(255, 255, 255, 0.08)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "10px" }}>
          <Terminal size={16} color="var(--accent-cyan)" />
          <span style={{ fontSize: "0.85rem", fontWeight: 700, color: "#ffffff" }}>
            Run Load Tests from Terminal
          </span>
        </div>

        <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
          <div
            style={{
              flex: "1 1 320px",
              padding: "10px 14px",
              background: "rgba(0,0,0,0.4)",
              borderRadius: "6px",
              border: "1px solid rgba(255,255,255,0.06)",
            }}
          >
            <div style={{ fontSize: "0.72rem", color: "var(--accent-cyan)", fontWeight: 600, marginBottom: "4px" }}>
              Standard 50 Users (Spawn 10/s)
            </div>
            <code style={{ fontSize: "0.75rem", color: "#e2e8f0" }}>
              uv run locust -f locustfile.py -u 50 -r 10 --host http://127.0.0.1:8000
            </code>
          </div>

          <div
            style={{
              flex: "1 1 320px",
              padding: "10px 14px",
              background: "rgba(0,0,0,0.4)",
              borderRadius: "6px",
              border: "1px solid rgba(255,255,255,0.06)",
            }}
          >
            <div style={{ fontSize: "0.72rem", color: "var(--accent-amber)", fontWeight: 600, marginBottom: "4px" }}>
              Stress Test: 100 Users (Spawn 20/s)
            </div>
            <code style={{ fontSize: "0.75rem", color: "#e2e8f0" }}>
              uv run locust -f locustfile.py -u 100 -r 20 --host http://127.0.0.1:8000
            </code>
          </div>
        </div>
      </div>
    </div>
  );
}
