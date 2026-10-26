import React from "react";
import { Activity, Radio, Database, HelpCircle, RefreshCw } from "lucide-react";

export function Header({
  connectionStatus,
  clientId,
  health,
  onReconnect,
  onOpenArchitecture,
}) {
  const isConnected = connectionStatus === "CONNECTED";
  const isReconnecting = connectionStatus === "RECONNECTING" || connectionStatus === "CONNECTING";

  return (
    <header className="app-header glass-panel" id="main-header">
      <div className="brand-section">
        <div className="brand-icon">
          <Activity size={24} color="#ffffff" />
        </div>
        <div className="brand-text">
          <h1>Stock Event Capture & Replay</h1>
          <p>Apache Kafka • Django Channels • Redis • WebSockets</p>
        </div>
      </div>

      <div className="header-actions">
        {/* System Health Indicators */}
        {health && (
          <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
            <span
              className="badge"
              style={{
                background: health.kafka?.reachable ? "rgba(16, 185, 129, 0.15)" : "rgba(244, 63, 94, 0.15)",
                color: health.kafka?.reachable ? "#10b981" : "#f43f5e",
                border: "1px solid rgba(255,255,255,0.08)",
              }}
              title={health.kafka?.details || "Kafka status"}
            >
              <Database size={12} />
              Kafka: {health.kafka?.reachable ? "Online" : "Offline"}
            </span>

            <span
              className="badge"
              style={{
                background: health.redis?.reachable ? "rgba(16, 185, 129, 0.15)" : "rgba(244, 63, 94, 0.15)",
                color: health.redis?.reachable ? "#10b981" : "#f43f5e",
                border: "1px solid rgba(255,255,255,0.08)",
              }}
              title={health.redis?.details || "Redis status"}
            >
              <Radio size={12} />
              Redis: {health.redis?.reachable ? "Online" : "Offline"}
            </span>
          </div>
        )}

        {/* WebSocket Connection Badge */}
        <span
          id="connection-badge"
          className={`badge ${
            isConnected
              ? "badge-connected"
              : isReconnecting
              ? "badge-connecting"
              : "badge-disconnected"
          }`}
          title={`Client ID: ${clientId || "unknown"}`}
        >
          <span
            className={`status-dot ${
              isConnected
                ? "status-dot-green"
                : isReconnecting
                ? "status-dot-amber"
                : "status-dot-red"
            }`}
          />
          {connectionStatus}
        </span>

        {/* Reconnect button */}
        {!isConnected && (
          <button
            id="btn-reconnect"
            className="btn btn-secondary btn-sm"
            onClick={onReconnect}
            title="Reconnect WebSocket"
          >
            <RefreshCw size={13} className={isReconnecting ? "animate-spin" : ""} />
            Reconnect
          </button>
        )}

        {/* Architecture Info Button */}
        <button
          id="btn-open-arch"
          className="btn btn-secondary btn-sm"
          onClick={onOpenArchitecture}
          title="View Event-Driven Architecture Explanation"
        >
          <HelpCircle size={14} />
          Architecture
        </button>

        {/* Dedicated Performance & Locust Monitor Page Link */}
        <a
          href="/performance"
          id="btn-open-performance"
          className="btn btn-secondary btn-sm"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "6px",
            background: "rgba(56, 189, 248, 0.12)",
            color: "#38bdf8",
            borderColor: "rgba(56, 189, 248, 0.3)",
            textDecoration: "none",
            fontWeight: 600,
          }}
          title="Open Dedicated Real-Time Performance & Locust Load Testing Monitor"
        >
          <Activity size={14} color="#38bdf8" />
          Load & Performance Monitor
        </a>
      </div>

    </header>
  );
}
