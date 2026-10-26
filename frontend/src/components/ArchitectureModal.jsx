import React from "react";
import { X, Layers, Server, Radio, Database, ShieldCheck } from "lucide-react";

export function ArchitectureModal({ isOpen, onClose }) {
  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <Layers size={20} color="var(--accent-cyan)" />
            <h2 style={{ fontSize: "1.1rem", fontWeight: 700 }}>System Architecture &amp; Technology Roles</h2>
          </div>
          <button className="btn btn-secondary btn-icon" onClick={onClose}>
            <X size={16} />
          </button>
        </div>

        <div className="modal-body" style={{ fontSize: "0.85rem", color: "var(--text-secondary)" }}>
          <div className="arch-diagram">
            {`PRICE SIMULATOR -> [Kafka Topic: stock-prices]
                          |
             Apache Flink & Flink CEP Engine
       (Keyed Thresholds + 3-Drop Downtrend CEP)
                          |
                     [Kafka Topic: stock-events] (7d Retention)
                          |
               +-----------+-----------+
               |                       |
        Live Consumer          Replay Consumer (Isolated seek)
               |                       |
               +-----------+-----------+
                           |
                   Django Channels Layer
                           |
                     Redis Backend
                           |
                     WebSockets (ws://)
                           |
                     Next.js Dashboard
                           ^
                           | (Tested by)
               Locust Benchmark (H1, W1, E1)`}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "14px", marginTop: "16px" }}>
            <div style={{ background: "rgba(15, 23, 42, 0.6)", padding: "12px", borderRadius: "8px" }}>
              <div style={{ fontWeight: 700, color: "var(--text-primary)", display: "flex", alignItems: "center", gap: "6px" }}>
                <Layers size={15} color="#c084fc" />
                Apache Flink &amp; Flink CEP
              </div>
              <p style={{ fontSize: "0.78rem", marginTop: "4px" }}>
                Dedicated stream-processing runtime with event-time watermarking, stateful crossing hysteresis (WAITING/ARMED/TRIGGERED), 3-drop consecutive pattern matching, and periodic checkpoints.
              </p>
            </div>

            <div style={{ background: "rgba(15, 23, 42, 0.6)", padding: "12px", borderRadius: "8px" }}>
              <div style={{ fontWeight: 700, color: "var(--text-primary)", display: "flex", alignItems: "center", gap: "6px" }}>
                <Server size={15} color="#06b6d4" />
                Locust Load Testing
              </div>
              <p style={{ fontSize: "0.78rem", marginTop: "4px" }}>
                Independent benchmarking tool testing REST API throughput (H1) and multi-user WebSocket delivery latency &amp; heartbeat stability (W1/E1) with p50/p95/p99 latency tracking.
              </p>
            </div>

            <div style={{ background: "rgba(15, 23, 42, 0.6)", padding: "12px", borderRadius: "8px" }}>
              <div style={{ fontWeight: 700, color: "var(--text-primary)", display: "flex", alignItems: "center", gap: "6px" }}>
                <Database size={15} color="#10b981" />
                Kafka Event Retention
              </div>
              <p style={{ fontSize: "0.78rem", marginTop: "4px" }}>
                Retains immutable partitioned event streams for 7 days. Topics: <code>stock-prices</code> and <code>stock-events</code>. Replay reads directly from Kafka log.
              </p>
            </div>

            <div style={{ background: "rgba(15, 23, 42, 0.6)", padding: "12px", borderRadius: "8px" }}>
              <div style={{ fontWeight: 700, color: "var(--text-primary)", display: "flex", alignItems: "center", gap: "6px" }}>
                <ShieldCheck size={15} color="#f59e0b" />
                Isolated Replay &amp; Safety
              </div>
              <p style={{ fontSize: "0.78rem", marginTop: "4px" }}>
                Historical replay uses ephemeral partition seek without touching live consumer group offsets. De-duplicates by event_id. Simulated prices only; educational system.
              </p>
            </div>
          </div>
        </div>

        <div className="modal-footer">
          <button className="btn btn-secondary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
