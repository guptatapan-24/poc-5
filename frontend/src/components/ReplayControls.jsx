import React, { useState } from "react";
import { RotateCcw, Clock, CheckCircle2, AlertTriangle, Layers } from "lucide-react";

export function ReplayControls({ onReplay, replayStatus, isConnected }) {
  const [limit, setLimit] = useState(20);
  const [symbol, setSymbol] = useState("ALL");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleReplayClick = async () => {
    setIsSubmitting(true);
    try {
      await onReplay({
        limit: parseInt(limit, 10),
        symbol: symbol === "ALL" ? null : symbol,
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const isReplaying = replayStatus?.status === "IN_PROGRESS" || replayStatus?.status === "STARTED";
  const isCompleted = replayStatus?.status === "COMPLETED";
  const isFailed = replayStatus?.status === "FAILED";

  return (
    <div className="glass-panel" id="replay-controls-card">
      <div className="card-header">
        <h2 className="card-title">
          <RotateCcw size={18} color="var(--accent-purple)" />
          Kafka Event Replay
        </h2>
        <span className="badge badge-replay" style={{ fontSize: "0.7rem" }}>
          Isolated Consumer
        </span>
      </div>

      <div className="card-body">
        <div style={{ fontSize: "0.8rem", color: "var(--text-secondary)", marginBottom: "16px" }}>
          Reads previously captured crossing records from Kafka <code>stock-events</code> topic. Seeks to beginning
          without resetting live offsets.
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "16px" }}>
          <div className="form-group" style={{ margin: 0 }}>
            <label className="form-label" htmlFor="replay-limit">
              Replay Batch Limit
            </label>
            <select
              id="replay-limit"
              className="form-select"
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
              disabled={isReplaying}
            >
              <option value={10}>10 events</option>
              <option value={20}>20 events</option>
              <option value={50}>50 events</option>
              <option value={100}>100 events</option>
            </select>
          </div>

          <div className="form-group" style={{ margin: 0 }}>
            <label className="form-label" htmlFor="replay-symbol">
              Symbol Filter
            </label>
            <select
              id="replay-symbol"
              className="form-select"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              disabled={isReplaying}
            >
              <option value="ALL">All Symbols</option>
              <option value="AAPL">AAPL Only</option>
              <option value="MSFT">MSFT Only</option>
              <option value="GOOGL">GOOGL Only</option>
            </select>
          </div>
        </div>

        <button
          id="btn-replay-events"
          className="btn btn-replay"
          style={{ width: "100%" }}
          onClick={handleReplayClick}
          disabled={!isConnected || isReplaying || isSubmitting}
        >
          <RotateCcw size={16} className={isReplaying ? "animate-spin" : ""} />
          {isReplaying ? "Streaming Replay from Kafka..." : "Replay Events from Kafka"}
        </button>

        {/* Live Replay Feedback Box */}
        {replayStatus?.status && (
          <div
            id="replay-status-box"
            style={{
              marginTop: "16px",
              padding: "12px 14px",
              borderRadius: "var(--radius-sm)",
              background: isCompleted
                ? "rgba(16, 185, 129, 0.1)"
                : isFailed
                ? "rgba(244, 63, 94, 0.1)"
                : "rgba(168, 85, 247, 0.1)",
              border: `1px solid ${
                isCompleted
                  ? "rgba(16, 185, 129, 0.3)"
                  : isFailed
                  ? "rgba(244, 63, 94, 0.3)"
                  : "rgba(168, 85, 247, 0.3)"
              }`,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "0.8rem", fontWeight: "600" }}>
              {isCompleted ? (
                <CheckCircle2 size={16} color="var(--accent-emerald)" />
              ) : isFailed ? (
                <AlertTriangle size={16} color="var(--accent-rose)" />
              ) : (
                <Clock size={16} color="var(--accent-purple)" className="animate-spin" />
              )}
              <span>Status: {replayStatus.status}</span>
              {replayStatus.count > 0 && (
                <span className="badge badge-replay" style={{ marginLeft: "auto", fontSize: "0.65rem" }}>
                  {replayStatus.count} replayed
                </span>
              )}
            </div>

            {replayStatus.message && (
              <div style={{ fontSize: "0.75rem", color: "var(--text-secondary)", marginTop: "4px" }}>
                {replayStatus.message}
              </div>
            )}
            {replayStatus.replayId && (
              <div className="mono-text" style={{ fontSize: "0.7rem", marginTop: "2px" }}>
                Session: {replayStatus.replayId}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
