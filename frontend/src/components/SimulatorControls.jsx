import React, { useState } from "react";
import { Play, Square, Settings, Zap } from "lucide-react";

export function SimulatorControls({ status, onStart, onStop }) {
  const [mode, setMode] = useState(status?.mode || "random");
  const [interval, setInterval] = useState(status?.interval || 1.0);
  const [selectedStocks, setSelectedStocks] = useState("ALL");
  const isRunning = status?.running || false;

  const handleStart = () => {
    const symbols =
      selectedStocks === "ALL"
        ? ["AAPL", "MSFT", "GOOGL"]
        : [selectedStocks];
    onStart({ mode, interval: parseFloat(interval), symbols });
  };

  return (
    <div className="glass-panel" id="simulator-controls-card">
      <div className="card-header">
        <h2 className="card-title">
          <Zap size={18} color="var(--accent-cyan)" />
          Price Simulator
        </h2>
        <span className={`badge ${isRunning ? "badge-live" : "badge-disconnected"}`}>
          <span className={`status-dot ${isRunning ? "status-dot-green" : "status-dot-red"}`} />
          {isRunning ? "Simulating" : "Idle"}
        </span>
      </div>

      <div className="card-body">
        <div className="form-group">
          <label className="form-label" htmlFor="simulator-stocks">
            Stocks to Simulate
          </label>
          <select
            id="simulator-stocks"
            className="form-select"
            value={selectedStocks}
            onChange={(e) => setSelectedStocks(e.target.value)}
            disabled={isRunning}
          >
            <option value="ALL">All Stocks (AAPL, MSFT, GOOGL)</option>
            <option value="AAPL">AAPL only</option>
            <option value="MSFT">MSFT only</option>
            <option value="GOOGL">GOOGL only</option>
          </select>
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="simulator-mode">
            Simulation Sequence Mode
          </label>
          <select
            id="simulator-mode"
            className="form-select"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
            disabled={isRunning}
          >
            <option value="random">Dynamic Random Walk (Simulated Market)</option>
            <option value="deterministic">
              Deterministic Demo ($185.00 → $180.40 → $179.80 Crossing)
            </option>
            <option value="cep_demo">
              Flink CEP Downtrend ($185.00 → $183.00 → $181.00 → $179.00 Match)
            </option>
          </select>
          <span style={{ fontSize: "0.7rem", color: "var(--text-muted)", display: "block", marginTop: "4px" }}>
            {mode === "cep_demo"
              ? "Replays canonical 4-step falling sequence ($185 → $183 → $181 → $179) within 10s to trigger Flink CEP pattern and threshold crossing."
              : mode === "deterministic"
              ? "Replays exact 8-step sequence specified in project requirements to reliably trigger AAPL threshold crossing."
              : "Generates realistic continuous stochastic brownian price updates for all selected stocks."}
          </span>
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="simulator-interval">
            Tick Interval ({interval}s)
          </label>
          <select
            id="simulator-interval"
            className="form-select"
            value={interval}
            onChange={(e) => setInterval(parseFloat(e.target.value))}
            disabled={isRunning}
          >
            <option value={0.5}>0.5 seconds (Fast)</option>
            <option value={1.0}>1.0 second (Standard)</option>
            <option value={2.0}>2.0 seconds (Relaxed)</option>
          </select>
        </div>

        <div style={{ display: "flex", gap: "10px", marginTop: "16px" }}>
          {!isRunning ? (
            <button
              id="btn-start-simulator"
              className="btn btn-primary"
              style={{ flex: 1 }}
              onClick={handleStart}
            >
              <Play size={15} />
              Start Simulator
            </button>
          ) : (
            <button
              id="btn-stop-simulator"
              className="btn btn-danger"
              style={{ flex: 1 }}
              onClick={onStop}
            >
              <Square size={15} />
              Stop Simulator
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
