"use client";

import React, { useState, useEffect, useCallback } from "react";
import { Header } from "./components/Header";
import { StockPriceDisplay } from "./components/StockPriceDisplay";
import { ThresholdConfig } from "./components/ThresholdConfig";
import { SimulatorControls } from "./components/SimulatorControls";
import { ReplayControls } from "./components/ReplayControls";
import { EventFeed } from "./components/EventFeed";
import { ArchitectureModal } from "./components/ArchitectureModal";
import { SimulatedTradeModal } from "./components/SimulatedTradeModal";
import { useWebSocket } from "./hooks/useWebSocket";
import {
  fetchHealth,
  fetchThresholds,
  createThreshold,
  deleteThreshold,
  fetchSimulatorStatus,
  startSimulator,
  stopSimulator,
  requestReplay,
} from "./services/api";
import { DollarSign } from "lucide-react";

export default function App() {
  const {
    status: wsStatus,
    events,
    replayStatus,
    clientId,
    telemetry,
    clearEvents,
    reconnect,
  } = useWebSocket();


  const [health, setHealth] = useState(null);
  const [thresholds, setThresholds] = useState([]);
  const [simulatorStatus, setSimulatorStatus] = useState(null);
  const [prices, setPrices] = useState({
    AAPL: 185.00,
    MSFT: 420.00,
    GOOGL: 175.00,
  });
  const [lastUpdate, setLastUpdate] = useState(null);

  const [isArchOpen, setIsArchOpen] = useState(false);
  const [isTradeOpen, setIsTradeOpen] = useState(false);

  // Load initial backend state
  const refreshState = useCallback(async () => {
    try {
      const [h, t, s] = await Promise.allSettled([
        fetchHealth(),
        fetchThresholds(),
        fetchSimulatorStatus(),
      ]);

      if (h.status === "fulfilled") setHealth(h.value);
      if (t.status === "fulfilled" && t.value.thresholds) setThresholds(t.value.thresholds);
      if (s.status === "fulfilled") {
        setSimulatorStatus(s.value);
        if (s.value.latest_prices) {
          setPrices((prev) => ({ ...prev, ...s.value.latest_prices }));
        }
      }
    } catch (e) {
      console.warn("Error refreshing state from backend:", e);
    }
  }, []);

  useEffect(() => {
    refreshState();
    const interval = setInterval(refreshState, 1000);
    return () => clearInterval(interval);
  }, [refreshState]);

  // Update prices when events arrive
  useEffect(() => {
    if (events.length > 0) {
      const latest = events[0];
      if (latest.symbol && latest.price) {
        setPrices((prev) => ({
          ...prev,
          [latest.symbol]: latest.price,
        }));
        setLastUpdate(latest.received_at || new Date().toISOString());
      }
    }
  }, [events]);

  const handleAddThreshold = async (ruleData) => {
    await createThreshold(ruleData);
    await refreshState();
  };

  const handleDeleteThreshold = async (ruleId) => {
    await deleteThreshold(ruleId);
    await refreshState();
  };

  const handleStartSimulator = async (params) => {
    await startSimulator(params);
    await refreshState();
  };

  const handleStopSimulator = async () => {
    await stopSimulator();
    await refreshState();
  };

  const handleReplay = async ({ limit, symbol }) => {
    await requestReplay({
      limit,
      symbol,
      client_id: clientId,
    });
  };

  return (
    <div className="app-container" id="app-root">
      <Header
        connectionStatus={wsStatus}
        clientId={clientId}
        health={health}
        onReconnect={reconnect}
        onOpenArchitecture={() => setIsArchOpen(true)}
      />

      <StockPriceDisplay
        prices={prices}
        thresholds={thresholds}
        lastUpdate={lastUpdate}
      />

      <div className="dashboard-grid">
        {/* Left Column: Controls and Configurations */}
        <div className="controls-column">
          <ThresholdConfig
            thresholds={thresholds}
            onAddThreshold={handleAddThreshold}
            onDeleteThreshold={handleDeleteThreshold}
          />

          <SimulatorControls
            status={simulatorStatus}
            onStart={handleStartSimulator}
            onStop={handleStopSimulator}
          />

          <ReplayControls
            onReplay={handleReplay}
            replayStatus={replayStatus}
            isConnected={wsStatus === "CONNECTED"}
          />

          {/* Optional Paper Trade button (meets section 14.H) */}
          <div className="glass-panel" style={{ padding: "14px 16px" }}>
            <button
              id="btn-open-paper-trade"
              className="btn btn-secondary"
              style={{ width: "100%", fontSize: "0.8rem" }}
              onClick={() => setIsTradeOpen(true)}
            >
              <DollarSign size={14} color="var(--accent-emerald)" />
              Log Simulated Paper Trade (Demo Action)
            </button>
          </div>
        </div>

        {/* Right Column: Live Event Stream and Replay History */}
        <div>
          <EventFeed events={events} onClearHistory={clearEvents} />
        </div>
      </div>

      {/* Modals */}
      <ArchitectureModal isOpen={isArchOpen} onClose={() => setIsArchOpen(false)} />
      <SimulatedTradeModal
        isOpen={isTradeOpen}
        onClose={() => setIsTradeOpen(false)}
        symbol="AAPL"
        currentPrice={prices.AAPL || 180.0}
      />
    </div>
  );
}
