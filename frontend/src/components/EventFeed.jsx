import React, { useState } from "react";
import { Radio, RotateCcw, Filter, Trash2, ArrowDownRight, ArrowUpRight, Clock, Hash } from "lucide-react";

export function EventFeed({ events, onClearHistory }) {
  const [deliveryFilter, setDeliveryFilter] = useState("ALL"); // ALL | LIVE | REPLAY
  const [symbolFilter, setSymbolFilter] = useState("ALL");

  const filteredEvents = events.filter((evt) => {
    if (deliveryFilter !== "ALL" && evt.delivery_mode !== deliveryFilter) {
      return false;
    }
    if (symbolFilter !== "ALL" && evt.symbol !== symbolFilter) {
      return false;
    }
    return true;
  });

  const liveCount = events.filter((e) => e.delivery_mode === "LIVE").length;
  const replayCount = events.filter((e) => e.delivery_mode === "REPLAY").length;

  return (
    <div className="glass-panel feed-container" id="event-feed-card">
      <div className="card-header">
        <h2 className="card-title">
          <Radio size={18} color="var(--accent-emerald)" />
          Streamed Event Feed & History
        </h2>

        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          <span className="badge badge-live" style={{ fontSize: "0.7rem" }}>
            {liveCount} Live
          </span>
          <span className="badge badge-replay" style={{ fontSize: "0.7rem" }}>
            {replayCount} Replayed
          </span>
          <button
            id="btn-clear-history"
            className="btn btn-secondary btn-sm"
            onClick={onClearHistory}
            title="Clears only frontend displayed history. Kafka records remain safely retained."
          >
            <Trash2 size={13} />
            Clear View
          </button>
        </div>
      </div>

      <div className="card-body" style={{ paddingTop: "12px" }}>
        {/* Filter bar */}
        <div className="feed-header-controls">
          <div className="filter-pills" id="delivery-filter-pills">
            <button
              className={`filter-pill ${deliveryFilter === "ALL" ? "active" : ""}`}
              onClick={() => setDeliveryFilter("ALL")}
            >
              All Events ({events.length})
            </button>
            <button
              className={`filter-pill ${deliveryFilter === "LIVE" ? "active" : ""}`}
              onClick={() => setDeliveryFilter("LIVE")}
            >
              Live Alerts ({liveCount})
            </button>
            <button
              className={`filter-pill ${deliveryFilter === "REPLAY" ? "active" : ""}`}
              onClick={() => setDeliveryFilter("REPLAY")}
            >
              Historical Replays ({replayCount})
            </button>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <Filter size={14} color="var(--text-muted)" />
            <select
              id="event-symbol-filter"
              className="form-select"
              style={{ padding: "6px 10px", fontSize: "0.8rem", width: "130px" }}
              value={symbolFilter}
              onChange={(e) => setSymbolFilter(e.target.value)}
            >
              <option value="ALL">All Symbols</option>
              <option value="AAPL">AAPL</option>
              <option value="MSFT">MSFT</option>
              <option value="GOOGL">GOOGL</option>
            </select>
          </div>
        </div>

        {/* Event Table */}
        <div className="event-table-container" style={{ marginTop: "16px" }}>
          {filteredEvents.length === 0 ? (
            <div className="empty-state" id="empty-events-state">
              <Radio size={40} className="empty-state-icon" />
              <div style={{ fontWeight: 600, fontSize: "0.95rem", color: "var(--text-secondary)" }}>
                No Captured Events in View
              </div>
              <p style={{ fontSize: "0.8rem", marginTop: "4px" }}>
                Start the price simulator to cross a configured threshold, or click "Replay Events" to stream
                historical records from Kafka.
              </p>
            </div>
          ) : (
            <table className="event-table" id="events-table">
              <thead>
                <tr>
                  <th>Delivery</th>
                  <th>Event ID</th>
                  <th>Symbol</th>
                  <th>Direction</th>
                  <th>Prev Price</th>
                  <th>Crossing Price</th>
                  <th>Threshold</th>
                  <th>Original Event Time</th>
                </tr>
              </thead>
              <tbody>
                {filteredEvents.map((evt, idx) => {
                  const isLive = evt.delivery_mode === "LIVE";
                  const isCep = evt.event_type === "PRICE_DOWNTREND_DETECTED" || (evt.pattern_id && evt.pattern_id.includes("DOWN"));
                  const isDownward = evt.condition === "LESS_THAN_OR_EQUAL" || isCep;

                  if (isCep) {
                    const priceList = evt.prices || [];
                    const firstPrice = priceList.length > 0 ? priceList[0] : 0;
                    const lastPrice = priceList.length > 0 ? priceList[priceList.length - 1] : 0;
                    const totalDrop = firstPrice - lastPrice;

                    return (
                      <tr
                        key={`${evt.event_id}-${idx}`}
                        className={isLive ? "event-row-live" : "event-row-replay"}
                        style={{ background: "rgba(168, 85, 247, 0.08)", borderLeft: "3px solid #a855f7" }}
                        id={`event-row-${evt.event_id}`}
                      >
                        <td>
                          <div style={{ display: "flex", flexDirection: "column", gap: "2px" }}>
                            <span className={`badge ${isLive ? "badge-live" : "badge-replay"}`}>
                              {isLive ? "LIVE" : "REPLAY"}
                            </span>
                            <span className="badge" style={{ background: "rgba(168, 85, 247, 0.2)", color: "#c084fc", fontSize: "0.62rem" }}>
                              FLINK CEP
                            </span>
                          </div>
                        </td>

                        <td className="mono-text" title={evt.event_id}>
                          {evt.event_id}
                        </td>

                        <td style={{ fontWeight: 700 }}>
                          <span>{evt.symbol}</span>
                        </td>

                        <td>
                          <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                            <ArrowDownRight size={15} color="#c084fc" />
                            <span style={{ fontSize: "0.75rem", color: "#c084fc", fontWeight: 600 }}>
                              3 Consecutive Drops (≤ 10s)
                            </span>
                          </div>
                        </td>

                        <td className="price-cell mono-text" style={{ color: "var(--text-secondary)" }}>
                          ${firstPrice.toFixed(2)}
                        </td>

                        <td className="price-cell price-change-down" title={priceList.map(p => `$${p.toFixed(2)}`).join(" → ")}>
                          <span style={{ fontSize: "0.8rem", color: "#c084fc", fontWeight: 700 }}>
                            ${lastPrice.toFixed(2)}
                          </span>
                          <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: "2px" }}>
                            {priceList.map(p => `$${p.toFixed(2)}`).join(" → ")}
                          </div>
                        </td>

                        <td className="price-cell mono-text" style={{ color: "#f43f5e" }}>
                          -${totalDrop.toFixed(2)}
                        </td>

                        <td className="mono-text" title={evt.event_timestamp || evt.last_observed_at}>
                          {new Date(evt.event_timestamp || evt.last_observed_at || Date.now()).toLocaleTimeString()}
                        </td>
                      </tr>
                    );
                  }

                  return (
                    <tr
                      key={`${evt.event_id}-${idx}`}
                      className={isLive ? "event-row-live" : "event-row-replay"}
                      id={`event-row-${evt.event_id}`}
                    >
                      <td>
                        <span className={`badge ${isLive ? "badge-live" : "badge-replay"}`}>
                          {isLive ? "LIVE" : "REPLAY"}
                        </span>
                      </td>

                      <td className="mono-text" title={evt.event_id}>
                        {evt.event_id}
                      </td>

                      <td style={{ fontWeight: 700 }}>{evt.symbol}</td>

                      <td>
                        <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                          {isDownward ? (
                            <ArrowDownRight size={15} color="#f43f5e" />
                          ) : (
                            <ArrowUpRight size={15} color="#10b981" />
                          )}
                          <span style={{ fontSize: "0.75rem", color: isDownward ? "#f43f5e" : "#10b981" }}>
                            {isDownward ? "Falls to ≤" : "Rises to ≥"}
                          </span>
                        </div>
                      </td>

                      <td className="price-cell mono-text" style={{ color: "var(--text-secondary)" }}>
                        ${evt.previous_price ? evt.previous_price.toFixed(2) : "—"}
                      </td>

                      <td
                        className={`price-cell ${isDownward ? "price-change-down" : "price-change-up"}`}
                      >
                        ${evt.price ? evt.price.toFixed(2) : "—"}
                      </td>

                      <td className="price-cell mono-text" style={{ color: "var(--accent-amber)" }}>
                        ${evt.threshold ? evt.threshold.toFixed(2) : "—"}
                      </td>

                      <td className="mono-text" title={evt.event_timestamp}>
                        {new Date(evt.event_timestamp || Date.now()).toLocaleTimeString()}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
