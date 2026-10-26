import React, { useState } from "react";
import { DollarSign, ShieldAlert, X, Check } from "lucide-react";

export function SimulatedTradeModal({ isOpen, onClose, symbol = "AAPL", currentPrice = 180.0 }) {
  const [action, setAction] = useState("BUY");
  const [quantity, setQuantity] = useState(10);
  const [submittedMessage, setSubmittedMessage] = useState(null);

  if (!isOpen) return null;

  const handleSimulate = (e) => {
    e.preventDefault();
    const total = (quantity * currentPrice).toFixed(2);
    setSubmittedMessage(
      `Simulated ${action} of ${quantity} shares of ${symbol} at $${currentPrice.toFixed(2)} (Total: $${total}). Logged in browser session only. No financial order was placed.`
    );
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <DollarSign size={20} color="var(--accent-emerald)" />
            <h2 style={{ fontSize: "1.1rem", fontWeight: 700 }}>Simulated Paper Order (Testing Only)</h2>
          </div>
          <button className="btn btn-secondary btn-icon" onClick={onClose}>
            <X size={16} />
          </button>
        </div>

        <div className="modal-body">
          <div
            style={{
              padding: "12px",
              background: "rgba(245, 158, 11, 0.1)",
              border: "1px solid rgba(245, 158, 11, 0.3)",
              borderRadius: "8px",
              display: "flex",
              gap: "10px",
              marginBottom: "16px",
            }}
          >
            <ShieldAlert size={20} color="var(--accent-amber)" style={{ flexShrink: 0 }} />
            <div style={{ fontSize: "0.78rem", color: "var(--accent-amber)" }}>
              <strong>Safety Notice:</strong> This action creates a local simulation decision log only. It does NOT connect
              to a broker or execute financial transactions. Replays NEVER trigger trading actions.
            </div>
          </div>

          {submittedMessage ? (
            <div
              style={{
                padding: "16px",
                background: "rgba(16, 185, 129, 0.15)",
                border: "1px solid rgba(16, 185, 129, 0.3)",
                borderRadius: "8px",
                textAlign: "center",
              }}
            >
              <Check size={28} color="var(--accent-emerald)" style={{ margin: "0 auto 8px" }} />
              <div style={{ fontWeight: 600, color: "#ffffff" }}>Paper Order Recorded</div>
              <p style={{ fontSize: "0.8rem", color: "var(--text-secondary)", marginTop: "4px" }}>
                {submittedMessage}
              </p>
              <button
                className="btn btn-secondary btn-sm"
                style={{ marginTop: "12px" }}
                onClick={() => setSubmittedMessage(null)}
              >
                Log Another Simulated Order
              </button>
            </div>
          ) : (
            <form onSubmit={handleSimulate}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "14px" }}>
                <div className="form-group" style={{ margin: 0 }}>
                  <label className="form-label">Order Type</label>
                  <select
                    className="form-select"
                    value={action}
                    onChange={(e) => setAction(e.target.value)}
                  >
                    <option value="BUY">SIMULATED BUY</option>
                    <option value="SELL">SIMULATED SELL</option>
                  </select>
                </div>

                <div className="form-group" style={{ margin: 0 }}>
                  <label className="form-label">Quantity (Shares)</label>
                  <input
                    type="number"
                    min="1"
                    max="10000"
                    className="form-input"
                    value={quantity}
                    onChange={(e) => setQuantity(parseInt(e.target.value) || 1)}
                  />
                </div>
              </div>

              <div style={{ padding: "12px", background: "rgba(15, 23, 42, 0.7)", borderRadius: "8px", marginBottom: "16px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.85rem" }}>
                  <span style={{ color: "var(--text-muted)" }}>Current Price:</span>
                  <span className="mono-text">${currentPrice.toFixed(2)}</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.85rem", marginTop: "4px" }}>
                  <span style={{ color: "var(--text-muted)" }}>Simulated Value:</span>
                  <span className="mono-text" style={{ fontWeight: 700, color: "var(--accent-emerald)" }}>
                    ${(quantity * currentPrice).toFixed(2)}
                  </span>
                </div>
              </div>

              <button type="submit" className="btn btn-primary" style={{ width: "100%" }}>
                Log Simulated {action} Decision
              </button>
            </form>
          )}
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
