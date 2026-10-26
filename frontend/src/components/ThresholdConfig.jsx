import React, { useState } from "react";
import { Sliders, Plus, Trash2, BellRing } from "lucide-react";

export function ThresholdConfig({ thresholds, onAddThreshold, onDeleteThreshold }) {
  const [symbol, setSymbol] = useState("AAPL");
  const [thresholdPrice, setThresholdPrice] = useState("180.00");
  const [condition, setCondition] = useState("LESS_THAN_OR_EQUAL");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    const priceNum = parseFloat(thresholdPrice);
    if (isNaN(priceNum) || priceNum <= 0) {
      alert("Please enter a valid positive threshold price.");
      return;
    }

    setIsSubmitting(true);
    try {
      await onAddThreshold({
        symbol,
        threshold: priceNum,
        condition,
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="glass-panel" id="threshold-config-card">
      <div className="card-header">
        <h2 className="card-title">
          <Sliders size={18} color="var(--accent-indigo)" />
          Price Alert Thresholds
        </h2>
        <span className="badge badge-live" style={{ fontSize: "0.7rem" }}>
          {thresholds.length} Active
        </span>
      </div>

      <div className="card-body">
        <form onSubmit={handleSubmit} id="threshold-form">
          <div className="form-group">
            <label className="form-label" htmlFor="threshold-symbol">
              Stock Symbol
            </label>
            <select
              id="threshold-symbol"
              className="form-select"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
            >
              <option value="AAPL">AAPL (Apple Inc.)</option>
              <option value="MSFT">MSFT (Microsoft)</option>
              <option value="GOOGL">GOOGL (Alphabet)</option>
            </select>
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="threshold-price">
              Alert Price ($ USD)
            </label>
            <input
              id="threshold-price"
              type="number"
              step="any"
              min="0.01"
              className="form-input"
              value={thresholdPrice}
              onChange={(e) => setThresholdPrice(e.target.value)}
              placeholder="e.g. 180.00"
              required
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="threshold-condition">
              Crossing Direction
            </label>
            <select
              id="threshold-condition"
              className="form-select"
              value={condition}
              onChange={(e) => setCondition(e.target.value)}
            >
              <option value="LESS_THAN_OR_EQUAL">Falls to or below (≤) [Downward]</option>
              <option value="GREATER_THAN_OR_EQUAL">Rises to or above (≥) [Upward]</option>
            </select>
          </div>

          <button
            id="btn-save-threshold"
            type="submit"
            className="btn btn-primary"
            style={{ width: "100%", marginTop: "8px" }}
            disabled={isSubmitting}
          >
            <Plus size={16} />
            {isSubmitting ? "Saving..." : "Save Threshold"}
          </button>
        </form>

        {/* List of active configured thresholds */}
        <div style={{ marginTop: "20px" }}>
          <div style={{ fontSize: "0.8rem", fontWeight: "600", color: "var(--text-muted)", marginBottom: "8px" }}>
            Active Threshold Evaluators
          </div>
          <div className="rules-list" id="active-thresholds-list">
            {thresholds.length === 0 ? (
              <div style={{ fontSize: "0.8rem", color: "var(--text-muted)", fontStyle: "italic" }}>
                No active threshold rules configured.
              </div>
            ) : (
              thresholds.map((rule) => (
                <div key={rule.id} className="rule-item" id={`rule-${rule.id}`}>
                  <div className="rule-info">
                    <BellRing size={14} color="var(--accent-amber)" />
                    <div>
                      <span className="rule-symbol">{rule.symbol}</span>
                      <div className="rule-cond">
                        {rule.condition === "LESS_THAN_OR_EQUAL" ? "Falls to or below ≤" : "Rises to or above ≥"} $
                        {rule.threshold.toFixed(2)}
                      </div>
                    </div>
                  </div>
                  <button
                    className="btn btn-danger btn-icon"
                    onClick={() => onDeleteThreshold(rule.id)}
                    title="Remove threshold rule"
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
