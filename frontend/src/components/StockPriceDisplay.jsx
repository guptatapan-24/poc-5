import React from "react";
import { TrendingDown, TrendingUp, AlertCircle } from "lucide-react";

export function StockPriceDisplay({ prices, thresholds, lastUpdate }) {
  const symbols = ["AAPL", "MSFT", "GOOGL"];

  return (
    <div className="ticker-row" id="stock-ticker-row">
      {symbols.map((sym) => {
        const price = prices[sym] || 180.0;
        const matchingRules = thresholds.filter((r) => r.symbol === sym);
        const primaryRule = matchingRules[0];

        return (
          <div key={sym} className="ticker-card glass-panel" id={`ticker-card-${sym}`}>
            <div>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <span className="ticker-symbol">{sym}</span>
                <span className="ticker-meta">NASDAQ</span>
              </div>
              <div style={{ fontSize: "0.75rem", color: "var(--text-muted)", marginTop: "4px" }}>
                {lastUpdate ? `Updated: ${new Date(lastUpdate).toLocaleTimeString()}` : "Awaiting tick..."}
              </div>
            </div>

            <div>
              <div className="ticker-price">${price.toFixed(2)}</div>
              {primaryRule ? (
                <div className="ticker-threshold" title={`Alert when price ${primaryRule.condition}`}>
                  Threshold: {primaryRule.condition === "LESS_THAN_OR_EQUAL" ? "≤" : "≥"} $
                  {primaryRule.threshold.toFixed(2)}
                </div>
              ) : (
                <div style={{ fontSize: "0.75rem", color: "var(--text-muted)", textAlign: "right" }}>
                  No threshold
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
