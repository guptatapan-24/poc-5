package com.stockstream.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

public class PriceDowntrendEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_type")
    private String eventType = "PRICE_DOWNTREND_DETECTED";

    @JsonProperty("pattern_id")
    private String patternId = "DOWN_3_WITHIN_10S";

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("prices")
    private List<Double> prices;

    @JsonProperty("first_observed_at")
    private String firstObservedAt;

    @JsonProperty("last_observed_at")
    private String lastObservedAt;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("processing_mode")
    private String processingMode = "STREAM";

    public PriceDowntrendEvent() {}

    public PriceDowntrendEvent(String eventId, String symbol, List<Double> prices,
                              String firstObservedAt, String lastObservedAt, String eventTimestamp) {
        this.eventId = eventId;
        this.symbol = symbol;
        this.prices = prices;
        this.firstObservedAt = firstObservedAt;
        this.lastObservedAt = lastObservedAt;
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getPatternId() { return patternId; }
    public void setPatternId(String patternId) { this.patternId = patternId; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public List<Double> getPrices() { return prices; }
    public void setPrices(List<Double> prices) { this.prices = prices; }

    public String getFirstObservedAt() { return firstObservedAt; }
    public void setFirstObservedAt(String firstObservedAt) { this.firstObservedAt = firstObservedAt; }

    public String getLastObservedAt() { return lastObservedAt; }
    public void setLastObservedAt(String lastObservedAt) { this.lastObservedAt = lastObservedAt; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public String getProcessingMode() { return processingMode; }
    public void setProcessingMode(String processingMode) { this.processingMode = processingMode; }
}
