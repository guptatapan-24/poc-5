package com.stockstream.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class ThresholdCrossingEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_type")
    private String eventType = "THRESHOLD_CROSSED";

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("previous_price")
    private double previousPrice;

    @JsonProperty("price")
    private double price;

    @JsonProperty("threshold")
    private double threshold;

    @JsonProperty("condition")
    private String condition;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("processing_mode")
    private String processingMode = "STREAM";

    public ThresholdCrossingEvent() {}

    public ThresholdCrossingEvent(String eventId, String symbol, double previousPrice, double price,
                                  double threshold, String condition, String eventTimestamp) {
        this.eventId = eventId;
        this.symbol = symbol;
        this.previousPrice = previousPrice;
        this.price = price;
        this.threshold = threshold;
        this.condition = condition;
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public double getPreviousPrice() { return previousPrice; }
    public void setPreviousPrice(double previousPrice) { this.previousPrice = previousPrice; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    public double getThreshold() { return threshold; }
    public void setThreshold(double threshold) { this.threshold = threshold; }

    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public String getProcessingMode() { return processingMode; }
    public void setProcessingMode(String processingMode) { this.processingMode = processingMode; }
}
