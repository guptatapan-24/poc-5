package com.stockstream.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PriceUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("price")
    private double price;

    @JsonProperty("observed_at")
    private String observedAt;

    public PriceUpdate() {}

    public PriceUpdate(String messageId, String symbol, double price, String observedAt) {
        this.messageId = messageId;
        this.symbol = symbol;
        this.price = price;
        this.observedAt = observedAt;
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    public String getObservedAt() { return observedAt; }
    public void setObservedAt(String observedAt) { this.observedAt = observedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PriceUpdate that = (PriceUpdate) o;
        return Double.compare(that.price, price) == 0 &&
                Objects.equals(messageId, that.messageId) &&
                Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, symbol, price);
    }

    @Override
    public String toString() {
        return "PriceUpdate{" + symbol + " @ $" + price + " (" + observedAt + ")}";
    }
}
