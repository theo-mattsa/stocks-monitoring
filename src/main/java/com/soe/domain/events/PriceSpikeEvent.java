package com.soe.domain.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record PriceSpikeEvent(
        String symbol,
        double currentPrice,
        double windowAveragePrice,
        double deviationPercent,
        Instant timestamp
) implements MarketEvent {
    @Override
    @JsonProperty("type")
    public String getType() {
        return "WINDOW_PRICE_SPIKE";
    }
    @Override
    public String getSymbol() {
        return symbol;
    }
}