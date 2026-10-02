package com.soe.domain.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record SignificantPriceChangeEvent(
        String symbol,
        double previousPrice,
        double currentPrice,
        double changePercent,
        Instant timestamp) implements MarketEvent {
    @Override
    @JsonProperty("type") 
    public String getType() {
        return "SIGNIFICANT_PRICE_CHANGE";
    }

    @Override
    public String getSymbol() {
        return symbol;
    }

    @Override
    public String constructMessage() {
        return String.format("Significant price change detected for %s: previous price was %.2f, current price is %.2f, change percent is %.2f%%", symbol, previousPrice, currentPrice, changePercent);
    }
}
