package com.soe.domain.events;

import java.time.Instant;

public record SignificantPriceChangeEvent(
        String symbol,
        double previousPrice,
        double currentPrice,
        double changePercent,
        Instant timestamp) implements MarketEvent {
    @Override
    public String getType() {
        return "SIGNIFICANT_PRICE_CHANGE";
    }
}
