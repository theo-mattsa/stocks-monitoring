package com.soe.domain.events;

public record ReachedDayLowEvent(String symbol, Double currentPrice) implements MarketEvent {
    @Override
    public String getType() {
        return "DAY_LOW_REACHED";
    }
}