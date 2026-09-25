package com.soe.domain.events;

public record ReachedDayHighEvent(String symbol, Double currentPrice) implements MarketEvent {
    @Override
    public String getType() {
        return "DAY_HIGH_REACHED";
    }

    @Override
    public String getSymbol() {
        return symbol;
    }
}