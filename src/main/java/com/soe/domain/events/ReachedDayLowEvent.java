package com.soe.domain.events;

public record ReachedDayLowEvent(String symbol, Double currentPrice) implements MarketEvent {
    @Override
    public String getType() {
        return "DAY_LOW_REACHED";
    }

    @Override
    public String getSymbol() {
        return symbol;
    }

    @Override
    public String constructMessage() {
        return String.format("Day low reached for %s: current price is %.2f", symbol, currentPrice);
    }
}