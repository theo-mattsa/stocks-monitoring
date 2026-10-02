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

    @Override
    public String constructMessage() {
        return String.format("Day high reached for %s: current price is %.2f", symbol, currentPrice);
    }
}