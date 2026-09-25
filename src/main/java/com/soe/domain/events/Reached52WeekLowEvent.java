package com.soe.domain.events;

public record Reached52WeekLowEvent(String symbol, Double currentPrice) implements MarketEvent {
    @Override
    public String getType() {
        return "52_WEEK_LOW_REACHED";
    }

    @Override
    public String getSymbol() {
        return symbol;
    }
}