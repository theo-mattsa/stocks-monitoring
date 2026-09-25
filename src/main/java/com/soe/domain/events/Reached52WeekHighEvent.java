package com.soe.domain.events;

public record Reached52WeekHighEvent(String symbol, Double currentPrice) implements MarketEvent {
    @Override
    public String getType() {
        return "52_WEEK_HIGH_REACHED";
    }
} 