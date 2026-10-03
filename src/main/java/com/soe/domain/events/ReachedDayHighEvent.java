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
        return String.format("MÁXIMA DO DIA em %s: atingiu R$ %.2f", symbol, currentPrice);
    }
}