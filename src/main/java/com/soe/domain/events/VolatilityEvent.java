package com.soe.domain.events;

public record VolatilityEvent(String symbol, double spreadPercent) implements MarketEvent {
    @Override
    public String getType() {
        return "VOLATILITY_ALERT";
    }
    @Override
    public String getSymbol() {
        return symbol;
    }
    @Override
    public String constructMessage() {
        return String.format("ALERTA DE VOLATILIDADE em %s: variação de preço de %.2f%%", symbol, spreadPercent * 100);
    }
}
