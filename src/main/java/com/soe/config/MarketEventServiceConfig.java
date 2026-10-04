package com.soe.config;

public class MarketEventServiceConfig {
    public static final double SIGNIFICANT_DAILY_CHANGE_THRESHOLD = 0.03;
    public static final double PRICE_SPIKE_THRESHOLD = 0.02;
    public static final int PRICE_SPIKE_MIN_SNAPSHOTS = 5;
    public static final int QUOTE_WINDOW_SIZE = 30;
    public static final long EVENT_COOLDOWN_SECONDS = 5;
}
