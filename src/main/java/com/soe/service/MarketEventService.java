package com.soe.service;
import com.soe.config.MarketEventServiceConfig;
import com.soe.domain.Quote;
import com.soe.domain.events.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class MarketEventService {

    private final Map<String, Deque<Quote>> windows = new HashMap<>();
    private final Map<String, Instant> lastEventTimes = new HashMap<>();
    private final Map<String, Boolean> isAtDayHigh = new HashMap<>();
    private final Map<String, Boolean> isAtDayLow = new HashMap<>();

    public List<MarketEvent> analyze(Quote quote) {
        if (quote == null || quote.getSymbol() == null) 
            return Collections.emptyList();

        List<MarketEvent> detectedEvents = new ArrayList<>();

        // Check for significant daily change
        checkSignificantDailyChange(quote).ifPresent(detectedEvents::add);
        checkHighLowExtremes(quote, detectedEvents);

        // Update the sliding window for price spike detection
        String symbol = quote.getSymbol();
        Deque<Quote> window = windows.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        window.addLast(quote);
        if (window.size() > MarketEventServiceConfig.QUOTE_WINDOW_SIZE) {
            window.removeFirst();
        }

        checkWindowPriceSpike(quote, window).ifPresent(event -> {
            if (shouldSendWithCooldown(event)) {
                detectedEvents.add(event);
            }
        });

        return detectedEvents;
    }

    private Optional<SignificantPriceChangeEvent> checkSignificantDailyChange(Quote quote) {
        Double changePercent = quote.getRegularMarketChangePercent();
        if (changePercent != null && Math.abs(changePercent) >= MarketEventServiceConfig.SIGNIFICANT_DAILY_CHANGE_THRESHOLD) {
            return Optional.of(new SignificantPriceChangeEvent(
                    quote.getSymbol(),
                    quote.getRegularMarketPreviousClose(),
                    quote.getRegularMarketPrice(),
                    changePercent,
                    quote.getRegularMarketTime()
            ));
        }
        return Optional.empty();
    }

    private void checkHighLowExtremes(Quote quote, List<MarketEvent> events) {
        Double price = quote.getRegularMarketPrice();
        if (price == null) return;

        Double dayHigh = quote.getRegularMarketDayHigh();
        if (dayHigh != null) {
            boolean currentlyAtHigh = isAtDayHigh.getOrDefault(quote.getSymbol(), false);
            if (price >= dayHigh) {
                if (!currentlyAtHigh) {
                    events.add(new ReachedDayHighEvent(quote.getSymbol(), price));
                    isAtDayHigh.put(quote.getSymbol(), true);
                }
            } else {
                isAtDayHigh.put(quote.getSymbol(), false);
            }
        }

        Double dayLow = quote.getRegularMarketDayLow();
        if (dayLow != null) {
            boolean currentlyAtLow = isAtDayLow.getOrDefault(quote.getSymbol(), false);
            if (price <= dayLow) {
                if (!currentlyAtLow) {
                    events.add(new ReachedDayLowEvent(quote.getSymbol(), price));
                    isAtDayLow.put(quote.getSymbol(), true);
                }
            } else {
                isAtDayLow.put(quote.getSymbol(), false);
            }
        }
    }

    private Optional<PriceSpikeEvent> checkWindowPriceSpike(Quote current, Deque<Quote> window) {
        if (window.size() < MarketEventServiceConfig.PRICE_SPIKE_MIN_SNAPSHOTS) 
            return Optional.empty();
        

        double sum = window.stream()
                .map(Quote::getRegularMarketPrice)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .sum();

        double avg = sum / window.size();
        Double currentPrice = current.getRegularMarketPrice();

        if (currentPrice != null && avg > 0 && Math.abs((currentPrice - avg) / avg) >= MarketEventServiceConfig.PRICE_SPIKE_THRESHOLD) {
            return Optional.of(new PriceSpikeEvent(
                    current.getSymbol(),
                    currentPrice,
                    avg,
                    ((currentPrice - avg) / avg) * 100,
                    current.getRegularMarketTime()
            ));
        }
        return Optional.empty();
    }

    private boolean shouldSendWithCooldown(MarketEvent event) {
        String eventKey = event.getSymbol() + "-" + event.getType();
        Instant now = Instant.now();
        Instant lastTime = lastEventTimes.get(eventKey);
        if (lastTime == null || Duration.between(lastTime, now).getSeconds() >= MarketEventServiceConfig.EVENT_COOLDOWN_SECONDS) {
            lastEventTimes.put(eventKey, now);
            return true;
        }
        return false;
    }
}