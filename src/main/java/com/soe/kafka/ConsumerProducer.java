package com.soe.kafka;

import com.soe.domain.Quote;
import com.soe.domain.events.*;
import com.soe.kafka.serialization.JsonDeserializer;
import com.soe.kafka.serialization.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerProducer {

    private static final double SIGNIFICANT_DAILY_CHANGE_THRESHOLD = 0.03;
    private static final double VOLATILITY_THRESHOLD = 0.015;
    private static final double PRICE_SPIKE_THRESHOLD = 0.02;
    private static final int PRICE_SPIKE_MIN_SNAPSHOTS = 5;
    private static final int WINDOW_SIZE = 30;

    private final KafkaConsumer<String, Quote> consumer;
    private final KafkaProducer<String, MarketEvent> producer;
    private final Map<String, Deque<Quote>> windows = new HashMap<>();

    // Cooldown map to prevent sending too many events for the same symbol
    private final Map<String, Instant> lastEventTimes = new HashMap<>();
    private static final long EVENT_COOLDOWN_SECONDS = 5;

    // Maps to track if a symbol is currently at its day high or low
    private final Map<String, Boolean> isAtDayHigh = new HashMap<>();
    private final Map<String, Boolean> isAtDayLow = new HashMap<>();

    public ConsumerProducer() {
        Properties consProps = KafkaConfig.getConsumerProps(KafkaConfig.MARKET_EVENT_PROCESSOR_GROUP_ID);
        this.consumer = new KafkaConsumer<String, Quote>(consProps, new StringDeserializer(),
                new JsonDeserializer<>(Quote.class));
        this.consumer.subscribe(Collections.singletonList(KafkaConfig.QUOTES_TOPIC));
        Properties prodProps = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<String, MarketEvent>(prodProps, new StringSerializer(),
                new JsonSerializer<MarketEvent>());
    }

    public void startProcessing() {
        new Thread(() -> {
            System.out.println("Starting processing of " + KafkaConfig.MARKET_EVENTS_TOPIC + "...");
            while (true) {
                try {
                    ConsumerRecords<String, Quote> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, Quote> record : records) {
                        processQuote(record.value());
                    }
                } catch (Exception e) {
                    System.err.println("Error during loop processing: " + e.getMessage());
                }
            }
        }, "processor-thread").start();
    }

    private void processQuote(Quote quote) {
        try {
            String symbol = quote.getSymbol();

            // Stateless Event Processing
            checkSignificantDailyChange(quote);
            checkHighLowExtremes(quote);

            // Stateful Event Processing with Sliding Window
            windows.putIfAbsent(symbol, new ArrayDeque<>());
            Deque<Quote> window = windows.get(symbol);
            window.addLast(quote);
            if (window.size() > WINDOW_SIZE)
                window.removeFirst();

            // Stateful Event Processing
            checkWindowPriceSpike(quote, window);
            checkWindowVolatility(symbol, window);
        } catch (Exception e) {
            System.err.println("Error processing quote: " + e.getMessage());
        }
    }

    private void checkWindowVolatility(String symbol, Deque<Quote> window) {
        if (window.size() < 5)
            return;
        double minPrice = Double.MAX_VALUE;
        double maxPrice = Double.MIN_VALUE;
        for (Quote q : window) {
            if (q.getRegularMarketPrice() != null) {
                minPrice = Math.min(minPrice, q.getRegularMarketPrice());
                maxPrice = Math.max(maxPrice, q.getRegularMarketPrice());
            }
        }
        if (minPrice > 0) {
            double spreadPercent = (maxPrice - minPrice) / minPrice;
            if (spreadPercent >= VOLATILITY_THRESHOLD) {
                VolatilityEvent event = new VolatilityEvent(symbol, spreadPercent);
                sendEvent(event);
            }
        }
    }

    private void checkSignificantDailyChange(Quote quote) {
        Double changePercent = quote.getRegularMarketChangePercent();
        if (changePercent != null && Math.abs(changePercent) >= SIGNIFICANT_DAILY_CHANGE_THRESHOLD) {
            Double prevClose = quote.getRegularMarketPreviousClose();
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    quote.getSymbol(),
                    prevClose,
                    quote.getRegularMarketPrice(),
                    changePercent,
                    quote.getRegularMarketTime());
            sendEvent(event);
        }
    }

    private void checkHighLowExtremes(Quote quote) {
        Double price = quote.getRegularMarketPrice();
        if (price == null)
            return;
        Double dayHigh = quote.getRegularMarketDayHigh();
        if (dayHigh != null) {
            boolean currentlyAtHigh = isAtDayHigh.getOrDefault(quote.getSymbol(), false);
            if (price >= dayHigh) {
                if (!currentlyAtHigh) {
                    sendEvent(new ReachedDayHighEvent(quote.getSymbol(), price));
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
                    sendEvent(new ReachedDayLowEvent(quote.getSymbol(), price));
                    isAtDayLow.put(quote.getSymbol(), true);
                }
            } else {
                isAtDayLow.put(quote.getSymbol(), false);
            }
        }
    }

    private void checkWindowPriceSpike(Quote current, Deque<Quote> window) {
        if (window.size() < PRICE_SPIKE_MIN_SNAPSHOTS) {
            return;
        }
        double sum = 0;
        for (Quote q : window) {
            if (q.getRegularMarketPrice() != null) {
                sum += q.getRegularMarketPrice();
            }
        }
        double avg = sum / window.size();
        Double currentPrice = current.getRegularMarketPrice();

        if (currentPrice != null && avg > 0 && Math.abs((currentPrice - avg) / avg) >= PRICE_SPIKE_THRESHOLD) {
            PriceSpikeEvent event = new PriceSpikeEvent(
                    current.getSymbol(),
                    currentPrice,
                    avg,
                    (currentPrice - avg) / avg * 100,
                    current.getRegularMarketTime());
            sendEventWithCooldown(event);
        }
    }

    private void sendEventWithCooldown(MarketEvent event) {
        String eventKey = event.getSymbol() + "-" + event.getType();
        Instant now = Instant.now();
        Instant lastTime = lastEventTimes.get(eventKey);
        if (lastTime == null || Duration.between(lastTime, now).getSeconds() >= EVENT_COOLDOWN_SECONDS) {
            lastEventTimes.put(eventKey, now);
            sendEvent(event);
        }
    }

    private void sendEvent(MarketEvent event) {
        try {
            ProducerRecord<String, MarketEvent> record = new ProducerRecord<>(KafkaConfig.MARKET_EVENTS_TOPIC,
                    event.getSymbol(), event);
            if (event.getType() != null)
                record.headers().add("eventType", event.getType().getBytes());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending event: " + exception.getMessage());
                } else {
                    System.out.printf("Sent event for %s to topic %s, partition %d, offset %d%n",
                            event.getSymbol(), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            System.err.println("Error sending event: " + e.getMessage());
        }
    }
}