package com.soe.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.soe.domain.Quote;
import com.soe.domain.events.SignificantPriceChangeEvent;
import com.soe.kafka.EventProducer;

import java.time.Instant;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class QuoteProcessor {

    private static final String TOPIC = "market-events";
    private static final int WINDOW_SIZE = 20;
    private static final double PRICE_CHANGE_THRESHOLD = 0.02;

    private final ObjectMapper objectMapper;
    private final EventProducer producer;
    private final Map<String, Deque<Quote>> windows = new HashMap<>();

    public QuoteProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.producer = new EventProducer();
    }

    public void process(String message) {
        try {
            Quote quote = objectMapper.readValue(message, Quote.class);
            checkDerivedEvents(quote, windows.getOrDefault(quote.getSymbol(), new LinkedList<>()));
            updateWindow(quote);
        } catch (Exception e) {
            System.err.println("Invalid quote: " + message);
        }
    }

    private void checkDerivedEvents(Quote current, Deque<Quote> window) {
        if (window.size() < 2) {
            return;
        }
        Quote previous = window.getLast();
        if (hasSignificantPriceChange(previous, current)) {
            double previousPrice = previous.getRegularMarketPrice();
            double currentPrice = current.getRegularMarketPrice();
            double changePercent = (currentPrice - previousPrice) / previousPrice;
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    current.getSymbol(),
                    previousPrice,
                    currentPrice,
                    changePercent,
                    Instant.now() 
            );
            try {
                String eventJson = objectMapper.writeValueAsString(event);
                producer.send(TOPIC, current.getSymbol(), eventJson);
                System.out.println("Significant price change event published for " + current.getSymbol());
            } catch (Exception e) {
                System.err.println("Failed to serialize or send event for " + current.getSymbol() + ": " + e.getMessage());
            }
        }
    }

    private void updateWindow(Quote quote) {
        String symbol = quote.getSymbol();
        windows.putIfAbsent(symbol, new LinkedList<>());
        Deque<Quote> quotes = windows.get(symbol);
        quotes.add(quote);
        if (quotes.size() > WINDOW_SIZE) {
            quotes.removeFirst();
        }
    }

    private boolean hasSignificantPriceChange(Quote previous, Quote current) {
        double previousPrice = previous.getRegularMarketPrice();
        double currentPrice = current.getRegularMarketPrice();
        double change = Math.abs((currentPrice - previousPrice) / previousPrice);
        return change >= PRICE_CHANGE_THRESHOLD;
    }
}