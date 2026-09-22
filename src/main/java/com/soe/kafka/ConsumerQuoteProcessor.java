package com.soe.kafka;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;

public class ConsumerQuoteProcessor {
    private static final String TOPIC_NAME = "market-quotes";
    private static final int WINDOW_SIZE = 20;
    private static final int PRICE_CHANGE_THRESHOLD_PERCENT = 2;

    private final Map<String, LinkedList<Quote>> window;
    private final ObjectMapper objectMapper;

    public ConsumerQuoteProcessor() {
        this.window = new java.util.HashMap<>();
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "quote-persistence-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.Collections.singletonList(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            System.err.println("The consumer has stopped: " + e.getMessage());
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            Quote quote = objectMapper.readValue(record.value(), Quote.class);
            updateWindow(quote);
            publishDerivedEvents(quote, window.get(quote.getSymbol()));
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }

    private void publishDerivedEvents(Quote newQuote, Deque<Quote> window) {
        if (window.isEmpty()) {
            return;
        }
        Quote lastQuote = window.getLast();
        if (isSignificantPriceChange(lastQuote, newQuote)) {
            System.out.println("Significant price change detected");
        }
    }

    private boolean isSignificantPriceChange(Quote lastQuote, Quote newQuote) {
        double lastPrice = lastQuote.getRegularMarketPrice();
        double newPrice = newQuote.getRegularMarketPrice();
        double changePercent = Math.abs((newPrice - lastPrice) / lastPrice);
        return changePercent >= PRICE_CHANGE_THRESHOLD_PERCENT / 100.0;
    }

    private void updateWindow(Quote quote) {
        String symbol = quote.getSymbol();
        window.putIfAbsent(symbol, new LinkedList<>());
        LinkedList<Quote> quotes = window.get(symbol);
        quotes.add(quote);
        if (quotes.size() > WINDOW_SIZE) {
            quotes.removeFirst();
        }
    }
}
