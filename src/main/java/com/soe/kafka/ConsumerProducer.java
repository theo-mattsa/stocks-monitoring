package com.soe.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.domain.events.PriceSpikeEvent;
import com.soe.domain.events.Reached52WeekHighEvent;
import com.soe.domain.events.Reached52WeekLowEvent;
import com.soe.domain.events.ReachedDayHighEvent;
import com.soe.domain.events.ReachedDayLowEvent;
import com.soe.domain.events.SignificantPriceChangeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerProducer {

    private static final String INPUT_TOPIC = "quotes-topic";
    private static final String OUTPUT_TOPIC = "market-events";
    private static final String GROUP_ID = "quote-processor-group";

    private static final double SIGNIFICANT_DAILY_CHANGE_THRESHOLD = 0.03;

    private static final double PRICE_SPIKE_THRESHOLD = 0.02; // 2% deviation from the average
    private static final double PRICE_SPIKE_MIN_SNAPSHOTS = 15; 

    private static final int WINDOW_SIZE = 30;
    private static final Map<String, List<Quote>> windows = new HashMap<>();

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps(GROUP_ID));
             KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProps())) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            System.out.println("Started ConsumerProducer. Awaiting quotes...");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    processQuote(record.value(), mapper, producer);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in ConsumerProducer: " + e.getMessage());
        }
    }

    private static void processQuote(String quoteJson, ObjectMapper mapper, KafkaProducer<String, String> producer) {
        try {
            Quote quote = mapper.readValue(quoteJson, Quote.class);
            String symbol = quote.getSymbol();

            // Stateless Events
            checkSignificantDailyChange(quote, producer, mapper);
            checkHighLowExtremes(quote, producer, mapper);
            
            // Add the quote snapshot to the sliding window for the symbol
            if (!windows.containsKey(symbol)) {
                windows.put(symbol, new ArrayList<>());
            }
            List<Quote> window = windows.get(symbol);

            // Add the new quote to the window and sort by timestamp
            window.add(quote);
            window.sort(Comparator.comparing(Quote::getRegularMarketTime));

            // Remove the oldest quote if the window exceeds the defined size
            if (window.size() > WINDOW_SIZE) {
                window.remove(0); 
            }

            // Statefull events
            checkWindowPriceSpike(quote, producer, window, mapper);

        } catch (Exception e) {
            System.err.println("Error processing quote: " + e.getMessage());
        }
    }

    private static void checkWindowPriceSpike(Quote current, KafkaProducer<String, String> producer, List<Quote> window, ObjectMapper mapper) {
        if (window.size() < PRICE_SPIKE_MIN_SNAPSHOTS) {
            return; 
        }

        // Calculate the average price in the window and compare it to the current price
        double sum = 0;
        for (Quote q : window) 
            sum += q.getRegularMarketPrice();
        double avg = sum / window.size();
        double currentPrice = current.getRegularMarketPrice();

        // Check if the current price deviates significantly from the average price in the window
        if (avg > 0 && Math.abs((currentPrice - avg) / avg) >= PRICE_SPIKE_THRESHOLD) {
            PriceSpikeEvent event = new PriceSpikeEvent(
                current.getSymbol(),
                currentPrice,
                avg,
                (currentPrice - avg) / avg * 100,
                current.getRegularMarketTime()
            );
            sendEvent(producer, mapper, event);
        }
    }

    private static void checkSignificantDailyChange(Quote quote, KafkaProducer<String, String> producer, ObjectMapper mapper) {
        Double changePercent = quote.getRegularMarketChangePercent();
        if (Math.abs(changePercent) >= (SIGNIFICANT_DAILY_CHANGE_THRESHOLD * 100)) {
            Double prevClose = quote.getRegularMarketPreviousClose();
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    quote.getSymbol(),
                    prevClose,
                    quote.getRegularMarketPrice(),
                    changePercent / 100.0,
                    quote.getRegularMarketTime());
            sendEvent(producer, mapper, event);
        }
    }

    private static void checkHighLowExtremes(Quote quote, KafkaProducer<String, String> producer, ObjectMapper mapper) {
        Double price = quote.getRegularMarketPrice();
        if (price >= quote.getRegularMarketDayHigh()) {
            ReachedDayHighEvent event = new ReachedDayHighEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, event);
        }
        if (price <= quote.getRegularMarketDayLow()) {
            ReachedDayLowEvent event = new ReachedDayLowEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, event);
        }
        if (price >= quote.getFiftyTwoWeekHigh()) {
            Reached52WeekHighEvent event = new Reached52WeekHighEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, event);
        }
        if (price <= quote.getFiftyTwoWeekLow()) {
            Reached52WeekLowEvent event = new Reached52WeekLowEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, event);
        }
    }

    private static void sendEvent(KafkaProducer<String, String> producer, ObjectMapper mapper, MarketEvent event) {
        try {
            String eventJson = mapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, eventJson);
            record.headers().add("eventType", event.getType().getBytes());
            producer.send(record);
            System.out.printf("[Event] %s -> %s%n", event.getSymbol(), event.getType());
        } catch (JsonProcessingException e) {
            System.err.println("Error serializing event: " + e.getMessage());
        }
    }
}