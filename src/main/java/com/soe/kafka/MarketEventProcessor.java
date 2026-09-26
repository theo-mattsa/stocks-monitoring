package com.soe.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.domain.events.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;

public class MarketEventProcessor {

    private static final String INPUT_TOPIC = "stock-quotes";
    private static final String OUTPUT_TOPIC = "market-events";
    private static final String GROUP_ID = "market-event-processor-group";

    private static final double SIGNIFICANT_DAILY_CHANGE_THRESHOLD = 0.03;
    private static final double PRICE_SPIKE_THRESHOLD = 0.02; 
    private static final int PRICE_SPIKE_MIN_SNAPSHOTS = 15;
    private static final int WINDOW_SIZE = 30;

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;

    private final Map<String, List<Quote>> windows = new HashMap<>();

    public MarketEventProcessor() {
        Properties consProps = KafkaConfig.getConsumerProps(GROUP_ID);
        this.consumer = new KafkaConsumer<>(consProps);
        this.consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
        Properties prodProps = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<>(prodProps);
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void startProcessing() {
        new Thread(() -> {
            System.out.println("Starting processing of " + INPUT_TOPIC + "...");
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        processQuote(record.value());
                    }
                } catch (Exception e) {
                    System.err.println("Error during loop processing: " + e.getMessage());
                }
            }
        }, "processor-thread").start();
    }

    private void processQuote(String quoteJson) {
        try {
            Quote quote = mapper.readValue(quoteJson, Quote.class);
            String symbol = quote.getSymbol();

            // Stateless Event Processing
            checkSignificantDailyChange(quote);
            checkHighLowExtremes(quote);

            // Stateful Event Processing with Sliding Window
            windows.putIfAbsent(symbol, new ArrayList<>());
            List<Quote> window = windows.get(symbol);

            window.add(quote);
            window.sort(Comparator.comparing(Quote::getRegularMarketTime));

            if (window.size() > WINDOW_SIZE) {
                window.remove(0); 
            }

            checkWindowPriceSpike(quote, window);

        } catch (Exception e) {
            System.err.println("Error processing quote: " + e.getMessage());
        }
    }

    private void checkSignificantDailyChange(Quote quote) {
        Double changePercent = quote.getRegularMarketChangePercent();
        if (changePercent != null && Math.abs(changePercent) >= (SIGNIFICANT_DAILY_CHANGE_THRESHOLD * 100)) {
            Double prevClose = quote.getRegularMarketPreviousClose();
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    quote.getSymbol(),
                    prevClose,
                    quote.getRegularMarketPrice(),
                    changePercent / 100.0,
                    quote.getRegularMarketTime()
            );
            sendEvent(event);
        }
    }

    private void checkHighLowExtremes(Quote quote) {
        Double price = quote.getRegularMarketPrice();
        if (price == null) return;

        if (quote.getRegularMarketDayHigh() != null && price >= quote.getRegularMarketDayHigh()) {
            sendEvent(new ReachedDayHighEvent(quote.getSymbol(), price));
        }
        if (quote.getRegularMarketDayLow() != null && price <= quote.getRegularMarketDayLow()) {
            sendEvent(new ReachedDayLowEvent(quote.getSymbol(), price));
        }
        if (quote.getFiftyTwoWeekHigh() != null && price >= quote.getFiftyTwoWeekHigh()) {
            sendEvent(new Reached52WeekHighEvent(quote.getSymbol(), price));
        }
        if (quote.getFiftyTwoWeekLow() != null && price <= quote.getFiftyTwoWeekLow()) {
            sendEvent(new Reached52WeekLowEvent(quote.getSymbol(), price));
        }
    }

    private void checkWindowPriceSpike(Quote current, List<Quote> window) {
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
                    current.getRegularMarketTime()
            );
            sendEvent(event);
        }
    }

    private void sendEvent(MarketEvent event) {
        try {
            String eventJson = mapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, event.getSymbol(), eventJson);
            if (event.getType() != null) 
                record.headers().add("eventType", event.getType().getBytes());
            producer.send(record);
            System.out.printf("[EVENTO GERADO] Ticker: %s | Tipo: %s%n", event.getSymbol(), event.getType());
        } catch (JsonProcessingException e) {
            System.err.println("Erro ao serializar evento: " + e.getMessage());
        }
    }
}