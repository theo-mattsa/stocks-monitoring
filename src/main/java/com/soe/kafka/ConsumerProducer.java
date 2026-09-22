package com.soe.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.domain.events.SignificantPriceChangeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ConsumerProducer {

    private static final String INPUT_TOPIC = "quotes-topic";
    private static final String OUTPUT_TOPIC = "market-events";
    private static final String GROUP_ID = "quote-processor-group";

    private static final int WINDOW_SIZE = 20;
    private static final double PRICE_CHANGE_THRESHOLD = 0.02;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Map<String, Deque<Quote>> windows = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps(GROUP_ID));
                KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProps())) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    processQuote(record.value(), mapper, windows, producer);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in ConsumerProducer: " + e.getMessage());
        }
    }

    private static void processQuote(String quoteJson, ObjectMapper mapper,
            Map<String, Deque<Quote>> windows,
            KafkaProducer<String, String> producer) {
        try {
            Quote current = mapper.readValue(quoteJson, Quote.class);
            String symbol = current.getSymbol();

            windows.putIfAbsent(symbol, new LinkedList<>());
            Deque<Quote> window = windows.get(symbol);

            if (!window.isEmpty()) {
                Quote previous = window.getLast();
                checkSignificantChange(previous, current, producer, mapper);
            }

            window.add(current);
            if (window.size() > WINDOW_SIZE) {
                window.removeFirst();
            }
        } catch (Exception e) {
            System.err.println("Erro ao processar cotação: " + e.getMessage());
        }
    }

    private static void checkSignificantChange(Quote previous, Quote current, KafkaProducer<String, String> producer, ObjectMapper mapper) {
        double prevPrice = previous.getRegularMarketPrice();
        double currPrice = current.getRegularMarketPrice();
        double change = Math.abs((currPrice - prevPrice) / prevPrice);

        if (change >= PRICE_CHANGE_THRESHOLD) {
            double changePercent = (currPrice - prevPrice) / prevPrice;
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    current.getSymbol(), prevPrice, currPrice, changePercent, current.getRegularMarketTime());
            try {
                String eventJson = mapper.writeValueAsString(event);
                ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, current.getSymbol(), eventJson);
                record.headers().add("eventType", event.getType().getBytes());
                producer.send(record);
            } catch (JsonProcessingException e) {
                System.err.println("Error while serializing event: " + e.getMessage());
            } catch (Exception e) {
                System.err.println("Error while sending event: " + e.getMessage());
            }
        }
    }
}