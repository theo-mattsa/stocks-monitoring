package com.soe.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
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
import java.util.Collections;

public class ConsumerProducer {

    private static final String INPUT_TOPIC = "quotes-topic";
    private static final String OUTPUT_TOPIC = "market-events";
    private static final String GROUP_ID = "quote-processor-group";

    private static final double SIGNIFICANT_DAILY_CHANGE_THRESHOLD = 0.03;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps(GROUP_ID));
                KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProps())) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            System.out.println("ConsumerProducer iniciado. Aguardando cotações...");
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
            checkSignificantDailyChange(quote, producer, mapper);
            checkHighLowExtremes(quote, producer, mapper);

        } catch (Exception e) {
            System.err.println("Error processing quote in ConsumerProducer: " + e.getMessage());
        }
    }

    private static void sendEvent(KafkaProducer<String, String> producer, ObjectMapper mapper, String symbol,
            Object event, String eventType) {
        try {
            String eventJson = mapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, eventJson);
            record.headers().add("eventType", eventType.getBytes());
            producer.send(record);
            System.out.printf("[Event] %s -> %s%n", symbol, eventType);
        } catch (JsonProcessingException e) {
            System.err.println("Error serializing event: " + e.getMessage());
        }
    }

    private static void checkSignificantDailyChange(Quote quote, KafkaProducer<String, String> producer,
            ObjectMapper mapper) {
        Double changePercent = quote.getRegularMarketChangePercent();
        if (Math.abs(changePercent) >= (SIGNIFICANT_DAILY_CHANGE_THRESHOLD * 100)) {
            double prevClose = quote.getRegularMarketPreviousClose();
            SignificantPriceChangeEvent event = new SignificantPriceChangeEvent(
                    quote.getSymbol(),
                    prevClose,
                    quote.getRegularMarketPrice(),
                    changePercent / 100.0,
                    quote.getRegularMarketTime());
            sendEvent(producer, mapper, quote.getSymbol(), event, event.getType());
        }
    }

    private static void checkHighLowExtremes(Quote quote, KafkaProducer<String, String> producer, ObjectMapper mapper) {
        Double price = quote.getRegularMarketPrice();
        if (price >= quote.getRegularMarketDayHigh()) {
            ReachedDayHighEvent event = new ReachedDayHighEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, quote.getSymbol(), event, event.getType());
        }
        if (price <= quote.getRegularMarketDayLow()) {
            ReachedDayLowEvent event = new ReachedDayLowEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, quote.getSymbol(), event, event.getType());
        }
        if (price >= quote.getFiftyTwoWeekHigh()) {
            Reached52WeekHighEvent event = new Reached52WeekHighEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, quote.getSymbol(), event, event.getType());
        }
        if (price <= quote.getFiftyTwoWeekLow()) {
            Reached52WeekLowEvent event = new Reached52WeekLowEvent(quote.getSymbol(), price);
            sendEvent(producer, mapper, quote.getSymbol(), event, event.getType());
        }
    }

}