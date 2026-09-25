package com.soe.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SimpleConsumer {

    // Quotes topic (snapshots of stock prices)
    private static final String QUOTES_TOPIC = "quote-snapshots";

    // Derived events topic
    private static final String EVENTS_TOPIC = "market-events";
    private static final String GROUP_ID = "simple-consumer-group";

    private static final boolean CONSUME_QUOTES = true;
    private static final boolean CONSUME_EVENTS = true;
    
    private static final String TARGET_EVENT_TYPE = "SIGNIFICANT_PRICE_CHANGE"; 

    public static void main(String[] args) {
        
        List<String> topics = new ArrayList<>();
        if (CONSUME_QUOTES) 
            topics.add(QUOTES_TOPIC);
        if (CONSUME_EVENTS) 
            topics.add(EVENTS_TOPIC);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps(GROUP_ID))) {
            consumer.subscribe(topics);
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.topic().equals(EVENTS_TOPIC) && TARGET_EVENT_TYPE != null) {
                        var header = record.headers().lastHeader("eventType");
                        if (header == null)
                            System.out.println("Warning: Received event without 'eventType' header. Skipping.");
                        String eventType = new String(header.value());
                        if (!TARGET_EVENT_TYPE.equals(eventType)) {
                            continue;
                        }
                    }
                    System.out.printf("Received message: topic = %s, key = %s, value = %s, offset = %d%n",
                            record.topic(), record.key(), record.value(), record.offset());
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
        }
    }
}