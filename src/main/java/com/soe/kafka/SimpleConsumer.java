package com.soe.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

public class SimpleConsumer {

    private static final String EVENTS_TOPIC = "quotes-topic"; 
    private static final String GROUP_ID = "simple-consumer-group";

    public static void main(String[] args) {
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProps(GROUP_ID))) {
            consumer.subscribe(Collections.singletonList(EVENTS_TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            System.out.println("Error consuming messages: " + e.getMessage());
        }
    }
}