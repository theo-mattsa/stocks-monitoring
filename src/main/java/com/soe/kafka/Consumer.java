package com.soe.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private final KafkaConsumer<String, String> consumer;
    private final String consumerId;

    public Consumer(String consumerId, String groupId, List<String> topicsToSubscribe) {
        this.consumerId = consumerId;
        Properties props = KafkaConfig.getConsumerProps(groupId);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(topicsToSubscribe);
    }

    public void startConsuming() {
        new Thread(() -> {
            System.out.println("Consumer " + consumerId + " started consuming from topics: " + consumer.subscription());
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(record -> {
                        System.out.printf("Consumer [%s] topic=%s | partition=%d | offset=%d | key=%s%n", consumerId, record.topic(), record.partition(), record.offset(), record.key());
                    });

                    // Commit only after processing the records
                    if (!records.isEmpty()) 
                        consumer.commitSync();

                }
            } catch (Exception e) {
                System.err.println("Error in consumer " + consumerId + ": " + e.getMessage());
            } finally {
                consumer.close();
            }
        }, consumerId + "-thread").start();
    }
}