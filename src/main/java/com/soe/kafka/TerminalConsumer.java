package com.soe.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TerminalConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final String consumerId;

    public TerminalConsumer(String consumerId, String groupId, List<String> topicsToSubscribe) {
        this.consumerId = consumerId;
        Properties props = KafkaConfig.getConsumerProps(groupId);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(topicsToSubscribe);
    }

    public void startConsuming() {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.printf("[%s | Topic: %s | Partition: %d | Key: %s] %s%n",
                            consumerId,
                            record.topic(),
                            record.partition(),
                            record.key(),
                            record.value());
                });
            }
        }, consumerId + "-thread").start();
    }
}