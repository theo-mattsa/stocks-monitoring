package com.soe.kafka;

import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.kafka.serialization.JsonDeserializer;
import com.soe.ui.DashboardUI;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private final KafkaConsumer<String, Object> consumer;
    private final String consumerId;

   public Consumer(String consumerId, String groupId, List<String> topicsToSubscribe) {
        this.consumerId = consumerId;
        Properties props = KafkaConfig.getConsumerProps(groupId);
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>());
        this.consumer.subscribe(topicsToSubscribe);
    }

    public void startConsuming() {
        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, Object> record : records) {
                        Object value = record.value();
                        if (value instanceof Quote quote && DashboardUI.isGuiInitialized()) 
                            DashboardUI.getInstance().updateQuote(quote);
                        if (value instanceof MarketEvent marketEvent && DashboardUI.isGuiInitialized()) 
                            DashboardUI.getInstance().logEvent(marketEvent);
                        System.out.printf("Consumer [%s] topic=%s | partition=%d | offset=%d | key=%s%n", consumerId, record.topic(), record.partition(), record.offset(), record.key());
                    }
                    // Commit only after processing all records to ensure at-least-once semantics
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