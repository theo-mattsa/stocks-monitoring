package com.soe.kafka;

import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.kafka.serialization.JsonDeserializer;
import com.soe.kafka.serialization.JsonSerializer;
import com.soe.service.MarketEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerProducer {

    private final KafkaConsumer<String, Quote> consumer;
    private final KafkaProducer<String, MarketEvent> producer;
    private final MarketEventService analyzer;
    private final String consumerProducerId;

    public ConsumerProducer(String consumerProducerId) {
        this.consumerProducerId = consumerProducerId;
        this.analyzer = new MarketEventService();
        Properties consProps = KafkaConfig.getConsumerProps(KafkaConfig.MARKET_EVENT_PROCESSOR_GROUP_ID);
        this.consumer = new KafkaConsumer<>(consProps, new StringDeserializer(), new JsonDeserializer<>(Quote.class));
        this.consumer.subscribe(Collections.singletonList(KafkaConfig.QUOTES_TOPIC));
        Properties prodProps = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<>(prodProps, new StringSerializer(), new JsonSerializer<>());
    }

    public void startProcessing() {
        new Thread(() -> {
            System.out.println("Starting processing of " + KafkaConfig.MARKET_EVENTS_TOPIC + "...");
            while (true) {
                try {
                    ConsumerRecords<String, Quote> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, Quote> record : records) {
                        processQuote(record.value());
                    }
                    if (!records.isEmpty()) 
                        consumer.commitSync();
                } catch (Exception e) {
                    System.err.println("Error during loop processing: " + e.getMessage());
                }
            }
        }, "processor-thread" + "-" + consumerProducerId).start();
    }

    private void processQuote(Quote quote) {
        List<MarketEvent> events = analyzer.analyze(quote);
        for (MarketEvent event : events) {
            sendEvent(event);
        }
    }

    private void sendEvent(MarketEvent event) {
        try {
            ProducerRecord<String, MarketEvent> record = new ProducerRecord<>(KafkaConfig.MARKET_EVENTS_TOPIC,
                    event.getSymbol(), event);
            if (event.getType() != null)
                record.headers().add("eventType", event.getType().getBytes());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending event: " + exception.getMessage());
                } else {
                    System.out.printf("Sent event for %s to topic %s, partition %d, offset %d%n",
                            event.getSymbol(), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            System.err.println("Error sending event: " + e.getMessage());
        }
    }
}