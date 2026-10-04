package com.soe.kafka;

import com.soe.domain.Quote;
import com.soe.kafka.serialization.JsonSerializer;
import com.soe.provider.QuoteProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Producer {
    private final KafkaProducer<String, Quote> producer;
    private final QuoteProvider quoteProvider;
    private final String producerId;

    public Producer(QuoteProvider quoteProvider, String producerId) {
        Properties props = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>());
        this.quoteProvider = quoteProvider;
        this.producerId = producerId;
    }

    public void startPublishing(List<String> symbols) {
        new Thread(() -> {
            try {
                while (true) {
                    for (String symbol : symbols) {
                        Quote quote = quoteProvider.fetchQuote(symbol);
                        if (quote == null)
                            continue;
                        // System.out.printf("Producer [%s] fetching quote for symbol %s%n", producerId, symbol);
                        ProducerRecord<String, Quote> record = new ProducerRecord<>(KafkaConfig.QUOTES_TOPIC, symbol,quote);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("Producer [%s] failed to send quote for symbol %s: %s%n",
                                        producerId, symbol, exception.getMessage());
                            } else {
                                System.out.printf("Producer [%s] sent quote for symbol %s to topic %s partition %d offset %d%n",
                                        producerId, symbol, metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "producer-thread-" + producerId).start();
    }
}