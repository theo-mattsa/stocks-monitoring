package com.soe.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.provider.QuoteProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class Producer {
    private final KafkaProducer<String, String> producer;
    private final QuoteProvider quoteProvider;
    private final ObjectMapper mapper;

    public Producer(QuoteProvider quoteProvider) {
        Properties props = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<>(props);
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.quoteProvider = quoteProvider;
    }

    public void startPublishing(List<String> symbols) {
        new Thread(() -> {
            try {
                while (true) {
                    for (String symbol : symbols) {
                        Quote quote = quoteProvider.fetchQuote(symbol);
                        if (quote == null) {
                            System.out.println("No more quotes available for symbol or something went wrong: " + symbol);
                            continue;
                        }
                        String json = mapper.writeValueAsString(quote);
                        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.QUOTES_TOPIC, symbol, json);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Error publishing quote for " + symbol + ": " + exception.getMessage());
                            } else {
                                System.out.printf("Published quote for %s to topic %s, partition %d, offset %d%n",
                                        symbol, metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "producer-thread").start();
    }
}