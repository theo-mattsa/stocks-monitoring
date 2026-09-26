package com.soe.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.provider.QuoteProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class QuoteProducer {
    private final KafkaProducer<String, String> producer;
    private final QuoteProvider quoteProvider;
    private final ObjectMapper mapper;
    private static final String TOPIC = "stock-quotes";

    public QuoteProducer(QuoteProvider quoteProvider) {
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
                        List<Quote> quotes = quoteProvider.fetchQuotes(symbol); //[cite: 1]
                        for (Quote quote : quotes) {
                            String json = mapper.writeValueAsString(quote);
                            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, symbol, json);
                            producer.send(record);
                            System.out.println("Published quote for " + symbol + ": " + json);
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "producer-thread").start();
    }
}