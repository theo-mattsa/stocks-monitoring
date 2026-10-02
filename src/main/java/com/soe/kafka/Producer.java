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

    public Producer(QuoteProvider quoteProvider) {
        Properties props = KafkaConfig.getProducerProps();
        this.producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>());
        this.quoteProvider = quoteProvider;
    }

    public void startPublishing(List<String> symbols) {
        new Thread(() -> {
            try {
                while (true) {
                    for (String symbol : symbols) {
                        Quote quote = quoteProvider.fetchQuote(symbol);
                        if (quote == null) continue;
                        ProducerRecord<String, Quote> record = new ProducerRecord<>(KafkaConfig.QUOTES_TOPIC, symbol, quote);
                        producer.send(record);
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "producer-thread").start();
    }
}