package com.soe.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.soe.domain.Quote;
import java.util.Properties;

public class Producer {
    private static final String TOPIC = "market-quotes";
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public Producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void publish(Quote quote) {
        try {
            String quoteJson = objectMapper.writeValueAsString(quote);

            // Symbol is a key for partitioning
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, quote.getSymbol(), quoteJson);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error while publishing quote: " + exception.getMessage());
                } else {
                    System.out.printf("Published quote for %s to partition %d with offset %d%n", 
                            quote.getSymbol(), metadata.partition(), metadata.offset());
                }
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error while serializing Quote", e);
        }
    }

    public void close() {
        producer.close();
    }
}