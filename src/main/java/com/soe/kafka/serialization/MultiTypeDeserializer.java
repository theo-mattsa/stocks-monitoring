package com.soe.kafka.serialization;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.kafka.KafkaConfig;
import org.apache.kafka.common.serialization.Deserializer;

public class MultiTypeDeserializer implements Deserializer<Object> {
    private final ObjectMapper objectMapper;

    public MultiTypeDeserializer() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            if (KafkaConfig.QUOTES_TOPIC.equals(topic)) {
                return objectMapper.readValue(data, Quote.class);
            } else if (KafkaConfig.MARKET_EVENTS_TOPIC.equals(topic)) {
                return objectMapper.readValue(data, MarketEvent.class);
            }
            return null; 
        } catch (Exception e) {
            System.err.println("Erro ao deserializar JSON do tópico " + topic + ": " + e.getMessage());
            return null; 
        }
    }
}