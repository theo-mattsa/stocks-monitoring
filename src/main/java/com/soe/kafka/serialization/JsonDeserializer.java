package com.soe.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.kafka.KafkaConfig;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> targetType;

    public JsonDeserializer() {
        this(null);
    }

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0)
            return null;
        try {
            // If a specific target type is provided, use it for deserialization
            if (targetType != null && !targetType.equals(Object.class)) {
                return objectMapper.readValue(data, targetType);
            }

            // Fallback to topic-based deserialization 
            if (KafkaConfig.QUOTES_TOPIC.equals(topic)) {
                return (T) objectMapper.readValue(data, Quote.class);
            } else if (KafkaConfig.MARKET_EVENTS_TOPIC.equals(topic)) {
                return (T) objectMapper.readValue(data, MarketEvent.class);
            }

            return (T) objectMapper.readValue(data, Object.class);
        } catch (Exception e) {
            System.err.println("WARNING: Error deserializing JSON [" + topic + "]: " + e.getMessage());
            return null;
        }
    }
}