package com.soe.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import com.soe.provider.MockQuoteProvider;
import com.soe.provider.QuoteProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public class EventProducer {

    private static final String TOPIC = "quote-snapshots";
    
    public static void main(String[] args) throws Exception {
        QuoteProvider provider = new MockQuoteProvider();
        List<Quote> quotes = provider.fetchQuotes("MGLU3");
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProps())) {
            for (Quote quote : quotes) {
                String json = mapper.writeValueAsString(quote);
                producer.send(new ProducerRecord<>(TOPIC, json));
                producer.flush();
                System.out.println("Sent: " + quote.getSymbol() + " - " + quote.getRegularMarketPrice());
                Thread.sleep(60000);
            }
        } catch (Exception e) {
            System.err.println("Error while producing messages: " + e.getMessage());
        }
    }
}