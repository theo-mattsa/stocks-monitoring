package com.soe.provider;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import java.io.InputStream;

public class MockQuoteProvider implements QuoteProvider {

    private final ObjectMapper objectMapper;

    public MockQuoteProvider() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public List<Quote> fetchQuotes(String symbol) {
       try (InputStream is = getClass().getResourceAsStream("/mocks/magalu.json")) {
           if (is == null) {
               throw new RuntimeException("Mock data file not found for symbol: " + symbol);
           }
           return objectMapper.readValue(is, new TypeReference<List<Quote>>() {});
       } catch (Exception e) {
        throw new RuntimeException("Failed to fetch quotes for symbol: " + symbol, e);
       }
    }
    
}
