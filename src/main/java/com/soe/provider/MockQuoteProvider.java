package com.soe.provider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.soe.domain.Quote;
import java.io.InputStream;

public class MockQuoteProvider implements QuoteProvider {

    private final ObjectMapper objectMapper;
    private final Map<String, List<Quote>> quotes = new HashMap<>();
    private final Map<String, Integer> indexes = new HashMap<>();

    public MockQuoteProvider() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public Quote fetchQuote(String symbol) {
        try {

            if (!quotes.containsKey(symbol)) 
                loadQuotes(symbol);
        
            List<Quote> symbolQuotes = quotes.get(symbol);
            int index = indexes.getOrDefault(symbol, 0);
            Quote quote = symbolQuotes.get(index);

            index++;

            // Consumed all quotes for this symbol
            if (index >= symbolQuotes.size()) 
                return null;
            
            indexes.put(symbol, index);
            return quote;
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to fetch quote for symbol: " + symbol, e
            );
        }
    }

    private void loadQuotes(String symbol) throws Exception {
        try (InputStream is = getClass()
                .getResourceAsStream("/mocks/" + symbol.toLowerCase() + ".json")) {
            if (is == null) 
                throw new RuntimeException(
                    "Mock data file not found for symbol: " + symbol
                );
            List<Quote> symbolQuotes = objectMapper.readValue(is, new TypeReference<List<Quote>>() {});
            quotes.put(symbol, symbolQuotes);
            indexes.put(symbol, 0);
        }
    }
}