package com.soe.provider;

import java.util.List;

import com.soe.client.BrapiClient;
import com.soe.dto.BrapiResponseDTO;
import com.soe.domain.Quote;
import java.util.stream.Collectors;

public class ApiQuoteProvider implements QuoteProvider {

    private final BrapiClient brapiClient;

    public ApiQuoteProvider() {
        this.brapiClient = new BrapiClient();
    }

    @Override
    public List<Quote> fetchQuotes(String symbol) {
        BrapiResponseDTO response = brapiClient.getQuote(symbol);
        return response.results().stream().map(Quote::new).collect(Collectors.toList());
    }
    
}
