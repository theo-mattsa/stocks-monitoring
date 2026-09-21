package com.soe.provider;

import java.util.List;

import com.soe.domain.Quote;

public interface QuoteProvider {
    List<Quote> fetchQuotes(String symbol);
}
