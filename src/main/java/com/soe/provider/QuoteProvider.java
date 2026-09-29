package com.soe.provider;
import com.soe.domain.Quote;
public interface QuoteProvider {
    Quote fetchQuote(String symbol);
}
