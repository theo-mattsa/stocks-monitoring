package com.soe;
import com.soe.domain.Quote;
import com.soe.kafka.Producer;
import com.soe.provider.MockQuoteProvider;
import com.soe.provider.QuoteProvider;

import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        QuoteProvider provider = new MockQuoteProvider();
        List<Quote> quotes = provider.fetchQuotes("MGLU3");

        Producer producer = new Producer();
        
        System.out.println("Starting publication of " + quotes.size() + " messages...");
        
        for (Quote quote : quotes) {
            producer.publish(quote);
            Thread.sleep(500); 
        }
        producer.close();
        System.out.println("All messages were published successfully!");
    }
}