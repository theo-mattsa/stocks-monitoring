package com.soe;

import com.soe.kafka.MarketEventProcessor;
import com.soe.kafka.QuoteProducer;
import com.soe.kafka.TerminalConsumer;
import com.soe.provider.MockQuoteProvider;

import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting the application...");

        List<String> activeSymbols = Arrays.asList("PETR4", "VALE3", "ITUB4", "BBDC4");
        List<String> topicsToSubscribe = Arrays.asList("stock-quotes", "market-trends");

  
        QuoteProducer producer = new QuoteProducer(new MockQuoteProvider());
        producer.startPublishing(activeSymbols);
     
        MarketEventProcessor processor = new MarketEventProcessor();
        processor.startProcessing();

        String sharedGroupId = "dashboard-group";
        TerminalConsumer consumerA = new TerminalConsumer("Consumidor-A", sharedGroupId, topicsToSubscribe);
        consumerA.startConsuming();
        TerminalConsumer consumerB = new TerminalConsumer("Consumidor-B", sharedGroupId, topicsToSubscribe);
        consumerB.startConsuming();

        Thread.currentThread().join();
    }
}