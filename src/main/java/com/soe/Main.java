package com.soe;

import com.soe.kafka.KafkaConfig;
import com.soe.kafka.Producer;
import com.soe.kafka.Consumer;
import com.soe.kafka.ConsumerProducer;
import com.soe.provider.MockQuoteProvider;
import com.soe.ui.DashboardUI;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        DashboardUI.getInstance().start();

        List<String> activeSymbolsProducer1 = Arrays.asList("PETR4", "VALE3");
        List<String> activeSymbolsProducer2 = Arrays.asList("ITUB4", "BBDC4");
        List<String> topicsToSubscribe = Arrays.asList(KafkaConfig.QUOTES_TOPIC, KafkaConfig.MARKET_EVENTS_TOPIC);

        Producer producer1 = new Producer(new MockQuoteProvider(), "p1");
        Producer producer2 = new Producer(new MockQuoteProvider(), "p2");
        producer1.startPublishing(activeSymbolsProducer1);
        producer2.startPublishing(activeSymbolsProducer2);

        Consumer consumer = new Consumer("c1", KafkaConfig.DASHBOARD_CONSUMERS_GROUP_ID, topicsToSubscribe);
        ConsumerProducer processor1 = new ConsumerProducer("cprod1");
        ConsumerProducer processor2 = new ConsumerProducer("cprod2");

        consumer.startConsuming();
        processor1.startProcessing();
        processor2.startProcessing();

        Thread.currentThread().join();
    }
}