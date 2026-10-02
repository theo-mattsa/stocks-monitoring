package com.soe;

import com.soe.kafka.KafkaConfig;
import com.soe.kafka.Producer;
import com.soe.kafka.Consumer;
import com.soe.provider.MockQuoteProvider;
import com.soe.ui.DashboardUI;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        DashboardUI.getInstance().start();

        List<String> activeSymbols = Arrays.asList("PETR4", "VALE3");
        List<String> topicsToSubscribe = Arrays.asList(KafkaConfig.QUOTES_TOPIC);

        Producer producer = new Producer(new MockQuoteProvider());
        producer.startPublishing(activeSymbols);

        String sharedGroupId = "dashboard-group";

        Consumer consumer1 = new Consumer("Consumer1", sharedGroupId, topicsToSubscribe);
        Consumer consumer2 = new Consumer("Consumer2", sharedGroupId, topicsToSubscribe);
        consumer1.startConsuming();
        consumer2.startConsuming();

        Thread.currentThread().join();
    }
}