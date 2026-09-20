package com.soe;

import com.soe.client.BrapiClient;
import com.soe.domain.Quote;
import com.soe.dto.BrapiResponseDTO;

public class Main {
    public static void main(String[] args) {
        BrapiClient client = new BrapiClient();
        BrapiResponseDTO response = client.getQuote("B3SA3");
        Quote quote = new Quote(response.results().get(0));
        System.out.println(quote);
    }
}