package com.soe;

import com.soe.client.BrapiClient;
import com.soe.dto.BrapiResponseDTO;
import com.soe.dto.QuoteDTO;

public class Main {
    public static void main(String[] args) {
        BrapiClient client = new BrapiClient();
        BrapiResponseDTO response = client.getQuote("B3SA3");
        QuoteDTO quote = response.results().get(0);
        System.out.println(quote.symbol());
        System.out.println(quote.regularMarketPrice());
    }
}