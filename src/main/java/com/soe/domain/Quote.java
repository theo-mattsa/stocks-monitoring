package com.soe.domain;

import java.time.Instant;
import com.soe.dto.QuoteDTO;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Quote {

    // Descriptive fields for the stock quote
    String symbol;
    String name;
    String logourl;
    String currency;

    // Timestamp of the last market update
    Instant regularMarketTime;

    // Number of shares traded today
    Long regularMarketVolume;

    // Price change information relative to the previous close
    Double regularMarketChange;
    Double regularMarketChangePercent;

    // Current market price
    Double regularMarketPrice;

    // Daily price minimum and maximum values
    Double regularMarketDayHigh;
    Double regularMarketDayLow;

    // Previous closing price
    Double regularMarketPreviousClose;

    // Opening price of the current trading day
    Double regularMarketOpen;

    // Maximum and minimum prices over the last 52 weeks
    Double fiftyTwoWeekLow;
    Double fiftyTwoWeekHigh;

    public Quote() {}   

    public Quote(QuoteDTO quoteDTO) {
        this.symbol = quoteDTO.symbol();
        this.name = quoteDTO.longName();
        this.logourl = quoteDTO.logourl();
        this.currency = quoteDTO.currency();
        this.regularMarketTime = Instant.parse(quoteDTO.regularMarketTime());
        this.regularMarketVolume = quoteDTO.regularMarketVolume();
        this.regularMarketChange = quoteDTO.regularMarketChange();
        this.regularMarketChangePercent = quoteDTO.regularMarketChangePercent();
        this.regularMarketPrice = quoteDTO.regularMarketPrice();
        this.regularMarketDayHigh = quoteDTO.regularMarketDayHigh();
        this.regularMarketDayLow = quoteDTO.regularMarketDayLow();
        this.regularMarketPreviousClose = quoteDTO.regularMarketPreviousClose();
        this.regularMarketOpen = quoteDTO.regularMarketOpen();
        this.fiftyTwoWeekLow = quoteDTO.fiftyTwoWeekLow();
        this.fiftyTwoWeekHigh = quoteDTO.fiftyTwoWeekHigh();
    }

}
