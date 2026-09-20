package com.soe.dto;

public record QuoteDTO(
        // Ticker symbol of the stock (e.g., "PETR4")
        String symbol,
        // Short company name
        String shortName,
        // Full legal company name
        String longName,
        // Currency used for prices (e.g., "BRL")
        String currency,
        // Current stock price
        Double regularMarketPrice,
        // Highest price of the day
        Double regularMarketDayHigh,
        // Lowest price of the day
        Double regularMarketDayLow,
        // Price range of the day (e.g., "35.10 - 36.80")
        String regularMarketDayRange,
        // Absolute price change today compared to previous close
        Double regularMarketChange,
        // Percentage price change today
        Double regularMarketChangePercent,
        // Timestamp of the last price update
        String regularMarketTime,
        // Total market value of the company
        Long marketCap,
        // Number of shares traded today
        Long regularMarketVolume,
        // Closing price of the previous trading day
        Double regularMarketPreviousClose,
        // Opening price of the current trading day
        Double regularMarketOpen,
        // Price range over the last 52 weeks
        String fiftyTwoWeekRange,
        // Lowest price in the last 52 weeks
        Double fiftyTwoWeekLow,
        // Highest price in the last 52 weeks
        Double fiftyTwoWeekHigh,
        // Price-to-Earnings ratio (P/E)
        Double priceEarnings,
        // Net income per share (EPS)
        Double earningsPerShare,
        // URL for the company logo image
        String logourl
) {}