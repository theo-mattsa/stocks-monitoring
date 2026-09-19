package com.soe.dto;

public record QuoteDTO(
        String symbol,
        String shortName,
        String longName,
        String currency,
        Double regularMarketPrice,
        Double regularMarketDayHigh,
        Double regularMarketDayLow,
        String regularMarketDayRange,
        Double regularMarketChange,
        Double regularMarketChangePercent,
        String regularMarketTime,
        Long marketCap,
        Long regularMarketVolume,
        Double regularMarketPreviousClose,
        Double regularMarketOpen,
        String fiftyTwoWeekRange,
        Double fiftyTwoWeekLow,
        Double fiftyTwoWeekHigh,
        Double priceEarnings,
        Double earningsPerShare,
        String logourl
) {}