package com.soe.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.soe.config.BrapiConfig;
import com.soe.dto.BrapiResponseDTO;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class BrapiClient {

    private final HttpClient httpClient;
    private final BrapiConfig config;
    private final ObjectMapper objectMapper;

    public BrapiClient() {
        this.httpClient = HttpClient.newHttpClient();
        this.config = new BrapiConfig();
        this.objectMapper = new ObjectMapper();
    }

    public BrapiResponseDTO getQuote(String ticker) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(
                        config.getBaseUrl()
                                + "/quote/"
                                + ticker.toUpperCase()))
                .header(
                        "Authorization",
                        "Bearer " + config.getApiKey())
                .header(
                        "Accept",
                        "application/json")
                .GET()
                .build();

        try {
            HttpResponse<String> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException(
                        "Error response from Brapi: "
                                + response.statusCode()
                                + " - "
                                + response.body());
            }

            return objectMapper.readValue(
                    response.body(),
                    BrapiResponseDTO.class);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to fetch quote for ticker: " + ticker,
                    e);
        }
    }
}
