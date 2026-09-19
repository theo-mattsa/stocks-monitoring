package com.soe.config;

import io.github.cdimascio.dotenv.Dotenv;

public class BrapiConfig {

    private static final String BASE_URL = "https://brapi.dev/api";
    private final String apiKey;

    public BrapiConfig() {
        Dotenv dotenv = Dotenv.load();
        this.apiKey = dotenv.get("BRAPI_TOKEN");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "BRAPI_TOKEN is not set in the environment variables. Please set it in the .env file.");
        }
    }

    public String getBaseUrl() {
        return BASE_URL;
    }

    public String getApiKey() {
        return apiKey;
    }
}
