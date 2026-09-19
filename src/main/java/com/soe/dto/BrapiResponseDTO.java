package com.soe.dto;

import java.util.List;

public record BrapiResponseDTO(List<QuoteDTO> results, String requestedAt,
        Integer took) {
}
