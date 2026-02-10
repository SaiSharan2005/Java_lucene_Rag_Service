package com.production.lucene_service.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IngestionResponse {
    private String documentId;
    private IngestionStatus status;
    private String message;
    private int totalPages;
    private int totalChunks;
    private int totalTokens;
    private long processingTimeMs;
}
