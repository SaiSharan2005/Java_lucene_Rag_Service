package com.production.lucene_service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IngestionResponse {
    private IngestionStatus status;
    private String message;
    private int documentsProcessed;
    private int totalChunks;
    private int totalTokens;
    private int totalPages;
    private String exportFileName;
    private long processingTimeMs;
    private List<DocumentDetail> documents;

    @Data
    @Builder
    public static class DocumentDetail {
        private String documentId;
        private String fileName;
        private int chunks;
        private int pages;
        private int tokens;
    }
}
