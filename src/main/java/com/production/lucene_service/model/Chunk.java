package com.production.lucene_service.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Chunk {
    private String chunkId;
    private String documentId;
    private String content;
    private int pageNumber;
    private int chunkIndex;
    private int tokenCount;
    private String createdAt;
}
