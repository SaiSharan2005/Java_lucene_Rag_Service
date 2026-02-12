package com.production.lucene_service.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ChunkExportDTO {

    private String id;

    @JsonProperty("document_id")
    private String documentId;

    private String content;

    private Metadata metadata;

    @Data
    @Builder
    public static class Metadata {
        private String source;

        @JsonProperty("page_number")
        private int pageNumber;

        @JsonProperty("chunk_index")
        private int chunkIndex;

        @JsonProperty("token_count")
        private int tokenCount;

        @JsonProperty("created_at")
        private String createdAt;
    }

    public static ChunkExportDTO fromChunk(Chunk chunk, String sourceFileName) {
        return ChunkExportDTO.builder()
                .id(chunk.getChunkId())
                .documentId(chunk.getDocumentId())
                .content(chunk.getContent())
                .metadata(Metadata.builder()
                        .source(sourceFileName)
                        .pageNumber(chunk.getPageNumber())
                        .chunkIndex(chunk.getChunkIndex())
                        .tokenCount(chunk.getTokenCount())
                        .createdAt(chunk.getCreatedAt())
                        .build())
                .build();
    }
}
