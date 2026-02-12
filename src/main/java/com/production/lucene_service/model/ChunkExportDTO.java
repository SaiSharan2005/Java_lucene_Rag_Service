package com.production.lucene_service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.production.lucene_service.service.PdfIngestionService.IngestionResult;
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
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
        private String source;
        private String title;
        private String author;

        @JsonProperty("page_number")
        private int pageNumber;

        @JsonProperty("total_pages")
        private int totalPages;

        @JsonProperty("chunk_index")
        private int chunkIndex;

        @JsonProperty("chunk_position")
        private String chunkPosition;

        @JsonProperty("token_count")
        private int tokenCount;

        @JsonProperty("created_at")
        private String createdAt;
    }

    public static ChunkExportDTO fromChunk(Chunk chunk, IngestionResult result) {
        int totalChunks = result.chunks().size();
        int chunkIndex = chunk.getChunkIndex();

        String position;
        if (chunkIndex == 0) {
            position = "start";
        } else if (chunkIndex == totalChunks - 1) {
            position = "end";
        } else {
            position = "middle";
        }

        return ChunkExportDTO.builder()
                .id(chunk.getChunkId())
                .documentId(chunk.getDocumentId())
                .content(chunk.getContent())
                .metadata(Metadata.builder()
                        .source(result.fileName())
                        .title(result.title())
                        .author(result.author())
                        .pageNumber(chunk.getPageNumber())
                        .totalPages(result.totalPages())
                        .chunkIndex(chunkIndex)
                        .chunkPosition(position)
                        .tokenCount(chunk.getTokenCount())
                        .createdAt(chunk.getCreatedAt())
                        .build())
                .build();
    }
}
