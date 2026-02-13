package com.production.lucene_service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IngestionResponse {
    private String jobId;
    private IngestionStatus status;
    private String message;
    private Integer filesSubmitted;
    private List<String> skippedFiles;
}
