package com.production.lucene_service.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SearchResponse {
    private String query;
    private int totalHits;
    private long searchTimeMs;
    private List<SearchResult> results;
}
