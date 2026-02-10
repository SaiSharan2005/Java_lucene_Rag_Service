package com.production.lucene_service.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PageContent {
    private int pageNumber;
    private String rawText;
    private String cleanedText;
}
