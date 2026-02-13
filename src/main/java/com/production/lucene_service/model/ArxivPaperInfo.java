package com.production.lucene_service.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ArxivPaperInfo {
    private String paperId;
    private String title;
    private String authors;
    private String summary;
    private String publishedDate;
    private String pdfUrl;
}
