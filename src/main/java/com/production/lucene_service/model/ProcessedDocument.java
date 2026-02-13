package com.production.lucene_service.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_documents")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedDocument {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String fileName;

    private String documentId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DocumentStatus status;

    private Integer totalPages;
    private Integer totalChunks;
    private Integer totalTokens;

    @Column(length = 2000)
    private String title;

    @Column(length = 2000)
    private String author;

    private Long fileSizeBytes;

    @Column(length = 2000)
    private String errorMessage;

    private LocalDateTime processedAt;
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public enum DocumentStatus {
        PROCESSING,
        COMPLETED,
        FAILED
    }
}
