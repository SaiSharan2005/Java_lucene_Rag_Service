package com.production.lucene_service.repository;

import com.production.lucene_service.model.ProcessedDocument;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ProcessedDocumentRepository extends JpaRepository<ProcessedDocument, Long> {

    Optional<ProcessedDocument> findByFileName(String fileName);
}
