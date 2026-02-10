package com.production.lucene_service.controller;

import com.production.lucene_service.model.IngestionResponse;
import com.production.lucene_service.model.IngestionStatus;
import com.production.lucene_service.service.PdfIngestionService;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingest")
@Validated
@Slf4j
public class PdfIngestionController {

    private final PdfIngestionService pdfIngestionService;

    public PdfIngestionController(PdfIngestionService pdfIngestionService) {
        this.pdfIngestionService = pdfIngestionService;
    }

    @PostMapping(value = "/pdf", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<IngestionResponse> ingestPdf(
            @RequestPart("file") @NotNull MultipartFile file,
            @RequestPart(value = "documentId", required = false) String documentId) {

        log.info("Received PDF ingestion request - file: {}, size: {} bytes, documentId: {}",
                file.getOriginalFilename(),
                file.getSize(),
                documentId);

        // Validate file
        if (file.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("File is empty")
                            .build());
        }

        String contentType = file.getContentType();
        if (contentType == null || !contentType.equals("application/pdf")) {
            log.warn("Invalid content type: {}", contentType);
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("Invalid file type. Only PDF files are accepted.")
                            .build());
        }

        // Process the PDF
        IngestionResponse response = pdfIngestionService.ingestPdf(file, documentId);

        if (response.getStatus() == IngestionStatus.SUCCESS) {
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @DeleteMapping("/document/{documentId}")
    public ResponseEntity<Map<String, String>> deleteDocument(@PathVariable String documentId) {
        log.info("Received delete request for document: {}", documentId);

        try {
            pdfIngestionService.deleteDocument(documentId);
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Document deleted successfully",
                    "documentId", documentId
            ));
        } catch (IOException e) {
            log.error("Failed to delete document: {}", documentId, e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "error",
                    "message", "Failed to delete document: " + e.getMessage(),
                    "documentId", documentId
            ));
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        try {
            long documentCount = pdfIngestionService.getIndexedDocumentCount();
            return ResponseEntity.ok(Map.of(
                    "indexedChunks", documentCount,
                    "status", "healthy"
            ));
        } catch (IOException e) {
            log.error("Failed to get stats", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "error",
                    "message", e.getMessage()
            ));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}
