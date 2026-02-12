package com.production.lucene_service.controller;

import com.production.lucene_service.model.IngestionResponse;
import com.production.lucene_service.model.IngestionStatus;
import com.production.lucene_service.model.JobStatus;
import com.production.lucene_service.service.IngestionJobService;
import com.production.lucene_service.service.IngestionJobService.PendingFile;
import com.production.lucene_service.service.PdfIngestionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Validates uploaded files, saves them to temp storage, and submits
 * a background ingestion job. Returns jobId immediately (non-blocking).
 *
 * Does NOT perform chunking, indexing, or export — that is IngestionJobService's job.
 */
@RestController
@RequestMapping("/api/v1/ingest")
@Slf4j
public class PdfIngestionController {

    private final IngestionJobService ingestionJobService;
    private final PdfIngestionService pdfIngestionService;

    public PdfIngestionController(IngestionJobService ingestionJobService,
                                  PdfIngestionService pdfIngestionService) {
        this.ingestionJobService = ingestionJobService;
        this.pdfIngestionService = pdfIngestionService;
    }

    @PostMapping(value = "/pdf", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<IngestionResponse> ingestPdf(
            @RequestPart("file") MultipartFile[] files) {

        log.info("Received PDF ingestion request - {} file(s)", files.length);

        // Validate: at least one file
        if (files.length == 0) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("No files provided")
                            .build());
        }

        // Validate all files before saving any
        for (MultipartFile file : files) {
            if (file.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(IngestionResponse.builder()
                                .status(IngestionStatus.FAILED)
                                .message("File is empty: " + file.getOriginalFilename())
                                .build());
            }

            String contentType = file.getContentType();
            if (contentType == null || !contentType.equals("application/pdf")) {
                return ResponseEntity.badRequest()
                        .body(IngestionResponse.builder()
                                .status(IngestionStatus.FAILED)
                                .message("Invalid file type for '" + file.getOriginalFilename()
                                        + "'. Only PDF files are accepted.")
                                .build());
            }
        }

        // Save files to temp directory (MultipartFile is request-scoped — won't survive async)
        Path tempDir = null;
        List<PendingFile> pendingFiles = new ArrayList<>();

        try {
            tempDir = Files.createTempDirectory("ingest-");

            for (MultipartFile file : files) {
                Path tempPath = tempDir.resolve(UUID.randomUUID() + ".pdf");
                file.transferTo(tempPath.toFile());
                pendingFiles.add(new PendingFile(tempPath, file.getOriginalFilename()));
            }

        } catch (IOException e) {
            log.error("Failed to save uploaded files to temp directory", e);

            // Clean up any files already saved
            cleanupTempFiles(pendingFiles, tempDir);

            return ResponseEntity.internalServerError()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("Failed to prepare files for processing: " + e.getMessage())
                            .build());
        }

        // Submit background job — returns immediately
        String jobId = ingestionJobService.startJob(pendingFiles);

        log.info("Submitted ingestion job {} for {} files", jobId, files.length);

        return ResponseEntity.accepted()
                .body(IngestionResponse.builder()
                        .jobId(jobId)
                        .status(IngestionStatus.PROCESSING)
                        .message(files.length + " file(s) submitted for background processing")
                        .build());
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<?> getJobStatus(@PathVariable String jobId) {
        JobStatus status = ingestionJobService.getJobStatus(jobId);

        if (status == null) {
            return ResponseEntity.status(404).body(Map.of(
                    "error", "Job not found",
                    "jobId", jobId
            ));
        }

        return ResponseEntity.ok(status);
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

    private void cleanupTempFiles(List<PendingFile> files, Path tempDir) {
        for (PendingFile pf : files) {
            try { Files.deleteIfExists(pf.tempPath()); } catch (IOException ignored) {}
        }
        if (tempDir != null) {
            try { Files.deleteIfExists(tempDir); } catch (IOException ignored) {}
        }
    }
}
