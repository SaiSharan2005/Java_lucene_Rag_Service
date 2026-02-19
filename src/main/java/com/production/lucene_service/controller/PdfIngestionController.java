package com.production.lucene_service.controller;

import com.production.lucene_service.model.IngestionResponse;
import com.production.lucene_service.model.IngestionStatus;
import com.production.lucene_service.model.JobStatus;
import com.production.lucene_service.model.LocalDirectoryRequest;
import com.production.lucene_service.model.ProcessedDocument;
import com.production.lucene_service.model.ProcessedDocument.DocumentStatus;
import com.production.lucene_service.repository.ProcessedDocumentRepository;
import com.production.lucene_service.service.IngestionJobService;
import com.production.lucene_service.service.IngestionJobService.PendingFile;
import com.production.lucene_service.service.PdfIngestionService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Validates uploaded files, checks for duplicates, saves them to temp storage,
 * and submits a background ingestion job. Returns jobId immediately (non-blocking).
 *
 * Does NOT perform chunking, indexing, or export — that is IngestionJobService's job.
 */
@RestController
@RequestMapping("/api/v1/ingest")
@Slf4j
public class PdfIngestionController {

    private final IngestionJobService ingestionJobService;
    private final PdfIngestionService pdfIngestionService;
    private final ProcessedDocumentRepository documentRepository;

    public PdfIngestionController(IngestionJobService ingestionJobService,
                                  PdfIngestionService pdfIngestionService,
                                  ProcessedDocumentRepository documentRepository) {
        this.ingestionJobService = ingestionJobService;
        this.pdfIngestionService = pdfIngestionService;
        this.documentRepository = documentRepository;
    }

    @PostMapping(value = "/pdf", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<IngestionResponse> ingestPdf(
            @RequestPart("file") MultipartFile[] files) {

        log.info("Ingestion request: {} file(s)", files.length);

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

        // Dedup check: separate files into toProcess and skipped
        List<MultipartFile> toProcess = new ArrayList<>();
        List<String> skippedFiles = new ArrayList<>();

        for (MultipartFile file : files) {
            String fileName = file.getOriginalFilename();
            Optional<ProcessedDocument> existing = documentRepository.findByFileName(fileName);

            if (existing.isPresent() && existing.get().getStatus() == DocumentStatus.COMPLETED) {
                log.debug("Skipping already-ingested: {}", fileName);
                skippedFiles.add(fileName);
            } else {
                if (existing.isPresent() && existing.get().getStatus() == DocumentStatus.FAILED) {
                    log.debug("Re-processing failed file: {}", fileName);
                    documentRepository.delete(existing.get());
                }
                toProcess.add(file);
            }
        }

        // Case 3: All files already processed
        if (toProcess.isEmpty()) {
            log.info("All {} file(s) already ingested — skipped", files.length);
            return ResponseEntity.ok()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.SUCCESS)
                            .message("All " + files.length + " file(s) already ingested")
                            .filesSubmitted(0)
                            .skippedFiles(skippedFiles)
                            .build());
        }

        // Save files to temp directory (MultipartFile is request-scoped — won't survive async)
        Path tempDir = null;
        List<PendingFile> pendingFiles = new ArrayList<>();

        try {
            tempDir = Files.createTempDirectory("ingest-");

            for (MultipartFile file : toProcess) {
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

        // Build response message
        String message = toProcess.size() + " file(s) submitted for processing";
        if (!skippedFiles.isEmpty()) {
            message += ", " + skippedFiles.size() + " skipped (already ingested)";
        }

        log.info("Job {} submitted — {} to process, {} skipped",
                jobId, toProcess.size(), skippedFiles.size());

        return ResponseEntity.accepted()
                .body(IngestionResponse.builder()
                        .jobId(jobId)
                        .status(IngestionStatus.PROCESSING)
                        .message(message)
                        .filesSubmitted(toProcess.size())
                        .skippedFiles(skippedFiles.isEmpty() ? null : skippedFiles)
                        .build());
    }

    @PostMapping("/local")
    public ResponseEntity<IngestionResponse> ingestLocal(@Valid @RequestBody LocalDirectoryRequest request) {
        Path dirPath = Paths.get(request.getDirectory());

        // Validate directory
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("Directory does not exist: " + request.getDirectory())
                            .build());
        }

        if (!Files.isReadable(dirPath)) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("Directory is not readable: " + request.getDirectory())
                            .build());
        }

        // List all PDF files in the directory
        List<Path> pdfFiles;
        try (Stream<Path> stream = Files.list(dirPath)) {
            pdfFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".pdf"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            log.error("Failed to list directory: {}", request.getDirectory(), e);
            return ResponseEntity.internalServerError()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("Failed to read directory: " + e.getMessage())
                            .build());
        }

        if (pdfFiles.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("No PDF files found in: " + request.getDirectory())
                            .build());
        }

        log.info("Local ingestion request: {} PDF(s) in {}", pdfFiles.size(), request.getDirectory());

        // Dedup check
        List<PendingFile> toProcess = new ArrayList<>();
        List<String> skippedFiles = new ArrayList<>();

        for (Path pdfFile : pdfFiles) {
            String fileName = pdfFile.getFileName().toString();
            Optional<ProcessedDocument> existing = documentRepository.findByFileName(fileName);

            if (existing.isPresent() && existing.get().getStatus() == DocumentStatus.COMPLETED) {
                log.debug("Skipping already-ingested: {}", fileName);
                skippedFiles.add(fileName);
            } else {
                if (existing.isPresent() && existing.get().getStatus() == DocumentStatus.FAILED) {
                    log.debug("Re-processing failed file: {}", fileName);
                    documentRepository.delete(existing.get());
                }
                toProcess.add(new PendingFile(pdfFile, fileName, false));
            }
        }

        // All files already processed
        if (toProcess.isEmpty()) {
            log.info("All {} file(s) already ingested — skipped", pdfFiles.size());
            return ResponseEntity.ok()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.SUCCESS)
                            .message("All " + pdfFiles.size() + " file(s) already ingested")
                            .filesSubmitted(0)
                            .skippedFiles(skippedFiles)
                            .build());
        }

        // Submit background job
        String jobId = ingestionJobService.startLocalJob(toProcess);

        String message = toProcess.size() + " file(s) submitted for processing";
        if (!skippedFiles.isEmpty()) {
            message += ", " + skippedFiles.size() + " skipped (already ingested)";
        }

        log.info("Local job {} submitted — {} to process, {} skipped",
                jobId, toProcess.size(), skippedFiles.size());

        return ResponseEntity.accepted()
                .body(IngestionResponse.builder()
                        .jobId(jobId)
                        .status(IngestionStatus.PROCESSING)
                        .message(message)
                        .filesSubmitted(toProcess.size())
                        .skippedFiles(skippedFiles.isEmpty() ? null : skippedFiles)
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
        log.debug("Delete request for document: {}", documentId);

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
            long chunkCount = pdfIngestionService.getIndexedChunkCount();
            long pdfCount = pdfIngestionService.getIndexedPdfCount();
            return ResponseEntity.ok(Map.of(
                    "indexedChunks", chunkCount,
                    "indexedPdfs", pdfCount,
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

    @GetMapping("/documents")
    public ResponseEntity<Map<String, Object>> getDocuments(
            @RequestParam(value = "limit", defaultValue = "50") int limit,
            @RequestParam(value = "status", required = false) String statusFilter) {
        try {
            List<ProcessedDocument> docs = limit > 0
                    ? documentRepository.findAll().stream().limit(limit).toList()
                    : documentRepository.findAll();

            long completed = docs.stream().filter(d -> d.getStatus() == DocumentStatus.COMPLETED).count();
            long failed = docs.stream().filter(d -> d.getStatus() == DocumentStatus.FAILED).count();
            long processing = docs.stream().filter(d -> d.getStatus() == DocumentStatus.PROCESSING).count();

            return ResponseEntity.ok(Map.of(
                    "total", documentRepository.count(),
                    "completed", completed,
                    "failed", failed,
                    "processing", processing,
                    "documents", docs.stream().map(d -> Map.of(
                            "id", d.getId(),
                            "fileName", d.getFileName(),
                            "documentId", d.getDocumentId(),
                            "status", d.getStatus(),
                            "totalChunks", d.getTotalChunks(),
                            "totalTokens", d.getTotalTokens(),
                            "fileSizeBytes", d.getFileSizeBytes(),
                            "title", d.getTitle(),
                            "author", d.getAuthor(),
                            "processedAt", d.getProcessedAt()
                    )).toList()
            ));
        } catch (Exception e) {
            log.error("Failed to get documents", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Failed to get documents: " + e.getMessage()
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
