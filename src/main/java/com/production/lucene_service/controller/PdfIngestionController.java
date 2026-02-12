package com.production.lucene_service.controller;

import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.IngestionResponse;
import com.production.lucene_service.model.IngestionResponse.DocumentDetail;
import com.production.lucene_service.model.IngestionStatus;
import com.production.lucene_service.service.ChunkExportService;
import com.production.lucene_service.service.PdfIngestionService;
import com.production.lucene_service.service.PdfIngestionService.IngestionResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.*;

/**
 * Orchestrates PDF ingestion: validates files, delegates processing to PdfIngestionService,
 * collects all chunks across files, then calls ChunkExportService once per request.
 */
@RestController
@RequestMapping("/api/v1/ingest")
@Slf4j
public class PdfIngestionController {

    private final PdfIngestionService pdfIngestionService;
    private final ChunkExportService chunkExportService;

    public PdfIngestionController(PdfIngestionService pdfIngestionService,
                                  ChunkExportService chunkExportService) {
        this.pdfIngestionService = pdfIngestionService;
        this.chunkExportService = chunkExportService;
    }

    @PostMapping(value = "/pdf", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<IngestionResponse> ingestPdf(
            @RequestPart("file") MultipartFile[] files,
            @RequestPart(value = "documentId", required = false) String documentId) {

        long startTime = System.currentTimeMillis();

        log.info("Received PDF ingestion request - {} file(s)", files.length);

        // Validate: at least one file
        if (files.length == 0) {
            return ResponseEntity.badRequest()
                    .body(IngestionResponse.builder()
                            .status(IngestionStatus.FAILED)
                            .message("No files provided")
                            .build());
        }

        // Validate all files before processing any
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

        // Process each file, collect all chunks
        List<Chunk> allChunks = new ArrayList<>();
        List<DocumentDetail> documentDetails = new ArrayList<>();
        Map<String, String> chunkSourceMap = new LinkedHashMap<>();
        int totalPages = 0;
        int totalTokens = 0;

        for (int i = 0; i < files.length; i++) {
            MultipartFile file = files[i];
            // Use provided documentId only for single-file requests; generate for multi-file
            String docId = (files.length == 1 && documentId != null && !documentId.trim().isEmpty())
                    ? documentId : null;

            try {
                IngestionResult result = pdfIngestionService.ingestPdf(file, docId);

                allChunks.addAll(result.chunks());
                chunkSourceMap.put(result.documentId(), result.fileName());
                totalPages += result.totalPages();
                totalTokens += result.totalTokens();

                documentDetails.add(DocumentDetail.builder()
                        .documentId(result.documentId())
                        .fileName(result.fileName())
                        .chunks(result.chunks().size())
                        .pages(result.totalPages())
                        .tokens(result.totalTokens())
                        .build());

            } catch (IOException e) {
                log.error("Failed to ingest file: {}", file.getOriginalFilename(), e);
                return ResponseEntity.internalServerError()
                        .body(IngestionResponse.builder()
                                .status(IngestionStatus.FAILED)
                                .message("Failed to ingest '" + file.getOriginalFilename()
                                        + "': " + e.getMessage())
                                .documentsProcessed(documentDetails.size())
                                .documents(documentDetails)
                                .processingTimeMs(System.currentTimeMillis() - startTime)
                                .build());
            }
        }

        // Export all chunks once for this entire request
        String exportFileName = chunkExportService.exportChunks(allChunks, chunkSourceMap);

        long processingTime = System.currentTimeMillis() - startTime;

        log.info("Completed ingestion - {} documents, {} chunks, {} tokens, export: {} in {}ms",
                documentDetails.size(), allChunks.size(), totalTokens, exportFileName, processingTime);

        return ResponseEntity.ok(IngestionResponse.builder()
                .status(IngestionStatus.SUCCESS)
                .message(documentDetails.size() + " document(s) ingested successfully")
                .documentsProcessed(documentDetails.size())
                .totalChunks(allChunks.size())
                .totalTokens(totalTokens)
                .totalPages(totalPages)
                .exportFileName(exportFileName)
                .processingTimeMs(processingTime)
                .documents(documentDetails)
                .build());
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
