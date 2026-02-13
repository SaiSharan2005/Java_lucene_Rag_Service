package com.production.lucene_service.controller;

import com.production.lucene_service.model.*;
import com.production.lucene_service.service.ArxivIngestionJobService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for direct arXiv paper ingestion.
 * Accepts arXiv category or paper IDs, returns a jobId immediately (HTTP 202).
 * Processing runs async in background — poll /status/{jobId} for progress.
 */
@RestController
@RequestMapping("/api/v1/arxiv")
@Slf4j
public class ArxivIngestionController {

    private final ArxivIngestionJobService arxivIngestionJobService;

    public ArxivIngestionController(ArxivIngestionJobService arxivIngestionJobService) {
        this.arxivIngestionJobService = arxivIngestionJobService;
    }

    /**
     * Ingest papers by arXiv category (e.g. cs.AI, cs.CL).
     * Returns jobId immediately. Papers are fetched, processed, and indexed in the background.
     */
    @PostMapping("/ingest")
    public ResponseEntity<IngestionResponse> ingestByCategory(@Valid @RequestBody ArxivCategoryRequest request) {
        log.info("arXiv category ingestion request: category={}, maxResults={}",
                request.getCategory(), request.getMaxResults());

        String jobId = arxivIngestionJobService.startCategoryJob(
                request.getCategory(), request.getMaxResults());

        return ResponseEntity.accepted()
                .body(IngestionResponse.builder()
                        .jobId(jobId)
                        .status(IngestionStatus.PROCESSING)
                        .message("arXiv ingestion started for category '" + request.getCategory()
                                + "' (max " + request.getMaxResults() + " papers)")
                        .build());
    }

    /**
     * Ingest specific papers by their arXiv IDs.
     * Returns jobId immediately. Papers are fetched, processed, and indexed in the background.
     */
    @PostMapping("/ingest/papers")
    public ResponseEntity<IngestionResponse> ingestByPaperIds(@Valid @RequestBody ArxivPaperIdsRequest request) {
        log.info("arXiv paper IDs ingestion request: {} papers", request.getPaperIds().size());

        String jobId = arxivIngestionJobService.startPaperIdsJob(request.getPaperIds());

        return ResponseEntity.accepted()
                .body(IngestionResponse.builder()
                        .jobId(jobId)
                        .status(IngestionStatus.PROCESSING)
                        .message(request.getPaperIds().size() + " arXiv paper(s) submitted for processing")
                        .filesSubmitted(request.getPaperIds().size())
                        .build());
    }

    /**
     * Poll job progress by jobId.
     */
    @GetMapping("/status/{jobId}")
    public ResponseEntity<?> getJobStatus(@PathVariable String jobId) {
        JobStatus status = arxivIngestionJobService.getJobStatus(jobId);

        if (status == null) {
            return ResponseEntity.status(404).body(Map.of(
                    "error", "Job not found",
                    "jobId", jobId
            ));
        }

        return ResponseEntity.ok(status);
    }
}
