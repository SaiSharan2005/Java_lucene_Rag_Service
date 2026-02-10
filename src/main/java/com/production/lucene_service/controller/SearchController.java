package com.production.lucene_service.controller;

import com.production.lucene_service.lucene.LuceneSearchService;
import com.production.lucene_service.model.SearchResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/search")
@Slf4j
public class SearchController {

    private final LuceneSearchService searchService;

    public SearchController(LuceneSearchService searchService) {
        this.searchService = searchService;
    }

    @GetMapping
    public ResponseEntity<?> search(
            @RequestParam("q") String query,
            @RequestParam(value = "topK", defaultValue = "10") int topK,
            @RequestParam(value = "documentId", required = false) String documentId) {

        log.info("Search request - query: '{}', topK: {}, documentId: {}",
                query, topK, documentId);

        if (query == null || query.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Query parameter 'q' is required"
            ));
        }

        if (topK < 1 || topK > 100) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "topK must be between 1 and 100"
            ));
        }

        try {
            SearchResponse response;
            if (documentId != null && !documentId.trim().isEmpty()) {
                response = searchService.searchByDocumentId(query, documentId, topK);
            } else {
                response = searchService.search(query, topK);
            }
            return ResponseEntity.ok(response);

        } catch (ParseException e) {
            log.error("Invalid query syntax: {}", query, e);
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Invalid query syntax: " + e.getMessage()
            ));
        } catch (IOException e) {
            log.error("Search failed", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Search failed: " + e.getMessage()
            ));
        }
    }

    @PostMapping
    public ResponseEntity<?> searchPost(@RequestBody SearchRequest request) {
        log.info("Search POST request - query: '{}', topK: {}, documentId: {}",
                request.query(), request.topK(), request.documentId());

        if (request.query() == null || request.query().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Query is required"
            ));
        }

        int topK = request.topK() != null ? request.topK() : 10;
        if (topK < 1 || topK > 100) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "topK must be between 1 and 100"
            ));
        }

        try {
            SearchResponse response;
            if (request.documentId() != null && !request.documentId().trim().isEmpty()) {
                response = searchService.searchByDocumentId(request.query(), request.documentId(), topK);
            } else {
                response = searchService.search(request.query(), topK);
            }
            return ResponseEntity.ok(response);

        } catch (ParseException e) {
            log.error("Invalid query syntax: {}", request.query(), e);
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Invalid query syntax: " + e.getMessage()
            ));
        } catch (IOException e) {
            log.error("Search failed", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Search failed: " + e.getMessage()
            ));
        }
    }

    public record SearchRequest(String query, Integer topK, String documentId) {}

    @GetMapping("/chunk-stats")
    public ResponseEntity<?> getChunkStatistics() {
        try {
            return ResponseEntity.ok(searchService.getChunkStatistics());
        } catch (IOException e) {
            log.error("Failed to get chunk statistics", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Failed to get chunk statistics: " + e.getMessage()
            ));
        }
    }
}
