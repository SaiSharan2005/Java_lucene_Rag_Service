package com.production.lucene_service.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.ChunkExportDTO;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Exports chunks as structured JSON files for offline embedding generation.
 *
 * Strategy: Per-document export files.
 *
 * Each document ingestion creates/overwrites a file: {export-path}/doc_{documentId}.json
 * This approach is production-ready because:
 *   - No need to read/parse existing files for appending (O(1) per ingestion)
 *   - Re-ingesting a document overwrites only that document's export (idempotent)
 *   - Deleting a document can also remove its export file
 *   - Merging into a single file is available via mergeAllChunks() for Kaggle upload
 *   - Each file is a valid JSON array, independently usable
 *
 * For Kaggle: call GET /api/v1/ingest/export/merge to produce a single all_chunks.json
 */
@Service
@Slf4j
public class ChunkExportService {

    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;
    private Path exportDir;

    public ChunkExportService(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() throws IOException {
        if (!appConfig.getRag().getExport().isEnabled()) {
            log.info("Chunk export is disabled");
            return;
        }

        exportDir = Paths.get(appConfig.getRag().getExport().getPath());
        if (!Files.exists(exportDir)) {
            Files.createDirectories(exportDir);
            log.info("Created chunk export directory: {}", exportDir.toAbsolutePath());
        }
        log.info("Chunk export enabled. Export path: {}", exportDir.toAbsolutePath());
    }

    /**
     * Exports chunks for a single document as a per-document JSON file.
     * File: {export-path}/doc_{documentId}.json
     *
     * If the file already exists (re-ingestion), it is overwritten.
     * This is intentional - re-ingesting should produce fresh export data.
     */
    public void exportChunks(List<Chunk> chunks, String documentId, String sourceFileName) {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return;
        }

        Path outputFile = exportDir.resolve("doc_" + sanitizeFileName(documentId) + ".json");

        try {
            List<ChunkExportDTO> dtos = chunks.stream()
                    .map(chunk -> ChunkExportDTO.fromChunk(chunk, sourceFileName))
                    .toList();

            objectMapper.writeValue(outputFile.toFile(), dtos);

            log.info("Exported {} chunks for document '{}' to {}",
                    chunks.size(), documentId, outputFile.toAbsolutePath());

        } catch (IOException e) {
            log.error("Failed to export chunks for document '{}': {}", documentId, e.getMessage(), e);
        }
    }

    /**
     * Merges all per-document export files into a single all_chunks.json.
     * Uses Jackson streaming (JsonGenerator) to avoid loading everything into memory.
     *
     * Output: {export-path}/all_chunks.json
     *
     * @return number of total chunks written, or -1 on failure
     */
    public long mergeAllChunks() throws IOException {
        if (!appConfig.getRag().getExport().isEnabled()) {
            throw new IllegalStateException("Chunk export is disabled");
        }

        Path mergedFile = exportDir.resolve("all_chunks.json");
        long totalChunks = 0;

        try (OutputStream os = Files.newOutputStream(mergedFile);
             JsonGenerator generator = objectMapper.getFactory().createGenerator(os)) {

            generator.useDefaultPrettyPrinter();
            generator.writeStartArray();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(exportDir, "doc_*.json")) {
                for (Path docFile : stream) {
                    log.debug("Merging: {}", docFile.getFileName());

                    ChunkExportDTO[] docChunks = objectMapper.readValue(docFile.toFile(), ChunkExportDTO[].class);
                    for (ChunkExportDTO chunk : docChunks) {
                        generator.writeObject(chunk);
                        totalChunks++;
                    }
                }
            }

            generator.writeEndArray();
        }

        log.info("Merged {} total chunks into {}", totalChunks, mergedFile.toAbsolutePath());
        return totalChunks;
    }

    /**
     * Deletes the export file for a specific document.
     * Called when a document is deleted from the index.
     */
    public void deleteExport(String documentId) {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return;
        }

        Path exportFile = exportDir.resolve("doc_" + sanitizeFileName(documentId) + ".json");
        try {
            if (Files.deleteIfExists(exportFile)) {
                log.info("Deleted export file for document: {}", documentId);
            }
        } catch (IOException e) {
            log.error("Failed to delete export file for document '{}': {}", documentId, e.getMessage(), e);
        }
    }

    /**
     * Returns export statistics.
     */
    public Map<String, Object> getExportStats() throws IOException {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return Map.of("enabled", false);
        }

        long fileCount = 0;
        long totalSizeBytes = 0;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(exportDir, "doc_*.json")) {
            for (Path file : stream) {
                fileCount++;
                totalSizeBytes += Files.size(file);
            }
        }

        Path mergedFile = exportDir.resolve("all_chunks.json");
        boolean mergedExists = Files.exists(mergedFile);

        return Map.of(
                "enabled", true,
                "exportPath", exportDir.toAbsolutePath().toString(),
                "documentExports", fileCount,
                "totalExportSizeMB", String.format("%.2f", totalSizeBytes / (1024.0 * 1024.0)),
                "mergedFileExists", mergedExists,
                "mergedFileSizeMB", mergedExists
                        ? String.format("%.2f", Files.size(mergedFile) / (1024.0 * 1024.0))
                        : "0.00"
        );
    }

    private String sanitizeFileName(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
