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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Exports chunks as structured JSON files for offline embedding generation.
 *
 * Strategy: One timestamp-based file per API request.
 *
 * Each ingestion API call produces a single file:
 *   {export-path}/2026-02-12T18-45-30.json
 *
 * All chunks from that request (regardless of how many documents)
 * are written into one JSON array using Jackson streaming for memory efficiency.
 */
@Service
@Slf4j
public class ChunkExportService {

    private static final DateTimeFormatter FILE_NAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

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
     * Exports all chunks from a single API request into one timestamp-based JSON file.
     * Uses Jackson streaming (JsonGenerator) to avoid loading all chunks into memory.
     *
     * @param chunks         all chunks generated during this API request
     * @param sourceFileName the original uploaded file name (e.g., "paper.pdf")
     */
    public void exportChunks(List<Chunk> chunks, String sourceFileName) {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return;
        }

        String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
        Path outputFile = exportDir.resolve(timestamp + ".json");

        try (OutputStream os = Files.newOutputStream(outputFile);
             JsonGenerator generator = objectMapper.getFactory().createGenerator(os)) {

            generator.useDefaultPrettyPrinter();
            generator.writeStartArray();

            for (Chunk chunk : chunks) {
                ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, sourceFileName);
                generator.writeObject(dto);
            }

            generator.writeEndArray();

            log.info("Exported {} chunks to {}", chunks.size(), outputFile.toAbsolutePath());

        } catch (IOException e) {
            log.error("Failed to export chunks to {}: {}", outputFile, e.getMessage(), e);
        }
    }

    /**
     * Returns export statistics: file count, total size, export path.
     */
    public Map<String, Object> getExportStats() throws IOException {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return Map.of("enabled", false);
        }

        long fileCount = 0;
        long totalSizeBytes = 0;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(exportDir, "*.json")) {
            for (Path file : stream) {
                fileCount++;
                totalSizeBytes += Files.size(file);
            }
        }

        return Map.of(
                "enabled", true,
                "exportPath", exportDir.toAbsolutePath().toString(),
                "exportFiles", fileCount,
                "totalExportSizeMB", String.format("%.2f", totalSizeBytes / (1024.0 * 1024.0))
        );
    }
}
