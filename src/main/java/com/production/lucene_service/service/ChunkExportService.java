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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Exports chunks as a single timestamp-based JSON file per API request.
 *
 * Each ingestion API call produces one file:
 *   {export-path}/2026-02-12T18-45-30-123.json
 *
 * Uses Jackson JsonGenerator (streaming) for memory-efficient writing.
 * Millisecond precision in filenames prevents collisions under concurrent requests.
 */
@Service
@Slf4j
public class ChunkExportService {

    private static final DateTimeFormatter FILE_NAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS");

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
     * Supports multiple documents — each chunk carries its own source filename via the mapping.
     *
     * @param chunks          all chunks generated during this API request
     * @param chunkSourceMap  maps documentId → source filename (e.g., "abc123" → "paper.pdf")
     * @return the generated filename (e.g., "2026-02-12T18-45-30-123.json"), or null if export disabled
     */
    public String exportChunks(List<Chunk> chunks, Map<String, String> chunkSourceMap) {
        if (!appConfig.getRag().getExport().isEnabled()) {
            return null;
        }

        String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
        String fileName = timestamp + ".json";
        Path outputFile = exportDir.resolve(fileName);

        try (OutputStream os = Files.newOutputStream(outputFile);
             JsonGenerator generator = objectMapper.getFactory()
                     .createGenerator(os, com.fasterxml.jackson.core.JsonEncoding.UTF8)) {

            generator.useDefaultPrettyPrinter();
            generator.writeStartArray();

            for (Chunk chunk : chunks) {
                String source = chunkSourceMap.getOrDefault(chunk.getDocumentId(), "unknown.pdf");
                ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, source);
                generator.writeObject(dto);
            }

            generator.writeEndArray();

            log.info("Exported {} chunks to {}", chunks.size(), outputFile.toAbsolutePath());
            return fileName;

        } catch (IOException e) {
            log.error("Failed to export chunks to {}: {}", outputFile, e.getMessage(), e);
            return null;
        }
    }

    public boolean isEnabled() {
        return appConfig.getRag().getExport().isEnabled();
    }
}
