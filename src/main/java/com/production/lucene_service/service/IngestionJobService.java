package com.production.lucene_service.service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.ChunkExportDTO;
import com.production.lucene_service.model.JobStatus;
import com.production.lucene_service.service.PdfIngestionService.IngestionResult;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages asynchronous PDF ingestion jobs.
 *
 * Each job processes files sequentially in a background thread:
 *   1. Opens a streaming JsonGenerator for the export file
 *   2. For each PDF: extract → clean → chunk → index into Lucene
 *   3. Writes each chunk to JSON immediately (no List accumulation across files)
 *   4. Updates job status atomically for real-time polling
 *
 * Memory-safe: only one document's chunks exist in memory at any time.
 */
@Service
@Slf4j
public class IngestionJobService {

    private static final DateTimeFormatter FILE_NAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS");

    private final ConcurrentHashMap<String, JobStatus> jobs = new ConcurrentHashMap<>();
    private final PdfIngestionService pdfIngestionService;
    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;

    private Path exportDir;
    private boolean exportEnabled;

    public IngestionJobService(PdfIngestionService pdfIngestionService, AppConfig appConfig) {
        this.pdfIngestionService = pdfIngestionService;
        this.appConfig = appConfig;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() throws IOException {
        exportEnabled = appConfig.getRag().getExport().isEnabled();

        if (!exportEnabled) {
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
     * Starts an async ingestion job. Returns the jobId immediately.
     * Files must already be saved to temp paths (MultipartFile is request-scoped).
     */
    public String startJob(List<PendingFile> files) {
        String jobId = "job_" + UUID.randomUUID().toString().substring(0, 12);
        JobStatus status = new JobStatus(jobId, files.size());
        jobs.put(jobId, status);

        log.info("Created ingestion job {} for {} files", jobId, files.size());

        processJob(jobId, files);

        return jobId;
    }

    public JobStatus getJobStatus(String jobId) {
        return jobs.get(jobId);
    }

    /**
     * Background processing: processes files one-by-one, streams chunks to JSON.
     *
     * Memory guarantee: only one PDF's chunks exist in memory at any time.
     * The chunk list from each IngestionResult is written to the JsonGenerator
     * and becomes eligible for GC before the next file is processed.
     */
    @Async("ingestionExecutor")
    public void processJob(String jobId, List<PendingFile> files) {
        JobStatus status = jobs.get(jobId);
        log.info("[{}] Starting background processing of {} files", jobId, files.size());

        String exportFileName = null;
        JsonGenerator generator = null;
        OutputStream outputStream = null;

        try {
            // Setup JSON export stream (opened once, kept open across all files)
            if (exportEnabled) {
                String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
                exportFileName = timestamp + ".json";
                Path exportFile = exportDir.resolve(exportFileName);

                outputStream = Files.newOutputStream(exportFile);
                generator = objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                generator.useDefaultPrettyPrinter();
                generator.writeStartArray();

                log.info("[{}] Streaming export to: {}", jobId, exportFile.toAbsolutePath());
            }

            // Process each file sequentially
            for (PendingFile pending : files) {
                log.info("[{}] Processing file: {}", jobId, pending.originalFileName());

                try {
                    // PdfIngestionService handles: extract → clean → chunk → Lucene index
                    IngestionResult result = pdfIngestionService.ingestPdf(
                            pending.tempPath(), pending.originalFileName(), null);

                    // Stream each chunk to JSON immediately — no cross-file accumulation
                    if (generator != null) {
                        for (Chunk chunk : result.chunks()) {
                            ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, result);
                            generator.writeObject(dto);
                        }
                    }

                    status.incrementDocuments();
                    status.addChunks(result.chunks().size());

                    log.info("[{}] Completed file: {} ({} chunks)",
                            jobId, pending.originalFileName(), result.chunks().size());

                } finally {
                    // Clean up temp file after processing
                    try {
                        Files.deleteIfExists(pending.tempPath());
                    } catch (IOException ignored) {
                    }
                }
            }

            // Close JSON array
            if (generator != null) {
                generator.writeEndArray();
                generator.close();
                generator = null;
                outputStream = null; // closed by generator
            }

            status.complete(exportFileName);
            log.info("[{}] Job completed - {} documents, {} chunks, export: {}",
                    jobId, status.getDocumentsProcessed(), status.getChunksProcessed(), exportFileName);

        } catch (Exception e) {
            log.error("[{}] Job failed: {}", jobId, e.getMessage(), e);
            status.fail(e.getMessage());

            // Close generator on failure
            if (generator != null) {
                try { generator.close(); } catch (IOException ignored) {}
            } else if (outputStream != null) {
                try { outputStream.close(); } catch (IOException ignored) {}
            }
        } finally {
            // Clean up any remaining temp files
            for (PendingFile pending : files) {
                try {
                    Files.deleteIfExists(pending.tempPath());
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * A file saved to a temp path, ready for background processing.
     * Created by the controller before the HTTP request completes.
     */
    public record PendingFile(Path tempPath, String originalFileName) {}
}
