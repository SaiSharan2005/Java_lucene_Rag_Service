package com.production.lucene_service.service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.ChunkExportDTO;
import com.production.lucene_service.model.JobStatus;
import com.production.lucene_service.model.ProcessedDocument;
import com.production.lucene_service.model.ProcessedDocument.DocumentStatus;
import com.production.lucene_service.repository.ProcessedDocumentRepository;
import com.production.lucene_service.service.PdfIngestionService.IngestionResult;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
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
import java.util.concurrent.Executor;

/**
 * Manages asynchronous PDF ingestion jobs.
 *
 * Each job processes files sequentially in a background thread:
 *   1. Opens a streaming JsonGenerator for the export file
 *   2. For each PDF: extract → clean → chunk → index into Lucene
 *   3. Writes each chunk to JSON immediately (no List accumulation across files)
 *   4. Updates job status atomically for real-time polling
 *   5. Tracks each document in H2 database for dedup
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
    private final ProcessedDocumentRepository documentRepository;
    private final Executor ingestionExecutor;

    private Path exportDir;
    private boolean exportEnabled;

    public IngestionJobService(PdfIngestionService pdfIngestionService,
                               AppConfig appConfig,
                               ProcessedDocumentRepository documentRepository,
                               @Qualifier("ingestionExecutor") Executor ingestionExecutor) {
        this.pdfIngestionService = pdfIngestionService;
        this.appConfig = appConfig;
        this.documentRepository = documentRepository;
        this.ingestionExecutor = ingestionExecutor;
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
        }
        log.info("Chunk export enabled → {}", exportDir.toAbsolutePath());
    }

    /**
     * Starts an async ingestion job. Returns the jobId immediately.
     * Files must already be saved to temp paths (MultipartFile is request-scoped).
     */
    public String startJob(List<PendingFile> files) {
        String jobId = "job_" + UUID.randomUUID().toString().substring(0, 12);
        JobStatus status = new JobStatus(jobId, files.size());
        jobs.put(jobId, status);

        log.info("[{}] Job created — {} files queued", jobId, files.size());

        ingestionExecutor.execute(() -> processJob(jobId, files));

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
     *
     * Called via injected executor (not @Async) to avoid self-invocation proxy bypass.
     */
    private void processJob(String jobId, List<PendingFile> files) {
        JobStatus status = jobs.get(jobId);
        int totalFiles = files.size();
        int filesProcessed = 0;
        int filesFailed = 0;
        int totalChunks = 0;
        long jobStartTime = System.currentTimeMillis();

        // Calculate progress interval (log every ~10%, minimum every 10 files)
        int progressInterval = Math.max(1, Math.min(totalFiles / 10, 10));

        String exportFileName = null;
        JsonGenerator generator = null;
        OutputStream outputStream = null;

        try {
            // Setup JSON export stream
            if (exportEnabled) {
                String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
                exportFileName = timestamp + ".json";
                Path exportFile = exportDir.resolve(exportFileName);

                outputStream = Files.newOutputStream(exportFile);
                generator = objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                generator.useDefaultPrettyPrinter();
                generator.writeStartArray();

                log.debug("[{}] Export file → {}", jobId, exportFile.toAbsolutePath());
            }

            // Process each file sequentially
            for (PendingFile pending : files) {
                log.debug("[{}] Processing: {}", jobId, pending.originalFileName());

                // Save document record with PROCESSING status
                String documentId = UUID.randomUUID().toString();
                long fileSizeBytes = 0;
                try {
                    fileSizeBytes = Files.size(pending.tempPath());
                } catch (IOException ignored) {}

                ProcessedDocument doc = ProcessedDocument.builder()
                        .fileName(pending.originalFileName())
                        .documentId(documentId)
                        .status(DocumentStatus.PROCESSING)
                        .fileSizeBytes(fileSizeBytes)
                        .build();
                doc = documentRepository.save(doc);

                try {
                    IngestionResult result = pdfIngestionService.ingestPdf(
                            pending.tempPath(), pending.originalFileName(), documentId);

                    // Stream chunks to JSON
                    if (generator != null) {
                        for (Chunk chunk : result.chunks()) {
                            ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, result);
                            generator.writeObject(dto);
                        }
                    }

                    // Update document record
                    doc.setStatus(DocumentStatus.COMPLETED);
                    doc.setTotalPages(result.totalPages());
                    doc.setTotalChunks(result.chunks().size());
                    doc.setTotalTokens(result.totalTokens());
                    doc.setTitle(truncate(result.title(), 2000));
                    doc.setAuthor(truncate(result.author(), 2000));
                    doc.setProcessedAt(LocalDateTime.now());
                    documentRepository.save(doc);

                    status.incrementDocuments();
                    status.addChunks(result.chunks().size());
                    filesProcessed++;
                    totalChunks += result.chunks().size();

                    log.debug("[{}] Done: {} → {} pages, {} chunks",
                            jobId, pending.originalFileName(), result.totalPages(), result.chunks().size());

                } catch (Exception e) {
                    filesFailed++;
                    log.error("[{}] FAILED: {} → {}", jobId, pending.originalFileName(), e.getMessage());
                    try {
                        doc.setStatus(DocumentStatus.FAILED);
                        doc.setTitle(truncate(doc.getTitle(), 2000));
                        doc.setAuthor(truncate(doc.getAuthor(), 2000));
                        doc.setErrorMessage(truncate(e.getMessage(), 2000));
                        doc.setProcessedAt(LocalDateTime.now());
                        documentRepository.save(doc);
                    } catch (Exception dbEx) {
                        log.error("[{}] DB update failed for: {}", jobId, pending.originalFileName());
                    }
                } finally {
                    try {
                        Files.deleteIfExists(pending.tempPath());
                    } catch (IOException ignored) {
                    }
                }

                // Progress log at intervals
                int done = filesProcessed + filesFailed;
                if (done % progressInterval == 0 || done == totalFiles) {
                    long elapsed = System.currentTimeMillis() - jobStartTime;
                    double rate = done * 1000.0 / elapsed;
                    log.info("[{}] Progress: {}/{} files, {} chunks, {} files/sec{}",
                            jobId, done, totalFiles, totalChunks, String.format("%.1f", rate),
                            filesFailed > 0 ? ", " + filesFailed + " failed" : "");
                }
            }

            // Close JSON array
            if (generator != null) {
                generator.writeEndArray();
                generator.close();
                generator = null;
                outputStream = null;
            }

            status.complete(exportFileName);

            long totalTime = System.currentTimeMillis() - jobStartTime;
            log.info("[{}] Job completed — {} docs, {} chunks, {} failed, {}s",
                    jobId, filesProcessed, totalChunks, filesFailed, String.format("%.1f", totalTime / 1000.0));

        } catch (Exception e) {
            log.error("[{}] Job crashed: {}", jobId, e.getMessage(), e);
            status.fail(e.getMessage());

            if (generator != null) {
                try { generator.close(); } catch (IOException ignored) {}
            } else if (outputStream != null) {
                try { outputStream.close(); } catch (IOException ignored) {}
            }
        } finally {
            for (PendingFile pending : files) {
                try {
                    Files.deleteIfExists(pending.tempPath());
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) return null;
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    /**
     * A file saved to a temp path, ready for background processing.
     * Created by the controller before the HTTP request completes.
     */
    public record PendingFile(Path tempPath, String originalFileName) {}
}
