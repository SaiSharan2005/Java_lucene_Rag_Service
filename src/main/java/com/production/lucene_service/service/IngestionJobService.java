package com.production.lucene_service.service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.config.IngestionConfig;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final IngestionConfig ingestionConfig;
    private final ObjectMapper objectMapper;
    private final ProcessedDocumentRepository documentRepository;
    private final Executor ingestionExecutor;
    private ExecutorService pdfProcessingPool;

    private Path exportDir;
    private boolean exportEnabled;

    public IngestionJobService(PdfIngestionService pdfIngestionService,
                               AppConfig appConfig,
                               IngestionConfig ingestionConfig,
                               ProcessedDocumentRepository documentRepository,
                               @Qualifier("ingestionExecutor") Executor ingestionExecutor) {
        this.pdfIngestionService = pdfIngestionService;
        this.appConfig = appConfig;
        this.ingestionConfig = ingestionConfig;
        this.documentRepository = documentRepository;
        this.ingestionExecutor = ingestionExecutor;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    public void init() throws IOException {
        // Initialize PDF processing thread pool
        int threadCount = ingestionConfig.resolveThreadCount();
        pdfProcessingPool = Executors.newFixedThreadPool(threadCount);

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

    /**
     * Starts an async ingestion job for local directory files (not copied — read in place).
     * Source files are NOT deleted after processing.
     */
    public String startLocalJob(List<PendingFile> files) {
        String jobId = "job_" + UUID.randomUUID().toString().substring(0, 12);
        JobStatus status = new JobStatus(jobId, files.size());
        jobs.put(jobId, status);

        log.info("[{}] Local job created — {} files queued", jobId, files.size());

        ingestionExecutor.execute(() -> processJob(jobId, files));

        return jobId;
    }

    public JobStatus getJobStatus(String jobId) {
        return jobs.get(jobId);
    }

    /**
     * Background processing: processes files in parallel, streams chunks to JSON after completion.
     *
     * Flow:
     *   1. Submit all PDFs to thread pool for parallel processing
     *   2. Wait for all to complete (futures.get())
     *   3. Export results to JSON
     *
     * Thread safety:
     *   - AtomicInteger for counters (documentsProcessed, chunksProcessed)
     *   - ConcurrentHashMap for ingestion results
     *   - Single JsonGenerator for export (after all PDFs complete)
     */
    private void processJob(String jobId, List<PendingFile> files) {
        JobStatus status = jobs.get(jobId);
        int totalFiles = files.size();
        long jobStartTime = System.currentTimeMillis();

        // Calculate progress interval (log every ~10%, minimum every 10 files)
        int progressInterval = Math.max(1, Math.min(totalFiles / 10, 10));

        // Thread-safe counters
        AtomicInteger filesProcessed = new AtomicInteger(0);
        AtomicInteger filesFailed = new AtomicInteger(0);
        AtomicInteger totalChunks = new AtomicInteger(0);

        // Store results for JSON export (after all threads complete)
        ConcurrentHashMap<String, IngestionResult> results = new ConcurrentHashMap<>();
        List<Future<?>> futures = new ArrayList<>();

        // Submit all PDFs for parallel processing
        for (PendingFile pending : files) {
            Future<?> future = pdfProcessingPool.submit(() -> {
                processSinglePdf(jobId, pending, status, filesProcessed, filesFailed, totalChunks, results, progressInterval, totalFiles, jobStartTime);
            });
            futures.add(future);
        }

        // Wait for all futures to complete
        try {
            for (Future<?> future : futures) {
                future.get();  // Block until this PDF finishes
            }
        } catch (Exception e) {
            log.error("[{}] Error waiting for PDF processing: {}", jobId, e.getMessage(), e);
            status.fail(e.getMessage());
            return;
        }

        // Export results to JSON after all PDFs processed
        exportResults(jobId, status, results);

        long totalTime = System.currentTimeMillis() - jobStartTime;
        log.info("[{}] Job completed — {} docs, {} chunks, {} failed, {}s",
                jobId, filesProcessed.get(), totalChunks.get(), filesFailed.get(), String.format("%.1f", totalTime / 1000.0));
    }

    /**
     * Process a single PDF in a worker thread.
     * Updates thread-safe counters and stores results for later JSON export.
     */
    private void processSinglePdf(String jobId, PendingFile pending, JobStatus status,
                                   AtomicInteger filesProcessed, AtomicInteger filesFailed,
                                   AtomicInteger totalChunks, ConcurrentHashMap<String, IngestionResult> results,
                                   int progressInterval, int totalFiles, long jobStartTime) {
        String documentId = UUID.randomUUID().toString();
        long fileSizeBytes = 0;
        ProcessedDocument doc = null;

        try {
            fileSizeBytes = Files.size(pending.tempPath());
        } catch (IOException ignored) {}

        // Save document record with PROCESSING status
        doc = ProcessedDocument.builder()
                .fileName(pending.originalFileName())
                .documentId(documentId)
                .status(DocumentStatus.PROCESSING)
                .fileSizeBytes(fileSizeBytes)
                .build();
        doc = documentRepository.save(doc);

        try {
            log.debug("[{}] Processing: {}", jobId, pending.originalFileName());

            // Extract → Clean → Chunk → Index (existing pipeline)
            IngestionResult result = pdfIngestionService.ingestPdf(
                    pending.tempPath(), pending.originalFileName(), documentId);

            // Store result for JSON export
            results.put(documentId, result);

            // Update document record
            doc.setStatus(DocumentStatus.COMPLETED);
            doc.setTotalPages(result.totalPages());
            doc.setTotalChunks(result.chunks().size());
            doc.setTotalTokens(result.totalTokens());
            doc.setTitle(truncate(result.title(), 2000));
            doc.setAuthor(truncate(result.author(), 2000));
            doc.setProcessedAt(LocalDateTime.now());
            documentRepository.save(doc);

            // Update thread-safe counters
            status.incrementDocuments();
            status.addChunks(result.chunks().size());
            filesProcessed.incrementAndGet();
            totalChunks.addAndGet(result.chunks().size());

            log.debug("[{}] Done: {} → {} pages, {} chunks",
                    jobId, pending.originalFileName(), result.totalPages(), result.chunks().size());

        } catch (Exception e) {
            filesFailed.incrementAndGet();
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
            if (pending.deleteAfter()) {
                try {
                    Files.deleteIfExists(pending.tempPath());
                } catch (IOException ignored) {}
            }

            // Log progress
            int done = filesProcessed.get() + filesFailed.get();
            if (done % progressInterval == 0 || done == totalFiles) {
                long elapsed = System.currentTimeMillis() - jobStartTime;
                double rate = done * 1000.0 / elapsed;
                log.info("[{}] Progress: {}/{} files, {} chunks, {} files/sec{}",
                        jobId, done, totalFiles, totalChunks.get(), String.format("%.1f", rate),
                        filesFailed.get() > 0 ? ", " + filesFailed.get() + " failed" : "");
            }
        }
    }

    /**
     * Export all ingested results to JSON file.
     * Called after all PDF processing completes.
     */
    private void exportResults(String jobId, JobStatus status, ConcurrentHashMap<String, IngestionResult> results) {
        if (!exportEnabled || results.isEmpty()) {
            return;
        }

        String exportFileName = null;
        JsonGenerator generator = null;
        OutputStream outputStream = null;

        try {
            String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
            exportFileName = timestamp + ".json";
            Path exportFile = exportDir.resolve(exportFileName);

            outputStream = Files.newOutputStream(exportFile);
            generator = objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            generator.useDefaultPrettyPrinter();
            generator.writeStartArray();

            log.debug("[{}] Export file → {}", jobId, exportFile.toAbsolutePath());

            // Write all results to JSON
            for (IngestionResult result : results.values()) {
                for (Chunk chunk : result.chunks()) {
                    ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, result);
                    generator.writeObject(dto);
                }
            }

            generator.writeEndArray();
            status.complete(exportFileName);
            log.debug("[{}] Export complete: {}", jobId, exportFileName);

        } catch (Exception e) {
            log.error("[{}] Export failed: {}", jobId, e.getMessage(), e);
        } finally {
            if (generator != null) {
                try { generator.close(); } catch (IOException ignored) {}
            } else if (outputStream != null) {
                try { outputStream.close(); } catch (IOException ignored) {}
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
    public record PendingFile(Path tempPath, String originalFileName, boolean deleteAfter) {
        /** Backwards-compatible constructor — defaults to deleteAfter=true (temp files). */
        public PendingFile(Path tempPath, String originalFileName) {
            this(tempPath, originalFileName, true);
        }
    }
}
