package com.production.lucene_service.service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.*;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Orchestrates arXiv paper ingestion: queries the arXiv API, downloads PDFs,
 * processes them through the existing pipeline, and cleans up immediately.
 *
 * Memory-safe: only one PDF on disk + in memory at any time.
 * Mirrors IngestionJobService patterns (async, JobStatus, streaming JSON export).
 */
@Service
@Slf4j
public class ArxivIngestionJobService {

    private static final DateTimeFormatter FILE_NAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS");

    private final ConcurrentHashMap<String, JobStatus> jobs = new ConcurrentHashMap<>();
    private final ArxivApiClient arxivApiClient;
    private final PdfIngestionService pdfIngestionService;
    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;
    private final ProcessedDocumentRepository documentRepository;
    private final Executor ingestionExecutor;

    private Path exportDir;
    private boolean exportEnabled;

    public ArxivIngestionJobService(ArxivApiClient arxivApiClient,
                                    PdfIngestionService pdfIngestionService,
                                    AppConfig appConfig,
                                    ProcessedDocumentRepository documentRepository,
                                    @Qualifier("ingestionExecutor") Executor ingestionExecutor) {
        this.arxivApiClient = arxivApiClient;
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
            log.info("[ArxivIngestion] Chunk export is disabled");
            return;
        }

        exportDir = Paths.get(appConfig.getRag().getExport().getPath());
        if (!Files.exists(exportDir)) {
            Files.createDirectories(exportDir);
        }
        log.info("[ArxivIngestion] Chunk export enabled → {}", exportDir.toAbsolutePath());
    }

    /**
     * Start a job that queries arXiv by category, then processes all found papers.
     * Returns jobId immediately. Total file count is set after the API query completes.
     *
     * Uses injected executor directly (not @Async) to avoid self-invocation proxy bypass.
     */
    public String startCategoryJob(String category, int maxResults) {
        String jobId = "arxiv_" + UUID.randomUUID().toString().substring(0, 12);
        JobStatus status = new JobStatus(jobId, 0); // totalFiles set after API query
        jobs.put(jobId, status);

        log.info("[{}] arXiv category job created — category={}, maxResults={}", jobId, category, maxResults);

        ingestionExecutor.execute(() -> {
            try {
                log.info("[{}] Querying arXiv API for category '{}' (max {})", jobId, category, maxResults);
                List<ArxivPaperInfo> papers = arxivApiClient.searchByCategory(category, maxResults);

                status.setTotalFiles(papers.size());
                log.info("[{}] arXiv API returned {} papers — starting ingestion", jobId, papers.size());

                processPapers(jobId, papers);

            } catch (Exception e) {
                log.error("[{}] Category job failed: {}", jobId, e.getMessage(), e);
                status.fail(e.getMessage());
            }
        });

        return jobId;
    }

    /**
     * Start a job that processes specific arXiv papers by ID.
     * Returns jobId immediately.
     *
     * Uses injected executor directly (not @Async) to avoid self-invocation proxy bypass.
     */
    public String startPaperIdsJob(List<String> paperIds) {
        String jobId = "arxiv_" + UUID.randomUUID().toString().substring(0, 12);
        JobStatus status = new JobStatus(jobId, paperIds.size());
        jobs.put(jobId, status);

        List<ArxivPaperInfo> papers = paperIds.stream()
                .map(id -> ArxivPaperInfo.builder()
                        .paperId(id)
                        .pdfUrl(ArxivApiClient.ARXIV_PDF_BASE + id + ".pdf")
                        .build())
                .collect(Collectors.toList());

        log.info("[{}] arXiv paper IDs job created — {} papers", jobId, paperIds.size());

        ingestionExecutor.execute(() -> {
            try {
                processPapers(jobId, papers);
            } catch (Exception e) {
                log.error("[{}] Paper IDs job failed: {}", jobId, e.getMessage(), e);
                status.fail(e.getMessage());
            }
        });

        return jobId;
    }

    public JobStatus getJobStatus(String jobId) {
        return jobs.get(jobId);
    }

    /**
     * Core processing loop. For each paper:
     *   1. Dedup check
     *   2. Download PDF to temp dir
     *   3. Process via PdfIngestionService
     *   4. Delete PDF immediately
     *   5. Stream chunks to JSON export
     *   6. Update ProcessedDocument record
     */
    private void processPapers(String jobId, List<ArxivPaperInfo> papers) {
        JobStatus status = jobs.get(jobId);
        int totalPapers = papers.size();
        int processed = 0;
        int failed = 0;
        int skipped = 0;
        int totalChunks = 0;
        long jobStartTime = System.currentTimeMillis();

        int progressInterval = Math.max(1, Math.min(totalPapers / 10, 10));

        String exportFileName = null;
        JsonGenerator generator = null;
        OutputStream outputStream = null;
        Path tempDir = null;

        try {
            // Create temp directory for PDF downloads
            tempDir = Files.createTempDirectory("arxiv-ingest-");
            log.debug("[{}] Temp directory: {}", jobId, tempDir);

            // Setup JSON export stream
            if (exportEnabled) {
                String timestamp = LocalDateTime.now().format(FILE_NAME_FORMATTER);
                exportFileName = "arxiv-" + timestamp + ".json";
                Path exportFile = exportDir.resolve(exportFileName);

                outputStream = Files.newOutputStream(exportFile);
                generator = objectMapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                generator.useDefaultPrettyPrinter();
                generator.writeStartArray();

                log.debug("[{}] Export file → {}", jobId, exportFile.toAbsolutePath());
            }

            // Process papers in batches
            for (int i = 0; i < totalPapers; i++) {
                ArxivPaperInfo paper = papers.get(i);
                String fileName = paper.getPaperId().replace("/", "_") + ".pdf";

                // 1. Dedup check
                Optional<ProcessedDocument> existing = documentRepository.findByFileName(fileName);
                if (existing.isPresent()) {
                    if (existing.get().getStatus() == DocumentStatus.COMPLETED) {
                        log.debug("[{}] Skipping already-ingested: {}", jobId, fileName);
                        skipped++;
                        status.incrementSkipped();

                        // Progress logging for skipped papers
                        int done = processed + failed + skipped;
                        if (done % progressInterval == 0 || done == totalPapers) {
                            long elapsed = System.currentTimeMillis() - jobStartTime;
                            double rate = done * 1000.0 / elapsed;
                            log.info("[{}] Progress: {}/{} papers ({} processed, {} skipped, {} failed), {} chunks, {} papers/sec",
                                    jobId, done, totalPapers, processed, skipped, failed, totalChunks,
                                    String.format("%.1f", rate));
                        }
                        continue;
                    } else if (existing.get().getStatus() == DocumentStatus.FAILED) {
                        log.debug("[{}] Re-processing failed paper: {}", jobId, fileName);
                        documentRepository.delete(existing.get());
                    }
                }

                // 2. Create ProcessedDocument with PROCESSING status
                String documentId = UUID.randomUUID().toString();
                ProcessedDocument doc = ProcessedDocument.builder()
                        .fileName(fileName)
                        .documentId(documentId)
                        .status(DocumentStatus.PROCESSING)
                        .title(truncate(paper.getTitle(), 2000))
                        .author(truncate(paper.getAuthors(), 2000))
                        .build();
                doc = documentRepository.save(doc);

                Path pdfPath = null;
                try {
                    // 3. Download PDF
                    pdfPath = arxivApiClient.downloadPdf(paper.getPaperId(), tempDir);

                    long fileSizeBytes = Files.size(pdfPath);
                    doc.setFileSizeBytes(fileSizeBytes);

                    // 4. Process via PdfIngestionService
                    IngestionResult result = pdfIngestionService.ingestPdf(pdfPath, fileName, documentId);

                    // 5. Stream chunks to JSON export
                    if (generator != null) {
                        for (Chunk chunk : result.chunks()) {
                            ChunkExportDTO dto = ChunkExportDTO.fromChunk(chunk, result);
                            generator.writeObject(dto);
                        }
                    }

                    // 6. Update ProcessedDocument to COMPLETED
                    doc.setStatus(DocumentStatus.COMPLETED);
                    doc.setTotalPages(result.totalPages());
                    doc.setTotalChunks(result.chunks().size());
                    doc.setTotalTokens(result.totalTokens());
                    doc.setTitle(truncate(result.title() != null ? result.title() : paper.getTitle(), 2000));
                    doc.setAuthor(truncate(result.author() != null ? result.author() : paper.getAuthors(), 2000));
                    doc.setProcessedAt(LocalDateTime.now());
                    documentRepository.save(doc);

                    status.incrementDocuments();
                    status.addChunks(result.chunks().size());
                    processed++;
                    totalChunks += result.chunks().size();

                    log.debug("[{}] Done: {} → {} pages, {} chunks",
                            jobId, fileName, result.totalPages(), result.chunks().size());

                } catch (Exception e) {
                    failed++;
                    log.error("[{}] FAILED: {} → {}", jobId, fileName, e.getMessage());
                    try {
                        doc.setStatus(DocumentStatus.FAILED);
                        doc.setErrorMessage(truncate(e.getMessage(), 2000));
                        doc.setProcessedAt(LocalDateTime.now());
                        documentRepository.save(doc);
                    } catch (Exception dbEx) {
                        log.error("[{}] DB update failed for: {}", jobId, fileName);
                    }
                } finally {
                    // 5. DELETE PDF immediately
                    if (pdfPath != null) {
                        try {
                            Files.deleteIfExists(pdfPath);
                        } catch (IOException ignored) {}
                    }
                }

                // Rate limit between downloads
                if (i < totalPapers - 1) {
                    Thread.sleep(appConfig.getArxiv().getDownloadDelayMs());
                }

                // Progress logging
                int done = processed + failed + skipped;
                if (done % progressInterval == 0 || done == totalPapers) {
                    long elapsed = System.currentTimeMillis() - jobStartTime;
                    double rate = done * 1000.0 / elapsed;
                    log.info("[{}] Progress: {}/{} papers ({} processed, {} skipped, {} failed), {} chunks, {} papers/sec",
                            jobId, done, totalPapers, processed, skipped, failed, totalChunks,
                            String.format("%.1f", rate));
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
            log.info("[{}] Job completed — {} processed, {} skipped, {} failed, {} chunks, {}s",
                    jobId, processed, skipped, failed, totalChunks,
                    String.format("%.1f", totalTime / 1000.0));

        } catch (Exception e) {
            log.error("[{}] Job crashed: {}", jobId, e.getMessage(), e);
            status.fail(e.getMessage());

            if (generator != null) {
                try { generator.close(); } catch (IOException ignored) {}
            } else if (outputStream != null) {
                try { outputStream.close(); } catch (IOException ignored) {}
            }
        } finally {
            // Cleanup temp directory (safety net)
            if (tempDir != null) {
                try {
                    try (var stream = Files.list(tempDir)) {
                        stream.forEach(file -> {
                            try { Files.deleteIfExists(file); } catch (IOException ignored) {}
                        });
                    }
                    Files.deleteIfExists(tempDir);
                    log.debug("[{}] Cleaned up temp directory: {}", jobId, tempDir);
                } catch (IOException ignored) {}
            }
        }
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) return null;
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
