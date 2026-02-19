package com.production.lucene_service.service;

import com.production.lucene_service.lucene.LuceneIndexService;
import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.PageContent;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Handles PDF processing: text extraction, cleaning, chunking, and Lucene indexing.
 * Does NOT handle JSON export — that responsibility belongs to IngestionJobService.
 */
@Service
@Slf4j
public class PdfIngestionService {

    private final TextCleaningService textCleaningService;
    private final ChunkingService chunkingService;
    private final LuceneIndexService luceneIndexService;

    public PdfIngestionService(TextCleaningService textCleaningService,
                               ChunkingService chunkingService,
                               LuceneIndexService luceneIndexService) {
        this.textCleaningService = textCleaningService;
        this.chunkingService = chunkingService;
        this.luceneIndexService = luceneIndexService;
    }

    /**
     * Processes a single PDF from a file path: extract text, clean, chunk, and index into Lucene.
     * Used by IngestionJobService for background processing (temp files from multipart upload).
     */
    public IngestionResult ingestPdf(Path filePath, String originalFileName, String documentId) throws IOException {
        if (documentId == null || documentId.trim().isEmpty()) {
            documentId = UUID.randomUUID().toString();
        }

        String fileName = originalFileName != null ? originalFileName : "unknown.pdf";

        log.debug("Ingesting: {} (docId: {})", fileName, documentId);

        // Step 1: Extract text and PDF metadata
        String title = null;
        String author = null;
        List<PageContent> pages;

        long extractStart = System.currentTimeMillis();
        try (PDDocument document = Loader.loadPDF(filePath.toFile())) {
            var info = document.getDocumentInformation();
            if (info != null) {
                title = info.getTitle();
                author = info.getAuthor();
            }
            pages = new ArrayList<>();
            extractPages(document, pages);
        }
        long extractMs = System.currentTimeMillis() - extractStart;

        log.info("[TIMING] {} — extract: {}ms ({} pages)", fileName, extractMs, pages.size());

        // Step 2-4: common processing
        return processPages(pages, documentId, fileName, title, author);
    }

    /**
     * Processes a single PDF from a MultipartFile.
     * Kept for direct usage (tests, single-file sync calls).
     */
    public IngestionResult ingestPdf(MultipartFile file, String documentId) throws IOException {
        if (documentId == null || documentId.trim().isEmpty()) {
            documentId = UUID.randomUUID().toString();
        }

        String fileName = file.getOriginalFilename() != null
                ? file.getOriginalFilename() : "unknown.pdf";

        log.debug("Ingesting: {} (docId: {})", fileName, documentId);

        // Step 1: Extract text and PDF metadata
        String title = null;
        String author = null;
        List<PageContent> pages;

        try (PDDocument document = Loader.loadPDF(file.getBytes())) {
            var info = document.getDocumentInformation();
            if (info != null) {
                title = info.getTitle();
                author = info.getAuthor();
            }
            pages = new ArrayList<>();
            extractPages(document, pages);
        }

        log.debug("Extracted {} pages from: {}", pages.size(), fileName);

        // Step 2-4: common processing
        return processPages(pages, documentId, fileName, title, author);
    }

    /**
     * Common processing pipeline: clean → chunk → index.
     */
    private IngestionResult processPages(List<PageContent> pages, String documentId, String fileName,
                                         String title, String author) throws IOException {

        // Step 2: Clean text for each page
        long cleanStart = System.currentTimeMillis();
        List<String> cleanedTexts = new ArrayList<>();
        for (PageContent page : pages) {
            String cleanedText = textCleaningService.fullClean(page.getRawText());
            page.setCleanedText(cleanedText);
            cleanedTexts.add(cleanedText);
        }
        long cleanMs = System.currentTimeMillis() - cleanStart;

        // Step 3: Chunk the document
        long chunkStart = System.currentTimeMillis();
        List<Chunk> chunks = chunkingService.chunkDocument(cleanedTexts, documentId);
        long chunkMs = System.currentTimeMillis() - chunkStart;

        // Step 4: Index chunks in Lucene
        long indexStart = System.currentTimeMillis();
        luceneIndexService.indexChunks(chunks);
        long indexMs = System.currentTimeMillis() - indexStart;

        int totalTokens = chunks.stream().mapToInt(Chunk::getTokenCount).sum();

        log.info("[TIMING] {} — clean: {}ms, chunk: {}ms ({} chunks), index: {}ms",
                fileName, cleanMs, chunkMs, chunks.size(), indexMs);

        return new IngestionResult(documentId, fileName, pages.size(), chunks, totalTokens, title, author);
    }

    private void extractPages(PDDocument document, List<PageContent> pages) throws IOException {
        PDFTextStripper stripper = new PDFTextStripper();
        int totalPages = document.getNumberOfPages();

        for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
            stripper.setStartPage(pageNum);
            stripper.setEndPage(pageNum);

            String rawText = stripper.getText(document);

            pages.add(PageContent.builder()
                    .pageNumber(pageNum)
                    .rawText(rawText)
                    .build());

            log.debug("Extracted page {}/{}: {} characters", pageNum, totalPages, rawText.length());
        }
    }

    public void deleteDocument(String documentId) throws IOException {
        log.debug("Deleting document from index: {}", documentId);
        luceneIndexService.deleteByDocumentId(documentId);
    }

    public long getIndexedChunkCount() throws IOException {
        return luceneIndexService.getChunkCount();
    }

    public long getIndexedPdfCount() throws IOException {
        return luceneIndexService.getPdfCount();
    }

    /**
     * Result of ingesting a single PDF. Carries chunks + metadata for export/reporting.
     */
    public record IngestionResult(
            String documentId,
            String fileName,
            int totalPages,
            List<Chunk> chunks,
            int totalTokens,
            String title,
            String author
    ) {}
}
