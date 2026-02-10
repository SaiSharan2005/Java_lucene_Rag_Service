package com.production.lucene_service.service;

import com.production.lucene_service.lucene.LuceneIndexService;
import com.production.lucene_service.model.Chunk;
import com.production.lucene_service.model.IngestionResponse;
import com.production.lucene_service.model.IngestionStatus;
import com.production.lucene_service.model.PageContent;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    public IngestionResponse ingestPdf(MultipartFile file, String documentId) {
        long startTime = System.currentTimeMillis();

        // Generate document ID if not provided
        if (documentId == null || documentId.trim().isEmpty()) {
            documentId = UUID.randomUUID().toString();
        }

        log.info("Starting PDF ingestion for document: {} (file: {})",
                documentId, file.getOriginalFilename());

        try {
            // Step 1: Extract text from PDF
            List<PageContent> pages = extractTextFromPdf(file);
            log.info("Extracted {} pages from PDF", pages.size());

            // Step 2: Clean text for each page
            List<String> cleanedTexts = new ArrayList<>();
            for (PageContent page : pages) {
                String cleanedText = textCleaningService.fullClean(page.getRawText());
                page.setCleanedText(cleanedText);
                cleanedTexts.add(cleanedText);
            }
            log.info("Cleaned text for {} pages", cleanedTexts.size());

            // Step 3: Chunk the document
            List<Chunk> chunks = chunkingService.chunkDocument(cleanedTexts, documentId);
            log.info("Created {} chunks from document", chunks.size());

            // Step 4: Index chunks in Lucene
            luceneIndexService.indexChunks(chunks);
            log.info("Indexed {} chunks in Lucene", chunks.size());

            // Calculate statistics
            int totalTokens = chunks.stream()
                    .mapToInt(Chunk::getTokenCount)
                    .sum();

            long processingTime = System.currentTimeMillis() - startTime;

            log.info("Completed ingestion for document: {} - {} pages, {} chunks, {} tokens in {}ms",
                    documentId, pages.size(), chunks.size(), totalTokens, processingTime);

            return IngestionResponse.builder()
                    .documentId(documentId)
                    .status(IngestionStatus.SUCCESS)
                    .message("Document ingested successfully")
                    .totalPages(pages.size())
                    .totalChunks(chunks.size())
                    .totalTokens(totalTokens)
                    .processingTimeMs(processingTime)
                    .build();

        } catch (Exception e) {
            log.error("Failed to ingest PDF document: {}", documentId, e);

            long processingTime = System.currentTimeMillis() - startTime;

            return IngestionResponse.builder()
                    .documentId(documentId)
                    .status(IngestionStatus.FAILED)
                    .message("Ingestion failed: " + e.getMessage())
                    .totalPages(0)
                    .totalChunks(0)
                    .totalTokens(0)
                    .processingTimeMs(processingTime)
                    .build();
        }
    }

    private List<PageContent> extractTextFromPdf(MultipartFile file) throws IOException {
        List<PageContent> pages = new ArrayList<>();

        try (PDDocument document = Loader.loadPDF(file.getBytes())) {
            PDFTextStripper stripper = new PDFTextStripper();
            int totalPages = document.getNumberOfPages();

            for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
                stripper.setStartPage(pageNum);
                stripper.setEndPage(pageNum);

                String rawText = stripper.getText(document);

                PageContent pageContent = PageContent.builder()
                        .pageNumber(pageNum)
                        .rawText(rawText)
                        .build();

                pages.add(pageContent);

                log.debug("Extracted page {}/{}: {} characters",
                        pageNum, totalPages, rawText.length());
            }
        }

        return pages;
    }

    public void deleteDocument(String documentId) throws IOException {
        log.info("Deleting document from index: {}", documentId);
        luceneIndexService.deleteByDocumentId(documentId);
    }

    public long getIndexedDocumentCount() throws IOException {
        return luceneIndexService.getDocumentCount();
    }
}
