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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Handles PDF processing: text extraction, cleaning, chunking, and Lucene indexing.
 * Does NOT handle JSON export — that responsibility belongs to the controller layer.
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
     * Processes a single PDF: extract text, clean, chunk, and index into Lucene.
     *
     * @return the generated chunks (already indexed in Lucene)
     */
    public IngestionResult ingestPdf(MultipartFile file, String documentId) throws IOException {
        // Generate document ID if not provided
        if (documentId == null || documentId.trim().isEmpty()) {
            documentId = UUID.randomUUID().toString();
        }

        String fileName = file.getOriginalFilename() != null
                ? file.getOriginalFilename() : "unknown.pdf";

        log.info("Starting PDF ingestion for document: {} (file: {})", documentId, fileName);

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

        // Step 3: Chunk the document
        List<Chunk> chunks = chunkingService.chunkDocument(cleanedTexts, documentId);
        log.info("Created {} chunks from document", chunks.size());

        // Step 4: Index chunks in Lucene
        luceneIndexService.indexChunks(chunks);
        log.info("Indexed {} chunks in Lucene for document: {}", chunks.size(), documentId);

        int totalTokens = chunks.stream().mapToInt(Chunk::getTokenCount).sum();

        return new IngestionResult(documentId, fileName, pages.size(), chunks, totalTokens);
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

                pages.add(PageContent.builder()
                        .pageNumber(pageNum)
                        .rawText(rawText)
                        .build());

                log.debug("Extracted page {}/{}: {} characters", pageNum, totalPages, rawText.length());
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

    /**
     * Result of ingesting a single PDF. Carries chunks + metadata back to the controller.
     */
    public record IngestionResult(
            String documentId,
            String fileName,
            int totalPages,
            List<Chunk> chunks,
            int totalTokens
    ) {}
}
