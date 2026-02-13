package com.production.lucene_service.service;

import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.Chunk;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class ChunkingService {

    private final AppConfig appConfig;

    public ChunkingService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public List<Chunk> chunkText(String text, String documentId, int pageNumber) {
        List<Chunk> chunks = new ArrayList<>();

        if (text == null || text.trim().isEmpty()) {
            return chunks;
        }

        int chunkSize = appConfig.getChunking().getChunkSizeTokens();
        int overlapSize = appConfig.getChunking().getChunkOverlapTokens();
        int minChunkSize = appConfig.getChunking().getMinChunkLengthTokens();

        // Tokenize by whitespace (simple approximation)
        String[] tokens = tokenize(text);

        if (tokens.length == 0) {
            return chunks;
        }

        // If total tokens less than minimum chunk size, create single chunk
        if (tokens.length < minChunkSize) {
            chunks.add(createChunk(tokens, 0, tokens.length, documentId, pageNumber, 0));
            return chunks;
        }

        int start = 0;
        int chunkIndex = 0;

        while (start < tokens.length) {
            int end = Math.min(start + chunkSize, tokens.length);

            // Try to align to sentence boundary if not at the end
            if (end < tokens.length) {
                end = findSentenceBoundary(tokens, start, end);
            }

            // Ensure minimum chunk size (unless it's the last chunk)
            int currentChunkSize = end - start;
            if (currentChunkSize < minChunkSize && end < tokens.length) {
                // Extend to meet minimum size
                end = Math.min(start + minChunkSize, tokens.length);
            }

            // Create chunk
            Chunk chunk = createChunk(tokens, start, end, documentId, pageNumber, chunkIndex);
            chunks.add(chunk);

            log.debug("Created chunk {} with {} tokens (positions {}-{})",
                    chunkIndex, end - start, start, end - 1);

            // Calculate next start position with overlap
            int nextStart = end - overlapSize;

            // Ensure we always make forward progress (at least 1 token)
            if (nextStart <= start) {
                nextStart = end;
            }

            // If we've reached the end, break
            if (end >= tokens.length) {
                break;
            }

            start = nextStart;
            chunkIndex++;
        }

        log.debug("Created {} chunks from {} tokens for document {} page {}",
                chunks.size(), tokens.length, documentId, pageNumber);

        return chunks;
    }

    /**
     * Chunks document text across page boundaries for better chunk quality.
     * Concatenates all pages and chunks as a single continuous text,
     * then maps chunks back to their primary page number.
     */
    public List<Chunk> chunkDocument(List<String> pageTexts, String documentId) {
        List<Chunk> allChunks = new ArrayList<>();

        // Build page boundary map: pageEndTokenIndex[i] = last token index for page i+1
        List<Integer> pageEndTokenIndex = new ArrayList<>();
        StringBuilder fullText = new StringBuilder();
        int tokenCount = 0;

        for (int pageNum = 0; pageNum < pageTexts.size(); pageNum++) {
            String pageText = pageTexts.get(pageNum);
            if (pageText != null && !pageText.trim().isEmpty()) {
                if (fullText.length() > 0) {
                    fullText.append(" ");
                }
                fullText.append(pageText.trim());
                tokenCount += countTokens(pageText);
            }
            pageEndTokenIndex.add(tokenCount);
        }

        String combinedText = fullText.toString();
        if (combinedText.trim().isEmpty()) {
            return allChunks;
        }

        log.debug("Combined {} pages into {} tokens for cross-page chunking",
                pageTexts.size(), tokenCount);

        // Chunk the entire document
        int chunkSize = appConfig.getChunking().getChunkSizeTokens();
        int overlapSize = appConfig.getChunking().getChunkOverlapTokens();
        int minChunkSize = appConfig.getChunking().getMinChunkLengthTokens();

        String[] tokens = tokenize(combinedText);

        if (tokens.length == 0) {
            return allChunks;
        }

        int start = 0;
        int chunkIndex = 0;

        while (start < tokens.length) {
            int end = Math.min(start + chunkSize, tokens.length);

            // Try to align to sentence boundary if not at the end
            if (end < tokens.length) {
                end = findSentenceBoundary(tokens, start, end);
            }

            // Ensure minimum chunk size (unless it's the last chunk)
            int currentChunkSize = end - start;
            if (currentChunkSize < minChunkSize && end < tokens.length) {
                end = Math.min(start + minChunkSize, tokens.length);
            }

            // Find the page number for this chunk (based on chunk midpoint)
            int midpoint = (start + end) / 2;
            int pageNumber = findPageNumber(midpoint, pageEndTokenIndex);

            // Create chunk
            Chunk chunk = createChunk(tokens, start, end, documentId, pageNumber, chunkIndex);
            allChunks.add(chunk);

            log.debug("Created chunk {} with {} tokens (positions {}-{}) on page {}",
                    chunkIndex, end - start, start, end - 1, pageNumber);

            // Calculate next start position with overlap
            int nextStart = end - overlapSize;

            // Ensure we always make forward progress
            if (nextStart <= start) {
                nextStart = end;
            }

            // If we've reached the end, break
            if (end >= tokens.length) {
                break;
            }

            start = nextStart;
            chunkIndex++;
        }

        log.debug("Created {} chunks from {} tokens across {} pages for document {}",
                allChunks.size(), tokens.length, pageTexts.size(), documentId);

        return allChunks;
    }

    /**
     * Find which page a token belongs to based on page boundaries.
     */
    private int findPageNumber(int tokenIndex, List<Integer> pageEndTokenIndex) {
        for (int i = 0; i < pageEndTokenIndex.size(); i++) {
            if (tokenIndex < pageEndTokenIndex.get(i)) {
                return i + 1; // Pages are 1-indexed
            }
        }
        return pageEndTokenIndex.size(); // Last page
    }

    private String[] tokenize(String text) {
        // Simple whitespace tokenization
        return text.trim().split("\\s+");
    }

    private Chunk createChunk(String[] tokens, int start, int end,
                              String documentId, int pageNumber, int chunkIndex) {
        StringBuilder contentBuilder = new StringBuilder();
        for (int i = start; i < end; i++) {
            if (i > start) {
                contentBuilder.append(" ");
            }
            contentBuilder.append(tokens[i]);
        }

        String content = contentBuilder.toString();
        int tokenCount = end - start;

        return Chunk.builder()
                .chunkId(generateChunkId(documentId, pageNumber, chunkIndex))
                .documentId(documentId)
                .content(content)
                .pageNumber(pageNumber)
                .chunkIndex(chunkIndex)
                .tokenCount(tokenCount)
                .createdAt(Instant.now().toString())
                .build();
    }

    private int findSentenceBoundary(String[] tokens, int start, int targetEnd) {
        // Look backwards from targetEnd to find a sentence boundary
        int searchStart = Math.max(start + (appConfig.getChunking().getMinChunkLengthTokens()), targetEnd - 50);

        for (int i = targetEnd - 1; i >= searchStart; i--) {
            String token = tokens[i];
            if (token.endsWith(".") || token.endsWith("!") || token.endsWith("?")) {
                return i + 1; // Include the sentence-ending token
            }
        }

        // No sentence boundary found, return original target
        return targetEnd;
    }

    private String generateChunkId(String documentId, int pageNumber, int chunkIndex) {
        return String.format("%s_p%d_c%d_%s",
                documentId,
                pageNumber,
                chunkIndex,
                UUID.randomUUID().toString().substring(0, 8));
    }

    public int countTokens(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }
        return tokenize(text).length;
    }
}
