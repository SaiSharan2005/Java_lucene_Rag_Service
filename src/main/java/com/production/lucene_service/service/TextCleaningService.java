package com.production.lucene_service.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.regex.Pattern;

@Service
@Slf4j
public class TextCleaningService {

    // Pattern for detecting common header/footer patterns
    private static final Pattern HEADER_FOOTER_PATTERN = Pattern.compile(
            "(?m)^\\s*(Page\\s*\\d+|\\d+\\s*of\\s*\\d+|©.*|All rights reserved.*|Confidential.*|CONFIDENTIAL.*)\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    // Pattern for multiple consecutive whitespace
    private static final Pattern MULTIPLE_SPACES = Pattern.compile("[ \\t]+");

    // Pattern for multiple consecutive newlines (more than 2)
    private static final Pattern MULTIPLE_NEWLINES = Pattern.compile("\\n{3,}");

    // Pattern for control characters (except newlines and tabs)
    private static final Pattern CONTROL_CHARS = Pattern.compile("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]");

    public String cleanText(String rawText) {
        if (rawText == null || rawText.isEmpty()) {
            return "";
        }

        String text = rawText;

        // Step 1: Unicode NFC normalization
        text = Normalizer.normalize(text, Normalizer.Form.NFC);

        // Step 2: Remove control characters (keep newlines and tabs)
        text = CONTROL_CHARS.matcher(text).replaceAll("");

        // Step 3: Normalize line endings to \n
        text = text.replace("\r\n", "\n").replace("\r", "\n");

        // Step 4: Remove common headers/footers
        text = HEADER_FOOTER_PATTERN.matcher(text).replaceAll("");

        // Step 5: Collapse multiple spaces/tabs to single space
        text = MULTIPLE_SPACES.matcher(text).replaceAll(" ");

        // Step 6: Collapse excessive newlines (preserve paragraph structure with max 2)
        text = MULTIPLE_NEWLINES.matcher(text).replaceAll("\n\n");

        // Step 7: Trim leading/trailing whitespace from each line
        text = trimLines(text);

        // Step 8: Final trim
        text = text.trim();

        log.debug("Cleaned text: {} chars -> {} chars", rawText.length(), text.length());
        return text;
    }

    private String trimLines(String text) {
        String[] lines = text.split("\n", -1);
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < lines.length; i++) {
            result.append(lines[i].trim());
            if (i < lines.length - 1) {
                result.append("\n");
            }
        }

        return result.toString();
    }

    public String removeHyphenation(String text) {
        // Remove soft hyphens
        text = text.replace("\u00AD", "");

        // Rejoin hyphenated words at line breaks
        text = text.replaceAll("-\\s*\\n\\s*", "");

        return text;
    }

    public String normalizeQuotes(String text) {
        // Normalize various quote characters to standard ASCII
        text = text.replace('\u2018', '\'');  // Left single quote
        text = text.replace('\u2019', '\'');  // Right single quote
        text = text.replace('\u201C', '"');   // Left double quote
        text = text.replace('\u201D', '"');   // Right double quote
        text = text.replace('\u2013', '-');   // En dash
        text = text.replace('\u2014', '-');   // Em dash

        return text;
    }

    public String fullClean(String rawText) {
        String text = cleanText(rawText);
        text = removeHyphenation(text);
        text = normalizeQuotes(text);
        return text;
    }
}
