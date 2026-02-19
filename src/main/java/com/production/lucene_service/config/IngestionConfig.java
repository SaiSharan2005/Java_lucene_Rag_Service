package com.production.lucene_service.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for PDF ingestion parallelization.
 * Supports both auto-detection and manual thread count configuration.
 */
@Component
@ConfigurationProperties(prefix = "ingestion")
@Data
@Slf4j
public class IngestionConfig {

    /**
     * Thread count: "auto" for auto-detection, or a specific number.
     * Auto mode: threads = max(2, cores - 2)
     */
    private String threads = "auto";

    /**
     * Resolves the actual thread count to use for ingestion.
     */
    public int resolveThreadCount() {
        if ("auto".equalsIgnoreCase(threads)) {
            int cores = Runtime.getRuntime().availableProcessors();
            int threadCount = Math.max(2, cores - 2);
            log.info("Detected CPU cores: {}", cores);
            log.info("Using ingestion threads: {}", threadCount);
            return threadCount;
        } else {
            try {
                int threadCount = Integer.parseInt(threads);
                log.info("Using ingestion threads: {} (configured)", threadCount);
                return threadCount;
            } catch (NumberFormatException e) {
                log.warn("Invalid thread config '{}', falling back to auto", threads);
                int cores = Runtime.getRuntime().availableProcessors();
                int threadCount = Math.max(2, cores - 2);
                log.info("Detected CPU cores: {}", cores);
                log.info("Using ingestion threads: {}", threadCount);
                return threadCount;
            }
        }
    }
}
