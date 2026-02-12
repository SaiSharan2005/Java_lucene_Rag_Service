package com.production.lucene_service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobStatus {

    private final String jobId;
    private volatile String status;
    private final AtomicInteger documentsProcessed = new AtomicInteger(0);
    private final AtomicInteger chunksProcessed = new AtomicInteger(0);
    private volatile String exportFileName;
    private final Instant startTime;
    private volatile Instant endTime;
    private volatile String errorMessage;
    private final int totalFiles;

    public JobStatus(String jobId, int totalFiles) {
        this.jobId = jobId;
        this.totalFiles = totalFiles;
        this.status = "PROCESSING";
        this.startTime = Instant.now();
    }

    public void incrementDocuments() {
        documentsProcessed.incrementAndGet();
    }

    public void addChunks(int count) {
        chunksProcessed.addAndGet(count);
    }

    public void complete(String exportFileName) {
        this.exportFileName = exportFileName;
        this.status = "COMPLETED";
        this.endTime = Instant.now();
    }

    public void fail(String errorMessage) {
        this.errorMessage = errorMessage;
        this.status = "FAILED";
        this.endTime = Instant.now();
    }

    // Custom getters for AtomicInteger fields (Jackson serialization)
    public int getDocumentsProcessed() {
        return documentsProcessed.get();
    }

    public int getChunksProcessed() {
        return chunksProcessed.get();
    }
}
