# Lucene Search Service

A Spring Boot REST API for PDF ingestion, full-text search, and chunk export. The service extracts text from PDFs, chunks them intelligently, indexes them for BM25-based search, and exports chunks as structured JSON for offline embedding generation.

## Features

- **PDF Ingestion** - Upload single or multiple PDFs via REST API
- **Async Background Processing** - Non-blocking ingestion with job status polling
- **Text Extraction** - Apache PDFBox with PDF metadata extraction (title, author)
- **Intelligent Chunking** - Token-based chunking with configurable overlap
- **Full-Text Search** - Apache Lucene 9.11 with BM25 ranking
- **Chunk JSON Export** - Streaming export for offline embedding generation (Kaggle, etc.)
- **Memory-Safe** - Processes one PDF at a time; streams JSON via Jackson JsonGenerator

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 21 |
| Spring Boot | 3.2.5 |
| Apache Lucene | 9.11.1 |
| Apache PDFBox | 3.0.2 |
| Lombok | Latest |
| Maven | 3.x |

## Quick Start

```bash
cd lucene-service
mvn clean package -DskipTests
mvn spring-boot:run
```

The service starts on **http://localhost:8080**

### Verify

```bash
curl http://localhost:8080/api/v1/ingest/health
# {"status": "UP"}
```

---

## API Endpoints

### Ingestion

#### Upload PDFs (Async)

```bash
POST /api/v1/ingest/pdf
Content-Type: multipart/form-data
```

Accepts one or more PDF files. Returns immediately with a `jobId` while processing happens in the background.

```bash
# Single file
curl -X POST http://localhost:8080/api/v1/ingest/pdf \
  -F "file=@paper.pdf"

# Multiple files
curl -X POST http://localhost:8080/api/v1/ingest/pdf \
  -F "file=@paper1.pdf" \
  -F "file=@paper2.pdf" \
  -F "file=@paper3.pdf"
```

**Response (HTTP 202):**
```json
{
  "jobId": "job_1c98a739-737",
  "status": "PROCESSING",
  "message": "3 file(s) submitted for background processing"
}
```

#### Poll Job Status

```bash
GET /api/v1/ingest/status/{jobId}
```

```json
{
  "jobId": "job_1c98a739-737",
  "status": "COMPLETED",
  "documentsProcessed": 3,
  "chunksProcessed": 87,
  "totalFiles": 3,
  "exportFileName": "2026-02-12T19-14-54-571.json",
  "startTime": "2026-02-12T13:44:53Z",
  "endTime": "2026-02-12T13:44:59Z"
}
```

Status values: `PROCESSING` | `COMPLETED` | `FAILED`

#### Delete Document

```bash
DELETE /api/v1/ingest/document/{documentId}
```

#### Get Stats

```bash
GET /api/v1/ingest/stats
# {"indexedChunks": 239, "status": "healthy"}
```

#### Health Check

```bash
GET /api/v1/ingest/health
# {"status": "UP"}
```

---

### Search

#### Search (GET)

```bash
GET /api/v1/search?q={query}&topK={n}&documentId={id}
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `q` | Yes | - | Search query |
| `topK` | No | 10 | Number of results (max 100) |
| `documentId` | No | - | Limit to specific document |

```bash
curl "http://localhost:8080/api/v1/search?q=neural+network&topK=5"
```

```json
{
  "query": "neural network",
  "totalHits": 127,
  "searchTimeMs": 3,
  "results": [
    {
      "documentId": "abc123",
      "chunkId": "abc123_p3_c5_9e6779a3",
      "content": "Neural networks are computing systems...",
      "score": 8.234,
      "pageNumber": 3
    }
  ]
}
```

#### Search (POST)

```bash
POST /api/v1/search
Content-Type: application/json

{"query": "neural network", "topK": 10}
```

#### Index Statistics

```bash
GET /api/v1/search/chunk-stats
# {"totalChunks": 76154, "totalTokens": 29200000}
```

---

## Chunk JSON Export

When `rag.export.enabled: true`, every ingestion job automatically exports all chunks to a timestamped JSON file in `./chunk-exports/`.

### Export Format

```json
[
  {
    "id": "4f9b2125_p1_c0_276970f7",
    "document_id": "4f9b2125-7675-4055-9a1a-f86c36dda75b",
    "content": "Neural networks are computing systems inspired by...",
    "metadata": {
      "source": "2601.09208v2.pdf",
      "title": "Mikasa: A Character-Driven Emotional AI Companion",
      "author": "Miki Ueno",
      "page_number": 1,
      "total_pages": 15,
      "chunk_index": 0,
      "chunk_position": "start",
      "token_count": 388,
      "created_at": "2026-02-12T13:44:55.762Z"
    }
  }
]
```

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Original PDF filename |
| `title` | string | PDF document title (omitted if not available) |
| `author` | string | PDF author (omitted if not available) |
| `page_number` | int | Page the chunk was extracted from |
| `total_pages` | int | Total pages in the source PDF |
| `chunk_index` | int | Sequential index within the document |
| `chunk_position` | string | `"start"` / `"middle"` / `"end"` position in document |
| `token_count` | int | Number of tokens in the chunk |
| `created_at` | string | ISO 8601 timestamp |

### Export Behavior

- One JSON file per ingestion job (timestamped: `2026-02-12T19-14-54-571.json`)
- Streamed via Jackson JsonGenerator (memory-safe for thousands of PDFs)
- Only one PDF's chunks in memory at a time
- `title` and `author` are omitted (not null) when PDF metadata is unavailable

---

## Configuration

`src/main/resources/application.yml`:

```yaml
server:
  port: 8080

spring:
  servlet:
    multipart:
      max-file-size: 50MB       # Per-file limit
      max-request-size: 5GB     # Total request limit (for bulk uploads)

lucene:
  index-path: ./lucene-index
  bm25:
    k1: 1.2                     # Term frequency saturation
    b: 0.75                     # Document length normalization
  batch-commit-size: 100

chunking:
  chunk-size-tokens: 400        # Target tokens per chunk
  chunk-overlap-tokens: 50      # Overlap between consecutive chunks
  min-chunk-length-tokens: 100  # Minimum chunk size

rag:
  export:
    enabled: true               # Enable/disable chunk JSON export
    path: ./chunk-exports       # Export directory

logging:
  level:
    com.production.lucene_service: DEBUG
    org.apache.lucene: INFO
    org.apache.pdfbox: WARN
```

---

## Architecture

### Async Ingestion Flow

```
Client                    Controller                 IngestionJobService          PdfIngestionService
  |                          |                              |                           |
  |-- POST /ingest/pdf ----->|                              |                           |
  |                          |-- validate files             |                           |
  |                          |-- save to temp dir           |                           |
  |                          |-- startJob(pendingFiles) --->|                           |
  |<-- 202 {jobId} ---------|                              |                           |
  |                          |                              |-- @Async processJob()     |
  |                          |                              |   open JsonGenerator      |
  |                          |                              |   for each file:          |
  |                          |                              |     ingestPdf() --------->|
  |                          |                              |     <-- chunks -----------|
  |                          |                              |     write chunks to JSON  |
  |                          |                              |     update JobStatus      |
  |                          |                              |     delete temp file      |
  |                          |                              |   close JsonGenerator     |
  |                          |                              |   status = COMPLETED      |
  |                          |                              |                           |
  |-- GET /status/{jobId} -->|                              |                           |
  |<-- {status, chunks} -----|                              |                           |
```

### Thread Pool

- Core pool: 2 threads
- Max pool: 2 threads
- Queue capacity: 10 jobs
- Rejected jobs are logged (not silently dropped)

### Memory Safety

- Files saved to temp directory before async processing (MultipartFile is request-scoped)
- One PDF processed at a time per job (no cross-file chunk accumulation)
- Jackson JsonGenerator streams directly to OutputStream
- Temp files cleaned up after each PDF is processed

---

## Project Structure

```
lucene-service/
├── src/main/java/com/production/lucene_service/
│   ├── LuceneServiceApplication.java
│   ├── config/
│   │   ├── AppConfig.java              # Spring configuration properties
│   │   └── AsyncConfig.java            # @Async thread pool config
│   ├── controller/
│   │   ├── PdfIngestionController.java # /api/v1/ingest endpoints
│   │   └── SearchController.java       # /api/v1/search endpoints
│   ├── lucene/
│   │   ├── LuceneConfig.java           # Lucene analyzer, similarity config
│   │   ├── LuceneIndexService.java     # Index write operations
│   │   └── LuceneSearchService.java    # Search/query operations
│   ├── model/
│   │   ├── Chunk.java                  # Chunk entity
│   │   ├── ChunkExportDTO.java         # JSON export structure
│   │   ├── IngestionResponse.java      # API response DTO
│   │   ├── IngestionStatus.java        # PROCESSING/SUCCESS/FAILED enum
│   │   ├── JobStatus.java              # Thread-safe job tracking
│   │   ├── PageContent.java            # Extracted page data
│   │   ├── SearchResponse.java         # Search response DTO
│   │   └── SearchResult.java           # Individual search result
│   └── service/
│       ├── ChunkingService.java        # Token-based text chunking
│       ├── IngestionJobService.java    # Async job processor + JSON export
│       ├── PdfIngestionService.java    # PDF extract → clean → chunk → index
│       └── TextCleaningService.java    # Text preprocessing/cleaning
├── src/main/resources/
│   ├── application.yml
│   └── stopwords.txt
├── lucene-index/                       # Generated Lucene index files
├── chunk-exports/                      # Exported JSON files
├── pom.xml
└── README.md
```

---

## Performance

Benchmarked with arXiv AI/ML research papers:

| PDFs | Chunks | Tokens | Index Size | Avg Latency | P95 Latency | QPS |
|------|--------|--------|------------|-------------|-------------|-----|
| 100 | 2,906 | 1.1M | 6.8 MB | 19 ms | 42 ms | 46 |
| 200 | 8,810 | 3.4M | 19.9 MB | 22 ms | 45 ms | 40 |
| 400 | 20,532 | 7.9M | 45.4 MB | 22 ms | 43 ms | 39 |
| 800 | 44,086 | 16.9M | 96.3 MB | 16 ms | 37 ms | 55 |
| 1082 | 76,154 | 29.2M | 168.7 MB | 17 ms | 33 ms | 51 |

Ingestion rate: ~1.2 PDFs/sec (includes text extraction, chunking, indexing, and JSON export).

---

## Troubleshooting

### Port Already in Use
```bash
netstat -ano | findstr :8080    # Windows
lsof -i :8080                   # Linux/Mac
```

### Out of Memory
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx4g"
```

### Index Corruption
```bash
rm -rf lucene-index/
mvn spring-boot:run
# Re-ingest PDFs
```

### 413 Payload Too Large
Increase `max-request-size` in `application.yml`. Currently set to 5GB for bulk uploads.

### PDF Extraction Issues
- Ensure PDF is not password-protected
- Scanned PDFs without OCR may have no extractable text
- Corrupted PDFs will be skipped (job continues with remaining files)

---

## License

This project is part of the Production RAG system.
