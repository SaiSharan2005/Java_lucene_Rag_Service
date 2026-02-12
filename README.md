# Lucene Search Service

A Spring Boot REST API service for PDF ingestion and full-text search using Apache Lucene. The service extracts text from PDFs, chunks them intelligently, and indexes them for fast BM25-based search.

## Features

- PDF text extraction using Apache PDFBox
- Intelligent text chunking with configurable overlap
- Full-text search with Apache Lucene 9.11
- BM25 ranking algorithm
- REST API for ingestion and search
- Document-scoped search support

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 21 |
| Spring Boot | 3.2.5 |
| Apache Lucene | 9.11.1 |
| Apache PDFBox | 3.0.2 |
| Maven | 3.x |

## Prerequisites

- **Java 21** or higher
- **Maven 3.x**

## Quick Start

```bash
# Clone and navigate to the service
cd lucene-service

# Build the project
mvn clean package -DskipTests

# Run the service
mvn spring-boot:run
```

The service starts on **http://localhost:8080**

## Verify Service is Running

```bash
curl http://localhost:8080/api/v1/ingest/health
```

Expected response:
```json
{"status": "UP"}
```

---

## API Endpoints

### Ingestion API

#### Upload PDF
```bash
POST /api/v1/ingest/pdf
Content-Type: multipart/form-data

# Parameters:
# - file: PDF file (required)
# - documentId: Custom document ID (optional)
```

**Example:**
```bash
curl -X POST http://localhost:8080/api/v1/ingest/pdf \
  -F "file=@document.pdf"
```

**Response:**
```json
{
  "status": "SUCCESS",
  "documentId": "abc123",
  "totalChunks": 45,
  "totalTokens": 18000,
  "message": "PDF processed successfully"
}
```

#### Delete Document
```bash
DELETE /api/v1/ingest/document/{documentId}
```

#### Get Ingestion Stats
```bash
GET /api/v1/ingest/stats
```

---

### Search API

#### Search (GET)
```bash
GET /api/v1/search?q={query}&topK={n}&documentId={id}

# Parameters:
# - q: Search query (required)
# - topK: Number of results (default: 10, max: 100)
# - documentId: Limit search to specific document (optional)
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/search?q=neural+network&topK=5"
```

**Response:**
```json
{
  "query": "neural network",
  "totalHits": 127,
  "searchTimeMs": 3,
  "results": [
    {
      "documentId": "abc123",
      "chunkId": "abc123_chunk_5",
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

{
  "query": "neural network",
  "topK": 10,
  "documentId": "optional-doc-id"
}
```

#### Get Index Statistics
```bash
GET /api/v1/search/chunk-stats
```

**Response:**
```json
{
  "totalChunks": 76154,
  "totalTokens": 29200000
}
```

---

## Configuration

Edit `src/main/resources/application.yml`:

```yaml
server:
  port: 8080

spring:
  servlet:
    multipart:
      max-file-size: 50MB
      max-request-size: 50MB

lucene:
  index-path: ./lucene-index      # Where index files are stored
  bm25:
    k1: 1.2                       # BM25 term frequency saturation
    b: 0.75                       # BM25 document length normalization
  batch-commit-size: 100          # Commits after N documents

chunking:
  chunk-size-tokens: 400          # Target tokens per chunk
  chunk-overlap-tokens: 50        # Overlap between chunks
  min-chunk-length-tokens: 100    # Minimum chunk size

logging:
  level:
    com.production.lucene_service: DEBUG
```

---

## Project Structure

```
lucene-service/
├── src/main/java/com/production/lucene_service/
│   ├── LuceneServiceApplication.java     # Main entry point
│   ├── config/
│   │   └── AppConfig.java                # Spring configuration
│   ├── controller/
│   │   ├── PdfIngestionController.java   # /api/v1/ingest endpoints
│   │   └── SearchController.java         # /api/v1/search endpoints
│   ├── lucene/
│   │   ├── LuceneConfig.java             # Lucene configuration
│   │   ├── LuceneIndexService.java       # Indexing operations
│   │   └── LuceneSearchService.java      # Search operations
│   ├── model/
│   │   ├── Chunk.java                    # Chunk entity
│   │   ├── SearchResponse.java           # Search response DTO
│   │   └── IngestionResponse.java        # Ingestion response DTO
│   └── service/
│       ├── PdfIngestionService.java      # PDF processing logic
│       ├── ChunkingService.java          # Text chunking logic
│       └── TextCleaningService.java      # Text preprocessing
├── src/main/resources/
│   ├── application.yml                   # Configuration
│   └── stopwords.txt                     # Custom stopwords
├── lucene-index/                         # Generated index files
├── pom.xml                               # Maven dependencies
└── README.md
```

---

## Index Management

### Index Location
The Lucene index is stored in `./lucene-index/` by default.

### Clear Index
To clear the index and start fresh:

```bash
# Stop the server first
# Windows
taskkill //F //IM java.exe

# Linux/Mac
pkill -f lucene

# Delete index folder
rm -rf lucene-index/

# Restart server
mvn spring-boot:run
```

### Index Size
Monitor index size:
```bash
# Linux/Mac
du -sh lucene-index/

# Windows
dir lucene-index
```

---

## Performance

Based on benchmarks with AI/ML research papers:

| PDFs | Chunks | Tokens | Index Size | Avg Latency | P95 Latency | QPS |
|------|--------|--------|------------|-------------|-------------|-----|
| 100 | 2,906 | 1.1M | 6.8 MB | 19 ms | 42 ms | 46 |
| 200 | 8,810 | 3.4M | 19.9 MB | 22 ms | 45 ms | 40 |
| 400 | 20,532 | 7.9M | 45.4 MB | 22 ms | 43 ms | 39 |
| 800 | 44,086 | 16.9M | 96.3 MB | 16 ms | 37 ms | 55 |
| 1082 | 76,154 | 29.2M | 168.7 MB | 17 ms | 33 ms | 51 |

---

## Building

### Development Build
```bash
mvn clean compile
```

### Production Build
```bash
mvn clean package -DskipTests
java -jar target/lucene-service-1.0.0-SNAPSHOT.jar
```

### Run Tests
```bash
mvn test
```

---

## Troubleshooting

### Port Already in Use
```bash
# Find process using port 8080
netstat -ano | findstr :8080    # Windows
lsof -i :8080                    # Linux/Mac

# Kill the process or change port in application.yml
```

### Out of Memory
Increase JVM heap size:
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx4g"
```

### Index Corruption
If you see index errors, delete and rebuild:
```bash
rm -rf lucene-index/
mvn spring-boot:run
# Re-ingest all PDFs
```

### PDF Extraction Fails
- Ensure PDF is not password-protected
- Check PDF is not corrupted
- Some scanned PDFs may have no extractable text

---

## License

This project is part of the Production RAG system.
