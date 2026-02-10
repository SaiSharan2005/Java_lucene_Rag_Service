package com.production.lucene_service.lucene;

import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.Chunk;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
public class LuceneIndexService {

    private final IndexWriter indexWriter;
    private final AppConfig appConfig;
    private final ReentrantLock writeLock = new ReentrantLock();

    public LuceneIndexService(IndexWriter indexWriter, AppConfig appConfig) {
        this.indexWriter = indexWriter;
        this.appConfig = appConfig;
    }

    public void indexChunks(List<Chunk> chunks) throws IOException {
        writeLock.lock();
        try {
            int batchSize = appConfig.getLucene().getBatchCommitSize();
            int indexed = 0;

            for (Chunk chunk : chunks) {
                Document doc = createDocument(chunk);
                indexWriter.addDocument(doc);
                indexed++;

                if (indexed % batchSize == 0) {
                    log.debug("Indexed {} chunks so far...", indexed);
                }
            }

            indexWriter.commit();
            log.info("Successfully indexed and committed {} chunks for document: {}",
                    chunks.size(),
                    chunks.isEmpty() ? "N/A" : chunks.get(0).getDocumentId());
        } finally {
            writeLock.unlock();
        }
    }

    public void indexChunk(Chunk chunk) throws IOException {
        writeLock.lock();
        try {
            Document doc = createDocument(chunk);
            indexWriter.addDocument(doc);
            log.debug("Indexed chunk: {}", chunk.getChunkId());
        } finally {
            writeLock.unlock();
        }
    }

    public void commit() throws IOException {
        writeLock.lock();
        try {
            indexWriter.commit();
            log.info("Index committed successfully");
        } finally {
            writeLock.unlock();
        }
    }

    public void deleteByDocumentId(String documentId) throws IOException {
        writeLock.lock();
        try {
            indexWriter.deleteDocuments(new Term("document_id", documentId));
            indexWriter.commit();
            log.info("Deleted all chunks for document: {}", documentId);
        } finally {
            writeLock.unlock();
        }
    }

    public long getDocumentCount() throws IOException {
        return indexWriter.getDocStats().numDocs;
    }

    private Document createDocument(Chunk chunk) {
        Document doc = new Document();

        // Content field - TextField for full-text search with BM25
        doc.add(new TextField("content", chunk.getContent(), Field.Store.YES));

        // Document ID - StringField for exact match filtering
        doc.add(new StringField("document_id", chunk.getDocumentId(), Field.Store.YES));

        // Chunk ID - StringField for exact match (unique identifier)
        doc.add(new StringField("chunk_id", chunk.getChunkId(), Field.Store.YES));

        // Page number - IntPoint for range queries + StoredField for retrieval
        doc.add(new IntPoint("page_number", chunk.getPageNumber()));
        doc.add(new StoredField("page_number_stored", chunk.getPageNumber()));

        // Chunk index - IntPoint for range queries + StoredField for retrieval
        doc.add(new IntPoint("chunk_index", chunk.getChunkIndex()));
        doc.add(new StoredField("chunk_index_stored", chunk.getChunkIndex()));

        // Token count - StoredField for metadata
        doc.add(new StoredField("token_count", chunk.getTokenCount()));

        // Created timestamp - StoredField for audit
        doc.add(new StoredField("created_at", chunk.getCreatedAt()));

        return doc;
    }
}
