package com.production.lucene_service.lucene;

import com.production.lucene_service.model.SearchResponse;
import com.production.lucene_service.model.SearchResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class LuceneSearchService {

    private final IndexWriter indexWriter;
    private final StandardAnalyzer analyzer;
    private final BM25Similarity similarity;

    public LuceneSearchService(IndexWriter indexWriter,
                               StandardAnalyzer analyzer,
                               BM25Similarity similarity) {
        this.indexWriter = indexWriter;
        this.analyzer = analyzer;
        this.similarity = similarity;
    }

    public SearchResponse search(String queryText, int topK) throws IOException, ParseException {
        long startTime = System.currentTimeMillis();

        // Get a reader from the writer (ensures we see latest commits)
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(similarity);

            // Parse the query for the content field
            QueryParser parser = new QueryParser("content", analyzer);
            Query query = parser.parse(queryText);

            // Execute search
            TopDocs topDocs = searcher.search(query, topK);

            List<SearchResult> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.storedFields().document(scoreDoc.doc);

                SearchResult result = SearchResult.builder()
                        .chunkId(doc.get("chunk_id"))
                        .documentId(doc.get("document_id"))
                        .content(doc.get("content"))
                        .pageNumber(doc.getField("page_number_stored").numericValue().intValue())
                        .chunkIndex(doc.getField("chunk_index_stored").numericValue().intValue())
                        .score(scoreDoc.score)
                        .build();

                results.add(result);
            }

            long searchTime = System.currentTimeMillis() - startTime;

            log.info("Search for '{}' returned {} results in {}ms",
                    queryText, results.size(), searchTime);

            return SearchResponse.builder()
                    .query(queryText)
                    .totalHits((int) topDocs.totalHits.value)
                    .searchTimeMs(searchTime)
                    .results(results)
                    .build();
        }
    }

    public SearchResponse searchByDocumentId(String queryText, String documentId, int topK)
            throws IOException, ParseException {
        long startTime = System.currentTimeMillis();

        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(similarity);

            // Parse the content query
            QueryParser parser = new QueryParser("content", analyzer);
            Query contentQuery = parser.parse(queryText);

            // Create document ID filter
            Query docIdQuery = new TermQuery(new org.apache.lucene.index.Term("document_id", documentId));

            // Combine queries with BooleanQuery
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
            booleanQuery.add(contentQuery, BooleanClause.Occur.MUST);
            booleanQuery.add(docIdQuery, BooleanClause.Occur.FILTER);

            // Execute search
            TopDocs topDocs = searcher.search(booleanQuery.build(), topK);

            List<SearchResult> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.storedFields().document(scoreDoc.doc);

                SearchResult result = SearchResult.builder()
                        .chunkId(doc.get("chunk_id"))
                        .documentId(doc.get("document_id"))
                        .content(doc.get("content"))
                        .pageNumber(doc.getField("page_number_stored").numericValue().intValue())
                        .chunkIndex(doc.getField("chunk_index_stored").numericValue().intValue())
                        .score(scoreDoc.score)
                        .build();

                results.add(result);
            }

            long searchTime = System.currentTimeMillis() - startTime;

            log.info("Search for '{}' in document '{}' returned {} results in {}ms",
                    queryText, documentId, results.size(), searchTime);

            return SearchResponse.builder()
                    .query(queryText)
                    .totalHits((int) topDocs.totalHits.value)
                    .searchTimeMs(searchTime)
                    .results(results)
                    .build();
        }
    }

    public Map<String, Object> getChunkStatistics() throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            int numDocs = reader.numDocs();

            List<Integer> tokenCounts = new ArrayList<>();
            int minTokens = Integer.MAX_VALUE;
            int maxTokens = 0;
            long totalTokens = 0;

            for (int i = 0; i < numDocs; i++) {
                Document doc = reader.storedFields().document(i);
                int tokenCount = doc.getField("token_count").numericValue().intValue();
                tokenCounts.add(tokenCount);
                totalTokens += tokenCount;
                minTokens = Math.min(minTokens, tokenCount);
                maxTokens = Math.max(maxTokens, tokenCount);
            }

            // Count chunks by size ranges
            int under50 = 0, under100 = 0, under200 = 0, under400 = 0, over400 = 0;
            for (int count : tokenCounts) {
                if (count < 50) under50++;
                else if (count < 100) under100++;
                else if (count < 200) under200++;
                else if (count < 400) under400++;
                else over400++;
            }

            Map<String, Object> stats = new HashMap<>();
            stats.put("totalChunks", numDocs);
            stats.put("totalTokens", totalTokens);
            stats.put("avgTokensPerChunk", numDocs > 0 ? totalTokens / numDocs : 0);
            stats.put("minTokens", numDocs > 0 ? minTokens : 0);
            stats.put("maxTokens", maxTokens);
            stats.put("chunksUnder50Tokens", under50);
            stats.put("chunks50to99Tokens", under100);
            stats.put("chunks100to199Tokens", under200);
            stats.put("chunks200to399Tokens", under400);
            stats.put("chunks400PlusTokens", over400);

            return stats;
        }
    }
}
