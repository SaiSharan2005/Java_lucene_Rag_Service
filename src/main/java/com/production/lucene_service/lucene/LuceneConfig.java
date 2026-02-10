package com.production.lucene_service.lucene;

import com.production.lucene_service.config.AppConfig;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

@Configuration
@Slf4j
public class LuceneConfig {

    private final AppConfig appConfig;
    private final ResourceLoader resourceLoader;
    private Directory directory;
    private IndexWriter indexWriter;

    public LuceneConfig(AppConfig appConfig, ResourceLoader resourceLoader) {
        this.appConfig = appConfig;
        this.resourceLoader = resourceLoader;
    }

    @Bean
    public Directory luceneDirectory() throws IOException {
        Path indexPath = Paths.get(appConfig.getLucene().getIndexPath());
        if (!Files.exists(indexPath)) {
            Files.createDirectories(indexPath);
            log.info("Created Lucene index directory at: {}", indexPath.toAbsolutePath());
        }
        this.directory = FSDirectory.open(indexPath);
        log.info("Initialized FSDirectory at: {}", indexPath.toAbsolutePath());
        return directory;
    }

    @Bean
    public CharArraySet stopwords() throws IOException {
        Set<String> stopwordSet = new HashSet<>();
        String stopwordsPath = appConfig.getLucene().getStopwordsPath();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        resourceLoader.getResource(stopwordsPath).getInputStream(),
                        StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    stopwordSet.add(line.toLowerCase());
                }
            }
        }

        log.info("Loaded {} custom stopwords from {}", stopwordSet.size(), stopwordsPath);
        return new CharArraySet(stopwordSet, true);
    }

    @Bean
    public StandardAnalyzer standardAnalyzer(CharArraySet stopwords) {
        StandardAnalyzer analyzer = new StandardAnalyzer(stopwords);
        log.info("Initialized StandardAnalyzer with custom stopwords");
        return analyzer;
    }

    @Bean
    public BM25Similarity bm25Similarity() {
        float k1 = appConfig.getLucene().getBm25().getK1();
        float b = appConfig.getLucene().getBm25().getB();
        BM25Similarity similarity = new BM25Similarity(k1, b);
        log.info("Initialized BM25Similarity with k1={}, b={}", k1, b);
        return similarity;
    }

    @Bean
    public IndexWriter indexWriter(Directory luceneDirectory,
                                   StandardAnalyzer standardAnalyzer,
                                   BM25Similarity bm25Similarity) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(standardAnalyzer);
        config.setSimilarity(bm25Similarity);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        config.setCommitOnClose(true);

        this.indexWriter = new IndexWriter(luceneDirectory, config);
        log.info("Initialized IndexWriter with BM25Similarity");
        return indexWriter;
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (indexWriter != null && indexWriter.isOpen()) {
                indexWriter.close();
                log.info("IndexWriter closed successfully");
            }
            if (directory != null) {
                directory.close();
                log.info("Directory closed successfully");
            }
        } catch (IOException e) {
            log.error("Error closing Lucene resources", e);
        }
    }
}
