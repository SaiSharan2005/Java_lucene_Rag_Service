package com.production.lucene_service.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "")
@Getter
@Setter
public class AppConfig {

    private Lucene lucene = new Lucene();
    private Chunking chunking = new Chunking();
    private Rag rag = new Rag();

    @Getter
    @Setter
    public static class Lucene {
        private String indexPath = "./lucene-index";
        private Bm25 bm25 = new Bm25();
        private String stopwordsPath = "classpath:stopwords.txt";
        private int batchCommitSize = 100;

        @Getter
        @Setter
        public static class Bm25 {
            private float k1 = 1.2f;
            private float b = 0.75f;
        }
    }

    @Getter
    @Setter
    public static class Chunking {
        private int chunkSizeTokens = 400;
        private int chunkOverlapTokens = 50;
        private int minChunkLengthTokens = 100;
    }

    @Getter
    @Setter
    public static class Rag {
        private Export export = new Export();

        @Getter
        @Setter
        public static class Export {
            private boolean enabled = true;
            private String path = "./chunk-exports";
        }
    }
}
