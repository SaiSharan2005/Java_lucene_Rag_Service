package com.production.lucene_service.service;

import com.production.lucene_service.config.AppConfig;
import com.production.lucene_service.model.ArxivPaperInfo;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Client for the arXiv API and PDF downloads.
 *
 * Uses JDK built-in HttpClient (no external dependencies).
 * Includes rate limiting and exponential backoff retry for 503/429 responses.
 */
@Service
@Slf4j
public class ArxivApiClient {

    private static final String ARXIV_API_BASE = "http://export.arxiv.org/api/query";
    public static final String ARXIV_PDF_BASE = "https://arxiv.org/pdf/";
    private static final String ATOM_NS = "http://www.w3.org/2005/Atom";
    private static final int PAGE_SIZE = 2000;

    private final AppConfig appConfig;
    private HttpClient httpClient;

    public ArxivApiClient(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @PostConstruct
    public void init() {
        var arxivConfig = appConfig.getArxiv();
        try {
            // Trust all certificates (mirrors Python fetcher's verify=False for corporate wifi/proxy)
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }}, new SecureRandom());

            this.httpClient = HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .connectTimeout(Duration.ofSeconds(arxivConfig.getConnectTimeoutSeconds()))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            log.info("ArxivApiClient initialized — connectTimeout={}s, readTimeout={}s, maxRetries={} (SSL verification disabled)",
                    arxivConfig.getConnectTimeoutSeconds(),
                    arxivConfig.getReadTimeoutSeconds(),
                    arxivConfig.getMaxRetries());
        } catch (Exception e) {
            // Fallback to default SSL if custom context fails
            log.warn("Failed to create custom SSLContext, falling back to default: {}", e.getMessage());
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(arxivConfig.getConnectTimeoutSeconds()))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
        }
    }

    /**
     * Search arXiv by category, returning paper metadata.
     * Paginates in chunks of 2000 (arXiv API max per request).
     */
    public List<ArxivPaperInfo> searchByCategory(String category, int maxResults) throws IOException, InterruptedException {
        List<ArxivPaperInfo> allPapers = new ArrayList<>();
        int remaining = maxResults;
        int start = 0;

        while (remaining > 0) {
            int batchSize = Math.min(remaining, PAGE_SIZE);
            String encodedQuery = URLEncoder.encode("cat:" + category, StandardCharsets.UTF_8);
            String url = ARXIV_API_BASE
                    + "?search_query=" + encodedQuery
                    + "&start=" + start
                    + "&max_results=" + batchSize
                    + "&sortBy=submittedDate&sortOrder=descending";

            log.info("Querying arXiv API: category={}, start={}, batchSize={}", category, start, batchSize);

            String xml = executeWithRetry(url);
            List<ArxivPaperInfo> batch = parseAtomFeed(xml);

            if (batch.isEmpty()) {
                log.info("No more results from arXiv API at offset {}", start);
                break;
            }

            allPapers.addAll(batch);
            start += batch.size();
            remaining -= batch.size();

            log.info("Fetched {} papers (total so far: {})", batch.size(), allPapers.size());

            // Rate limit between API pages
            if (remaining > 0) {
                Thread.sleep(appConfig.getArxiv().getApiDelayMs());
            }
        }

        log.info("arXiv search complete: {} papers found for category '{}'", allPapers.size(), category);
        return allPapers;
    }

    /**
     * Download a PDF from arXiv to the target directory.
     * Returns the path to the downloaded file.
     */
    public Path downloadPdf(String paperId, Path targetDir) throws IOException, InterruptedException {
        String url = ARXIV_PDF_BASE + paperId + ".pdf";
        Path targetFile = targetDir.resolve(paperId.replace("/", "_") + ".pdf");

        var arxivConfig = appConfig.getArxiv();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(arxivConfig.getReadTimeoutSeconds()))
                .GET()
                .build();

        int maxRetries = arxivConfig.getMaxRetries();
        IOException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    long backoffMs = (long) Math.pow(2, attempt) * 1000;
                    log.warn("PDF download retry {}/{} for {} — waiting {}ms", attempt, maxRetries, paperId, backoffMs);
                    Thread.sleep(backoffMs);
                }

                HttpResponse<Path> response = httpClient.send(request, HttpResponse.BodyHandlers.ofFile(targetFile));

                if (response.statusCode() == 200) {
                    log.debug("Downloaded PDF: {} → {}", paperId, targetFile);
                    return targetFile;
                } else if (response.statusCode() == 429 || response.statusCode() == 503) {
                    log.warn("arXiv returned {} for PDF {} — will retry", response.statusCode(), paperId);
                    lastException = new IOException("HTTP " + response.statusCode() + " downloading " + paperId);
                } else {
                    throw new IOException("HTTP " + response.statusCode() + " downloading PDF for " + paperId);
                }
            } catch (IOException e) {
                lastException = e;
                if (attempt == maxRetries) break;
            }
        }

        throw lastException != null ? lastException : new IOException("Failed to download PDF for " + paperId);
    }

    /**
     * Execute an HTTP GET with exponential backoff retry on 503/429.
     */
    private String executeWithRetry(String url) throws IOException, InterruptedException {
        var arxivConfig = appConfig.getArxiv();
        int maxRetries = arxivConfig.getMaxRetries();
        IOException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                long backoffMs = (long) Math.pow(2, attempt) * 1000;
                log.warn("API retry {}/{} — waiting {}ms", attempt, maxRetries, backoffMs);
                Thread.sleep(backoffMs);
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(arxivConfig.getReadTimeoutSeconds()))
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    return response.body();
                } else if (response.statusCode() == 429 || response.statusCode() == 503) {
                    log.warn("arXiv API returned {} — will retry", response.statusCode());
                    lastException = new IOException("HTTP " + response.statusCode());
                } else {
                    throw new IOException("arXiv API returned HTTP " + response.statusCode());
                }
            } catch (IOException e) {
                lastException = e;
                if (attempt == maxRetries) break;
            }
        }

        throw lastException != null ? lastException : new IOException("Failed after " + maxRetries + " retries");
    }

    /**
     * Parse arXiv Atom XML feed into ArxivPaperInfo list.
     */
    private List<ArxivPaperInfo> parseAtomFeed(String xml) throws IOException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

            NodeList entries = doc.getElementsByTagNameNS(ATOM_NS, "entry");
            List<ArxivPaperInfo> papers = new ArrayList<>();

            for (int i = 0; i < entries.getLength(); i++) {
                Element entry = (Element) entries.item(i);

                String id = getElementText(entry, "id");
                String title = getElementText(entry, "title");
                String summary = getElementText(entry, "summary");
                String published = getElementText(entry, "published");

                // Extract paper ID from the full arXiv URL (e.g. http://arxiv.org/abs/2602.12276v1)
                String paperId = id;
                if (id != null && id.contains("/abs/")) {
                    paperId = id.substring(id.lastIndexOf("/abs/") + 5);
                }

                // Collect authors
                StringBuilder authors = new StringBuilder();
                NodeList authorNodes = entry.getElementsByTagNameNS(ATOM_NS, "author");
                for (int j = 0; j < authorNodes.getLength(); j++) {
                    Element authorEl = (Element) authorNodes.item(j);
                    String name = getElementText(authorEl, "name");
                    if (name != null) {
                        if (!authors.isEmpty()) authors.append(", ");
                        authors.append(name);
                    }
                }

                // Find PDF link
                String pdfUrl = null;
                NodeList links = entry.getElementsByTagNameNS(ATOM_NS, "link");
                for (int j = 0; j < links.getLength(); j++) {
                    Element link = (Element) links.item(j);
                    if ("application/pdf".equals(link.getAttribute("type"))
                            || (link.getAttribute("href") != null && link.getAttribute("href").contains("/pdf/"))) {
                        pdfUrl = link.getAttribute("href");
                        break;
                    }
                }

                if (pdfUrl == null && paperId != null) {
                    pdfUrl = ARXIV_PDF_BASE + paperId + ".pdf";
                }

                // Clean whitespace from title and summary
                if (title != null) title = title.replaceAll("\\s+", " ").trim();
                if (summary != null) summary = summary.replaceAll("\\s+", " ").trim();

                papers.add(ArxivPaperInfo.builder()
                        .paperId(paperId)
                        .title(title)
                        .authors(authors.toString())
                        .summary(summary)
                        .publishedDate(published)
                        .pdfUrl(pdfUrl)
                        .build());
            }

            return papers;
        } catch (Exception e) {
            throw new IOException("Failed to parse arXiv Atom feed: " + e.getMessage(), e);
        }
    }

    private String getElementText(Element parent, String localName) {
        NodeList nodes = parent.getElementsByTagNameNS(ATOM_NS, localName);
        if (nodes.getLength() > 0) {
            return nodes.item(0).getTextContent();
        }
        return null;
    }
}
