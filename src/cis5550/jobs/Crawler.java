package cis5550.jobs;

import cis5550.flame.BatchedKVSClient;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.CrawlerLogger;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Crawler {
    private static Logger log = Logger.getLogger(Crawler.class);

    // Thread-safe cache: host -> robots.txt content (empty string if host was checked but had no robots.txt)
    private static final ConcurrentHashMap<String, String> robotsCache = new ConcurrentHashMap<>();
    
    // Cached blacklist patterns - loaded once at first use, thread-safe
    private static volatile List<String> cachedBlacklistPatterns = null;
    private static final Object blacklistLock = new Object();
    
    // Shared executor for async external robots.txt fetches (reduced from 32 to 8)
    private static final ExecutorService childKvsExecutor = Executors.newFixedThreadPool(8);

    // Timeout constants
    private static final int CONNECT_TIMEOUT_MS = 1250;
    private static final int READ_TIMEOUT_MS = 2500;
    private static final int RESPONSE_CODE_HEAD_CONNECT_TIMEOUT = -1;
    private static final int RESPONSE_CODE_HEAD_READ_TIMEOUT = -2;
    private static final int RESPONSE_CODE_BODY_CONNECT_TIMEOUT = -3;
    private static final int RESPONSE_CODE_BODY_READ_TIMEOUT = -4;
    private static final int RESPONSE_CODE_DNS_FAILURE = -5;
    // New unified codes for streaming fetch
    private static final int RESPONSE_CODE_CONNECT_TIMEOUT = -6;
    private static final int RESPONSE_CODE_READ_TIMEOUT = -7;
    private static final int SORT_AND_TAKE_SAMPLE_SIZE = 75000;


    static CrawlerLogger clog;
    
    /**
     * Escapes HTML special characters to prevent HTML from disturbing logs/storage.
     * Escapes the same characters as Apache Commons Text's escapeHtml4():
     * & -> &amp;, < -> &lt;, > -> &gt;, " -> &quot;, ' -> &#39;
     */
    private static String escapeHtml4(String input) {
        if (input == null) {
            return null;
        }
        return input.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\"", "&quot;")
                    .replace("'", "&#39;");
    } 

    static class FetchResult {
        boolean isRedirect = false;
        String url;
        int responseCode;
        String contentType;
        Integer contentLength;
        String location;
        byte[] body;
    }

    // Helper class for bulk child URL processing
    static class ChildInfo {
        final String url;
        final String hash;
        final String host;
        final String anchors;
        
        ChildInfo(String url, String hash, String host, String anchors) {
            this.url = url;
            this.hash = hash;
            this.host = host;
            this.anchors = anchors;
        }
    }

    public static FetchResult getHeadDataFromURL(String url, CrawlerLogger logger) {
        HttpURLConnection conn = null;
        long startNanos = System.nanoTime();
        if (logger != null) {
            logger.headFetchStart(url);
        }
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("HEAD");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);

            try {
                conn.connect();
            } catch (SocketTimeoutException e) {
                log.error("[HEAD] Connection timeout: " + url);  
                FetchResult fr = new FetchResult();
                fr.url = url;
                fr.responseCode = RESPONSE_CODE_HEAD_CONNECT_TIMEOUT;
                if (logger != null) {
                    logger.headFail(url, "Head Connect Timeout");
                    logger.headFetchEnd(url, fr.responseCode, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }

            int code;
            String ct;
            try {
                code = conn.getResponseCode();
                ct = conn.getContentType();
            } catch (SocketTimeoutException e) {
                log.error("[HEAD] Read timeout: " + url);  
                FetchResult fr = new FetchResult();
                fr.url = url;
                fr.responseCode = RESPONSE_CODE_HEAD_READ_TIMEOUT;
                if (logger != null) {
                    logger.headFail(url, "Head Read Timeout");
                    logger.headFetchEnd(url, fr.responseCode, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }

            FetchResult fr = new FetchResult();
            fr.url = url;
            fr.responseCode = code;

            if (code == 200) {
                fr.contentType = ct;
                String contentLength = conn.getHeaderField("Content-Length");
                if (contentLength != null) {
                    try {
                        fr.contentLength = Integer.parseInt(contentLength);
                    } catch (NumberFormatException e) {
                        log.debug("[HEAD] Invalid Content-Length: " + contentLength);  
                    }
                }
                log.info("[HEAD] 200 OK: " + url);
                if (logger != null) {
                    logger.headSuccess(url, code, ct);
                    logger.headFetchEnd(url, code, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            } else if (code == 301 || code == 302 || code == 303 || code == 307 || code == 308) {
                fr.isRedirect = true;
                fr.location = conn.getHeaderField("Location");
                log.info("[HEAD] Redirect " + code + ": " + url + " -> " + fr.location);  
                if (logger != null) {
                    logger.headFetchEnd(url, code, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            } else {
                log.warn("[HEAD] Error " + code + ": " + url);
                if (logger != null) {
                    logger.headErrorCode(url, code);
                    logger.headFetchEnd(url, code, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }
        } catch (java.net.UnknownHostException e) {
            // DNS failure - return special code so URL can be requeued
            log.warn("[HEAD] DNS failure (will requeue): " + url + " - " + e.getMessage());
            FetchResult fr = new FetchResult();
            fr.url = url;
            fr.responseCode = RESPONSE_CODE_DNS_FAILURE;
            if (logger != null) {
                logger.headFail(url, "DNS_FAILURE: " + e.getMessage());
                logger.headFetchEnd(url, fr.responseCode, (System.nanoTime() - startNanos) / 1_000_000);
            }
            return fr;
        } catch (Exception e) {
            log.error("[HEAD] Exception: " + url + " - " + e.getMessage(), e);
            if (logger != null) {
                logger.headFail(url, e.getMessage());
                // Use synthetic code -1 to indicate HEAD exception in timing log
                logger.headFetchEnd(url, -1, (System.nanoTime() - startNanos) / 1_000_000);
            }
            return null;
        } finally {
            // Always disconnect to release the connection and prevent socket leaks
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static FetchResult getDataFromURL(String url, CrawlerLogger logger) {
        HttpURLConnection conn = null;
        long startNanos = System.nanoTime();
        if (logger != null) {
            logger.getFetchStart(url);
        }
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);

            int code;
            String ct;
            try {
                code = conn.getResponseCode();
                ct = conn.getContentType();
            } catch (SocketTimeoutException e) {
                log.error("[GET] Connection timeout: " + url);  
                FetchResult fr = new FetchResult();
                fr.url = url;
                fr.responseCode = RESPONSE_CODE_BODY_CONNECT_TIMEOUT;
                if (logger != null) {
                    logger.getFail(url, "Get Connection Timeout");
                    logger.getFetchEnd(url, fr.responseCode, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }

            if (code == 200) {
                try (InputStream is = conn.getInputStream()) {
                    // check for langauge tag in html - only checkign first 1.2Kb
                    byte[] initialBuffer = is.readNBytes(1200);

                    if (initialBuffer.length == 0) {
                        log.warn("[GET] Empty response: " + url);
                        return null;
                    }

                    // actual check done here
                    String snippet = new String(initialBuffer, StandardCharsets.UTF_8);
                    if (!isEnglishContent(snippet)) {
                        log.info("[GET] Non-English page filtered: " + url);

                        FetchResult fr = new FetchResult();
                        fr.url = url;
                        fr.responseCode = code;
                        fr.contentType = "filtered:non-english";
                        fr.body = null;
                        if (logger != null) {
                            logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                        }
                        return fr;
                    }


                    log.info("[GET] English page detected, downloading full content: " + url);

                    ByteArrayOutputStream fullContent = new ByteArrayOutputStream();
                    fullContent.write(initialBuffer, 0, initialBuffer.length); // Preserve first 1200 bytes

                    // get rest of page
                    byte[] buffer = new byte[8192];
                    int n;
                    try {
                        while ((n = is.read(buffer)) != -1) {
                            fullContent.write(buffer, 0, n);
                        }
                    } catch (SocketTimeoutException e) {
                        log.error("[GET] Read timeout (body): " + url);
                        FetchResult fr = new FetchResult();
                        fr.url = url;
                        fr.responseCode = RESPONSE_CODE_BODY_READ_TIMEOUT;
                        if (logger != null) {
                            logger.getFail(url, "Get Read Timeout");
                            logger.getFetchEnd(url, fr.responseCode, null, (System.nanoTime() - startNanos) / 1_000_000);
                        }
                        return fr;
                    }

                    byte[] bytes = fullContent.toByteArray();

                    FetchResult fr = new FetchResult();
                    fr.url = url;
                    fr.responseCode = code;
                    fr.contentType = ct;
                    if (ct != null && ct.startsWith("text/html")) {
                        fr.body = bytes;
                        log.info("[GET] 200 OK (" + bytes.length + " bytes): " + url);
                    } else {
                        fr.body = null;
                        log.info("[GET] 200 OK (non-HTML " + ct + "): " + url);
                    }
                    if (logger != null) {
                        logger.getSuccess(url, code, ct);
                        logger.getFetchEnd(url, code, (fr.body != null ? (long) bytes.length : null), (System.nanoTime() - startNanos) / 1_000_000);
                    }
                    return fr;
                }
            } else {
                log.warn("[GET] Non-200 response " + code + ": " + url);  
                FetchResult fr = new FetchResult();
                fr.url = url;
                fr.responseCode = code;
                if (logger != null) {
                    logger.getSuccess(url, code, null);
                    logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }
        } catch (Exception e) {
            log.error("[GET] Exception: " + url + " - " + e.getMessage(), e);
            if (logger != null) {
                logger.getFail(url, e.getMessage());
                // Use synthetic code -1 to indicate GET exception in timing log
                logger.getFetchEnd(url, -1, null, (System.nanoTime() - startNanos) / 1_000_000);
            }
            return null;
        } finally {
            // Always disconnect to release the connection and prevent socket leaks
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Streaming fetch that makes a single GET request, reads headers first,
     * and only downloads body if all early-abort conditions pass.
     * 
     * Early abort conditions (in order):
     * 1. Connect/header timeout
     * 2. DNS failure  
     * 3. Redirect (301/302/303/307/308)
     * 4. Non-200 response code
     * 5. Non-HTML content type
     * 6. Non-English content (first 1200 bytes check)
     * 
     * Benefits over HEAD+GET:
     * - Single HTTP connection instead of two
     * - Can abort before body download for non-HTML content types
     * - Simpler code flow
     */
    public static FetchResult streamingFetch(String url, CrawlerLogger logger) {
        HttpURLConnection conn = null;
        long startNanos = System.nanoTime();
        
        if (logger != null) {
            logger.getFetchStart(url);  // Reuse existing logger method
        }
        
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            
            // === PHASE 1: Get headers (no body download yet) ===
            int code;
            String ct;
            try {
                code = conn.getResponseCode();  // Sends request, receives headers only
                ct = conn.getContentType();
            } catch (SocketTimeoutException e) {
                log.error("[FETCH] Connect/header timeout: " + url);
                FetchResult fr = new FetchResult();
                fr.url = url;
                fr.responseCode = RESPONSE_CODE_CONNECT_TIMEOUT;
                if (logger != null) {
                    logger.getFail(url, "Connect Timeout");
                    logger.getFetchEnd(url, fr.responseCode, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }
            
            FetchResult fr = new FetchResult();
            fr.url = url;
            fr.responseCode = code;
            fr.contentType = ct;
            
            // Get Content-Length from headers (available without body download)
            String contentLength = conn.getHeaderField("Content-Length");
            if (contentLength != null) {
                try {
                    fr.contentLength = Integer.parseInt(contentLength);
                } catch (NumberFormatException e) {
                    log.debug("[FETCH] Invalid Content-Length: " + contentLength);
                }
            }
            
            // === EARLY ABORT CHECK 1: Redirects ===
            if (code == 301 || code == 302 || code == 303 || code == 307 || code == 308) {
                fr.isRedirect = true;
                fr.location = conn.getHeaderField("Location");
                log.info("[FETCH] Redirect " + code + ": " + url + " -> " + fr.location);
                if (logger != null) {
                    logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;  // No body download
            }
            
            // === EARLY ABORT CHECK 2: Non-200 ===
            if (code != 200) {
                log.warn("[FETCH] Non-200 response " + code + ": " + url);
                if (logger != null) {
                    logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;  // No body download
            }
            
            // === EARLY ABORT CHECK 3: Non-HTML content type ===
            if (ct == null || !ct.startsWith("text/html")) {
                log.info("[FETCH] Non-HTML content type (" + ct + "): " + url);
                fr.body = null;
                if (logger != null) {
                    logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;  // No body download - saves bandwidth!
            }
            
            // === PHASE 2: Download body (only for 200 + HTML) ===
            try (InputStream is = conn.getInputStream()) {
                // Read first 1200 bytes for English check
                byte[] initialBuffer = is.readNBytes(1700);
                
                if (initialBuffer.length == 0) {
                    log.warn("[FETCH] Empty response: " + url);
                    if (logger != null) {
                        logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                    }
                    return null;
                }
                
                // === EARLY ABORT CHECK 4: Non-English content ===
                String snippet = new String(initialBuffer, StandardCharsets.UTF_8);
                if (!isEnglishContent(snippet)) {
                    log.info("[FETCH] Non-English page filtered: " + url);
                    fr.contentType = "filtered:non-english";
                    fr.body = null;
                    if (logger != null) {
                        logger.getFetchEnd(url, code, null, (System.nanoTime() - startNanos) / 1_000_000);
                    }
                    return fr;  // Disconnects here, partial body only
                }
                
                // === PHASE 3: Download rest of body ===
                log.info("[FETCH] English page detected, downloading full content: " + url);
                ByteArrayOutputStream fullContent = new ByteArrayOutputStream();
                fullContent.write(initialBuffer, 0, initialBuffer.length);
                
                byte[] buffer = new byte[8192];
                int n;
                try {
                    while ((n = is.read(buffer)) != -1) {
                        fullContent.write(buffer, 0, n);
                    }
                } catch (SocketTimeoutException e) {
                    log.error("[FETCH] Read timeout (body): " + url);
                    fr.responseCode = RESPONSE_CODE_READ_TIMEOUT;
                    if (logger != null) {
                        logger.getFail(url, "Read Timeout");
                        logger.getFetchEnd(url, fr.responseCode, null, (System.nanoTime() - startNanos) / 1_000_000);
                    }
                    return fr;
                }
                
                byte[] bytes = fullContent.toByteArray();
                fr.body = bytes;
                log.info("[FETCH] 200 OK (" + bytes.length + " bytes): " + url);
                if (logger != null) {
                    logger.getSuccess(url, code, ct);
                    logger.getFetchEnd(url, code, (long) bytes.length, (System.nanoTime() - startNanos) / 1_000_000);
                }
                return fr;
            }
            
        } catch (java.net.UnknownHostException e) {
            log.warn("[FETCH] DNS failure (will requeue): " + url + " - " + e.getMessage());
            FetchResult fr = new FetchResult();
            fr.url = url;
            fr.responseCode = RESPONSE_CODE_DNS_FAILURE;
            if (logger != null) {
                logger.getFail(url, "DNS_FAILURE: " + e.getMessage());
                logger.getFetchEnd(url, fr.responseCode, null, (System.nanoTime() - startNanos) / 1_000_000);
            }
            return fr;
        } catch (Exception e) {
            log.error("[FETCH] Exception: " + url + " - " + e.getMessage(), e);
            if (logger != null) {
                logger.getFail(url, e.getMessage());
                logger.getFetchEnd(url, -1, null, (System.nanoTime() - startNanos) / 1_000_000);
            }
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static boolean isEnglishContent(String htmlSnippet) {
        // matches lang="en", lang='en', lang=en, lang="en-US", etc. - case insensitive
        // 
        Pattern pattern = Pattern.compile("(?i)\\s+lang\\s*=\\s*[\"']?en(?:-?[a-z]+)?[\"']?");
        Matcher matcher = pattern.matcher(htmlSnippet);

        if (matcher.find()) {
            log.debug("[isEnglishContent] English language tag found: " + matcher.group().trim());
            return true;
        }

        log.debug("[isEnglishContent] No English language tag found in snippet");
        return false;
    }

    /**
     * Public static method to extract URLs from HTML content.
     * Used by PageRank and other jobs.
     */
    public static List<String> extractUrls(String html, String baseUrl) {
        if (html == null || html.isEmpty()) {
            return new ArrayList<>();
        }
        // Use URLParser to extract URLs
        List<String> rawUrls = URLParser.extractURLs(html);
        // Normalize URLs using baseUrl
        return URLParser.normalizeURLs(rawUrls, baseUrl);
    }

    /**
     * Public static method to normalize a single URL relative to a base URL.
     * Used by PageRank and other jobs.
     */
    public static String normalizeUrl(String url, String baseUrl) {
        if (url == null || baseUrl == null) {
            return null;
        }
        List<String> normalized = URLParser.normalizeURLs(Collections.singletonList(url), baseUrl);
        return normalized.isEmpty() ? null : normalized.get(0);
    }

    private static String extractHost(String url) {
        try {
            URL u = new URL(url);
            String host = u.getHost();
            int port = u.getPort();
            if (port == -1) {
                port = u.getProtocol().equals("https") ? 443 : 80;
            }
            return host + ":" + port;
        } catch (Exception e) {
            return null;
        }
    }

    private static long getWaitTime(KVSClient kvs, String host, double crawlDelay) {
        try {
            String hostKey = Hasher.hash(host);
            Row hostRow = kvs.getRow("hosts", hostKey);

            long currentTime = System.currentTimeMillis();
            long lastAccessTime = 0;

            if (hostRow != null && hostRow.get("lastAccess") != null) {
                lastAccessTime = Long.parseLong(hostRow.get("lastAccess"));
            }

            long delayMs = (long)(crawlDelay * 1000);
            long timeSinceLastAccess = currentTime - lastAccessTime;

            if (timeSinceLastAccess < delayMs) {
                return delayMs - timeSinceLastAccess;
            }
            return 0;
        } catch (Exception e) {
            return 0;
        }
    }

    private static void updateLastAccessTime(KVSClient kvs, String host) {
        try {
            String hostKey = Hasher.hash(host);
            Row hostRow = kvs.getRow("hosts", hostKey);
            if (hostRow == null) {
                hostRow = new Row(hostKey);
            }
            hostRow.put("lastAccess", String.valueOf(System.currentTimeMillis()));
            kvs.putRow("hosts", hostRow);
        } catch (Exception e) {
        }
    }

    private static String fetchRobotsTxt(KVSClient kvs, String host) {
        HttpURLConnection conn = null;
        try {
            String hostKey = Hasher.hash(host);
            Row hostRow = kvs.getRow("hosts", hostKey);

            if (hostRow != null && hostRow.get("robotsTxt") != null) {
                return hostRow.get("robotsTxt");
            }

            String protocol = "http";
            String hostOnly = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            if (port == 443) {
                protocol = "https";
            }

            String robotsUrl = protocol + "://" + hostOnly + ":" + port + "/robots.txt";

            conn = (HttpURLConnection) new URL(robotsUrl).openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);

            int code = conn.getResponseCode();
            String robotsTxt = null;

            if (code == 200) {
                try (InputStream is = conn.getInputStream()) {
                    byte[] bytes = is.readAllBytes();
                    robotsTxt = new String(bytes, StandardCharsets.UTF_8);
                }
            }

            if (hostRow == null) {
                hostRow = new Row(hostKey);
            }
            hostRow.put("robotsTxt", robotsTxt != null ? robotsTxt : "");
            kvs.putRow("hosts", hostRow);

            return robotsTxt;
        } catch (Exception e) {
            try {
                String hostKey = Hasher.hash(host);
                Row hostRow = kvs.getRow("hosts", hostKey);
                if (hostRow == null) {
                    hostRow = new Row(hostKey);
                }
                hostRow.put("robotsTxt", "");
                kvs.putRow("hosts", hostRow);
            } catch (Exception e2) {
            }
            return null;
        } finally {
            // Always disconnect to release the connection and prevent socket leaks
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Fetch robots.txt from external website only (no KVS check).
     * Used for async parallel fetching after bulk KVS read determines host is not in DB.
     * Does NOT store result in KVS - caller should do that.
     */
    private static String fetchRobotsTxtExternal(String host) {
        HttpURLConnection conn = null;
        try {
            String protocol = "http";
            String hostOnly = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            if (port == 443) {
                protocol = "https";
            }

            String robotsUrl = protocol + "://" + hostOnly + ":" + port + "/robots.txt";

            conn = (HttpURLConnection) new URL(robotsUrl).openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);

            int code = conn.getResponseCode();
            if (code == 200) {
                try (InputStream is = conn.getInputStream()) {
                    byte[] bytes = is.readAllBytes();
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static boolean isUrlAllowed(String robotsTxt, String url) {
        if (robotsTxt == null || robotsTxt.isEmpty()) {
            return true;
        }

        String path;
        try {
            URL u = new URL(url);
            path = u.getPath();
            if (path.isEmpty()) {
                path = "/";
            }
        } catch (Exception e) {
            return true;
        }

        String[] lines = robotsTxt.split("\n");
        boolean inOurSection = false;
        boolean inWildcardSection = false;
        List<String> rules = new ArrayList<>();

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            int colonIndex = line.indexOf(':');
            if (colonIndex < 0) {
                continue;
            }

            String key = line.substring(0, colonIndex).trim().toLowerCase();
            String value = line.substring(colonIndex + 1).trim();

            if (key.equals("user-agent")) {
                if (value.equals("cis5550-crawler")) {
                    inOurSection = true;
                    inWildcardSection = false;
                    rules.clear();
                } else if (value.equals("*")) {
                    if (!inOurSection) {
                        inWildcardSection = true;
                        rules.clear();
                    }
                } else {
                    if (!inOurSection) {
                        inWildcardSection = false;
                    }
                }
            } else if (key.equals("disallow") || key.equals("allow")) {
                if (inOurSection || (inWildcardSection && !inOurSection)) {
                    if (!value.isEmpty()) {
                        rules.add(key + ":" + value);
                    } else if (key.equals("disallow") && value.isEmpty()) {
                        rules.clear();
                    }
                }
            }
        }

        if (!inOurSection && !inWildcardSection) {
            return true;
        }

        for (String rule : rules) {
            String[] parts = rule.split(":", 2);
            if (parts.length != 2) {
                continue;
            }
            String ruleType = parts[0];
            String rulePath = parts[1];

            if (path.startsWith(rulePath)) {
                return ruleType.equals("allow");
            }
        }
        return true;
    }

    private static double parseCrawlDelay(String robotsTxt) {
        if (robotsTxt == null || robotsTxt.isEmpty()) {
            return 0.1;
        }

        String[] lines = robotsTxt.split("\n");
        double crawlDelay = 0.1;
        boolean inOurSection = false;
        boolean inWildcardSection = false;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            int colonIndex = line.indexOf(':');
            if (colonIndex < 0) {
                continue;
            }

            String key = line.substring(0, colonIndex).trim().toLowerCase();
            String value = line.substring(colonIndex + 1).trim();

            if (key.equals("user-agent")) {
                if (value.equals("cis5550-crawler")) {
                    inOurSection = true;
                    inWildcardSection = false;
                    crawlDelay = 0.1;
                } else if (value.equals("*")) {
                    if (!inOurSection) {
                        inWildcardSection = true;
                        crawlDelay = 0.1;
                    }
                } else {
                    if (!inOurSection) {
                        inWildcardSection = false;
                    }
                }
            } else if (key.equals("crawl-delay")) {
                if (inOurSection || (inWildcardSection && !inOurSection)) {
                    try {
                        crawlDelay = Double.parseDouble(value);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        return crawlDelay;
    }

    private static void storeCrawlDelay(KVSClient kvs, String host, double crawlDelay) {
        try {
            String hostKey = Hasher.hash(host);
            Row hostRow = kvs.getRow("hosts", hostKey);
            if (hostRow == null) {
                hostRow = new Row(hostKey);
            }
            hostRow.put("crawlDelay", String.valueOf(crawlDelay));
            kvs.putRow("hosts", hostRow);
        } catch (Exception e) {
        }
    }

    private static String findCanonicalURL(KVSClient kvs, byte[] body) {
        if (body == null) return null;
        try {
            String contentHash = Hasher.hash(new String(body, StandardCharsets.UTF_8));
            Row contentRow = kvs.getRow("content-hash", contentHash);
            if (contentRow != null && contentRow.get("canonicalURL") != null) {
                return contentRow.get("canonicalURL");
            }
        } catch (Exception e) {
        }
        return null;
    }

    // Uses BatchedKVSClient for writes, regular KVSClient for reads
    public static void storeDataToTable(KVSClient kvs, BatchedKVSClient batchedKvs, FetchResult fr, CrawlerLogger logger){
        if (fr == null) {
            return;
        }
        try {
            String key = Hasher.hash(fr.url);
            Row r = new Row(key);
            r.put("url", fr.url);

            String responseCodeStr;
            if (fr.responseCode == RESPONSE_CODE_HEAD_CONNECT_TIMEOUT) {
                responseCodeStr = "HeadConnectionTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_HEAD_READ_TIMEOUT) {
                responseCodeStr = "HeadReadTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_BODY_CONNECT_TIMEOUT) {
                responseCodeStr = "BodyConnectionTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_BODY_READ_TIMEOUT) {
                responseCodeStr = "BodyReadTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_CONNECT_TIMEOUT) {
                responseCodeStr = "ConnectTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_READ_TIMEOUT) {
                responseCodeStr = "ReadTimeout";
            } else if (fr.responseCode == RESPONSE_CODE_DNS_FAILURE) {
                responseCodeStr = "DNSFailure";
            } else {
                responseCodeStr = String.valueOf(fr.responseCode);
            }
            r.put("responseCode", responseCodeStr);
            if (fr.contentType != null) {
                r.put("contentType", fr.contentType);
            }
            if (fr.contentLength != null) {
                r.put("length", String.valueOf(fr.contentLength));
            }
            boolean storedPageContent = false;
            int contentLength = 0;
            if (fr.body != null) {
                String canonicalURL = findCanonicalURL(kvs, fr.body);  // Read with regular KVS
                if (canonicalURL != null && !canonicalURL.equals(fr.url)) {
                    r.put("canonicalURL", canonicalURL);
                } else {
                    String pageContent = new String(fr.body, StandardCharsets.UTF_8);
                    String escapedContent = escapeHtml4(pageContent);
                    r.put("page", escapedContent);
                    storeContentHashBatched(kvs, batchedKvs, fr.url, fr.body);  // Batched write
                    storedPageContent = true;
                    contentLength = fr.body.length;
                }
            }

            batchedKvs.putRow("pt-crawl", r);  // Batched write
            
            if (storedPageContent && logger != null) {
                logger.pageStored(fr.url, contentLength);
            }
        } catch (Exception e) {
            log.error("[storeDataToTable] Failed to store: " + fr.url, e);  
        }
    }

    // Batched version of storeContentHash
    private static void storeContentHashBatched(KVSClient kvs, BatchedKVSClient batchedKvs, String url, byte[] body) {
        if (body == null) return;
        try {
            String contentHash = Hasher.hash(new String(body, StandardCharsets.UTF_8));
            Row contentRow = kvs.getRow("content-hash", contentHash);  // Read with regular KVS
            if (contentRow == null) {
                contentRow = new Row(contentHash);
                contentRow.put("canonicalURL", url);
                batchedKvs.putRow("content-hash", contentRow);  // Write with batched KVS
            }
        } catch (Exception e) {
        }
    }

    /**
     * Load blacklist patterns from KVS into memory cache.
     * Called once per JVM, patterns are cached for all subsequent checks.
     */
    private static void loadBlacklistPatterns(KVSClient kvs, String blacklistTable) {
        if (cachedBlacklistPatterns != null) return;  // Already loaded
        
        synchronized (blacklistLock) {
            if (cachedBlacklistPatterns != null) return;  // Double-check after lock
            
            List<String> patterns = new ArrayList<>();
            if (blacklistTable != null && !blacklistTable.isEmpty()) {
                try {
                    Iterator<Row> rows = kvs.scan(blacklistTable);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String pattern = row.get("pattern");
                        if (pattern != null) {
                            patterns.add(pattern);
                        }
                    }
                    log.info("Loaded " + patterns.size() + " blacklist patterns from " + blacklistTable);
                } catch (Exception e) {
                    log.warn("Failed to load blacklist patterns: " + e.getMessage());
                }
            }
            cachedBlacklistPatterns = patterns;
        }
    }

    /**
     * Check if URL matches any cached blacklist pattern.
     * Uses patterns loaded once at startup, no KVS calls.
     */
    private static boolean matchesBlacklistCached(String url) {
        if (cachedBlacklistPatterns == null || cachedBlacklistPatterns.isEmpty()) {
            return false;
        }
        for (String pattern : cachedBlacklistPatterns) {
            if (matchesPattern(url, pattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesPattern(String url, String pattern) {
        String regex = pattern.replace(".", "\\.").replace("*", ".*");
        return url.matches(regex);
    }

    private static String decodeHtmlEntities(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            if (text.charAt(i) == '&' && i + 1 < text.length()) {
                int semicolonIndex = text.indexOf(';', i);
                if (semicolonIndex > i) {
                    String entity = text.substring(i + 1, semicolonIndex);
                    String decoded = null;

                    switch (entity) {
                        case "amp": decoded = "&"; break;
                        case "lt": decoded = "<"; break;
                        case "gt": decoded = ">"; break;
                        case "quot": decoded = "\""; break;
                        case "apos": decoded = "'"; break;
                        case "nbsp": decoded = " "; break;
                        case "copy": decoded = "©"; break;
                        case "reg": decoded = "®"; break;
                        case "trade": decoded = "™"; break;
                        case "mdash": decoded = "—"; break;
                        case "ndash": decoded = "–"; break;
                        case "hellip": decoded = "…"; break;
                        default:
                            if (entity.startsWith("#")) {
                                try {
                                    int codePoint;
                                    if (entity.length() > 1 && (entity.charAt(1) == 'x' || entity.charAt(1) == 'X')) {
                                        codePoint = Integer.parseInt(entity.substring(2), 16);
                                    } else {
                                        codePoint = Integer.parseInt(entity.substring(1));
                                    }
                                    if (codePoint >= 0 && codePoint <= 0x10FFFF) {
                                        decoded = new String(new int[]{codePoint}, 0, 1);
                                    }
                                } catch (NumberFormatException e) {
                                }
                            }
                            break;
                    }

                    if (decoded != null) {
                        result.append(decoded);
                        i = semicolonIndex + 1;
                        continue;
                    }
                }
            }
            result.append(text.charAt(i));
            i++;
        }
        return result.toString();
    }

    private static java.util.Map<String, StringBuilder> extractUrlsWithAnchors(String html, String parentUrl) {
        java.util.Map<String, StringBuilder> anchorTextMap = new java.util.HashMap<>();

        if (html == null || html.isEmpty()) {
            return anchorTextMap;
        }

        java.util.regex.Pattern anchorPattern = java.util.regex.Pattern.compile(
                "<\\s*a\\s+[^>]*href\\s*=\\s*['\"]([^'\"]+)['\"][^>]*>(.*?)</a\\s*>",
                java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher anchorMatcher = anchorPattern.matcher(html);

        while (anchorMatcher.find()) {
            String href = anchorMatcher.group(1).trim();
            String anchorText = anchorMatcher.group(2);

            if (href.isEmpty()) {
                continue;
            }

            anchorText = anchorText.replaceAll("<[^>]+>", "");
            anchorText = decodeHtmlEntities(anchorText);
            anchorText = anchorText.replaceAll("\\s+", " ").trim();

            String normalized = null;
            if (href.contains("://")) {
                normalized = URLParser.normalizeSeedURL(href);
            } else {
                List<String> temp = new ArrayList<>();
                temp.add(href);
                List<String> normalizedList = URLParser.normalizeURLs(temp, parentUrl);
                if (!normalizedList.isEmpty()) {
                    normalized = normalizedList.get(0);
                }
            }

            if (normalized == null) {
                continue;
            }

            if (!anchorText.isEmpty()) {
                anchorTextMap
                        .computeIfAbsent(normalized, k -> new StringBuilder())
                        .append(anchorText).append(" ");
            }
        }

        return anchorTextMap;
    }

    private static String getFlameWorkerID() {
        // Try Environment Variable (Best Case)
        String workerId = System.getenv("FLAME_WORKER_ID");
        if (workerId != null && !workerId.isEmpty()) {
            return workerId;
        }

        // Fallback: Try to identify by Hostname + PID (Useful for debugging)
        try {
            String hostname = java.net.InetAddress.getLocalHost().getHostName();
            long pid = ProcessHandle.current().pid();

            // If we are on a worker, usually print to StdErr/StdOut in the tmux window
            // helps you see IF the env var is failing
            System.err.println("WARN: FLAME_WORKER_ID env var missing. Using fallback ID.");

            return hostname + "-pid-" + pid;
        } catch (Exception e) {
            // Last Resort
            return "worker-thread-" + Thread.currentThread().getId();
        }
    }
    

    public static void run(FlameContext ctx, String[] args) {
        if (args == null || args.length < 1 || args[0] == null || args[0].isEmpty()) {
            ctx.output("Error: expected seed URL(s) argument (single URL or comma-separated URLs).");
            log.error("[run] Missing seed URL argument");
            return;
        }
        ctx.output("OK");
        

        log.info("[run] Crawler Crawler, Yes papa!?");


        // ------------------------------------------------------------
        // Parse arguments
        // ------------------------------------------------------------

        // Parse comma-separated URLs from first argument
        String[] urlStrings = args[0].split(",");
        List<String> seedList = new ArrayList<>();
        
        for (String urlStr : urlStrings) {
            urlStr = urlStr.trim();
            if (urlStr.isEmpty()) {
                continue;
            }
            String seedUrl = URLParser.normalizeSeedURL(urlStr);
            if (seedUrl == null) {
                ctx.output("Warning: invalid seed URL skipped: " + urlStr);
                log.warn("[run] Invalid seed URL skipped: " + urlStr);
                continue;
            }
            seedList.add(seedUrl);
        }
        
        if (seedList.isEmpty()) {
            ctx.output("Error: no valid seed URLs provided.");
            log.error("[run] No valid seed URLs");
            return;
        }

        final String kvsCoordinator = ctx.getKVS().getCoordinator();
        final KVSClient kvs = ctx.getKVS();

        // Parse arguments: args[0] = seed URLs (required)
        //                   args[1] = blacklist table (optional)
        //                   args[2] = resume argument (optional, "resume" or queue ID)
        String blacklistTable = null;
        
        if (args.length > 1) {
            String arg1 = args[1];
            blacklistTable = arg1;
        }
        
        final String blacklistTableFinal = blacklistTable;

        // ------------------------------------------------------------
        // Initial queue: just the (normalized) seed URLs
        // ------------------------------------------------------------

        int roundNumber = 0;

        // Log seed URLs - show all if <= 10, otherwise show first 5 and count
        if (seedList.size() <= 10) {
            log.info("[run] Starting fresh crawl from " + seedList.size() + " seed URL(s): " + seedList);
        } else {
            List<String> preview = seedList.subList(0, Math.min(5, seedList.size()));
            log.info("[run] Starting fresh crawl from " + seedList.size() + " seed URL(s): " + preview + " ... (and " + (seedList.size() - preview.size()) + " more)");
        }

        FlameRDD urlQueue;
        try {
            // Load from frontier using RDD with 25% sampling at source
            log.info("[run] Loading unexplored URLs from frontier with 25% sampling...");
            FlameRDD frontierRdd = ctx.fromTableParallel("pt-frontier-urls", row -> {
                // Only exploring URLs
                if (!"1".equals(row.get("exploring"))) {
                    return null;
                }
                String url = row.get("url");
                if (url == null || url.isEmpty()) {
                    return null;
                }
                // 50% coin toss sampling
                if (Math.random() < 0.50) {
                    return null;
                }
                return url;
            });
            
            long frontierCount = frontierRdd.count();
            
            if (frontierCount > 0) {
                log.info("[run] Resuming from frontier: ~" + frontierCount + " URLs (25% sample)");
                // Apply rebalancing to the frontier sample
                if (frontierCount > SORT_AND_TAKE_SAMPLE_SIZE) {
                    urlQueue = rebalanceUrls(ctx, frontierRdd, SORT_AND_TAKE_SAMPLE_SIZE);
                    log.info("[run] Rebalanced frontier to " + urlQueue.count() + " URLs");
                } else {
                    urlQueue = frontierRdd;
                }
            } else {
                log.info("[run] No frontier URLs found, starting fresh with seed URLs");
                log.info("[run] Seed URLs: " + seedList);
                urlQueue = ctx.parallelize(seedList);
                long initialCount = urlQueue.count();
                log.info("[run] Created RDD with " + initialCount + " URL(s) from seeds");
            }
        } catch (Exception e) {
            ctx.output("Error: failed to create initial RDD: " + e.getMessage());
            log.fatal("[run] Failed to create initial RDD", e);
            return;
        }


        try {
            while (urlQueue.count() > 0) {
                roundNumber++;
                final int currentRound = roundNumber;  // Make final for lambda
                // if (roundNumber == 2) {
                //     break;
                // }
                // if (roundNumber == 4) {
                //     break;
                // }
                long queueSize = urlQueue.count();
                log.info("[run] Round " + currentRound + " starting with " + queueSize + " URLs");
                //log.info("[run] Queue: " + urlQueue.collect().toString());
                
                // Use flatMapParallel - provides SHARED BatchedKVSClient across all threads
                // All writes from all lambda invocations are pooled and batched together
                urlQueue = urlQueue.flatMapParallel((baseUrl, workerKvs, batchedKvs) -> {
                    List<String> result = new ArrayList<>();
                    // workerKvs = shared KVSClient for reads (not batched)
                    // batchedKvs = SHARED BatchedKVSClient for writes (pooled across all threads!)

                    //  Create logger INSIDE flatMap (runs on worker)
                    String workerID = getFlameWorkerID();
                    CrawlerLogger workerLogger = new CrawlerLogger(batchedKvs, workerID);
                    workerLogger.setRound(currentRound);

                    // Timing variables (only logged on successful page processing)
                    long tStart = System.nanoTime();
                    long tKvsCheck = 0, tRobots = 0, tFetch = 0, tExtract = 0, tChildren = 0;

                    try {
                        long t0 = System.nanoTime();
                        String hashedURL = Hasher.hash(baseUrl);
                        Row urow = workerKvs.getRow("pt-crawl", hashedURL);
                        tKvsCheck = System.nanoTime() - t0;
                        
                        if (urow != null && urow.get("url") != null) {
                            // Already have this URL in pt-crawl, don't process again
                            workerLogger.skipped(baseUrl, "already_crawled");
                            return Collections.emptyList();
                        }

                        String host = extractHost(baseUrl);
                        if (host == null) {
                            workerLogger.skipped(baseUrl, "invalid_host");
                            return Collections.emptyList();
                        }

                        t0 = System.nanoTime();
                        String robotsTxt = fetchRobotsTxt(workerKvs, host);
                        tRobots = System.nanoTime() - t0;
                        double crawlDelay = parseCrawlDelay(robotsTxt);
                        storeCrawlDelay(workerKvs, host, crawlDelay);

                        if (!isUrlAllowed(robotsTxt, baseUrl)) {
                            workerLogger.robotsDisallowed(baseUrl);  
                            return Collections.emptyList();
                        }

                        long waitTime = getWaitTime(workerKvs, host, crawlDelay);
                        if (waitTime > 0) {
                            workerLogger.crawlerWait(baseUrl);  
                            return Collections.singletonList(baseUrl);
                        }

                        // === STREAMING FETCH (single GET request) ===
                        t0 = System.nanoTime();
                        FetchResult fetchResult = streamingFetch(baseUrl, workerLogger);
                        tFetch = System.nanoTime() - t0;

                        if (fetchResult == null) {
                            return Collections.emptyList();
                        }

                        // DNS failure - requeue for retry after negative cache expires
                        if (fetchResult.responseCode == RESPONSE_CODE_DNS_FAILURE) {
                            log.info("[DNS] Requeuing URL for retry: " + baseUrl);
                            return Collections.singletonList(baseUrl);
                        }

                        updateLastAccessTime(workerKvs, host);

                        if (fetchResult.isRedirect && fetchResult.location != null) {
                            storeDataToTable(workerKvs, batchedKvs, fetchResult, workerLogger);
                            String redirectUrl = fetchResult.location;
                            if (!redirectUrl.contains("://")) {
                                try {
                                    URL base = new URL(baseUrl);
                                    URL resolved = new URL(base, redirectUrl);
                                    redirectUrl = resolved.toString();
                                } catch (Exception e) {
                                    workerLogger.redirectFail(baseUrl, "Redirect URL unresolved");
                                    return Collections.emptyList();
                                }
                            }
                            String normalizedRedirect = URLParser.normalizeSeedURL(redirectUrl);
                            if (normalizedRedirect != null) {
                                workerLogger.redirectSuccess(baseUrl, normalizedRedirect);
                                return Collections.singletonList(normalizedRedirect);
                            }
                            workerLogger.redirectFail(baseUrl, "Redirect URL is null");
                            return Collections.emptyList();
                        } else if (fetchResult.responseCode != 200) {
                            storeDataToTable(workerKvs, batchedKvs, fetchResult, workerLogger);
                            return Collections.emptyList();
                        }

                        // add url to frontier - batched writes (shared pool)
                        batchedKvs.put("pt-frontier-urls", Hasher.hash(baseUrl), "url", baseUrl);
                        batchedKvs.put("pt-frontier-urls", Hasher.hash(baseUrl), "exploring", "1");

                        // Store fetch result (body already included from streaming fetch)
                        storeDataToTable(workerKvs, batchedKvs, fetchResult, workerLogger);

                        if (fetchResult.body != null) {
                                t0 = System.nanoTime();
                                String pageContent = new String(fetchResult.body, StandardCharsets.UTF_8);
                                java.util.Map<String, StringBuilder> anchorTextMap = extractUrlsWithAnchors(pageContent, baseUrl);
                                tExtract = System.nanoTime() - t0;
                                
                                int childCount = anchorTextMap.size();
                                int childKvsCrawlReads = 0;  // reads to pt-crawl
                                int childRobotsReads = 0;    // reads for robots.txt
                                long tChildKvsCrawlTotal = 0; // total time for bulk pt-crawl read
                                long tChildRobotsTotal = 0;   // total time for robots reads
                                long tChildrenStart = System.nanoTime();

                                // === BULK CHILD PROCESSING ===
                                // Load blacklist patterns once (cached globally)
                                loadBlacklistPatterns(workerKvs, blacklistTableFinal);
                                
                                // Step 1: Collect valid children (local filtering only - no KVS calls)
                                List<ChildInfo> validChildren = new ArrayList<>();
                                for (java.util.Map.Entry<String, StringBuilder> entry : anchorTextMap.entrySet()) {
                                    String childUrl = entry.getKey();
                                    String anchors = entry.getValue().toString().trim();
                                    
                                    // Blacklist check (cached patterns, no KVS call)
                                    if (matchesBlacklistCached(childUrl)) {
                                        workerLogger.blacklisted(childUrl);
                                        continue;
                                    }
                                    
                                    String urlHost = extractHost(childUrl);
                                    if (urlHost == null) {
                                        workerLogger.skipped(childUrl, "invalid_host_child");
                                        continue;
                                    }
                                    
                                    String hash = Hasher.hash(childUrl);
                                    validChildren.add(new ChildInfo(childUrl, hash, urlHost, anchors));
                                }
                                
                                // DISABLED: Child URL robots filtering - URLs will be checked when crawled
                                // Steps 2-5 commented out for performance
                                /*
                                // Step 2: Collect unique hosts not in memory cache for robots check
                                Set<String> uniqueHosts = new HashSet<>();
                                for (ChildInfo child : validChildren) {
                                    if (!robotsCache.containsKey(child.host)) {
                                        uniqueHosts.add(child.host);
                                    }
                                }
                                
                                // Step 3: Bulk read "hosts" table for robots.txt (1-6 requests instead of N)
                                long tRobotsStart = System.nanoTime();
                                if (!uniqueHosts.isEmpty()) {
                                    List<String> hostHashes = new ArrayList<>();
                                    Map<String, String> hostToHash = new HashMap<>();
                                    for (String h : uniqueHosts) {
                                        String hHash = Hasher.hash(h);
                                        hostHashes.add(hHash);
                                        hostToHash.put(h, hHash);
                                    }
                                    
                                    try {
                                        Map<String, Row> hostResults = workerKvs.bulkGetRows("hosts", hostHashes);
                                        
                                        // Populate robots cache from KVS results
                                        for (String h : uniqueHosts) {
                                            Row row = hostResults.get(hostToHash.get(h));
                                            if (row != null && row.get("robotsTxt") != null) {
                                                String hostRobotsTxt = row.get("robotsTxt");
                                                robotsCache.put(h, hostRobotsTxt);
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.debug("[bulk] Error in bulk hosts read: " + e.getMessage());
                                    }
                                }
                                
                                // Step 4: Async fetch robots.txt for hosts not in memory cache AND not in KVS
                                List<String> needsExternalFetch = new ArrayList<>();
                                for (String h : uniqueHosts) {
                                    if (!robotsCache.containsKey(h)) {
                                        needsExternalFetch.add(h);
                                    }
                                }
                                
                                if (!needsExternalFetch.isEmpty()) {
                                    Map<String, CompletableFuture<String>> robotsFutures = new HashMap<>();
                                    for (String h : needsExternalFetch) {
                                        final String hostFinal = h;
                                        robotsFutures.put(h, CompletableFuture.supplyAsync(() -> {
                                            return fetchRobotsTxtExternal(hostFinal);
                                        }, childKvsExecutor));
                                    }
                                    
                                    // Wait for all and cache results
                                    for (Map.Entry<String, CompletableFuture<String>> futureEntry : robotsFutures.entrySet()) {
                                        try {
                                            String fetchedRobotsTxt = futureEntry.getValue().get(10, java.util.concurrent.TimeUnit.SECONDS);
                                            robotsCache.put(futureEntry.getKey(), fetchedRobotsTxt != null ? fetchedRobotsTxt : "");
                                            // Store in KVS for future runs (async, don't wait)
                                            final String hostForKvs = futureEntry.getKey();
                                            final String robotsForKvs = fetchedRobotsTxt;
                                            CompletableFuture.runAsync(() -> {
                                                try {
                                                    String hostKey = Hasher.hash(hostForKvs);
                                                    Row hostRow = new Row(hostKey);
                                                    hostRow.put("robotsTxt", robotsForKvs != null ? robotsForKvs : "");
                                                    batchedKvs.putRow("hosts", hostRow);
                                                } catch (Exception e) {}
                                            }, childKvsExecutor);
                                        } catch (Exception e) {
                                            robotsCache.put(futureEntry.getKey(), "");  // Mark as checked (allow all)
                                        }
                                    }
                                }
                                tChildRobotsTotal = System.nanoTime() - tRobotsStart;
                                childRobotsReads = uniqueHosts.size();
                                
                                // Step 5: Filter by robots (all hosts now in cache)
                                List<ChildInfo> robotsAllowed = new ArrayList<>();
                                for (ChildInfo child : validChildren) {
                                    String childRobotsTxt = robotsCache.get(child.host);
                                    // Empty string or null means no restrictions
                                    if (childRobotsTxt == null || childRobotsTxt.isEmpty() || isUrlAllowed(childRobotsTxt, child.url)) {
                                        robotsAllowed.add(child);
                                    } else {
                                        workerLogger.robotsDisallowed(child.url);
                                    }
                                }
                                */
                                
                                // Step 2: Bulk read pt-crawl for all valid children (1-6 requests)
                                long tCrawlStart = System.nanoTime();
                                Map<String, Row> crawlResults = Collections.emptyMap();
                                if (!validChildren.isEmpty()) {
                                    List<String> crawlHashes = new ArrayList<>();
                                    for (ChildInfo child : validChildren) {
                                        crawlHashes.add(child.hash);
                                    }
                                    
                                    try {
                                        crawlResults = workerKvs.bulkGetRows("pt-crawl", crawlHashes);
                                        if (crawlResults.isEmpty()){log.debug("[bulk] Bulk pt-crawl read returned empty");}
                                    } catch (Exception e) {
                                        log.debug("[bulk] Error in bulk pt-crawl read: " + e.getMessage());
                                        crawlResults = Collections.emptyMap();
                                    }
                                }
                                
                                tChildKvsCrawlTotal = System.nanoTime() - tCrawlStart;
                                childKvsCrawlReads = validChildren.size();
                                
                                // Step 3: Process results - check if already crawled and handle new URLs
                                for (ChildInfo child : validChildren) {
                                    Row existingRow = crawlResults.get(child.hash);
                                    boolean alreadyCrawled = (existingRow != null && existingRow.get("url") != null);
                                    
                                    if (alreadyCrawled) {
                                        workerLogger.skipped(child.url, "already_crawled_child");
                                        continue;
                                    }
                                    
                                    // New URL - add to result and frontier
                                    result.add(child.url);
                                    workerLogger.queued(child.url, baseUrl);
                                    batchedKvs.put("pt-frontier-urls", child.hash, "url", child.url);
                                    batchedKvs.put("pt-frontier-urls", child.hash, "exploring", "1");
                                    
                                    // Handle anchor text (preserve existing anchor text)
                                    if (!child.anchors.isEmpty()) {
                                        String escapedAnchors = escapeHtml4(child.anchors);
                                        String columnName = "anchors:" + baseUrl;
                                        String existingText = (existingRow != null) ? existingRow.get(columnName) : null;
                                        
                                        if (existingText != null) {
                                            batchedKvs.put("pt-crawl", child.hash, columnName, existingText + " " + escapedAnchors);
                                        } else {
                                            batchedKvs.put("pt-crawl", child.hash, columnName, escapedAnchors);
                                        }
                                    }
                                }
                                tChildren = System.nanoTime() - tChildrenStart;
                                
                                // Calculate averages
                                double avgChildKvsMs = childKvsCrawlReads > 0 ? (tChildKvsCrawlTotal / 1_000_000.0) / childKvsCrawlReads : 0;
                                double avgChildRobotsMs = childRobotsReads > 0 ? (tChildRobotsTotal / 1_000_000.0) / childRobotsReads : 0;
                                
                                // Log timing for successfully processed page
                                long tTotal = System.nanoTime() - tStart;
                                log.info(String.format("[TIMING] url=%s total=%.1fms kvs=%.2fms robots=%.1fms fetch=%.1fms extract=%.1fms children=%.1fms(n=%d) childKvs=%.1fms(n=%d,avg=%.2fms) childRobots=%.1fms(n=%d,avg=%.2fms)",
                                    baseUrl,
                                    tTotal / 1_000_000.0,
                                    tKvsCheck / 1_000_000.0,
                                    tRobots / 1_000_000.0,
                                    tFetch / 1_000_000.0,
                                    tExtract / 1_000_000.0,
                                    tChildren / 1_000_000.0,
                                    childCount,
                                    tChildKvsCrawlTotal / 1_000_000.0,
                                    childKvsCrawlReads,
                                    avgChildKvsMs,
                                    tChildRobotsTotal / 1_000_000.0,
                                    childRobotsReads,
                                    avgChildRobotsMs
                                ));
                            }
                        // remove url from frontier if present
                        Row urlRow = workerKvs.getRow("pt-frontier-urls", Hasher.hash(baseUrl));
                        if (urlRow != null &&  "1".equals(urlRow.get("exploring")) ) {
                            batchedKvs.put("pt-frontier-urls", Hasher.hash(baseUrl), "exploring", "0");
                        }

                        return result;
                    } catch (Exception e) {
                        workerLogger.skipped(baseUrl, "seen_check_error");
                        return Collections.emptyList();
                    }
                    // NO flushAndClose() needed - executor handles flushing the shared writer
                });

                long discoveredCount = urlQueue.count();
                // log.info("[DISTINCT] Round " + currentRound + " discovered URLs: " + urlQueue.collect().toString());
                List<String> urls = urlQueue.collect();
                Set<String> uniqueUrls = new HashSet<>(urls);
                urlQueue = ctx.parallelize(new ArrayList<String>(uniqueUrls));
                long uniqueDiscoveredCount = urlQueue.count();
                // log.info("[DISTINCT] Round " + currentRound + " discovered URLs after distinct: " + urlQueue.collect().toString());
                log.info("[run] Round " + currentRound + " completed: discovered " + discoveredCount + " of which " + uniqueDiscoveredCount + " are new URL(s) for next round");

                // Rebalance: keep only top 20,000 URLs sorted by quality
                if (urlQueue.count() > SORT_AND_TAKE_SAMPLE_SIZE) {
                    log.info("[run] Rebalancing frontier: " + discoveredCount + " URLs -> top 20,000");
                    urlQueue = rebalanceUrls(ctx, urlQueue, SORT_AND_TAKE_SAMPLE_SIZE);
                    log.info("[run] After rebalancing: " + urlQueue.count() + " URLs");
                }
                
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.warn("[run] Sleep interrupted", e);
                }
            }

            log.info("[run] Crawl completed after " + roundNumber + " rounds");
        } catch (Exception e) {
            ctx.output("Error during crawl: " + e.getMessage());
            log.fatal("[run] Crawl failed", e);
        }
    }

    /**
     * Rebalance URLs by scoring and keeping only top N URLs.
     * Scoring factors (higher = better):
     * - Shorter path depth
     * - HTTPS over HTTP
     * - Fewer query parameters
     * - Fewer numbers in path (avoids pagination/session IDs)
     * - Known quality domains (wikipedia, edu, gov, etc.)
     * - Domain diversity (cap URLs per domain)
     */
    /**
     * Rebalance child URLs discovered in the current round.
     * Does NOT reload from frontier table - only works with the passed-in RDD.
     * 
     * Flow:
     * 1. Collect URLs from RDD
     * 2. Deduplicate
     * 3. Score by quality factors
     * 4. Pick top 100k with domain diversity
     * 5. Randomly sample final set
     */
    private static FlameRDD rebalanceUrls(FlameContext ctx, FlameRDD urlRdd, int maxUrls) throws Exception {
        final int TOP_POOL_SIZE = 100000;
        final int SAMPLE_SIZE = maxUrls;
        
        log.info("[rebalance] Rebalancing child URLs from current round (no frontier reload)");
        long startTime = System.currentTimeMillis();
        
        // Collect URLs from the RDD (these are child URLs discovered this round)
        List<String> allUrls = urlRdd.collect();
        log.info("[rebalance] Collected " + allUrls.size() + " child URLs");
        
        // Fast dedup + score in single pass
        Set<String> seen = new HashSet<>(allUrls.size());
        List<ScoredUrl> scored = new ArrayList<>(allUrls.size());
        for (String url : allUrls) {
            if (url != null && seen.add(url)) {
                scored.add(new ScoredUrl(url, scoreUrl(url)));
            }
        }
        log.info("[rebalance] " + scored.size() + " unique URLs after dedup");
        
        // Sort by score descending
        scored.sort((a, b) -> Double.compare(b.score, a.score));
        
        // Apply domain diversity: pick top 100k with max per domain
        Map<String, Integer> domainCounts = new HashMap<>();
        List<String> topPool = new ArrayList<>(Math.min(scored.size(), TOP_POOL_SIZE));
        int maxPerDomain = 10000;
        
        for (ScoredUrl su : scored) {
            if (topPool.size() >= TOP_POOL_SIZE) break;
            String domain = extractHost(su.url);
            if (domain == null) continue;
            int count = domainCounts.getOrDefault(domain, 0);
            if (count < maxPerDomain) {
                topPool.add(su.url);
                domainCounts.put(domain, count + 1);
            }
        }
        
        log.info("[rebalance] Top pool: " + topPool.size() + " URLs across " + domainCounts.size() + " domains");
        
        // Random sample from pool
        List<String> finalUrls;
        if (topPool.size() <= SAMPLE_SIZE) {
            finalUrls = topPool;
        } else {
            Collections.shuffle(topPool, new Random());
            finalUrls = new ArrayList<>(topPool.subList(0, SAMPLE_SIZE));
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("[rebalance] Complete: " + finalUrls.size() + " URLs selected in " + elapsed + "ms");
        
        if (finalUrls.size() > 0) {
            log.info("[rebalance] Sample: " + finalUrls.subList(0, Math.min(5, finalUrls.size())));
        }
        
        return ctx.parallelize(finalUrls);
    }
    
    /**
     * Score a URL for crawl priority. Higher = better.
     */
    private static double scoreUrl(String url) {
        double score = 100.0;
        
        try {
            URL parsed = new URL(url);
            String host = parsed.getHost().toLowerCase();
            String path = parsed.getPath();
            String query = parsed.getQuery();
            
            // HTTPS bonus
            if ("https".equals(parsed.getProtocol())) {
                score += 5;
            }
            
            // Path depth penalty (each / costs points)
            int depth = 0;
            for (char c : path.toCharArray()) {
                if (c == '/') depth++;
            }
            score -= depth * 3;
            
            // Short path bonus
            if (path.length() < 30) {
                score += 10;
            } else if (path.length() > 100) {
                score -= 15;
            }
            
            // Query string penalty
            if (query != null && !query.isEmpty()) {
                int paramCount = 1;
                for (char c : query.toCharArray()) {
                    if (c == '&') paramCount++;
                }
                score -= paramCount * 5;
            }
            
            // Numbers in path penalty (pagination, IDs, dates)
            int numCount = 0;
            for (char c : path.toCharArray()) {
                if (Character.isDigit(c)) numCount++;
            }
            if (numCount > 4) {
                score -= (numCount - 4) * 2;
            }
            
            // Quality domain bonuses
            if (host.endsWith(".edu")) {
                score += 20;
            } else if (host.endsWith(".gov")) {
                score += 20;
            } else if (host.endsWith(".org")) {
                score += 10;
            } else if (host.contains("wikipedia")) {
                score += 25;
            } else if (host.contains("wiki")) {
                score += 15;
            }
            
            // Known high-value path patterns
            String lowerPath = path.toLowerCase();
            if (lowerPath.contains("/article") || lowerPath.contains("/wiki/")) {
                score += 10;
            }
            if (lowerPath.contains("/news") || lowerPath.contains("/blog")) {
                score += 5;
            }
            
            // Penalize likely non-content paths
            if (lowerPath.contains("/login") || lowerPath.contains("/signup") || 
                lowerPath.contains("/register") || lowerPath.contains("/cart") ||
                lowerPath.contains("/checkout") || lowerPath.contains("/admin")) {
                score -= 30;
            }
            if (lowerPath.contains("/search") || lowerPath.contains("/filter")) {
                score -= 20;
            }
            
            // File extension bonuses/penalties
            if (lowerPath.endsWith(".html") || lowerPath.endsWith(".htm") || lowerPath.endsWith("/")) {
                score += 5;
            }
            if (lowerPath.endsWith(".pdf") || lowerPath.endsWith(".xml") || lowerPath.endsWith(".json")) {
                score -= 1;
            }
            
            // Root/index page bonus
            if (path.equals("/") || path.isEmpty() || path.equals("/index.html")) {
                score += 15;
            }
            
        } catch (Exception e) {
            // Invalid URL, low score
            score = 0;
        }
        
        return score;
    }
    
    private static class ScoredUrl {
        String url;
        double score;
        
        ScoredUrl(String url, double score) {
            this.url = url;
            this.score = score;
        }
    }
}