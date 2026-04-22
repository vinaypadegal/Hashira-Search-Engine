package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.*;

/**
 * MetaDataExtractor - Extracts metadata from crawled pages and stores in separate tables:
 * 
 * Tables created:
 * - pt-meta-info: title and description (key: hashed parent URL, columns: url, title, description)
 * - pt-image-links: image links (key: hashed image URL, columns: url, anchor)
 *   anchor format: word1 word2 word3 ... (space-separated words, no stop words)
 * - pt-pdf-links: PDF links (key: hashed PDF URL, columns: url, anchortext)
 * - pt-video-links: video links (key: hashed video URL, columns: url, anchortext)
 * - pt-outlinks: FILTERED outlinks - only keeps links that exist in pt-crawl
 *   (key: hashed parent URL, columns: url, outlinks)
 */
public class MetaDataExtractor {
    private static final Logger logger = Logger.getLogger(MetaDataExtractor.class);

    private static final Set<String> VIDEO_EXTENSIONS = Set.of(
        ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mkv", ".m4v", ".mpeg", ".mpg", ".3gp", ".ogv"
    );

    private static final Set<String> IMAGE_EXTENSIONS = Set.of(
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".ico", ".tiff", ".tif"
    );

    /**
     * Simple URL normalization that adds explicit port without filtering extensions.
     * Use this for image/pdf/video URLs since URLParser.normalizeSeedURL filters those out.
     */
    private static String normalizeUrlWithPort(String url) {
        if (url == null || url.isEmpty()) return null;
        
        // Remove fragment
        int hashIndex = url.indexOf('#');
        if (hashIndex != -1) {
            url = url.substring(0, hashIndex);
        }
        if (url.isEmpty()) return null;
        
        try {
            String[] parsed = URLParser.parseURL(url);
            String protocol = parsed[0];
            String host = parsed[1];
            String portStr = parsed[2];
            String path = parsed[3];
            
            if (protocol == null || host == null) return null;
            if (!protocol.equals("http") && !protocol.equals("https")) return null;
            
            int port;
            if (portStr == null || portStr.isEmpty()) {
                port = protocol.equals("http") ? 80 : 443;
            } else {
                port = Integer.parseInt(portStr);
            }
            
            if (path == null || path.isEmpty()) {
                path = "/";
            }
            
            return protocol + "://" + host + ":" + port + path;
        } catch (Exception e) {
            return null;
        }
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("MetaDataExtractor job started");
        long jobStartTime = System.currentTimeMillis();

        // Clear old tables
        String[] tablesToClear = {"pt-meta-info", "pt-image-links", "pt-pdf-links", "pt-video-links", "pt-outlinks"};
        for (String table : tablesToClear) {
            try {
                ctx.getKVS().delete(table);
                logger.info("Cleared table: " + table);
            } catch (Exception e) {
                logger.info("Table " + table + " did not exist or could not be deleted: " + e.getMessage());
            }
        }

        try {
            // Phase 1: Get all URLs from pt-crawl that have non-null page content
            logger.info("Phase 1: Loading URLs from pt-crawl");
            long phase1Start = System.currentTimeMillis();

            FlameRDD urlRDD = ctx.fromTableParallel("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null || page.isEmpty()) {
                    return null;
                }
                return url;
            });

            long urlCount = urlRDD.count();
            long phase1Time = System.currentTimeMillis() - phase1Start;
            logger.info(String.format("Phase 1 complete: Found %d URLs with page content in %dms", urlCount, phase1Time));

            // Phase 2: Process each URL and extract metadata
            // This phase extracts meta-info, pdf/video links, and emits image data for aggregation
            logger.info("Phase 2: Extracting metadata from pages");
            long phase2Start = System.currentTimeMillis();

            // Emit image pairs: (imageUrlHash, imageUrl + "\t" + parentUrl + "," + anchorText)
            FlamePairRDD imagePairs = urlRDD.flatMapToPairParallel((url, workerKvs, batchedKvs) -> {
                List<FlamePair> imageEmissions = new ArrayList<>();
                // TODO: Currently deduplicates by image+parentUrl, which means we only keep one anchor
                // text per image per page. In the future, we may want to include different anchor texts
                // from the same page (e.g., dedupe by imgHash+url+anchorText instead of just imgHash+url)
                Set<String> seenImageEmissions = new HashSet<>(); // Deduplicate per page
                
                try {
                    String hashedUrl = Hasher.hash(url);
                    Row crawlRow = workerKvs.getRow("pt-crawl", hashedUrl);

                    if (crawlRow == null) {
                        return Collections.emptyList();
                    }

                    String page = crawlRow.get("page");
                    if (page == null || page.isEmpty()) {
                        return Collections.emptyList();
                    }

                    // Unescape HTML entities that were escaped by escapeHtml4() in Crawler
                    page = unescapeHtml(page);

                    // Parse HTML with JSoup
                    Document doc = Jsoup.parse(page, url);
                    doc.select("script, style, noscript").remove();

                    // Extract title
                    String title = "";
                    Element titleEl = doc.selectFirst("title");
                    if (titleEl != null) {
                        title = titleEl.text().trim();
                    }

                    // Extract description
                    String description = "";
                    Element metaDesc = doc.selectFirst("meta[name=description]");
                    if (metaDesc != null) {
                        description = metaDesc.attr("content").trim();
                    }
                    if (description.isEmpty()) {
                        Element ogDesc = doc.selectFirst("meta[property=og:description]");
                        if (ogDesc != null) {
                            description = ogDesc.attr("content").trim();
                        }
                    }

                    // Store meta info
                    batchedKvs.put("pt-meta-info", hashedUrl, "url", url);
                    batchedKvs.put("pt-meta-info", hashedUrl, "title", title);
                    batchedKvs.put("pt-meta-info", hashedUrl, "description", description);

                    // Collect all outlinks first
                    Elements links = doc.select("a[href]");
                    List<String> allOutlinks = new ArrayList<>();

                    for (Element link : links) {
                        String href = link.attr("abs:href");
                        String anchorText = link.text().trim();

                        if (href == null || href.isEmpty()) continue;

                        // Normalize URL to match how crawler stores URLs (with explicit port)
                        String normalizedHref = URLParser.normalizeSeedURL(href);
                        if (normalizedHref != null) {
                            allOutlinks.add(normalizedHref);
                        }

                        // Classify and store PDF/video/image links directly
                        // Normalize URLs to include explicit port for consistent hashing
                        String normalizedLinkUrl = normalizeUrlWithPort(href);
                        if (normalizedLinkUrl == null) continue;
                        
                        String pathLower = stripQueryAndFragment(normalizedLinkUrl).toLowerCase();

                        if (pathLower.endsWith(".pdf")) {
                            String linkHash = Hasher.hash(normalizedLinkUrl);
                            batchedKvs.put("pt-pdf-links", linkHash, "url", normalizedLinkUrl);
                            batchedKvs.put("pt-pdf-links", linkHash, "anchortext", anchorText);
                        } else if (isVideo(pathLower)) {
                            String linkHash = Hasher.hash(normalizedLinkUrl);
                            batchedKvs.put("pt-video-links", linkHash, "url", normalizedLinkUrl);
                            batchedKvs.put("pt-video-links", linkHash, "anchortext", anchorText);
                        } else if (isImage(pathLower)) {
                            // Emit for aggregation: key=imageUrlHash, value=imageUrl\twords
                            String imgHash = Hasher.hash(normalizedLinkUrl);
                            // Tokenize anchor text into comma-separated words
                            String words = tokenizeToWords(anchorText);
                            if (!words.isEmpty()) {
                                String emissionKey = imgHash + "|" + url; // Dedupe by image+parentUrl
                                if (seenImageEmissions.add(emissionKey)) {
                                    imageEmissions.add(new FlamePair(imgHash, normalizedLinkUrl + "\t" + words));
                                }
                            }
                        }
                    }

                    // Extract images from img tags
                    Elements images = doc.select("img[src]");
                    for (Element img : images) {
                        String src = img.attr("abs:src");
                        String alt = img.attr("alt").trim();

                        if (src == null || src.isEmpty()) continue;
                        
                        // Normalize image URL
                        String normalizedSrc = normalizeUrlWithPort(src);
                        if (normalizedSrc == null) continue;

                        String imgHash = Hasher.hash(normalizedSrc);
                        // Tokenize alt text into comma-separated words
                        String words = tokenizeToWords(alt);
                        if (!words.isEmpty()) {
                            String emissionKey = imgHash + "|" + url; // Dedupe by image+parentUrl
                            if (seenImageEmissions.add(emissionKey)) {
                                imageEmissions.add(new FlamePair(imgHash, normalizedSrc + "\t" + words));
                            }
                        }
                    }

                    // Also check for video sources
                    Elements videoSources = doc.select("video source[src], video[src]");
                    for (Element video : videoSources) {
                        String src = video.attr("abs:src");
                        if (src == null || src.isEmpty()) continue;
                        
                        // Normalize video URL
                        String normalizedVideoSrc = normalizeUrlWithPort(src);
                        if (normalizedVideoSrc == null) continue;

                        String videoHash = Hasher.hash(normalizedVideoSrc);
                        batchedKvs.put("pt-video-links", videoHash, "url", normalizedVideoSrc);
                        batchedKvs.put("pt-video-links", videoHash, "anchortext", "");
                    }

                    // FILTER OUTLINKS: bulk check which ones exist in pt-crawl
                    // Build list of hashes and map back to URLs
                    List<String> outlinkHashes = new ArrayList<>();
                    Map<String, String> hashToUrl = new HashMap<>();
                    for (String outlink : allOutlinks) {
                        String hash = Hasher.hash(outlink);
                        outlinkHashes.add(hash);
                        hashToUrl.put(hash, outlink);
                    }
                    
                    // Bulk check which hashes exist (uses existing bulkGetRows - already parallelized)
                    Map<String, Row> existingRows = workerKvs.bulkGetRows("pt-crawl", outlinkHashes);
                    
                    // Build filtered outlinks from existing hashes (non-null in map means exists)
                    StringBuilder filteredOutlinks = new StringBuilder();
                    for (String hash : outlinkHashes) {
                        if (existingRows.get(hash) != null) {
                            if (filteredOutlinks.length() > 0) {
                                filteredOutlinks.append("\n");
                            }
                            filteredOutlinks.append(hashToUrl.get(hash));
                        }
                    }

                    // Store filtered outlinks
                    batchedKvs.put("pt-outlinks", hashedUrl, "url", url);
                    batchedKvs.put("pt-outlinks", hashedUrl, "outlinks", filteredOutlinks.toString());

                    return imageEmissions;

                } catch (Exception e) {
                    logger.warn("Error processing URL " + url + ": " + e.getMessage());
                    return Collections.emptyList();
                }
            });

            long phase2Time = System.currentTimeMillis() - phase2Start;
            logger.info(String.format("Phase 2 complete: Extracted metadata in %dms", phase2Time));

            // Phase 3: Aggregate image data by image URL
            logger.info("Phase 3: Aggregating image anchor data");
            long phase3Start = System.currentTimeMillis();

            // Fold by key to aggregate: word1 word2 word3 ...
            FlamePairRDD aggregatedImages = imagePairs.foldByKeyParallel("", (existing, newEntry) -> {
                // newEntry format: imageUrl\twords (space-separated)
                int tabIdx = newEntry.indexOf('\t');
                if (tabIdx < 0) {
                    return existing.isEmpty() ? newEntry : existing;
                }
                
                String imageUrl = newEntry.substring(0, tabIdx);
                String newWords = newEntry.substring(tabIdx + 1);
                
                if (existing == null || existing.isEmpty()) {
                    return newEntry; // Keep full entry (imageUrl\twords) for first one
                }
                
                // For subsequent entries, merge words (space-separated, dedupe)
                int existingTabIdx = existing.indexOf('\t');
                if (existingTabIdx < 0) {
                    return newEntry;
                }
                
                String existingWordsStr = existing.substring(existingTabIdx + 1);
                Set<String> allWords = new LinkedHashSet<>(); // Preserve order, avoid duplicates
                
                // Add existing words
                if (!existingWordsStr.isEmpty()) {
                    for (String word : existingWordsStr.split("\\s+")) {
                        word = word.trim();
                        if (!word.isEmpty()) {
                            allWords.add(word);
                        }
                    }
                }
                
                // Add new words
                if (!newWords.isEmpty()) {
                    for (String word : newWords.split("\\s+")) {
                        word = word.trim();
                        if (!word.isEmpty()) {
                            allWords.add(word);
                        }
                    }
                }
                
                return imageUrl + "\t" + String.join(" ", allWords);
            });

            // Save aggregated images to pt-image-links
            aggregatedImages.flatMapToPairParallel((pair, workerKvs, batchedKvs) -> {
                try {
                    String imageHash = pair._1();
                    String aggregatedValue = pair._2();
                    
                    // aggregatedValue format: imageUrl\tword1,word2,word3,...
                    int tabIdx = aggregatedValue.indexOf('\t');
                    if (tabIdx < 0) {
                        return Collections.emptyList();
                    }
                    
                    String imageUrl = aggregatedValue.substring(0, tabIdx);
                    String words = aggregatedValue.substring(tabIdx + 1);
                    
                    batchedKvs.put("pt-image-links", imageHash, "url", imageUrl);
                    batchedKvs.put("pt-image-links", imageHash, "anchor", words);
                    
                } catch (Exception e) {
                    logger.warn("Error saving image: " + e.getMessage());
                }
                return Collections.emptyList();
            });

            long phase3Time = System.currentTimeMillis() - phase3Start;
            logger.info(String.format("Phase 3 complete: Aggregated images in %dms", phase3Time));

            // Cleanup
            try {
                urlRDD.destroy();
                imagePairs.destroy();
                aggregatedImages.destroy();
            } catch (Exception e) {
                logger.warn("Failed to destroy RDDs: " + e.getMessage());
            }

            long totalTime = System.currentTimeMillis() - jobStartTime;
            logger.info(String.format("MetaDataExtractor complete: Total time %dms (%.1fs)", totalTime, totalTime / 1000.0));

            ctx.output("MetaDataExtractor completed successfully. Created tables: pt-meta-info, pt-image-links, pt-pdf-links, pt-video-links, pt-outlinks");

        } catch (Exception e) {
            logger.error("MetaDataExtractor failed: " + e.getMessage(), e);
            ctx.output("MetaDataExtractor failed: " + e.getMessage());
            throw e;
        }
    }

    private static String stripQueryAndFragment(String url) {
        int idx = url.indexOf('?');
        if (idx > 0) return url.substring(0, idx);
        idx = url.indexOf('#');
        if (idx > 0) return url.substring(0, idx);
        return url;
    }

    private static boolean isVideo(String path) {
        for (String ext : VIDEO_EXTENSIONS) {
            if (path.endsWith(ext)) return true;
        }
        return false;
    }

    private static boolean isImage(String path) {
        for (String ext : IMAGE_EXTENSIONS) {
            if (path.endsWith(ext)) return true;
        }
        return false;
    }

    /**
     * Unescape HTML entities that were escaped by escapeHtml4() in Crawler
     */
    private static String unescapeHtml(String html) {
        if (html == null || html.isEmpty()) {
            return html;
        }
        return html.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&amp;", "&");
    }

    /**
     * Tokenize anchor text into space-separated words (lowercase, no stop words, no special chars)
     */
    private static String tokenizeToWords(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        
        // Clean and normalize
        String cleaned = text.toLowerCase()
                .replaceAll("[^\\p{L}\\p{N}\\s]", " ") // Remove special chars
                .replaceAll("\\s+", " ") // Normalize whitespace
                .trim();
        
        if (cleaned.isEmpty()) {
            return "";
        }
        
        // Split into words and filter
        String[] tokens = cleaned.split("\\s+");
        Set<String> words = new LinkedHashSet<>(); // Preserve order, avoid duplicates
        Set<String> stopWords = Set.of(
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "were", "will", "with", "this", "but", "they",
            "have", "had", "what", "said", "each", "which", "their", "time",
            "if", "up", "out", "many", "then", "them", "these", "so", "some",
            "her", "would", "make", "like", "into", "him", "two", "more",
            "very", "after", "words", "long", "than", "first", "been", "call",
            "who", "oil", "sit", "now", "find", "down", "day", "did", "get",
            "come", "made", "may", "part", "over", "sound", "take",
            "only", "little", "work", "know", "place", "year", "live", "me",
            "back", "give", "most", "thing", "our", "just",
            "name", "good", "sentence", "man", "think", "say", "great", "where",
            "help", "through", "much", "before", "line", "right", "too", "mean",
            "old", "any", "same", "tell", "boy", "follow", "came", "want",
            "show", "also", "around", "form", "three", "small", "set", "put",
            "end", "does", "another", "well", "large", "must", "big", "even",
            "such", "because", "turn", "here", "why", "ask", "went", "men",
            "read", "need", "land", "different", "home", "us", "move", "try",
            "kind", "hand", "picture", "again", "change", "off", "play", "spell",
            "air", "away", "animal", "house", "point", "page", "letter",
            "answer", "found", "study", "still", "learn", "should"
        );
        
        for (String token : tokens) {
            token = token.trim();
            if (!token.isEmpty() && token.length() >= 2 && !stopWords.contains(token)) {
                words.add(token);
            }
        }
        
        return String.join(" ", words);
    }
}