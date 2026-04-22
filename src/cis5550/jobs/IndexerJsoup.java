package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * JSOUP-OPTIMIZED Indexer - Uses JSoup for HTML parsing instead of regex
 * 
 * KEY IMPROVEMENTS OVER REGEX VERSION:
 * - JSoup parses HTML in a single pass (vs multiple regex passes)
 * - Handles malformed HTML gracefully
 * - CSS selectors for precise element extraction
 * - ~3-5x faster HTML processing
 * 
 * FLOW (same as IndexerOptimized):
 * 1. Read URLs from pt-crawl (small strings only)
 * 2. flatMapParallelWithContext:
 * - Reads full row from pt-crawl inside lambda
 * - Uses JSoup to parse and extract fields
 * - Emits word-url pairs
 * 3. foldByKey aggregates word entries
 * 4. Save to pt-index
 */
public class IndexerJsoup {
    private static final Logger logger = Logger.getLogger(IndexerJsoup.class);

    // Minimal patterns needed (JSoup handles most HTML parsing)
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    // Latin script (incl. accented letters) plus digits. Rejects other scripts.
    private static final Pattern ENGLISH_WORD_PATTERN = Pattern.compile("^[\\p{IsLatin}\\p{Nd}]+$");
    private static final Pattern URL_SPLIT_PATTERN = Pattern.compile("[^\\p{L}\\p{N}]+");
    private static final Pattern NON_ALPHANUM_PATTERN = Pattern.compile("[^\\p{L}\\p{N}\\s]");

    // Used only for manual cleaning, in case JSoup doesn't handle it correctly
    private static final Pattern SCRIPT_PATTERN = Pattern.compile("(?is)<script[^>]*>.*?</script>");
    private static final Pattern STYLE_PATTERN = Pattern.compile("(?is)<style[^>]*>.*?</style>");
    private static final Pattern NOSCRIPT_PATTERN = Pattern.compile("(?is)<noscript[^>]*>.*?</noscript>");
    private static final Pattern IFRAME_PATTERN = Pattern.compile("(?is)<iframe[^>]*>.*?</iframe>");
    private static final Pattern OBJECT_PATTERN = Pattern.compile("(?is)<object[^>]*>.*?</object>");
    private static final Pattern EMBED_PATTERN = Pattern.compile("(?is)<embed[^>]*>.*?</embed>");

    // Content extraction patterns
    private static final Pattern TITLE_PATTERN = Pattern.compile("<title[^>]*>(.*?)</title>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern META_DESC_PATTERN1 = Pattern.compile(
            "<meta[^>]*name\\s*=\\s*['\"]?description['\"]?[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"]",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern META_DESC_PATTERN2 = Pattern.compile(
            "<meta[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"][^>]*name\\s*=\\s*['\"]?description['\"]?",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern META_KEYWORDS_PATTERN1 = Pattern.compile(
            "<meta[^>]*name\\s*=\\s*['\"]?keywords['\"]?[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"]",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern META_KEYWORDS_PATTERN2 = Pattern.compile(
            "<meta[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"][^>]*name\\s*=\\s*['\"]?keywords['\"]?",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HEADER_PATTERN = Pattern.compile("<h[1-6][^>]*>(.*?)</h[1-6]>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern BODY_PATTERN = Pattern.compile("<body[^>]*>(.*?)</body>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // Stop words set
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "were", "will", "with", "the", "this", "but", "they",
            "have", "had", "what", "said", "each", "which", "their", "time",
            "if", "up", "out", "many", "then", "them", "these", "so", "some",
            "her", "would", "make", "like", "into", "him", "has", "two", "more",
            "very", "after", "words", "long", "than", "first", "been", "call",
            "who", "oil", "sit", "now", "find", "down", "day", "did", "get",
            "come", "made", "may", "part", "over", "sound", "take",
            "only", "little", "work", "know", "place", "year", "live", "me",
            "back", "give", "most", "very", "after", "thing", "our", "just",
            "name", "good", "sentence", "man", "think", "say", "great", "where",
            "help", "through", "much", "before", "line", "right", "too", "mean",
            "old", "any", "same", "tell", "boy", "follow", "came", "want",
            "show", "also", "around", "form", "three", "small", "set", "put",
            "end", "does", "another", "well", "large", "must", "big", "even",
            "such", "because", "turn", "here", "why", "ask", "went", "men",
            "read", "need", "land", "different", "home", "us", "move", "try",
            "kind", "hand", "picture", "again", "change", "off", "play", "spell",
            "air", "away", "animal", "house", "point", "page", "letter",
            "answer", "found", "study", "still", "learn", "should"));

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("IndexerJsoup job started - using JSoup for fast HTML parsing");

        long jobStartTime = System.currentTimeMillis();

        // Clear old index tables
        try {
            ctx.getKVS().delete("pt-index");
        } catch (Exception e) {
            logger.info("Error deleting index table: " + e.getMessage());
        }

        try {
            logger.info("Phase 1: Loading URLs from pt-crawl");
            long phase1StartTime = System.currentTimeMillis();

            FlameRDD urlRDD = ctx.fromTableParallel("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null) {
                    return null;
                }
                return url;
            });
            
            long urlCount = urlRDD.count();
            long phase1Time = System.currentTimeMillis() - phase1StartTime;
            logger.info(String.format("[INDEXER_PHASE1_COMPLETE] Found %d URLs without nulls | Took %dms (%.1fs)",
                    urlCount, phase1Time, phase1Time / 1000.0));

            logger.info("Phase 2: Processing pages with JSoup");

            // Timing statistics
            final AtomicLong totalDocsProcessed = new AtomicLong(0);
            final AtomicLong totalKvsReadTimeMs = new AtomicLong(0);
            final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
            final AtomicLong totalFullTimeMs = new AtomicLong(0);
            final AtomicLong totalTermsEmitted = new AtomicLong(0);
            final AtomicLong totalPageBytes = new AtomicLong(0);
            final long phase2StartTime = System.currentTimeMillis();

            FlamePairRDD wordUrlPairsRDD = urlRDD
                    .flatMapToPairParallel((url, workerKvs, batchedKvs) -> {
                        long docStartTime = System.currentTimeMillis();
                        long kvsReadTime = 0;
                        long processTime = 0;
                        int pageSize = 0;
                        int termsCount = 0;

                        try {
                            // KVS Read
                            long kvsReadStart = System.currentTimeMillis();
                            String hashedUrl = Hasher.hash(url);
                            Row crawlRow = workerKvs.getRow("pt-crawl", hashedUrl);
                            kvsReadTime = System.currentTimeMillis() - kvsReadStart;

                            if (crawlRow == null) {
                                return Collections.emptyList();
                            }

                            String page = crawlRow.get("page");
                            if (page == null || page.isEmpty()) {
                                return Collections.emptyList();
                            }

                            pageSize = page.length();

                            // Collect anchor text
                            StringBuilder anchorData = new StringBuilder();
                            for (String col : crawlRow.columns()) {
                                if (col.startsWith("anchors:")) {
                                    String anchorText = crawlRow.get(col);
                                    if (anchorText != null && !anchorText.isEmpty()) {
                                        if (anchorData.length() > 0)
                                            anchorData.append(" ");
                                        anchorData.append(anchorText);
                                    }
                                }
                            }
                            String anchorText = anchorData.toString();

                            // Process with JSoup
                            long processStart = System.currentTimeMillis();
                            List<FlamePair> wordUrlPairs = processPage(url, page, anchorText);
                            processTime = System.currentTimeMillis() - processStart;
                            termsCount = wordUrlPairs.size();

                            // Update stats
                            long fullDocTime = System.currentTimeMillis() - docStartTime;
                            long docNum = totalDocsProcessed.incrementAndGet();
                            totalKvsReadTimeMs.addAndGet(kvsReadTime);
                            totalProcessingTimeMs.addAndGet(processTime);
                            totalFullTimeMs.addAndGet(fullDocTime);
                            totalTermsEmitted.addAndGet(termsCount);
                            totalPageBytes.addAndGet(pageSize);

                            // Log every 100 documents
                            if (docNum % 100 == 0) {
                                double avgKvsRead = totalKvsReadTimeMs.get() / (double) docNum;
                                double avgProcess = totalProcessingTimeMs.get() / (double) docNum;
                                double avgFull = totalFullTimeMs.get() / (double) docNum;
                                double docsPerSec = docNum * 1000.0 / (System.currentTimeMillis() - phase2StartTime);
                                double avgTermsPerDoc = totalTermsEmitted.get() / (double) docNum;
                                double avgPageKB = totalPageBytes.get() / (double) docNum / 1024.0;

                                logger.info(String.format(
                                        "[INDEXER_TIMING] Doc #%d | This doc: kvs=%dms, process=%dms, total=%dms, terms=%d, pageKB=%.1f | "
                                                +
                                                "Avg: kvs=%.1fms, process=%.1fms, total=%.1fms, terms=%.0f, pageKB=%.1f | %.1f docs/sec",
                                        docNum, kvsReadTime, processTime, fullDocTime, termsCount, pageSize / 1024.0,
                                        avgKvsRead, avgProcess, avgFull, avgTermsPerDoc, avgPageKB, docsPerSec));
                            }

                            // Log slow documents
                            if (fullDocTime > 500) {
                                logger.warn(String.format(
                                        "[INDEXER_SLOW_DOC] URL=%s | kvs=%dms, process=%dms, total=%dms, terms=%d, pageKB=%.1f",
                                        url.length() > 80 ? url.substring(0, 80) + "..." : url,
                                        kvsReadTime, processTime, fullDocTime, termsCount, pageSize / 1024.0));
                            }

                            return wordUrlPairs;

                        } catch (Exception e) {
                            long errorTime = System.currentTimeMillis() - docStartTime;
                            logger.warn(String.format("[INDEXER_ERROR] URL=%s | time=%dms | error=%s",
                                    url.length() > 60 ? url.substring(0, 60) + "..." : url, errorTime, e.getMessage()));
                            return Collections.emptyList();
                        }
                    });

            long phase2Time = System.currentTimeMillis() - phase2StartTime;
            long docsProcessed = totalDocsProcessed.get();
            if (docsProcessed > 0) {
                logger.info(String.format(
                        "[INDEXER_PHASE2_COMPLETE] Docs=%d | TotalTime=%dms (%.1fs) | " +
                                "AvgPerDoc: kvs=%.1fms, process=%.1fms, total=%.1fms | " +
                                "AvgTerms=%.0f | AvgPageKB=%.1f | Throughput=%.1f docs/sec",
                        docsProcessed, phase2Time, phase2Time / 1000.0,
                        totalKvsReadTimeMs.get() / (double) docsProcessed,
                        totalProcessingTimeMs.get() / (double) docsProcessed,
                        totalFullTimeMs.get() / (double) docsProcessed,
                        totalTermsEmitted.get() / (double) docsProcessed,
                        totalPageBytes.get() / (double) docsProcessed / 1024.0,
                        docsProcessed * 1000.0 / phase2Time));
            }

            // Destroy urlRDD to free memory - it's no longer needed after Phase 2
            try {
                urlRDD.destroy();
                logger.info("[INDEXER_MEMORY] Destroyed urlRDD table to free memory");
            } catch (Exception e) {
                logger.warn("[INDEXER_MEMORY] Failed to destroy urlRDD: " + e.getMessage());
            }

            logger.info("Phase 3: Aggregating with foldByKey");
            long phase3StartTime = System.currentTimeMillis();

            FlamePairRDD invertedIndex = wordUrlPairsRDD.foldByKey("", (a, b) -> {
                if (a == null || a.isEmpty())
                    return b;
                if (b == null || b.isEmpty())
                    return a;
                return a + "," + b;
            });

            long phase3Time = System.currentTimeMillis() - phase3StartTime;
            logger.info(String.format("[INDEXER_PHASE3_COMPLETE] foldByKey took %dms (%.1fs)", phase3Time,
                    phase3Time / 1000.0));

            // Destroy wordUrlPairsRDD to free memory - it's no longer needed after Phase 3
            try {
                wordUrlPairsRDD.destroy();
                logger.info("[INDEXER_MEMORY] Destroyed wordUrlPairsRDD table to free memory");
            } catch (Exception e) {
                logger.warn("[INDEXER_MEMORY] Failed to destroy wordUrlPairsRDD: " + e.getMessage());
            }

            logger.info("Phase 4: Saving to pt-index");
            long phase4StartTime = System.currentTimeMillis();

            invertedIndex.saveAsTable("pt-index");

            long phase4Time = System.currentTimeMillis() - phase4StartTime;
            logger.info(String.format("[INDEXER_PHASE4_COMPLETE] saveAsTable took %dms (%.1fs)", phase4Time,
                    phase4Time / 1000.0));

            long totalTime = phase1Time + phase2Time + phase3Time + phase4Time;
            long actualTotalTime = System.currentTimeMillis() - jobStartTime;
            logger.info(String.format(
                    "[INDEXER_COMPLETE] Total indexing time: %dms (%.1fs) | Actual wall-clock time: %dms (%.1fs) | " +
                            "Phase1(readUrls)=%dms, Phase2(jsoupParse)=%dms, Phase3(foldByKey)=%dms, Phase4(save)=%dms",
                    totalTime, totalTime / 1000.0, actualTotalTime, actualTotalTime / 1000.0,
                    phase1Time, phase2Time, phase3Time, phase4Time));

            ctx.output(
                    "Inverted index built successfully (JSoup optimized). Total time: " + (totalTime / 1000.0) + "s");

        } catch (Exception e) {
            logger.error("Error during indexing: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Process a single page using JSoup for HTML parsing.
     * Returns list of FlamePairs: (word, url|totalTf|titleTf|...|positions)
     */
    private static List<FlamePair> processPage(String url, String page, String anchorText) {
        List<FlamePair> results = new ArrayList<>();

        try {
            logger.debug("========================================");
            logger.debug("  [processPage] Processing URL: " + (url.length() > 80 ? url.substring(0, 80) + "..." : url));
            logger.debug("  [processPage] Page HTML length: " + (page != null ? page.length() : 0) + " characters");
            if (anchorText != null && !anchorText.isEmpty()) {
                logger.debug("  [processPage] Anchor text length: " + anchorText.length() + " characters");
            }

            // Unescape HTML entities that were escaped by escapeHtml4() in Crawler
            page = unescapeHtml(page);

            FieldContent fields;
            // Try parsing HTML with JSoup - handles malformed HTML gracefully
            try {
                Document doc = Jsoup.parse(page, url);
                // Remove script, style, and other non-content elements
                doc.select("script, style, noscript, iframe, object, embed, svg, canvas").remove();
                // Extract fields using CSS selectors (much faster than regex)
                fields = extractFieldsWithJsoup(doc);
            } catch (Exception e) {
                logger.warn("JSoup parse failed for URL: " + url + ", error: " + e.getMessage());
                logger.warn("Falling back to manual cleaning");
                fields = extractFields(page, url);
            }

            logger.debug("  [processPage] Extracted fields summary:");
            logger.debug("    Title: [" + fields.title + "] (" + fields.titleWordCount + " words)");
            logger.debug("    Meta description: [" + fields.metaDescription + "]");
            logger.debug("    Meta keywords: [" + fields.metaKeywords + "]");
            logger.debug("    Headers: "
                    + (fields.headers.length() > 200 ? fields.headers.substring(0, 200) + "..." : fields.headers) + " ("
                    + fields.headerWordCount + " words)");
            logger.debug(
                    "    Body: [" + (fields.body.length() > 200 ? fields.body.substring(0, 200) + "..." : fields.body)
                            + "] (" + fields.bodyWordCount + " words)");

            // Process each field with global position tracking
            Map<String, TermInfo> termInfoMap = new HashMap<>();
            int globalPosition = 0;

            // Index title
            globalPosition = processText(fields.title, "title", termInfoMap, globalPosition);

            // Index meta tags
            String metaText = fields.metaDescription + " " + fields.metaKeywords;
            globalPosition = processText(metaText, "meta", termInfoMap, globalPosition);

            // Index headers
            globalPosition = processText(fields.headers, "header", termInfoMap, globalPosition);

            // Index body
            globalPosition = processText(fields.body, "body", termInfoMap, globalPosition);

            // Index URL tokens
            int totalUrlTokens = processUrlTokens(url, termInfoMap);

            // Index anchor text
            int totalAnchorWords = 0;
            if (anchorText != null && !anchorText.isEmpty()) {
                logger.debug("  [processPage] Processing anchor text, raw length: " + anchorText.length());
                String anchorPreview = anchorText.length() > 200 ? anchorText.substring(0, 200) + "..." : anchorText;
                logger.debug("  [processPage] Anchor text preview: [" + anchorPreview + "]");
                String cleanedAnchorText = cleanTextSimple(anchorText);
                String[] anchorWords = WHITESPACE_PATTERN.split(cleanedAnchorText);
                for (String word : anchorWords) {
                    if (!word.isEmpty()) {
                        totalAnchorWords++;
                    }
                }
                logger.debug("  [processPage] Found " + totalAnchorWords + " valid anchor word(s)");
                processText(cleanedAnchorText, "anchor", termInfoMap, 0);
            } else {
                logger.debug("  [processPage] No anchor text found");
            }

            int totalDocLength = fields.titleWordCount + fields.headerWordCount + fields.bodyWordCount;

            // Create index entries for each term
            SnowballStemmer stemmer = new englishStemmer();
            for (Map.Entry<String, TermInfo> entry : termInfoMap.entrySet()) {
                String w = entry.getKey();
                TermInfo info = entry.getValue();

                if (w == null || w.isEmpty()) {
                    continue;
                }

                if (STOP_WORDS.contains(w.toLowerCase())) {
                    continue;
                }

                // Stem the word
                stemmer.setCurrent(w);
                stemmer.stem();
                String stemmedWord = stemmer.getCurrent();
                // logger.debug(" [processPage] Stemmed word: '" + w + "' -> '" + stemmedWord +
                // "'");

                if (stemmedWord == null || stemmedWord.isEmpty()) {
                    continue;
                }

                // Calculate TF values
                double titleTf = totalDocLength > 0 ? (double) info.titleFreq / totalDocLength : 0.0;
                double metaTf = totalDocLength > 0 ? (double) info.metaFreq / totalDocLength : 0.0;
                double headerTf = totalDocLength > 0 ? (double) info.headerFreq / totalDocLength : 0.0;
                double bodyTf = totalDocLength > 0 ? (double) info.bodyFreq / totalDocLength : 0.0;
                double urlTf = totalUrlTokens > 0 ? (double) info.urlFreq / totalUrlTokens : 0.0;
                double anchorTf = totalAnchorWords > 0 ? (double) info.anchorFreq / totalAnchorWords : 0.0;
                double totalTf = totalDocLength > 0 ? (double) info.totalFreq / totalDocLength : 0.0;

                // Sort positions
                List<Integer> sortedPositions = new ArrayList<>(info.positions);
                sortedPositions.sort(Integer::compareTo);

                List<String> sortedPosStrings = new ArrayList<>(sortedPositions.size());
                for (Integer pos : sortedPositions) {
                    sortedPosStrings.add(String.valueOf(pos));
                }

                // Build value string
                StringBuilder valueBuilder = new StringBuilder();
                valueBuilder.append(url).append("|");
                valueBuilder.append(totalTf).append("|");
                valueBuilder.append(titleTf).append("|");
                valueBuilder.append(metaTf).append("|");
                valueBuilder.append(headerTf).append("|");
                valueBuilder.append(bodyTf).append("|");
                valueBuilder.append(urlTf).append("|");
                valueBuilder.append(anchorTf).append("|");
                valueBuilder.append(String.join(" ", sortedPosStrings));

                // Emit both original and stemmed if different
                if (Objects.equals(w, stemmedWord)) {
                    if (!w.isEmpty()) {
                        results.add(new FlamePair(w, valueBuilder.toString()));
                    }
                } else {
                    if (!stemmedWord.isEmpty()) {
                        results.add(new FlamePair(stemmedWord, valueBuilder.toString()));
                    }
                    if (!w.isEmpty()) {
                        results.add(new FlamePair(w, valueBuilder.toString()));
                    }
                }
            }

            logger.debug("  [processPage] Indexing complete for URL: "
                    + (url.length() > 80 ? url.substring(0, 80) + "..." : url));
            logger.debug("  [processPage] Total unique terms indexed: " + termInfoMap.size());
            logger.debug("  [processPage] Total index entries generated: " + results.size());
            logger.debug("========================================");

            return results;
        } catch (Exception e) {
            logger.error("  [processPage] Error processing URL " + url + ": " + e.getMessage(), e);
            logger.error("  [processPage] Skipping URL and continuing...");
            logger.debug("========================================");
            return Collections.emptyList();
        }
    }

    /**
     * Extract fields using JSoup CSS selectors - much faster than regex
     */
    private static FieldContent extractFieldsWithJsoup(Document doc) {
        FieldContent fields = new FieldContent();

        // Extract title (single element)
        Element titleEl = doc.selectFirst("title");
        if (titleEl != null) {
            fields.title = cleanTextSimple(titleEl.text());
        }

        // Extract meta description
        Element metaDesc = doc.selectFirst("meta[name=description]");
        if (metaDesc != null) {
            fields.metaDescription = cleanTextSimple(metaDesc.attr("content"));
        }

        // Extract meta keywords
        Element metaKeywords = doc.selectFirst("meta[name=keywords]");
        if (metaKeywords != null) {
            fields.metaKeywords = cleanTextSimple(metaKeywords.attr("content"));
        }

        // Extract headers (h1-h6) - single selector for all
        Elements headers = doc.select("h1, h2, h3, h4, h5, h6");
        StringBuilder headerText = new StringBuilder();
        for (Element h : headers) {
            if (headerText.length() > 0)
                headerText.append(" ");
            headerText.append(h.text());
        }
        fields.headers = cleanTextSimple(headerText.toString());

        // Extract body text - JSoup handles this elegantly
        Element body = doc.body();
        if (body != null) {
            // Remove headers from body to avoid double-counting
            body.select("h1, h2, h3, h4, h5, h6").remove();
            // Get text content - JSoup automatically handles whitespace
            fields.body = cleanTextSimple(body.text());
        }

        // Count words
        fields.titleWordCount = countWords(fields.title);
        fields.headerWordCount = countWords(fields.headers);
        fields.bodyWordCount = countWords(fields.body);

        return fields;
    }

    /**
     * Extract fields manually using regex, in case JSoup doesn't handle it
     * correctly
     */
    private static FieldContent extractFields(String html, String url) {
        FieldContent fields = new FieldContent();
        if (html == null || html.isEmpty()) {
            return fields;
        }

        // Remove script and style tags (using pre-compiled patterns)
        html = SCRIPT_PATTERN.matcher(html).replaceAll(" ");
        html = STYLE_PATTERN.matcher(html).replaceAll(" ");
        html = NOSCRIPT_PATTERN.matcher(html).replaceAll(" ");
        html = IFRAME_PATTERN.matcher(html).replaceAll(" ");
        html = OBJECT_PATTERN.matcher(html).replaceAll(" ");
        html = EMBED_PATTERN.matcher(html).replaceAll(" ");

        // Extract title (using pre-compiled pattern)
        Matcher titleMatcher = TITLE_PATTERN.matcher(html);
        if (titleMatcher.find()) {
            String rawTitle = titleMatcher.group(1);
            logger.debug("  [extractFields] Found <title> tag, raw content length: " + rawTitle.length());
            fields.title = cleanText(rawTitle, "title");
        } else {
            logger.debug("  [extractFields] No <title> tag found");
        }

        // Extract meta description (using pre-compiled patterns)
        Matcher metaDescMatcher = META_DESC_PATTERN1.matcher(html);
        if (metaDescMatcher.find()) {
            String rawMetaDesc = metaDescMatcher.group(1);
            logger.debug(
                    "  [extractFields] Found meta description (pattern1), raw content length: " + rawMetaDesc.length());
            fields.metaDescription = cleanText(rawMetaDesc, "meta-description");
        }
        if (fields.metaDescription.isEmpty()) {
            metaDescMatcher = META_DESC_PATTERN2.matcher(html);
            if (metaDescMatcher.find()) {
                String rawMetaDesc = metaDescMatcher.group(1);
                logger.debug("  [extractFields] Found meta description (pattern2), raw content length: "
                        + rawMetaDesc.length());
                fields.metaDescription = cleanText(rawMetaDesc, "meta-description");
            } else {
                logger.debug("  [extractFields] No meta description found");
            }
        }

        // Extract meta keywords (using pre-compiled patterns)
        Matcher metaKeywordsMatcher = META_KEYWORDS_PATTERN1.matcher(html);
        if (metaKeywordsMatcher.find()) {
            String rawMetaKeywords = metaKeywordsMatcher.group(1);
            logger.debug("  [extractFields] Found meta keywords (pattern1), raw content length: "
                    + rawMetaKeywords.length());
            fields.metaKeywords = cleanText(rawMetaKeywords, "meta-keywords");
        }
        if (fields.metaKeywords.isEmpty()) {
            metaKeywordsMatcher = META_KEYWORDS_PATTERN2.matcher(html);
            if (metaKeywordsMatcher.find()) {
                String rawMetaKeywords = metaKeywordsMatcher.group(1);
                logger.debug("  [extractFields] Found meta keywords (pattern2), raw content length: "
                        + rawMetaKeywords.length());
                fields.metaKeywords = cleanText(rawMetaKeywords, "meta-keywords");
            } else {
                logger.debug("  [extractFields] No meta keywords found");
            }
        }

        // Extract headers (h1-h6) using pre-compiled pattern
        StringBuilder headersBuilder = new StringBuilder();
        Matcher headerMatcher = HEADER_PATTERN.matcher(html);
        int headerCount = 0;
        while (headerMatcher.find()) {
            headerCount++;
            String rawHeader = headerMatcher.group(1);
            logger.debug("  [extractFields] Found header #" + headerCount + ", raw content: [" + rawHeader + "]");
            if (headersBuilder.length() > 0)
                headersBuilder.append(" ");
            String cleanedHeader = cleanText(rawHeader, "header-" + headerCount);
            headersBuilder.append(cleanedHeader);
        }
        fields.headers = headersBuilder.toString();
        logger.debug("  [extractFields] Found " + headerCount + " header(s) (h1-h6)");

        // Extract body content
        String bodyHtml = html;
        Matcher bodyMatcher = BODY_PATTERN.matcher(bodyHtml);
        if (bodyMatcher.find()) {
            bodyHtml = bodyMatcher.group(1);
            logger.debug(
                    "  [extractFields] Found <body> tag, extracted body content (length: " + bodyHtml.length() + ")");
        } else {
            // If no body tag, try to remove head section and DOCTYPE to avoid structural
            // terms
            logger.debug("  [extractFields] No <body> tag found, removing head section and DOCTYPE");
            // Remove head section if present
            bodyHtml = Pattern.compile("(?is)<head[^>]*>.*?</head>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)
                    .matcher(bodyHtml).replaceAll(" ");
            // Remove DOCTYPE if present
            bodyHtml = Pattern.compile("(?i)<!DOCTYPE[^>]*>", Pattern.CASE_INSENSITIVE)
                    .matcher(bodyHtml).replaceAll(" ");
            logger.debug("  [extractFields] Using HTML after head removal (length: " + bodyHtml.length() + ")");
        }

        // OPTIMIZATION: Body already had scripts/styles removed above, skip redundant
        // removals
        // Only remove title and headers that might appear in body
        int beforeRemoval = bodyHtml.length();
        bodyHtml = TITLE_PATTERN.matcher(bodyHtml).replaceAll(" ");
        bodyHtml = HEADER_PATTERN.matcher(bodyHtml).replaceAll(" ");
        if (bodyHtml.length() != beforeRemoval) {
            logger.debug("  [extractFields] Removed title/headers from body (length: " + beforeRemoval + " -> "
                    + bodyHtml.length() + ")");
        }

        String bodyPreview = bodyHtml.length() > 300 ? bodyHtml.substring(0, 300) + "..." : bodyHtml;
        logger.debug("  [extractFields] Body HTML before cleaning (preview): [" + bodyPreview + "]");
        fields.body = cleanText(bodyHtml, "body");

        // Count words
        fields.titleWordCount = countWords(fields.title);
        fields.headerWordCount = countWords(fields.headers);
        fields.bodyWordCount = countWords(fields.body);

        logger.debug("  [extractFields] Word counts - Title: " + fields.titleWordCount +
                ", Headers: " + fields.headerWordCount + ", Body: " + fields.bodyWordCount);

        return fields;
    }

    /**
     * Simple text cleaning - JSoup already handles HTML parsing
     */
    private static String cleanTextSimple(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }

        // Remove any remaining URLs and emails
        text = text.replaceAll("https?://\\S+", " ");
        text = text.replaceAll("www\\.\\S+", " ");
        text = text.replaceAll("[\\w.-]+@[\\w.-]+\\.\\w+", " ");

        // Remove special characters, keep only letters, numbers, spaces
        text = NON_ALPHANUM_PATTERN.matcher(text).replaceAll(" ");

        // Lowercase and normalize whitespace
        text = text.toLowerCase();
        text = WHITESPACE_PATTERN.matcher(text).replaceAll(" ").trim();

        return text;
    }

    /**
     * Text cleaning - uses regex for more comprehensive cleaning, used during
     * manual cleaning only if JSoup doesn't handle it correctly
     */
    private static String cleanText(String text, String fieldName) {
        if (text == null) {
            logger.debug("    [cleanText] Input text is null for field: " + fieldName);
            return "";
        }

        String preview = text.length() > 200 ? text.substring(0, 200) + "..." : text;
        logger.debug("    [cleanText] Cleaning field: " + fieldName + ", input length: " + text.length());
        logger.debug("    [cleanText] Input preview: [" + preview + "]");

        // STEP 1: Decode HTML entities
        // Note: extractFields() already unescapes HTML before extraction, but the
        // extracted
        // text content may still contain entities (e.g., &nbsp; in content, &amp; in
        // attributes)
        // Order matters: &amp; must be decoded LAST to avoid double-decoding
        text = text.replace("&lt;", "<");
        text = text.replace("&gt;", ">");
        text = text.replace("&quot;", "\"");
        text = text.replace("&apos;", "'");
        text = text.replace("&#39;", "'");
        text = text.replace("&amp;", "&"); // Do this last to avoid double-decoding
        text = text.replace("&nbsp;", " ");

        // Handle other common HTML entities
        text = text.replace("&copy;", " ");
        text = text.replace("&reg;", " ");
        text = text.replace("&trade;", " ");
        text = text.replace("&mdash;", " ");
        text = text.replace("&ndash;", " ");
        text = text.replace("&hellip;", " ");

        // Handle numeric entities (convert to space to avoid gibberish)
        text = text.replaceAll("&#(\\d+);", " "); // Remove numeric entities like &#123;
        text = text.replaceAll("&#x([0-9a-fA-F]+);", " "); // Remove hex entities like &#x3C;

        // Handle entities without semicolons (malformed)
        text = text.replace("&lt", "<");
        text = text.replace("&gt", ">");
        text = text.replace("&amp", "&");
        text = text.replace("&nbsp", " ");

        // STEP 2: Remove HTML comments (must be after entity decoding)
        String beforeCommentRemoval = text;
        text = Pattern.compile("(?s)<!--.*?-->", Pattern.CASE_INSENSITIVE).matcher(text).replaceAll(" ");
        if (!text.equals(beforeCommentRemoval)) {
            logger.debug("    [cleanText] Removed HTML comments");
        }

        // STEP 3: Remove CDATA sections
        String beforeCDataRemoval = text;
        text = Pattern.compile("(?s)<!\\[CDATA\\[.*?\\]\\]>", Pattern.CASE_INSENSITIVE).matcher(text).replaceAll(" ");
        if (!text.equals(beforeCDataRemoval)) {
            logger.debug("    [cleanText] Removed CDATA sections");
        }

        // STEP 4: Remove DOCTYPE and processing instructions
        String beforeDocTypeRemoval = text;
        text = Pattern.compile("(?s)<!DOCTYPE[^>]*>", Pattern.CASE_INSENSITIVE).matcher(text).replaceAll(" ");
        text = Pattern.compile("(?s)<\\?[^>]*\\?>", Pattern.CASE_INSENSITIVE).matcher(text).replaceAll(" ");
        if (!text.equals(beforeDocTypeRemoval)) {
            logger.debug("    [cleanText] Removed DOCTYPE/processing instructions");
        }

        // STEP 5: Remove script/style tags and their content (defensive, in case
        // extractFields missed some)
        String beforeScriptRemoval = text;
        text = Pattern.compile("(?is)<script[^>]*>.*?</script>").matcher(text).replaceAll(" ");
        text = Pattern.compile("(?is)<style[^>]*>.*?</style>").matcher(text).replaceAll(" ");
        text = Pattern.compile("(?is)<noscript[^>]*>.*?</noscript>").matcher(text).replaceAll(" ");
        if (!text.equals(beforeScriptRemoval)) {
            logger.debug("    [cleanText] Removed additional script/style tags");
        }

        // STEP 6: Remove HTML tags - ITERATIVE to handle nested/malformed tags
        String beforeTagRemoval = text;
        int iterations = 0;
        int maxIterations = 10; // Safety limit
        String previousText = text;

        while (iterations < maxIterations) {
            // Match well-formed tags: <tag>, </tag>, <tag attr="value">, <tag/>, <tag />
            text = text.replaceAll("(?s)<[^>]+>", " ");

            // Match malformed/unclosed tags: <tag (without closing >)
            text = Pattern.compile("<[^>]*$", Pattern.MULTILINE).matcher(text).replaceAll(" ");
            text = Pattern.compile("^[^<]*>", Pattern.MULTILINE).matcher(text).replaceAll(" ");

            // Check if we made progress
            if (text.equals(previousText)) {
                break; // No more tags found
            }
            previousText = text;
            iterations++;
        }

        if (!text.equals(beforeTagRemoval)) {
            int tagCount = countMatches(beforeTagRemoval, "(?s)<[^>]+>");
            logger.debug("    [cleanText] Removed " + tagCount + " HTML tag(s) in " + iterations + " iteration(s)");
        }

        // STEP 7: Remove any remaining HTML entities (catch any we missed)
        text = text.replaceAll("&[#\\w]+;?", " "); // ? makes semicolon optional
        text = text.replaceAll("&[^\\s;]+", " "); // Catch ampersands not followed by space/semicolon

        // STEP 8: Remove CSS selectors and class/id patterns (defensive cleanup)
        text = text.replaceAll("\\.[\\w-]+", " "); // CSS class selectors
        text = text.replaceAll("#[\\w-]+", " "); // CSS ID selectors
        text = text.replaceAll("\\[\\w[^\\]]*\\]", " "); // Attribute selectors
        text = text.replaceAll("[\\w-]+[>+~][\\w-]+", " "); // Combinators
        text = text.replaceAll("\\.[\\w-]+[>+~]", " ");
        text = text.replaceAll("[>+~]\\.[\\w-]+", " ");

        // STEP 9: Remove URLs and email addresses
        text = text.replaceAll("https?://[\\S]+", " ");
        text = text.replaceAll("www\\.[\\S]+", " ");
        text = text.replaceAll("[\\w.-]+@[\\w.-]+\\.[\\w]+", " ");

        // STEP 10: Remove all special characters and punctuation
        // Keep only letters, numbers, and spaces
        text = text.replaceAll("[^\\p{L}\\p{N}\\s]", " ");

        // STEP 11: Convert to lowercase
        text = text.toLowerCase();

        // STEP 12: Normalize whitespace
        text = text.replaceAll("\\s+", " ").trim();

        List<String> words = Arrays.asList(text.split("\\s+"));
        logger.debug("    [cleanText] Final cleaned text for " + fieldName + " (length: "
                + text.length() + ")");
        logger.debug("    [cleanText] Extracted " + words.size() + " word(s)");
        if (words.size() > 0 && words.size() <= 50) {
            logger.debug("    [cleanText] Words: " + words);
        } else if (words.size() > 50) {
            List<String> first50 = words.subList(0, 50);
            logger.debug("    [cleanText] First 50 words: " + first50 + " ... (total: "
                    + words.size() + ")");
        }

        return text;
    }

    private static int countMatches(String text, String pattern) {
        if (text == null || text.isEmpty())
            return 0;
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(text);
        int count = 0;
        while (m.find())
            count++;
        return count;
    }

    /**
     * Unescape HTML entities from Crawler's escapeHtml4()
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

    private static int countWords(String text) {
        if (text == null || text.isEmpty())
            return 0;
        String[] words = WHITESPACE_PATTERN.split(text);
        int count = 0;
        for (String w : words) {
            if (!w.isEmpty())
                count++;
        }
        return count;
    }

    private static int processText(String text, String fieldType, Map<String, TermInfo> termInfoMap,
            int globalPosition) {
        try {
            if (text == null || text.isEmpty()) {
                logger.debug("  [processText] Skipping empty text for field: " + fieldType);
                return globalPosition;
            }

            String[] words = WHITESPACE_PATTERN.split(text);
            logger.debug("  [processText] Processing field: " + fieldType + ", total words before filtering: "
                    + words.length);

            List<String> validWords = new ArrayList<>();

            for (String w : words) {
                if (w.isEmpty())
                    continue;

                // Skip non-English words (must be purely A-Z/a-z)
                if (!ENGLISH_WORD_PATTERN.matcher(w).matches()) {
                    continue;
                }

                if (STOP_WORDS.contains(w.toLowerCase()))
                    continue;

                validWords.add(w);
                TermInfo info = termInfoMap.computeIfAbsent(w, k -> new TermInfo());
                info.positions.add(globalPosition);
                info.totalFreq++;

                switch (fieldType) {
                    case "title":
                        info.titleFreq++;
                        break;
                    case "meta":
                        info.metaFreq++;
                        break;
                    case "header":
                        info.headerFreq++;
                        break;
                    case "body":
                        info.bodyFreq++;
                        break;
                    case "anchor":
                        info.anchorFreq++;
                        break;
                }

                globalPosition++;
            }

            logger.debug("  [processText] Field " + fieldType + " - Valid words after filtering: " + validWords.size());
            if (validWords.size() > 0 && validWords.size() <= 30) {
                logger.debug("  [processText] Words indexed from " + fieldType + ": " + validWords);
            } else if (validWords.size() > 30) {
                List<String> first30 = validWords.subList(0, 30);
                logger.debug("  [processText] First 30 words indexed from " + fieldType + ": " + first30
                        + " ... (total: " + validWords.size() + ")");
            }

            return globalPosition;
        } catch (Exception e) {
            logger.warn("  [processText] Error in field " + fieldType + ": " + e.getMessage(), e);
            return globalPosition;
        }
    }

    private static int processUrlTokens(String url, Map<String, TermInfo> termInfoMap) {
        try {
            if (url == null || url.isEmpty()) {
                logger.debug("  [processUrlTokens] URL is null or empty");
                return 0;
            }

            logger.debug("  [processUrlTokens] Processing URL: "
                    + (url.length() > 100 ? url.substring(0, 100) + "..." : url));
            String urlLower = url.toLowerCase();
            String[] tokens = URL_SPLIT_PATTERN.split(urlLower);
            logger.debug("  [processUrlTokens] Split URL into " + tokens.length + " token(s)");

            int tokensIndexed = 0;
            List<String> validTokens = new ArrayList<>();
            for (String token : tokens) {
                if (token.isEmpty())
                    continue;

                // Skip non-English URL tokens
                if (!ENGLISH_WORD_PATTERN.matcher(token).matches()) {
                    continue;
                }

                // Skip common URL parts
                if (token.equals("http") || token.equals("https") || token.equals("www") ||
                        token.equals("com") || token.equals("org") || token.equals("net") ||
                        token.equals("edu") || token.equals("gov") ||
                        STOP_WORDS.contains(token.toLowerCase())) {
                    continue;
                }

                validTokens.add(token);
                TermInfo info = termInfoMap.computeIfAbsent(token, k -> new TermInfo());
                info.totalFreq++;
                info.urlFreq++;
                tokensIndexed++;
            }

            logger.debug("  [processUrlTokens] Indexed " + tokensIndexed + " valid URL token(s)");
            if (validTokens.size() > 0 && validTokens.size() <= 20) {
                logger.debug("  [processUrlTokens] URL tokens indexed: " + validTokens);
            } else if (validTokens.size() > 20) {
                List<String> first20 = validTokens.subList(0, 20);
                logger.debug("  [processUrlTokens] First 20 URL tokens indexed: " + first20 + " ... (total: "
                        + validTokens.size() + ")");
            }

            return tokensIndexed;
        } catch (Exception e) {
            logger.warn("  [processUrlTokens] Error processing URL tokens for: " + url + " -> " + e.getMessage(), e);
            return 0;
        }
    }

    // Helper classes
    private static class FieldContent {
        String title = "";
        String headers = "";
        String body = "";
        String metaDescription = "";
        String metaKeywords = "";
        int titleWordCount = 0;
        int headerWordCount = 0;
        int bodyWordCount = 0;
    }

    public static class TermInfo {
        public int totalFreq = 0;
        public int titleFreq = 0;
        public int metaFreq = 0;
        public int headerFreq = 0;
        public int bodyFreq = 0;
        public int urlFreq = 0;
        public int anchorFreq = 0;
        public List<Integer> positions = new ArrayList<>();
    }
}