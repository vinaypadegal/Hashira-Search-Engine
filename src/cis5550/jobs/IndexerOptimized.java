package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.BatchedKVSClient;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.PorterStemmer;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * OPTIMIZED Indexer - Uses flatMapParallelWithContext to avoid OOM
 * 
 * KEY DIFFERENCE FROM ORIGINAL:
 * - Original: fromTable returns "url\tpage\tanchor" (300KB+ strings) → OOM
 * - Optimized: flatMapParallelWithContext reads pt-crawl directly, emits small
 * word-url pairs
 * 
 * FLOW:
 * 1. Read URLs from pt-crawl (small strings only)
 * 2. flatMapParallelWithContext:
 * - Reads full row from pt-crawl inside lambda (page content stays in worker
 * memory only)
 * - Extracts fields, processes text, emits word-url pairs
 * - No intermediate table contains page content!
 * 3. foldByKeyOptimized aggregates word entries
 * 4. Save to pt-index
 */
public class IndexerOptimized {
    private static final Logger logger = Logger.getLogger(IndexerOptimized.class);

    // ================== PRE-COMPILED PATTERNS (HUGE PERFORMANCE BOOST)
    // ==================
    // Pattern compilation is expensive - doing it once at class load instead of
    // per-document

    // HTML tag removal patterns
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

    // Text cleaning patterns
    private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]+>");
    private static final Pattern HTML_ENTITY_PATTERN = Pattern.compile("&[#\\w]+;");
    private static final Pattern CSS_CLASS_PATTERN = Pattern.compile("\\.[\\w-]+");
    private static final Pattern CSS_ID_PATTERN = Pattern.compile("#[\\w-]+");
    private static final Pattern CSS_ATTR_PATTERN = Pattern.compile("\\[\\w[^\\]]*\\]");
    private static final Pattern CSS_COMBINATOR_PATTERN = Pattern.compile("[\\w-]+[>+~][\\w-]+");
    private static final Pattern URL_PATTERN = Pattern.compile("https?://[\\S]+");
    private static final Pattern WWW_PATTERN = Pattern.compile("www\\.[\\S]+");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("[\\w.-]+@[\\w.-]+\\.[\\w]+");
    private static final Pattern NON_ALPHANUM_PATTERN = Pattern.compile("[^\\p{L}\\p{N}\\s]");
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

    // Word validation pattern - must contain at least one letter
    private static final Pattern WORD_PATTERN = Pattern.compile("^[\\p{L}\\p{N}]+$");
    // private static final Pattern HAS_LETTER_PATTERN =
    // Pattern.compile(".*[\\p{L}].*");
    // private static final int MIN_WORD_LENGTH = 2; // Minimum word length (exclude
    // single chars)
    // private static final int MAX_WORD_LENGTH = 50; // Maximum word length (filter
    // out encoded strings)

    // URL tokenization patterns
    private static final Pattern URL_SPLIT_PATTERN = Pattern.compile("[^\\p{L}\\p{N}]+");

    // Stop words set - common English stop words to ignore during indexing
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "were", "will", "with", "the", "this", "but", "they",
            "have", "had", "what", "said", "each", "which", "their", "time",
            "if", "up", "out", "many", "then", "them", "these", "so", "some",
            "her", "would", "make", "like", "into", "him", "has", "two", "more",
            "very", "after", "words", "long", "than", "first", "been", "call",
            "who", "oil", "sit", "now", "find", "down", "day", "did", "get",
            "come", "made", "may", "part", "over", "new", "sound", "take",
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
            "air", "away", "animal", "house", "point", "page", "letter", "mother",
            "answer", "found", "study", "still", "learn", "should", "america", "world"));

    // HTML structural terms - words from HTML tags, attributes, and DOCTYPE that
    // should be excluded
    // private static final Set<String> HTML_STRUCTURAL_TERMS = new
    // HashSet<>(Arrays.asList(
    // // DOCTYPE and document structure
    // "doctype", "html", "xml",
    // // Common HTML tag names
    // "head", "body", "meta", "title", "link", "script", "style", "noscript",
    // "div", "span", "p", "br", "hr", "img", "a", "ul", "ol", "li", "table",
    // "tr", "td", "th", "thead", "tbody", "tfoot", "form", "input", "button",
    // "select", "option", "textarea", "label", "h1", "h2", "h3", "h4", "h5", "h6",
    // // Common HTML attributes
    // "lang", "charset", "name", "content", "property", "http", "equiv",
    // "viewport", "width", "height", "device", "initial", "scale", "maximum",
    // "minimum", "user", "scalable", "yes", "no", "target", "rel", "href", "src",
    // "alt", "title", "id", "class", "style", "type", "media", "async", "defer",
    // "data", "next", "prev", "canonical", "og", "twitter", "card", "site",
    // "description", "keywords", "author", "robots", "index", "follow", "noindex",
    // "nofollow", "refresh", "redirect", "url", "image", "video", "audio",
    // // Common attribute values
    // "utf", "8", "en", "us", "gb", "fr", "de", "es", "it", "pt", "ru", "zh", "ja",
    // "text", "html", "css", "javascript", "js", "json", "xml", "rss", "atom",
    // "application", "image", "video", "audio", "font", "woff", "woff2", "ttf",
    // "eot", "svg", "png", "jpg", "jpeg", "gif", "webp", "ico", "favicon",
    // // Common meta tag values
    // "screen", "print", "all", "handheld", "projection", "tv", "braille",
    // "aural", "tty", "embossed", "portrait", "landscape",
    // // Common protocol/format identifiers
    // "http", "https", "ftp", "mailto", "tel", "sms", "javascript", "void",
    // // Boolean-like values
    // "true", "false", "on", "off", "yes", "no", "1", "0",
    // // JavaScript keywords and DOM API terms (from logs: const, function,
    // document,
    // // etc.)
    // "const", "let", "var", "function", "if", "else", "for", "while", "return",
    // "document", "window", "navigator", "useragent", "user", "agent", "dcom",
    // "ios", "android", "adhesionstyle", "display", "none", "important",
    // "context", "v1", "prefix", "ns", "security", "policy", "upgrade", "insecure",
    // "requests", "profile", "xfn", "pingback", "xmlrpc", "adhesion"));

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("IndexerOptimized job started - using flatMapParallelWithContext to avoid OOM");

        // Track total job start time
        long jobStartTime = System.currentTimeMillis();

        // Clear old index tables if needed
        try {
            ctx.getKVS().delete("pt-index");
        } catch (Exception e) {
            logger.info("Error deleting index table: " + e.getMessage());
        }

        try {
            logger.info("Phase 1: Loading URLs from pt-crawl (small strings only, no page content)");
            long phase1StartTime = System.currentTimeMillis();

            // STEP 1: Only extract URLs - NO page content in intermediate table!
            FlameRDD urlRDD = ctx.fromTableParallel("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                // Only return URL if the row has valid data
                if (url == null || page == null) {
                    return null;
                }
                return url; // Small string! Not the page content!
            }).filterParallel((url, workerKvs, batchedKvs) -> url != null);

            long urlCount = urlRDD.count();
            long phase1Time = System.currentTimeMillis() - phase1StartTime;
            logger.info(String.format("[INDEXER_PHASE1_COMPLETE] Found %d URLs | Took %dms (%.1fs)",
                    urlCount, phase1Time, phase1Time / 1000.0));

            logger.info("Phase 2: Processing pages with flatMapParallelWithContext");
            logger.info("  - Page content read inside lambda (no intermediate table)");
            logger.info("  - Word-URL pairs emitted directly");

            // Timing statistics (thread-safe counters)
            final AtomicLong totalDocsProcessed = new AtomicLong(0);
            final AtomicLong totalKvsReadTimeMs = new AtomicLong(0);
            final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
            final AtomicLong totalFullTimeMs = new AtomicLong(0);
            final AtomicLong totalTermsEmitted = new AtomicLong(0);
            final AtomicLong totalPageBytes = new AtomicLong(0);
            final long phase2StartTime = System.currentTimeMillis();

            // STEP 2: Use flatMapToPairParallel to process pages without OOM
            // The lambda reads the full row from pt-crawl, processes it, and emits word-url
            // pairs
            // Page content NEVER goes to an intermediate table!
            // batchedReader can be used for pooled batch reads if needed
            FlamePairRDD wordUrlPairsRDD = urlRDD
                    .flatMapToPairParallel((url, workerKvs, batchedKvs) -> {
                        // ============== TIMING: Start of document processing ==============
                        long docStartTime = System.currentTimeMillis();
                        long kvsReadTime = 0;
                        long processTime = 0;
                        int pageSize = 0;
                        int termsCount = 0;

                        try {
                            // ============== TIMING: KVS Read ==============
                            long kvsReadStart = System.currentTimeMillis();

                            // Read the full row from pt-crawl (including page content)
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

                            // Collect anchor text from all anchors columns
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

                            // ============== TIMING: Page Processing ==============
                            long processStart = System.currentTimeMillis();

                            // Process the page - extract fields, calculate TF, emit word-url pairs
                            // List<String> wordUrlPairs = processPage(url, page, anchorText);
                            List<FlamePair> wordUrlPairs = processPage(url, page, anchorText);

                            processTime = System.currentTimeMillis() - processStart;
                            termsCount = wordUrlPairs.size();

                            // ============== TIMING: Update aggregate stats ==============
                            long fullDocTime = System.currentTimeMillis() - docStartTime;
                            long docNum = totalDocsProcessed.incrementAndGet();
                            totalKvsReadTimeMs.addAndGet(kvsReadTime);
                            totalProcessingTimeMs.addAndGet(processTime);
                            totalFullTimeMs.addAndGet(fullDocTime);
                            totalTermsEmitted.addAndGet(termsCount);
                            totalPageBytes.addAndGet(pageSize);

                            // Log every 100 documents with detailed timing
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

                            // Log slow documents (>500ms)
                            if (fullDocTime > 500) {
                                logger.warn(String.format(
                                        "[INDEXER_SLOW_DOC] URL=%s | kvs=%dms, process=%dms, total=%dms, terms=%d, pageKB=%.1f",
                                        url.length() > 80 ? url.substring(0, 80) + "..." : url,
                                        kvsReadTime, processTime, fullDocTime, termsCount, pageSize / 1024.0));
                            }

                            // Return small word, url|tf|positions

                            return wordUrlPairs;

                        } catch (Exception e) {
                            long errorTime = System.currentTimeMillis() - docStartTime;
                            logger.warn(String.format("[INDEXER_ERROR] URL=%s | time=%dms | error=%s",
                                    url.length() > 60 ? url.substring(0, 60) + "..." : url, errorTime, e.getMessage()));
                            return Collections.emptyList();
                        }
                    });

            // Log final Phase 2 statistics
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

            logger.info("Phase 2 complete: Word-URL pairs generated");

            // logger.info("Phase 3: Converting to PairRDD for aggregation");
            // long phase3StartTime = System.currentTimeMillis();

            // // STEP 3: Convert to PairRDD (word, url|tf|positions)
            // // Format from processPage: "word\turl|tf|titleTf|...|positions"
            // FlamePairRDD wordUrlPairs = wordUrlPairsRDD.mapToPair(s -> {
            // int tabIdx = s.indexOf('\t');
            // if (tabIdx < 0)
            // return null;
            // String word = s.substring(0, tabIdx);
            // String value = s.substring(tabIdx + 1);
            // return new FlamePair(word, value);
            // });

            // long phase3Time = System.currentTimeMillis() - phase3StartTime;
            // logger.info(String.format("[INDEXER_PHASE3_COMPLETE] mapToPair took %dms
            // (%.1fs)", phase3Time,
            // phase3Time / 1000.0));

            logger.info("Phase 3: Aggregating with foldByKey");
            long phase3StartTime = System.currentTimeMillis();

            // STEP 4: Aggregate using foldByKey
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

            logger.info("Phase 4: Saving to pt-index");
            long phase4StartTime = System.currentTimeMillis();

            invertedIndex.saveAsTable("pt-index");

            long phase4Time = System.currentTimeMillis() - phase4StartTime;
            logger.info(String.format("[INDEXER_PHASE4_COMPLETE] saveAsTable took %dms (%.1fs)", phase4Time,
                    phase4Time / 1000.0));

            // Final summary - calculate total time correctly by summing all phases
            long totalTime = phase1Time + phase2Time + phase3Time + phase4Time;
            long actualTotalTime = System.currentTimeMillis() - jobStartTime;
            logger.info(String.format(
                    "[INDEXER_COMPLETE] Total indexing time: %dms (%.1fs) | Actual wall-clock time: %dms (%.1fs) | " +
                            "Phase1(readUrls)=%dms, Phase2(flatMapToPair)=%dms, Phase3(foldByKey)=%dms, Phase4(save)=%dms",
                    totalTime, totalTime / 1000.0, actualTotalTime, actualTotalTime / 1000.0,
                    phase1Time, phase2Time, phase3Time, phase4Time));

            ctx.output("Inverted index built successfully (optimized). Total time: " + (totalTime / 1000.0) + "s");

        } catch (Exception e) {
            logger.error("Error during indexing: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Process a single page and return word-url pairs.
     * Returns list of strings in format:
     * "word", "url|totalTf|titleTf|metaTf|headerTf|bodyTf|urlTf|anchorTf|positions"
     */
    private static List<FlamePair> processPage(String url, String page, String anchorText) {
        // List<String> results = new ArrayList<>();
        List<FlamePair> res = new ArrayList<>();

        logger.debug("========================================");
        logger.debug("  [processPage] Processing URL: " + (url.length() > 80 ? url.substring(0, 80) + "..." : url));
        logger.debug("  [processPage] Page HTML length: " + (page != null ? page.length() : 0) + " characters");
        if (anchorText != null && !anchorText.isEmpty()) {
            logger.debug("  [processPage] Anchor text length: " + anchorText.length() + " characters");
        }

        // Extract fields from HTML
        FieldContent fields = extractFields(page, url);

        logger.debug("  [processPage] Extracted fields summary:");
        logger.debug("    Title: [" + fields.title + "] (" + fields.titleWordCount + " words)");
        logger.debug("    Meta description: [" + fields.metaDescription + "]");
        logger.debug("    Meta keywords: [" + fields.metaKeywords + "]");
        logger.debug("    Headers: ["
                + (fields.headers.length() > 200 ? fields.headers.substring(0, 200) + "..." : fields.headers) + "] ("
                + fields.headerWordCount + " words)");
        logger.debug("    Body: [" + (fields.body.length() > 200 ? fields.body.substring(0, 200) + "..." : fields.body)
                + "] (" + fields.bodyWordCount + " words)");

        // Process each field separately with global position tracking
        Map<String, TermInfo> termInfoMap = new HashMap<>();
        int globalPosition = 0;

        // Index title field
        globalPosition = processText(fields.title, "title", termInfoMap, globalPosition);

        // Index meta tags
        String metaText = fields.metaDescription + " " + fields.metaKeywords;
        globalPosition = processText(metaText, "meta", termInfoMap, globalPosition);

        // Index header fields
        globalPosition = processText(fields.headers, "header", termInfoMap, globalPosition);

        // Index body field
        globalPosition = processText(fields.body, "body", termInfoMap, globalPosition);

        // Index URL tokens
        int totalUrlTokens = processUrlTokens(url, termInfoMap);

        // Index anchor text
        int totalAnchorWords = 0;
        if (anchorText != null && !anchorText.isEmpty()) {
            logger.debug("  [processPage] Processing anchor text, raw length: " + anchorText.length());
            String anchorPreview = anchorText.length() > 200 ? anchorText.substring(0, 200) + "..." : anchorText;
            logger.debug("  [processPage] Anchor text preview: [" + anchorPreview + "]");
            String cleanedAnchorText = cleanText(anchorText, "anchor");
            String[] anchorWords = WHITESPACE_PATTERN.split(cleanedAnchorText);
            for (String word : anchorWords) {
                if (!word.isEmpty() && WORD_PATTERN.matcher(word).matches()) {
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
        for (Map.Entry<String, TermInfo> entry : termInfoMap.entrySet()) {
            String w = entry.getKey();
            TermInfo info = entry.getValue();

            // Skip invalid words (using pre-compiled pattern) and stop words
            if (w == null || w.isEmpty() || w.trim().isEmpty() || !WORD_PATTERN.matcher(w).matches()) {
                continue;
            }

            // // Filter by length
            // if (w.length() < MIN_WORD_LENGTH || w.length() > MAX_WORD_LENGTH) {
            // continue;
            // }

            // // Filter out words that are all numbers (no letters)
            // if (!HAS_LETTER_PATTERN.matcher(w).matches()) {
            // continue;
            // }

            // Filter out stop words before stemming
            if (STOP_WORDS.contains(w.toLowerCase())) {
                continue;
            }

            // Stem the word (create new stemmer instance to avoid state issues)
            PorterStemmer stemmer = new PorterStemmer();
            stemmer.add(w.toCharArray(), w.length());
            stemmer.stem();
            String stemmedWord = stemmer.toString();
            logger.debug("  [processPage] Stemmed word: '" + w + "' -> '" + stemmedWord + "'");

            // Skip if stemmed word is invalid
            if (stemmedWord == null || stemmedWord.isEmpty() || !WORD_PATTERN.matcher(stemmedWord).matches()) {
                continue;
            }

            // Filter stemmed word by length and content
            // if (stemmedWord.length() < MIN_WORD_LENGTH || stemmedWord.length() >
            // MAX_WORD_LENGTH) {
            // continue;
            // }

            // // Filter out stemmed words that are all numbers
            // if (!HAS_LETTER_PATTERN.matcher(stemmedWord).matches()) {
            // continue;
            // }

            // Calculate TF values
            double titleTf = totalDocLength > 0 ? (double) info.titleFreq / totalDocLength : 0.0;
            double metaTf = totalDocLength > 0 ? (double) info.metaFreq / totalDocLength : 0.0;
            double headerTf = totalDocLength > 0 ? (double) info.headerFreq / totalDocLength : 0.0;
            double bodyTf = totalDocLength > 0 ? (double) info.bodyFreq / totalDocLength : 0.0;
            double urlTf = totalUrlTokens > 0 ? (double) info.urlFreq / totalUrlTokens : 0.0;
            double anchorTf = totalAnchorWords > 0 ? (double) info.anchorFreq / totalAnchorWords : 0.0;
            double totalTf = totalDocLength > 0 ? (double) info.totalFreq / totalDocLength : 0.0;

            // OPTIMIZATION: Positions are already integers, just sort them
            List<Integer> sortedPositions = new ArrayList<>(info.positions);
            sortedPositions.sort(Integer::compareTo);

            // Convert to strings for output
            List<String> sortedPosStrings = new ArrayList<>(sortedPositions.size());
            for (Integer pos : sortedPositions) {
                sortedPosStrings.add(String.valueOf(pos));
            }

            // Build value:
            // url|totalTf|titleTf|metaTf|headerTf|bodyTf|urlTf|anchorTf|positions
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

            // Format: "word\tvalue" - word will become key, value becomes value
            if (Objects.equals(w, stemmedWord)) {
                if (!w.isEmpty()) {
                    // results.add(w + "\t" + valueBuilder.toString());
                    res.add(new FlamePair(w, valueBuilder.toString()));
                }
            } else {
                if (!stemmedWord.isEmpty()) {
                    res.add(new FlamePair(stemmedWord, valueBuilder.toString()));
                }
                if (!w.isEmpty()) {
                    res.add(new FlamePair(w, valueBuilder.toString()));
                }
            }
        }

        logger.debug("  [processPage] Indexing complete for URL: "
                + (url.length() > 80 ? url.substring(0, 80) + "..." : url));
        logger.debug("  [processPage] Total unique terms indexed: " + termInfoMap.size());
        logger.debug("  [processPage] Total index entries generated: " + res.size());
        logger.debug("========================================");

        return res;
    }

    // ============== Helper classes and methods (same as original) ==============

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

    /**
     * Unescape HTML entities that were escaped by escapeHtml4() in Crawler.
     * Reverses: &amp; -> &, &lt; -> <, &gt; -> >, &quot; -> ", &#39; -> '
     * Must be done BEFORE trying to match HTML patterns, otherwise tags won't be
     * found.
     */
    private static String unescapeHtml(String html) {
        if (html == null || html.isEmpty()) {
            return html;
        }
        // Order matters: &amp; must be decoded LAST to avoid double-decoding
        // e.g., &amp;lt; should become &lt; first, then <
        String result = html.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&amp;", "&"); // Must be last!
        return result;
    }

    private static FieldContent extractFields(String html, String url) {
        FieldContent fields = new FieldContent();
        if (html == null || html.isEmpty()) {
            return fields;
        }

        // CRITICAL: Unescape HTML entities FIRST before trying to match HTML patterns
        // The HTML is stored escaped (escapeHtml4 in Crawler), so we need to unescape
        // it
        // to find actual HTML tags like <title> instead of &lt;title&gt;
        logger.debug("  [extractFields] Unescaping HTML entities (HTML is stored escaped from Crawler)");
        String beforeUnescape = html;
        html = unescapeHtml(html);
        if (!html.equals(beforeUnescape)) {
            logger.debug("  [extractFields] Unescaped HTML entities (length: " + beforeUnescape.length() +
                    " -> " + html.length() + ")");
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

    // ============== OPTIMIZED VERSION (COMMENTED OUT - KEPT AS BACKUP)
    // ==============
    // /**
    // * Optimized single-pass text cleaning for all pages using StringBuilder
    // * approach
    // * This avoids multiple string allocations from sequential replaceAll() calls
    // */
    // private static String cleanText(String text) {
    // return cleanText(text, null);
    // }
    //
    // /**
    // * Optimized single-pass text cleaning with optional field name for debug
    // * logging
    // */
    // private static String cleanText(String text, String fieldName) {
    // if (text == null || text.isEmpty()) {
    // if (fieldName != null) {
    // logger.debug(" [cleanText] Input text is null or empty for field: " +
    // fieldName);
    // }
    // return "";
    // }
    //
    // String inputPreview = text.length() > 200 ? text.substring(0, 200) + "..." :
    // text;
    // if (fieldName != null) {
    // logger.debug(" [cleanText] Cleaning field: " + fieldName + ", input length: "
    // + text.length());
    // logger.debug(" [cleanText] Input preview: [" + inputPreview + "]");
    // }
    //
    // StringBuilder sb = new StringBuilder(text.length()); // Pre-allocate capacity
    // char[] chars = text.toLowerCase().toCharArray();
    // boolean inTag = false;
    // boolean inEntity = false;
    // int entityStart = -1;
    // boolean lastWasSpace = true;
    // int tagsRemoved = 0;
    // int entitiesRemoved = 0;
    //
    // for (int i = 0; i < chars.length; i++) {
    // char c = chars[i];
    //
    // // Handle HTML tags
    // if (c == '<') {
    // inTag = true;
    // tagsRemoved++;
    // if (!lastWasSpace) {
    // sb.append(' ');
    // lastWasSpace = true;
    // }
    // continue;
    // }
    // if (inTag) {
    // if (c == '>') {
    // inTag = false;
    // }
    // continue;
    // }
    //
    // // Handle HTML entities - skip them completely to avoid gibberish
    // if (c == '&') {
    // inEntity = true;
    // entityStart = i;
    // entitiesRemoved++;
    // if (!lastWasSpace) {
    // sb.append(' ');
    // lastWasSpace = true;
    // }
    // continue;
    // }
    // if (inEntity) {
    // if (c == ';') {
    // inEntity = false;
    // // Entity complete, already added space above
    // } else if (i - entityStart > 20) {
    // // Entity too long (likely malformed), skip the entire entity
    // inEntity = false;
    // // Don't process the malformed entity content
    // } else {
    // continue; // Still in entity, skip this character
    // }
    // continue; // Skip the semicolon or continue after malformed entity
    // }
    //
    // // Keep only alphanumeric and spaces
    // if (Character.isLetterOrDigit(c)) {
    // sb.append(c);
    // lastWasSpace = false;
    // } else if (!lastWasSpace) {
    // sb.append(' ');
    // lastWasSpace = true;
    // }
    // }
    //
    // // Final whitespace normalization
    // String result = sb.toString();
    // result = WHITESPACE_PATTERN.matcher(result).replaceAll(" ").trim();
    //
    // if (fieldName != null) {
    // logger.debug(
    // " [cleanText] Removed " + tagsRemoved + " HTML tag(s) and " + entitiesRemoved
    // + " entity/ies");
    // logger.debug(" [cleanText] Final cleaned text for " + fieldName + ": [" +
    // (result.length() > 200 ? result.substring(0, 200) + "..." : result) + "]
    // (length: "
    // + result.length() + ")");
    // String[] words = WHITESPACE_PATTERN.split(result);
    // logger.debug(" [cleanText] Extracted " + words.length + " word(s) from " +
    // fieldName);
    // if (words.length > 0 && words.length <= 20) {
    // logger.debug(" [cleanText] Words from " + fieldName + ": " +
    // Arrays.toString(words));
    // } else if (words.length > 20) {
    // String[] first20 = Arrays.copyOf(words, 20);
    // logger.debug(" [cleanText] First 20 words from " + fieldName + ": " +
    // Arrays.toString(first20)
    // + " ... (total: " + words.length + ")");
    // }
    // }
    //
    // return result;
    // }
    // ============== END OF OPTIMIZED VERSION ==============

    /**
     * Text cleaning method from Indexer.java
     * Uses multiple replaceAll() calls for comprehensive text cleaning
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

        // Filter out HTML structural terms that don't represent actual content
        // These are words from HTML tags, attributes, DOCTYPE, etc.
        List<String> words = Arrays.asList(text.split("\\s+"));
        // List<String> words = new ArrayList<>();
        // int htmlTermsRemoved = 0;
        // for (String word : words) {
        // if (!word.isEmpty() && !HTML_STRUCTURAL_TERMS.contains(word.toLowerCase())) {
        // words.add(word);
        // } else if (!word.isEmpty()) {
        // htmlTermsRemoved++;
        // }
        // }
        // text = String.join(" ", words);

        // logger.debug(" [cleanText] Removed " + htmlTermsRemoved + " HTML structural
        // term(s)");
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
        if (text == null || text.isEmpty()) {
            logger.debug("  [processText] Skipping empty text for field: " + fieldType);
            return globalPosition;
        }

        String[] words = WHITESPACE_PATTERN.split(text);
        logger.debug(
                "  [processText] Processing field: " + fieldType + ", total words before filtering: " + words.length);

        // OPTIMIZATION: Pre-compile matcher once and reuse
        Matcher wordMatcher = WORD_PATTERN.matcher("");
        List<String> validWords = new ArrayList<>();

        for (String w : words) {
            if (w.isEmpty())
                continue;

            // Filter by length first (fast check)
            // if (w.length() < MIN_WORD_LENGTH || w.length() > MAX_WORD_LENGTH)
            // continue;

            // OPTIMIZATION: Reuse matcher instead of creating new one
            wordMatcher.reset(w);
            if (!wordMatcher.matches())
                continue;

            // Filter out words that are all numbers (no letters)
            // if (!HAS_LETTER_PATTERN.matcher(w).matches())
            // continue;

            // Filter out stop words
            if (STOP_WORDS.contains(w.toLowerCase()))
                continue;

            validWords.add(w);
            TermInfo info = termInfoMap.computeIfAbsent(w, k -> new TermInfo());
            // OPTIMIZATION: Store integer directly instead of converting from string
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
            logger.debug("  [processText] First 30 words indexed from " + fieldType + ": " + first30 + " ... (total: "
                    + validWords.size() + ")");
        }

        return globalPosition;
    }

    private static int processUrlTokens(String url, Map<String, TermInfo> termInfoMap) {
        if (url == null || url.isEmpty()) {
            logger.debug("  [processUrlTokens] URL is null or empty");
            return 0;
        }

        logger.debug(
                "  [processUrlTokens] Processing URL: " + (url.length() > 100 ? url.substring(0, 100) + "..." : url));
        String urlLower = url.toLowerCase();
        String[] tokens = URL_SPLIT_PATTERN.split(urlLower);
        logger.debug("  [processUrlTokens] Split URL into " + tokens.length + " token(s)");

        int tokensIndexed = 0;
        List<String> validTokens = new ArrayList<>();
        for (String token : tokens) {
            // if (token.isEmpty() || token.length() < MIN_WORD_LENGTH || token.length() >
            // MAX_WORD_LENGTH) {
            // continue;
            // }
            if (token.isEmpty()) {
                continue;
            }

            if (!WORD_PATTERN.matcher(token).matches()) {
                continue;
            }

            // Filter out tokens that are all numbers (no letters)
            // if (!HAS_LETTER_PATTERN.matcher(token).matches()) {
            // continue;
            // }

            // Skip common URL parts and stop words
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
    }

    public static class TermInfo {
        public int totalFreq = 0;
        public int titleFreq = 0;
        public int metaFreq = 0;
        public int headerFreq = 0;
        public int bodyFreq = 0;
        public int urlFreq = 0;
        public int anchorFreq = 0;
        // OPTIMIZATION: Store positions as integers directly instead of strings
        public List<Integer> positions = new ArrayList<>();
    }
}
