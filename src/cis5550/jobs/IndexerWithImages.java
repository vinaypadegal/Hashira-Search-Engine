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
public class IndexerWithImages {
    private static final Logger logger = Logger.getLogger(IndexerWithImages.class);

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
        try {

            logger.info("Phase IMG: Building pt-image-index from pt-image-crawl");
            long imgStart = System.currentTimeMillis();

            FlameRDD imageRows = ctx.fromTableParallel("pt-image-crawl", row -> {
                String url = row.key();
                String anchors = row.get("page");
                if (url == null || anchors == null) {
                    return null;
                }
                return url + "\t" + anchors;
            });

            FlamePairRDD imageWordPairs = imageRows.flatMapToPairParallel((str, kvs, writer) -> {
                int idx = str.indexOf('\t');
                if (idx < 0) {
                    return Collections.emptyList();
                }
                String url = str.substring(0, idx);
                String anchors = str.substring(idx + 1);
                return processImageAnchors(url, anchors);
            });

            FlamePairRDD imageInverted = imageWordPairs.foldByKey("", (a, b) -> {
                if (a == null || a.isEmpty())
                    return b;
                if (b == null || b.isEmpty())
                    return a;
                return a + "," + b;
            });

            imageInverted.saveAsTable("pt-image-index");

            try {
                imageRows.destroy();
                imageWordPairs.destroy();
                imageInverted.destroy();
            } catch (Exception e) {
                logger.warn("Could not destroy temp image RDDs: " + e.getMessage());
            }

            long imgTime = System.currentTimeMillis() - imgStart;
            logger.info(String.format("[INDEXER_IMAGE_COMPLETE] Built pt-image-index in %dms (%.1fs)", imgTime,
                    imgTime / 1000.0));

        } catch (Exception e) {
            logger.error("Error during indexing: " + e.getMessage(), e);
            throw e;
        }
    }

    // Process anchor text for image URLs to build image index entries
    private static List<FlamePair> processImageAnchors(String url, String anchorText) {
        List<FlamePair> results = new ArrayList<>();
        if (anchorText == null || anchorText.isEmpty()) {
            return results;
        }

        // Tokenize and stem
        String cleaned = NON_ALPHANUM_PATTERN.matcher(anchorText.toLowerCase()).replaceAll(" ");
        String[] tokens = cleaned.split("\\s+");
        Map<String, TermInfo> termInfoMap = new HashMap<>();

        int pos = 0;
        SnowballStemmer stemmer = new englishStemmer();
        for (String token : tokens) {
            if (token.isEmpty()) {
                pos++;
                continue;
            }
            if (STOP_WORDS.contains(token)) {
                pos++;
                continue;
            }
            stemmer.setCurrent(token);
            stemmer.stem();
            String stem = stemmer.getCurrent();
            if (stem == null || stem.isEmpty()) {
                pos++;
                continue;
            }

            TermInfo info = termInfoMap.computeIfAbsent(stem, k -> new TermInfo());
            info.anchorFreq++;
            info.totalFreq++;
            info.positions.add(pos);
            pos++;
        }

        int totalTokens = Math.max(1, pos);
        for (Map.Entry<String, TermInfo> e : termInfoMap.entrySet()) {
            String word = e.getKey();
            TermInfo info = e.getValue();

            double tf = (double) info.anchorFreq / totalTokens; // single TF based on anchor text only

            StringBuilder valueBuilder = new StringBuilder();
            valueBuilder.append(url).append("|");
            valueBuilder.append(tf);

            results.add(new FlamePair(word, valueBuilder.toString())); // format: url|tf
        }

        return results;
    }

    /**
     * Extract fields using JSoup CSS selectors - much faster than regex
     */
    public static class TermInfo {
        public int totalFreq = 0;
        public int anchorFreq = 0;
        public List<Integer> positions = new ArrayList<>();
    }
}