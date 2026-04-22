package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.tools.Logger;
import cis5550.tools.PorterStemmer;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Arrays;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("Indexer job started");

        // Clear old index tables if needed
        try {
            ctx.getKVS().delete("pt-index");
        } catch (Exception e) {
            // Tables might not exist, ignore
            logger.info("Error deleting index table: " + e.getMessage());
        }

        FlamePairRDD invertedIndex;
        try {
            logger.info("Loading data from pt-crawl - this will distribute work to all Flame workers");
            logger.info("If this hangs, check if all Flame workers are responsive");

            FlamePairRDD urlPageRDD = ctx.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null)
                    return null; // skip incomplete rows
                // Also get anchor text columns
                StringBuilder anchorData = new StringBuilder();
                for (String col : row.columns()) {
                    if (col.startsWith("anchors:")) {
                        String anchorText = row.get(col);
                        if (anchorText != null && !anchorText.isEmpty()) {
                            if (anchorData.length() > 0)
                                anchorData.append(" ");
                            anchorData.append(anchorText);
                        }
                    }
                }
                return url + "\t" + page + "\t" + anchorData.toString();
            })
                    .filter(Objects::nonNull)
                    .mapToPair(s -> {
                        String[] parts = s.split("\t", 3);
                        String url = parts[0];
                        String page = parts.length > 1 ? parts[1] : "";
                        String anchorText = parts.length > 2 ? parts[2] : "";
                        return new FlamePair(url, page + "\t" + anchorText);
                    });

            logger.info("Loaded data from pt-crawl and converted to pairs");

            logger.info("Flat mapping data to word-url pairs with field-based indexing");
            FlamePairRDD wordUrlPairs = urlPageRDD.flatMapToPair(pair -> {
                String url = pair._1();
                String[] parts = pair._2().split("\t", 2);
                String page = parts[0];
                String anchorText = parts.length > 1 ? parts[1] : "";

                logger.debug("========================================");
                logger.debug("Processing URL: " + url);
                logger.debug("Original HTML length: " + (page != null ? page.length() : 0) + " characters");
                if (page != null && page.length() > 0) {
                    String preview = page.length() > 500 ? page.substring(0, 500) + "..." : page;
                    logger.debug("Original HTML preview: " + preview);
                }
                if (!anchorText.isEmpty()) {
                    logger.debug("Anchor text: " + anchorText);
                }

                List<FlamePair> results = new ArrayList<>();

                // Extract fields from HTML
                FieldContent fields = extractFields(page, url);

                // Process each field separately with global position tracking
                Map<String, TermInfo> termInfoMap = new HashMap<>();
                int globalPosition = 0; // Global position counter across entire document

                logger.debug("Extracted fields:");
                logger.debug("  Title: [" + fields.title + "] (" + fields.titleWordCount + " words)");
                logger.debug("  Meta description: [" + fields.metaDescription + "]");
                logger.debug("  Meta keywords: [" + fields.metaKeywords + "]");
                logger.debug("  Headers: [" + fields.headers + "] (" + fields.headerWordCount + " words)");
                logger.debug("  Body: ["
                        + (fields.body.length() > 200 ? fields.body.substring(0, 200) + "..." : fields.body) + "] ("
                        + fields.bodyWordCount + " words)");

                // Index title field
                globalPosition = processText(fields.title, "title", termInfoMap, globalPosition, url);

                // Index meta tags (description and keywords)
                String metaText = fields.metaDescription + " " + fields.metaKeywords;
                globalPosition = processText(metaText, "meta", termInfoMap, globalPosition, url);

                // Index header fields (h1-h6)
                globalPosition = processText(fields.headers, "header", termInfoMap, globalPosition, url);

                // Index body field
                globalPosition = processText(fields.body, "body", termInfoMap, globalPosition, url);

                // Index URL tokens (off-page feature, but tracked separately)
                int totalUrlTokens = processUrlTokens(url, termInfoMap);

                // Index anchor text (off-page feature, but tracked separately)
                // Clean anchor text to remove special characters and convert to lowercase
                int totalAnchorWords = 0;
                if (!anchorText.isEmpty()) {
                    String cleanedAnchorText = cleanText(anchorText, "anchor");
                    // Count words for TF calculation
                    String[] anchorWords = cleanedAnchorText.split("\\s+");
                    for (String word : anchorWords) {
                        if (!word.isEmpty() && word.matches("^[\\p{L}\\p{N}]+$")) {
                            totalAnchorWords++;
                        }
                    }
                    processText(cleanedAnchorText, "anchor", termInfoMap, 0, url); // Position doesn't matter for
                                                                                   // off-page
                }

                int totalDocLength = fields.titleWordCount + fields.headerWordCount + fields.bodyWordCount;

                logger.debug("Total document length: " + totalDocLength + " words");
                logger.debug("Total unique terms found: " + termInfoMap.size());

                // Create index entries for each term
                for (Map.Entry<String, TermInfo> entry : termInfoMap.entrySet()) {
                    String w = entry.getKey();
                    TermInfo info = entry.getValue();

                    // Skip empty or null words, and words with special characters
                    if (w == null || w.isEmpty() || w.trim().isEmpty() || !w.matches("^[\\p{L}\\p{N}]+$")) {
                        logger.debug("Skipping empty word entry or words with special characters: '" + w + "'");
                        continue;
                    }

                    logger.debug("Processing term: '" + w + "' - totalFreq=" + info.totalFreq +
                            ", title=" + info.titleFreq + ", meta=" + info.metaFreq +
                            ", header=" + info.headerFreq + ", body=" + info.bodyFreq +
                            ", url=" + info.urlFreq + ", anchor=" + info.anchorFreq +
                            ", positions=" + info.positions.size());

                    PorterStemmer stemmer = new PorterStemmer();
                    stemmer.add(w.toCharArray(), w.length());
                    stemmer.stem();
                    String stemmedWord = stemmer.toString();

                    // Skip if stemmed word is empty or contains special characters
                    if (stemmedWord == null || stemmedWord.isEmpty() || stemmedWord.trim().isEmpty()
                            || !stemmedWord.matches("^[\\p{L}\\p{N}]+$")) {
                        logger.debug("Skipping word with invalid stemmed form: '" + w + "' -> '" + stemmedWord + "'");
                        continue;
                    }

                    // Calculate TF for each field
                    double titleTf = totalDocLength > 0 ? (double) info.titleFreq / totalDocLength : 0.0;
                    double metaTf = totalDocLength > 0 ? (double) info.metaFreq / totalDocLength : 0.0;
                    double headerTf = totalDocLength > 0 ? (double) info.headerFreq / totalDocLength : 0.0;
                    double bodyTf = totalDocLength > 0 ? (double) info.bodyFreq / totalDocLength : 0.0;
                    // Calculate URL TF: frequency normalized by total URL tokens
                    double urlTf = totalUrlTokens > 0 ? (double) info.urlFreq / totalUrlTokens : 0.0;
                    // Calculate anchor TF: frequency normalized by total anchor words
                    double anchorTf = totalAnchorWords > 0 ? (double) info.anchorFreq / totalAnchorWords : 0.0;
                    double totalTf = totalDocLength > 0 ? (double) info.totalFreq / totalDocLength : 0.0;

                    // Sort positions to ensure they're in global order
                    List<Integer> sortedPositions = new ArrayList<>();
                    for (String posStr : info.positions) {
                        try {
                            sortedPositions.add(Integer.parseInt(posStr));
                        } catch (NumberFormatException e) {
                            // Skip invalid positions
                        }
                    }
                    sortedPositions.sort(Integer::compareTo);

                    // Convert back to string list
                    List<String> sortedPosStrings = new ArrayList<>();
                    for (Integer pos : sortedPositions) {
                        sortedPosStrings.add(String.valueOf(pos));
                    }

                    // Format: url|totalTf|titleTf|metaTf|headerTf|bodyTf|urlTf|anchorTf|positions
                    // e.g., http://example.com|0.05|0.1|0.02|0.02|0.03|1.0|1.0|3 15 27
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

                    // Index both original and stemmed word
                    // Ensure both keys are non-empty before adding
                    if (Objects.equals(w, stemmedWord)) {
                        logger.debug("  Indexing word (no stemming): '" + w + "'");
                        if (!w.isEmpty() && !w.trim().isEmpty()) {
                            results.add(new FlamePair(w, valueBuilder.toString()));
                        }
                    } else {
                        logger.debug("  Indexing word: '" + w + "' -> stemmed: '" + stemmedWord + "'");
                        if (!stemmedWord.isEmpty() && !stemmedWord.trim().isEmpty()) {
                            results.add(new FlamePair(stemmedWord, valueBuilder.toString()));
                        }
                        if (!w.isEmpty() && !w.trim().isEmpty()) {
                            results.add(new FlamePair(w, valueBuilder.toString()));
                        }
                    }
                }

                logger.debug("Generated " + results.size() + " index entries for URL: " + url);
                logger.debug("========================================");

                return results;
            });

            logger.info("Starting foldByKey operation - aggregating word-document pairs");
            invertedIndex = wordUrlPairs.foldByKey("", (a, b) -> {
                if (a.isEmpty())
                    return b;
                return a + "," + b;
            });

            logger.info("Completed foldByKey, saving to pt-index table");
            invertedIndex.saveAsTable("pt-index");
            logger.info("Inverted index saved successfully");
            ctx.output("Inverted index built successfully.");

        } catch (Exception e) {
            logger.error("Error during indexing: " + e.getMessage());
            throw e;
        }
    }

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

    private static FieldContent extractFields(String html, String url) {
        FieldContent fields = new FieldContent();
        if (html == null || html.isEmpty()) {
            logger.debug("  [extractFields] HTML is null or empty for URL: " + url);
            return fields;
        }

        logger.debug("  [extractFields] Starting field extraction for URL: " + url);
        int originalLength = html.length();
        logger.debug("  [extractFields] Original HTML length: " + originalLength);

        // Count script/style tags before removal
        int scriptCount = countMatches(html, "(?i)<script[^>]*>");
        int styleCount = countMatches(html, "(?i)<style[^>]*>");
        int noscriptCount = countMatches(html, "(?i)<noscript[^>]*>");
        logger.debug("  [extractFields] Found " + scriptCount + " script tags, " + styleCount + " style tags, "
                + noscriptCount + " noscript tags");

        // Remove script and style tags completely (including their content)
        // Use DOTALL flag (?s) to match across newlines
        String beforeScriptRemoval = html;
        html = Pattern.compile("(?is)<script[^>]*>.*?</script>").matcher(html).replaceAll(" ");
        if (!html.equals(beforeScriptRemoval)) {
            logger.debug("  [extractFields] Removed <script> tags (length: " + beforeScriptRemoval.length() + " -> "
                    + html.length() + ")");
        }

        html = Pattern.compile("(?is)<style[^>]*>.*?</style>").matcher(html).replaceAll(" ");
        html = Pattern.compile("(?is)<noscript[^>]*>.*?</noscript>").matcher(html).replaceAll(" ");
        html = Pattern.compile("(?is)<iframe[^>]*>.*?</iframe>").matcher(html).replaceAll(" ");
        html = Pattern.compile("(?is)<object[^>]*>.*?</object>").matcher(html).replaceAll(" ");
        html = Pattern.compile("(?is)<embed[^>]*>.*?</embed>").matcher(html).replaceAll(" ");

        logger.debug("  [extractFields] After script/style removal, HTML length: " + html.length());

        // Extract title
        Pattern titlePattern = Pattern.compile("<title[^>]*>(.*?)</title>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher titleMatcher = titlePattern.matcher(html);
        if (titleMatcher.find()) {
            String rawTitle = titleMatcher.group(1);
            logger.debug("  [extractFields] Found title tag, raw content: [" + rawTitle + "]");
            fields.title = cleanText(rawTitle, "title");
        } else {
            logger.debug("  [extractFields] No title tag found");
        }

        // Extract meta tags
        // Meta description
        Pattern metaDescPattern = Pattern.compile(
                "<meta[^>]*name\\s*=\\s*['\"]?description['\"]?[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"]",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher metaDescMatcher = metaDescPattern.matcher(html);
        if (metaDescMatcher.find()) {
            String rawMetaDesc = metaDescMatcher.group(1);
            logger.debug("  [extractFields] Found meta description, raw content: [" + rawMetaDesc + "]");
            fields.metaDescription = cleanText(rawMetaDesc, "meta-description");
        }
        // Also try content before name
        if (fields.metaDescription.isEmpty()) {
            metaDescPattern = Pattern.compile(
                    "<meta[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"][^>]*name\\s*=\\s*['\"]?description['\"]?",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            metaDescMatcher = metaDescPattern.matcher(html);
            if (metaDescMatcher.find()) {
                String rawMetaDesc = metaDescMatcher.group(1);
                logger.debug("  [extractFields] Found meta description (alt pattern), raw content: [" + rawMetaDesc
                        + "]");
                fields.metaDescription = cleanText(rawMetaDesc, "meta-description");
            } else {
                logger.debug("  [extractFields] No meta description found");
            }
        }

        // Meta keywords
        Pattern metaKeywordsPattern = Pattern.compile(
                "<meta[^>]*name\\s*=\\s*['\"]?keywords['\"]?[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"]",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher metaKeywordsMatcher = metaKeywordsPattern.matcher(html);
        if (metaKeywordsMatcher.find()) {
            String rawMetaKeywords = metaKeywordsMatcher.group(1);
            logger.debug("  [extractFields] Found meta keywords, raw content: [" + rawMetaKeywords + "]");
            fields.metaKeywords = cleanText(rawMetaKeywords, "meta-keywords");
        }
        // Also try content before name
        if (fields.metaKeywords.isEmpty()) {
            metaKeywordsPattern = Pattern.compile(
                    "<meta[^>]*content\\s*=\\s*['\"]([^'\"]+)['\"][^>]*name\\s*=\\s*['\"]?keywords['\"]?",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            metaKeywordsMatcher = metaKeywordsPattern.matcher(html);
            if (metaKeywordsMatcher.find()) {
                String rawMetaKeywords = metaKeywordsMatcher.group(1);
                logger.debug("  [extractFields] Found meta keywords (alt pattern), raw content: [" + rawMetaKeywords
                        + "]");
                fields.metaKeywords = cleanText(rawMetaKeywords, "meta-keywords");
            } else {
                logger.debug("  [extractFields] No meta keywords found");
            }
        }

        // Extract headers (h1-h6)
        StringBuilder headersBuilder = new StringBuilder();
        Pattern headerPattern = Pattern.compile("<h[1-6][^>]*>(.*?)</h[1-6]>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher headerMatcher = headerPattern.matcher(html);
        int headerCount = 0;
        while (headerMatcher.find()) {
            headerCount++;
            String rawHeader = headerMatcher.group(1);
            logger.debug("  [extractFields] Found header #" + headerCount + ", raw content: [" + rawHeader + "]");
            if (headersBuilder.length() > 0)
                headersBuilder.append(" ");
            headersBuilder.append(cleanText(rawHeader, "header"));
        }
        fields.headers = headersBuilder.toString();
        logger.debug("  [extractFields] Found " + headerCount + " header(s), combined: [" + fields.headers + "]");

        // Extract body content
        // First, try to extract from body tag
        String bodyHtml = html;
        Pattern bodyPattern = Pattern.compile("<body[^>]*>(.*?)</body>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher bodyMatcher = bodyPattern.matcher(bodyHtml);
        if (bodyMatcher.find()) {
            bodyHtml = bodyMatcher.group(1);
            logger.debug("  [extractFields] Found <body> tag, extracted body content (length: " + bodyHtml.length()
                    + ")");
        } else {
            logger.debug("  [extractFields] No <body> tag found, using entire HTML");
        }

        // Remove title and headers from body content (already extracted separately)
        int beforeRemoval = bodyHtml.length();
        bodyHtml = titlePattern.matcher(bodyHtml).replaceAll(" ");
        bodyHtml = headerPattern.matcher(bodyHtml).replaceAll(" ");
        if (bodyHtml.length() != beforeRemoval) {
            logger.debug("  [extractFields] Removed title/headers from body (length: " + beforeRemoval + " -> "
                    + bodyHtml.length() + ")");
        }

        // Remove any remaining script/style tags that might have been missed
        // Use DOTALL flag (?s) to match across newlines
        beforeRemoval = bodyHtml.length();
        bodyHtml = Pattern.compile("(?is)<script[^>]*>.*?</script>").matcher(bodyHtml).replaceAll(" ");
        bodyHtml = Pattern.compile("(?is)<style[^>]*>.*?</style>").matcher(bodyHtml).replaceAll(" ");
        bodyHtml = Pattern.compile("(?is)<noscript[^>]*>.*?</noscript>").matcher(bodyHtml).replaceAll(" ");
        if (bodyHtml.length() != beforeRemoval) {
            logger.debug("  [extractFields] Removed additional script/style tags from body (length: " + beforeRemoval
                    + " -> " + bodyHtml.length() + ")");
        }

        String bodyPreview = bodyHtml.length() > 300 ? bodyHtml.substring(0, 300) + "..." : bodyHtml;
        logger.debug("  [extractFields] Body HTML before cleaning (preview): [" + bodyPreview + "]");

        // Clean the body HTML - this removes all HTML tags and extracts all text
        // content
        // This will capture text from p, div, span, li, td, and any other tags
        fields.body = cleanText(bodyHtml, "body");

        // Count words in each field
        fields.titleWordCount = countWords(fields.title);
        fields.headerWordCount = countWords(fields.headers);
        fields.bodyWordCount = countWords(fields.body);

        return fields;
    }

    private static String cleanText(String text, String fieldName) {
        if (text == null) {
            logger.debug("    [cleanText] Input text is null for field: " + fieldName);
            return "";
        }

        String preview = text.length() > 200 ? text.substring(0, 200) + "..." : text;
        logger.debug("    [cleanText] Cleaning field: " + fieldName + ", input length: " + text.length());
        logger.debug("    [cleanText] Input preview: [" + preview + "]");

        // Remove HTML tags and attributes (including CSS selectors in attributes)
        // This regex matches: <tag>, </tag>, <tag attr="value">, etc.
        String beforeTagRemoval = text;
        text = text.replaceAll("<[^>]+>", " ");
        if (!text.equals(beforeTagRemoval)) {
            int tagCount = countMatches(beforeTagRemoval, "<[^>]+>");
            logger.debug("    [cleanText] Removed " + tagCount + " HTML tag(s) (length: " + beforeTagRemoval.length()
                    + " -> " + text.length() + ")");
        }

        // Remove HTML entities (e.g., &nbsp;, &amp;, &lt;, etc.)
        text = text.replaceAll("&[#\\w]+;", " ");

        // Remove CSS selectors and class/id patterns that might leak through
        // Patterns like .class-name, #id-name, [attribute], compound selectors, etc.
        text = text.replaceAll("\\.[\\w-]+", " "); // CSS class selectors (e.g., .breadcrumb-text-color)
        text = text.replaceAll("#[\\w-]+", " "); // CSS ID selectors (e.g., #main-content)
        text = text.replaceAll("\\[\\w[^\\]]*\\]", " "); // Attribute selectors (e.g., [data-id="123"])
        // Remove compound CSS selectors (e.g., .class>tag, tag.class, etc.)
        text = text.replaceAll("[\\w-]+[>+~][\\w-]+", " "); // Combinators: >, +, ~
        text = text.replaceAll("\\.[\\w-]+[>+~]", " "); // Class followed by combinator
        text = text.replaceAll("[>+~]\\.[\\w-]+", " "); // Combinator followed by class

        // Remove URLs and email addresses
        text = text.replaceAll("https?://[\\S]+", " ");
        text = text.replaceAll("www\\.[\\S]+", " ");
        text = text.replaceAll("[\\w.-]+@[\\w.-]+\\.[\\w]+", " ");

        // Remove all special characters and punctuation
        // Keep only letters, numbers, and spaces
        // This removes: quotes, dollar signs, percent, ampersands, etc.
        text = text.replaceAll("[^\\p{L}\\p{N}\\s]", " ");

        // Convert to lowercase
        text = text.toLowerCase();

        // Normalize whitespace
        text = text.replaceAll("\\s+", " ").trim();

        logger.debug("    [cleanText] Final cleaned text for " + fieldName + ": [" + text + "] (length: "
                + text.length() + ")");
        String[] words = text.split("\\s+");
        logger.debug("    [cleanText] Extracted " + words.length + " word(s)");
        if (words.length > 0 && words.length <= 20) {
            logger.debug("    [cleanText] Words: " + Arrays.toString(words));
        } else if (words.length > 20) {
            String[] first20 = Arrays.copyOf(words, 20);
            logger.debug("    [cleanText] First 20 words: " + Arrays.toString(first20) + " ... (total: "
                    + words.length + ")");
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
        String[] words = text.split("\\s+");
        int count = 0;
        for (String w : words) {
            if (!w.isEmpty())
                count++;
        }
        return count;
    }

    private static int processText(String text, String fieldType, Map<String, TermInfo> termInfoMap,
            int globalPosition, String url) {
        if (text == null || text.isEmpty()) {
            logger.debug("  [processText] Skipping empty text for field: " + fieldType);
            return globalPosition;
        }

        String[] words = text.split("\\s+");

        logger.debug("  [processText] Processing field: " + fieldType + ", " + words.length
                + " word(s), starting at position: " + globalPosition);

        int wordsProcessed = 0;
        for (String w : words) {
            // Skip empty words and words that contain non-alphanumeric characters
            // (shouldn't happen after cleanText, but double-check for safety)
            if (w.isEmpty() || !w.matches("^[\\p{L}\\p{N}]+$")) {
                if (!w.isEmpty()) {
                    logger.debug("    [processText] Skipping word with special characters: '" + w + "'");
                }
                continue;
            }

            TermInfo info = termInfoMap.computeIfAbsent(w, k -> new TermInfo());
            // Add global position to positions array
            info.positions.add(String.valueOf(globalPosition));
            info.totalFreq++;

            // Track field-specific frequency
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

            if (wordsProcessed < 10) {
                logger.debug("    [processText] Word #" + (wordsProcessed + 1) + ": '" + w + "' at position "
                        + globalPosition +
                        " (field: " + fieldType + ")");
            }

            // Increment global position for next word
            globalPosition++;
            wordsProcessed++;
        }

        logger.debug("  [processText] Processed " + wordsProcessed + " word(s) from field: " + fieldType
                + ", next position: " + globalPosition);

        return globalPosition;
    }

    private static int processUrlTokens(String url, Map<String, TermInfo> termInfoMap) {
        if (url == null || url.isEmpty()) {
            logger.debug("  [processUrlTokens] URL is null or empty");
            return 0;
        }

        logger.debug("  [processUrlTokens] Processing URL: " + url);

        // Tokenize URL: split by /, -, _, ., and other separators
        String urlLower = url.toLowerCase();
        String[] tokens = urlLower.split("[/\\-_\\.?=&]+");

        logger.debug("  [processUrlTokens] Extracted " + tokens.length + " token(s) from URL");

        int tokensIndexed = 0;
        int tokensSkipped = 0;
        for (String token : tokens) {
            // Skip empty tokens, single character tokens, and tokens with special
            // characters
            // URL tokens should only contain alphanumeric characters
            if (token.isEmpty() || token.length() <= 1 || !token.matches("^[\\p{L}\\p{N}]+$")) {
                if (!token.isEmpty() && token.length() > 1) {
                    logger.debug("    [processUrlTokens] Skipping URL token with special characters: '" + token + "'");
                }
                tokensSkipped++;
                continue;
            }

            // Remove common URL parts
            if (token.equals("http") || token.equals("https") || token.equals("www") ||
                    token.equals("com") || token.equals("org") || token.equals("net") ||
                    token.equals("edu") || token.equals("gov")) {
                logger.debug("    [processUrlTokens] Skipping common URL token: '" + token + "'");
                tokensSkipped++;
                continue;
            }

            // Token is already lowercase (from urlLower), and validated to be alphanumeric
            // only
            TermInfo info = termInfoMap.computeIfAbsent(token, k -> new TermInfo());
            info.totalFreq++;
            info.urlFreq++;
            tokensIndexed++;

            logger.debug("    [processUrlTokens] Indexed URL token: '" + token + "'");
        }

        logger.debug("  [processUrlTokens] Indexed " + tokensIndexed + " URL token(s), skipped " + tokensSkipped);
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
        public List<String> positions = new ArrayList<>();
    }
}
