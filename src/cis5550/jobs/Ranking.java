package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class Ranking {
    static KVSClient kvs;
    static Integer corpusSize;
    // Weight for combining different ranking signals
    // These weights should sum to less than 1.0 to leave room for PageRank
    private static final Double ALPHA = 0.45; // TF-IDF/Cosine similarity weight
    private static final Double BETA = 0.15; // Field-based ranking weight
    private static final Double GAMMA = 0.1; // Proximity weight
    private static final Double DELTA = 0.1; // Phrase/URL/Anchor weight
    // PageRank weight = 1 - ALPHA - BETA - GAMMA - DELTA = 0.2 (20%)
    private static final Logger logger = Logger.getLogger(Ranking.class);

    // Field weights for field-based ranking (on-page content only)
    private static final double TITLE_WEIGHT = 2.0;
    private static final double META_WEIGHT = 1.2;
    private static final double HEADER_WEIGHT = 1.5;
    private static final double BODY_WEIGHT = 1.0;
    // URL and anchor weights for off-page feature scoring
    private static final double URL_WEIGHT = 1.8;
    private static final double ANCHOR_WEIGHT = 1.3;

    // Stop words set - common English stop words to ignore during ranking
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
            "air", "away", "animal", "house", "point", "page", "letter", "mother",
            "answer", "found", "study", "still", "learn", "should"));

    public static void initialize(KVSClient kvsClient) {
        kvs = kvsClient;
        try {
            corpusSize = getCorpusSize();
        } catch (Exception e) {
            logger.error("Error initializing corpus size: " + e.getMessage());
            corpusSize = 0;
        }
    }

    public static Map<String, Object> searchWithDetails(String query, boolean debug) throws IOException {
        // Normalize and stem query
        String[] queryWords = normalizeAndStemQuery(query);
        logger.info("Query words: " + Arrays.toString(queryWords));

        // Get candidate URLs from index
        DocumentInfoResult docInfoResult = getDocumentInfo(queryWords);
        Map<String, DocumentInfo> docMap = docInfoResult.docMap;
        Map<String, Integer> documentFrequencies = docInfoResult.documentFrequencies;

        if (docMap.isEmpty()) {
            Map<String, Object> result = new HashMap<>();
            result.put("results", new ArrayList<>());
            result.put("query", query);
            return result;
        }

        // Calculate various ranking scores
        Map<String, Double> cosineScores = calculateCosineSimilarity(queryWords, docMap, documentFrequencies);
        Map<String, Double> fieldScores = calculateFieldBasedScores(queryWords, docMap);
        Map<String, Double> proximityScores = calculateProximityScores(queryWords, docMap);
        Map<String, Double> phraseScores = calculatePhraseScores(query, docMap);
        Map<String, Double> urlAnchorScores = calculateUrlAnchorScores(queryWords, docMap);
        Map<String, Double> pageRanks = getPageRanks(new ArrayList<>(docMap.keySet()));

        // Build results with details
        List<Map<String, Object>> results = new ArrayList<>();
        for (String url : docMap.keySet()) {
            double cosine = cosineScores.getOrDefault(url, 0.0);
            double field = fieldScores.getOrDefault(url, 0.0);
            double proximity = proximityScores.getOrDefault(url, 0.0);
            double phrase = phraseScores.getOrDefault(url, 0.0);
            double urlAnchor = urlAnchorScores.getOrDefault(url, 0.0);
            double pagerank = normalizePageRank(pageRanks.getOrDefault(url, 0.0));

            double finalScore = ALPHA * cosine +
                    BETA * field +
                    GAMMA * proximity +
                    DELTA * (phrase + urlAnchor) +
                    (1 - ALPHA - BETA - GAMMA - DELTA) * pagerank;

            Map<String, Object> result = new HashMap<>();
            result.put("url", url);
            result.put("finalScore", finalScore);

            if (debug) {
                Map<String, Object> debugInfo = new HashMap<>();
                debugInfo.put("cosineScore", cosine);
                debugInfo.put("fieldScore", field);
                debugInfo.put("proximityScore", proximity);
                debugInfo.put("phraseScore", phrase);
                debugInfo.put("urlAnchorScore", urlAnchor);
                debugInfo.put("pageRank", pageRanks.getOrDefault(url, 0.0));

                // Get term frequencies for each query word
                DocumentInfo doc = docMap.get(url);
                Map<String, Map<String, Double>> termFrequencies = new HashMap<>();
                for (String word : queryWords) {
                    TermData termData = doc.terms.get(word);
                    if (termData != null) {
                        Map<String, Double> tfs = new HashMap<>();
                        tfs.put("title", termData.titleTf);
                        tfs.put("meta", termData.metaTf);
                        tfs.put("header", termData.headerTf);
                        tfs.put("body", termData.bodyTf);
                        tfs.put("url", termData.urlTf);
                        tfs.put("anchor", termData.anchorTf);
                        tfs.put("total", termData.tf);
                        termFrequencies.put(word, tfs);
                    }
                }
                debugInfo.put("termFrequencies", termFrequencies);
                result.put("debug", debugInfo);
            }

            results.add(result);
        }

        // Sort by final score descending
        results.sort((a, b) -> Double.compare(
                (Double) b.get("finalScore"),
                (Double) a.get("finalScore")));

        // Limit to top 100
        if (results.size() > 100) {
            results = results.subList(0, 100);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("results", results);
        response.put("query", query);
        return response;
    }

    // Image search: use pt-image-index (format: url|tf,...), ranking based on
    // anchor-text tf only
    public static Map<String, Object> searchImagesWithDetails(String query, boolean debug) throws IOException {
        // Normalize and stem query
        String[] queryWords = normalizeAndStemQuery(query);
        logger.info("Image query words: " + Arrays.toString(queryWords));

        // Get candidate URLs from pt-image-index
        DocumentInfoResult docInfoResult = getImageDocumentInfo(queryWords);
        Map<String, DocumentInfo> docMap = docInfoResult.docMap;
        Map<String, Integer> documentFrequencies = docInfoResult.documentFrequencies;

        if (docMap.isEmpty()) {
            Map<String, Object> result = new HashMap<>();
            result.put("results", new ArrayList<>());
            result.put("query", query);
            return result;
        }

        // Calculate scores (reuse cosine & proximity with available anchor tf)
        Map<String, Double> cosineScores = calculateCosineSimilarity(queryWords, docMap, documentFrequencies);
        Map<String, Double> proximityScores = calculateProximityScores(queryWords, docMap);
        Map<String, Double> phraseScores = calculatePhraseScores(query, docMap);

        List<Map<String, Object>> results = new ArrayList<>();
        for (String url : docMap.keySet()) {
            double cosine = cosineScores.getOrDefault(url, 0.0);
            double proximity = proximityScores.getOrDefault(url, 0.0);
            double phrase = phraseScores.getOrDefault(url, 0.0);

            // Weight only cosine + proximity + phrase (no pagerank, no field/url/anchor
            // extras beyond TF)
            double finalScore = 0.6 * cosine + 0.25 * proximity + 0.15 * phrase;

            Map<String, Object> result = new HashMap<>();
            result.put("url", url);
            result.put("finalScore", finalScore);

            if (debug) {
                Map<String, Object> debugInfo = new HashMap<>();
                debugInfo.put("cosineScore", cosine);
                debugInfo.put("proximityScore", proximity);
                debugInfo.put("phraseScore", phrase);

                DocumentInfo doc = docMap.get(url);
                Map<String, Map<String, Double>> termFrequencies = new HashMap<>();
                for (String word : queryWords) {
                    TermData termData = doc.terms.get(word);
                    if (termData != null) {
                        Map<String, Double> tfs = new HashMap<>();
                        tfs.put("anchor", termData.anchorTf);
                        tfs.put("total", termData.tf);
                        termFrequencies.put(word, tfs);
                    }
                }
                debugInfo.put("termFrequencies", termFrequencies);
                result.put("debug", debugInfo);
            }

            results.add(result);
        }

        results.sort((a, b) -> Double.compare(
                (Double) b.get("finalScore"),
                (Double) a.get("finalScore")));

        if (results.size() > 100) {
            results = results.subList(0, 100);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("results", results);
        response.put("query", query);
        return response;
    }

    private static String[] normalizeAndStemQuery(String query) {
        if (query == null || query.isEmpty())
            return new String[0];

        String normalized = query.toLowerCase().trim();
        String[] words = normalized.split("\\s+");
        List<String> stemmedWords = new ArrayList<>();

        SnowballStemmer stemmer = new englishStemmer();
        for (String word : words) {
            if (!word.isEmpty()) {
                // Filter out stop words before stemming
                if (STOP_WORDS.contains(word.toLowerCase())) {
                    continue;
                }
                stemmer.setCurrent(word);
                stemmer.stem();
                String stemmed = stemmer.getCurrent();
                // Also filter out stop words after stemming (in case stemming produces a stop
                // word)
                if (!stemmed.isEmpty() && !STOP_WORDS.contains(stemmed.toLowerCase())) {
                    stemmedWords.add(stemmed);
                }
            }
        }

        return stemmedWords.toArray(new String[0]);
    }

    private static class DocumentInfo {
        Map<String, TermData> terms = new HashMap<>();

        // Field-specific term frequencies
        Map<String, Double> titleTfs = new HashMap<>();
        Map<String, Double> headerTfs = new HashMap<>();
        Map<String, Double> bodyTfs = new HashMap<>();
        Map<String, Double> urlTfs = new HashMap<>();
        Map<String, Double> anchorTfs = new HashMap<>();
    }

    private static class TermData {
        double tf;
        double titleTf;
        double metaTf;
        double headerTf;
        double bodyTf;
        double urlTf;
        double anchorTf;
        List<Integer> positions = new ArrayList<>();
    }

    private static class DocumentInfoResult {
        Map<String, DocumentInfo> docMap;
        Map<String, Integer> documentFrequencies;

        DocumentInfoResult(Map<String, DocumentInfo> docMap, Map<String, Integer> documentFrequencies) {
            this.docMap = docMap;
            this.documentFrequencies = documentFrequencies;
        }
    }

    private static DocumentInfoResult getDocumentInfo(String[] queryWords) throws IOException {
        Map<String, DocumentInfo> docMap = new HashMap<>();
        Map<String, Integer> documentFrequencies = new HashMap<>();

        // First pass: collect all documents and calculate document frequencies
        for (String word : queryWords) {
            Row indexEntry = kvs.getRow("pt-index", word);
            if (indexEntry == null) {
                // Word not in index, DF is 0
                documentFrequencies.put(word, 0);
                continue;
            }

            String urls = indexEntry.get("value");
            if (urls == null || urls.isEmpty()) {
                documentFrequencies.put(word, 0);
                continue;
            }

            String[] urlEntries = urls.split(",");
            // DF is the number of documents containing this word in the entire corpus
            documentFrequencies.put(word, urlEntries.length);

            for (String urlEntry : urlEntries) {
                // Format: url|totalTf|titleTf|metaTf|headerTf|bodyTf|urlTf|anchorTf|positions
                String[] parts = urlEntry.split("\\|");
                if (parts.length < 9)
                    continue; // Skip malformed entries

                String url = parts[0];
                DocumentInfo doc = docMap.computeIfAbsent(url, k -> new DocumentInfo());

                TermData termData = new TermData();
                try {
                    termData.tf = Double.parseDouble(parts[1]);
                    termData.titleTf = Double.parseDouble(parts[2]);
                    termData.metaTf = Double.parseDouble(parts[3]);
                    termData.headerTf = Double.parseDouble(parts[4]);
                    termData.bodyTf = Double.parseDouble(parts[5]);
                    termData.urlTf = Double.parseDouble(parts[6]);
                    termData.anchorTf = Double.parseDouble(parts[7]);

                    // Parse positions
                    if (parts.length > 8 && !parts[8].isEmpty()) {
                        String[] posStrings = parts[8].split("\\s+");
                        for (String posStr : posStrings) {
                            try {
                                termData.positions.add(Integer.parseInt(posStr));
                            } catch (NumberFormatException e) {
                                // Skip invalid positions
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Error parsing term data for word " + word + " in URL " + url);
                    continue;
                }

                doc.terms.put(word, termData);
                doc.titleTfs.put(word, termData.titleTf);
                doc.headerTfs.put(word, termData.headerTf);
                doc.bodyTfs.put(word, termData.bodyTf);
                doc.urlTfs.put(word, termData.urlTf);
                doc.anchorTfs.put(word, termData.anchorTf);
            }
        }

        // Document lengths are calculated on-the-fly in cosine similarity

        return new DocumentInfoResult(docMap, documentFrequencies);
    }

    private static DocumentInfoResult getImageDocumentInfo(String[] queryWords) throws IOException {
        Map<String, DocumentInfo> docMap = new HashMap<>();
        Map<String, Integer> documentFrequencies = new HashMap<>();

        for (String word : queryWords) {
            Row indexEntry = kvs.getRow("pt-image-index", word);
            if (indexEntry == null) {
                documentFrequencies.put(word, 0);
                continue;
            }

            String urls = indexEntry.get("value");
            if (urls == null || urls.isEmpty()) {
                documentFrequencies.put(word, 0);
                continue;
            }

            String[] urlEntries = urls.split(",");
            documentFrequencies.put(word, urlEntries.length);

            for (String urlEntry : urlEntries) {
                // format: url|tf
                int idx = urlEntry.indexOf('|');
                if (idx < 0) {
                    continue;
                }
                String url = urlEntry.substring(0, idx);
                double tf = 0.0;
                try {
                    tf = Double.parseDouble(urlEntry.substring(idx + 1));
                } catch (NumberFormatException ignored) {
                }

                DocumentInfo doc = docMap.computeIfAbsent(url, k -> new DocumentInfo());
                TermData termData = doc.terms.computeIfAbsent(word, k -> new TermData());
                termData.anchorTf += tf;
                termData.tf += tf;
                // Add a placeholder position sequence to enable proximity/phrase scoring
                termData.positions.add(termData.positions.size());
            }
        }

        return new DocumentInfoResult(docMap, documentFrequencies);
    }

    private static Map<String, Double> calculateCosineSimilarity(String[] queryWords,
            Map<String, DocumentInfo> docMap, Map<String, Integer> documentFrequencies) {
        Map<String, Double> scores = new HashMap<>();

        // Calculate query vector magnitude
        Map<String, Integer> queryTf = new HashMap<>();
        for (String word : queryWords) {
            queryTf.put(word, queryTf.getOrDefault(word, 0) + 1);
        }

        // Normalize query TF by query length and calculate magnitude using TF-IDF
        // Use actual DF for each word from the index
        double queryLength = queryWords.length;
        Map<String, Double> queryTfIdfMap = new HashMap<>();

        double queryMagnitude = 0.0;
        for (Map.Entry<String, Integer> entry : queryTf.entrySet()) {
            String word = entry.getKey();
            // Get actual DF for this word from the index
            int df = documentFrequencies.getOrDefault(word, 0);
            double idf = Math.log10((double) corpusSize / Math.max(df, 1));

            double normalizedQueryTf = entry.getValue() / queryLength;
            double queryTfIdf = (1 + Math.log10(1 + normalizedQueryTf)) * idf;
            queryTfIdfMap.put(word, queryTfIdf);
            queryMagnitude += queryTfIdf * queryTfIdf;
        }
        queryMagnitude = Math.sqrt(queryMagnitude);

        // Calculate cosine similarity for each document
        for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
            String url = entry.getKey();
            DocumentInfo doc = entry.getValue();

            double dotProduct = 0.0;
            double docMagnitude = 0.0;

            for (String word : queryWords) {
                TermData termData = doc.terms.get(word);
                if (termData == null)
                    continue;

                // Use pre-calculated query TF-IDF
                double queryTfIdf = queryTfIdfMap.getOrDefault(word, 0.0);

                // Calculate TF-IDF for document term
                // Use actual DF for this word from the index
                int df = documentFrequencies.getOrDefault(word, 0);
                double idf = Math.log10((double) corpusSize / Math.max(df, 1));

                // termData.tf is already normalized (divided by doc length) from the index
                // Apply same log normalization for consistency: (1 + log(tf)) * idf
                double docTf = Math.max(termData.tf, 0.0);
                double docTfIdf = (1 + Math.log10(1 + docTf)) * idf;

                dotProduct += queryTfIdf * docTfIdf;
                docMagnitude += docTfIdf * docTfIdf;
            }

            docMagnitude = Math.sqrt(docMagnitude);

            if (queryMagnitude > 0 && docMagnitude > 0) {
                double cosine = dotProduct / (queryMagnitude * docMagnitude);
                scores.put(url, cosine);
            } else {
                scores.put(url, 0.0);
            }
        }

        return scores;
    }

    private static Map<String, Double> calculateFieldBasedScores(String[] queryWords,
            Map<String, DocumentInfo> docMap) {
        Map<String, Double> scores = new HashMap<>();

        for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
            String url = entry.getKey();
            DocumentInfo doc = entry.getValue();

            double fieldScore = 0.0;
            for (String word : queryWords) {
                TermData termData = doc.terms.get(word);
                if (termData == null)
                    continue;

                // Weighted sum of field-specific TFs (on-page content only)
                // Note: URL and anchor are off-page features and scored separately
                fieldScore += TITLE_WEIGHT * termData.titleTf +
                        META_WEIGHT * termData.metaTf +
                        HEADER_WEIGHT * termData.headerTf +
                        BODY_WEIGHT * termData.bodyTf;
            }

            // Normalize by number of query terms
            fieldScore /= queryWords.length;
            scores.put(url, fieldScore);
        }

        return normalizeScores(scores);
    }

    private static Map<String, Double> calculateProximityScores(String[] queryWords, Map<String, DocumentInfo> docMap) {
        Map<String, Double> scores = new HashMap<>();

        for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
            String url = entry.getKey();
            DocumentInfo doc = entry.getValue();

            if (queryWords.length < 2) {
                scores.put(url, 0.0);
                continue;
            }

            double proximityScore = 0.0;
            int pairCount = 0;

            // Calculate proximity for all pairs of query terms
            for (int i = 0; i < queryWords.length - 1; i++) {
                String word1 = queryWords[i];
                String word2 = queryWords[i + 1];

                TermData term1 = doc.terms.get(word1);
                TermData term2 = doc.terms.get(word2);

                if (term1 == null || term2 == null || term1.positions.isEmpty() || term2.positions.isEmpty()) {
                    continue;
                }

                // Find minimum distance between positions
                int minDistance = Integer.MAX_VALUE;
                for (int pos1 : term1.positions) {
                    for (int pos2 : term2.positions) {
                        int distance = Math.abs(pos1 - pos2);
                        if (distance < minDistance) {
                            minDistance = distance;
                        }
                    }
                }

                if (minDistance != Integer.MAX_VALUE) {
                    // Inverse distance scoring (closer = better)
                    proximityScore += 1.0 / (1.0 + minDistance);
                    pairCount++;
                }
            }

            if (pairCount > 0) {
                proximityScore /= pairCount;
            }

            scores.put(url, proximityScore);
        }

        return normalizeScores(scores);
    }

    private static Map<String, Double> calculatePhraseScores(String originalQuery, Map<String, DocumentInfo> docMap) {
        Map<String, Double> scores = new HashMap<>();

        // Check for phrase queries (quoted strings)
        List<String> phrases = extractPhrases(originalQuery);

        if (phrases.isEmpty()) {
            // No explicit phrases, check if query terms appear in order
            String[] queryWords = normalizeAndStemQuery(originalQuery);
            for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
                String url = entry.getKey();
                DocumentInfo doc = entry.getValue();

                // Check if all terms appear in sequence
                boolean inSequence = checkTermSequence(queryWords, doc);
                scores.put(url, inSequence ? 1.0 : 0.0);
            }
        } else {
            // Score based on phrase matches
            for (String phrase : phrases) {
                String[] phraseWords = normalizeAndStemQuery(phrase);
                for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
                    String url = entry.getKey();
                    DocumentInfo doc = entry.getValue();

                    boolean phraseFound = checkTermSequence(phraseWords, doc);
                    scores.put(url, scores.getOrDefault(url, 0.0) + (phraseFound ? 1.0 : 0.0));
                }
            }
        }

        return normalizeScores(scores);
    }

    private static List<String> extractPhrases(String query) {
        List<String> phrases = new ArrayList<>();
        if (query == null)
            return phrases;

        // Extract quoted phrases
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\"([^\"]+)\"");
        java.util.regex.Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {
            phrases.add(matcher.group(1));
        }

        return phrases;
    }

    private static boolean checkTermSequence(String[] terms, DocumentInfo doc) {
        if (terms.length < 2)
            return true;

        // Get all positions for all terms
        List<List<Integer>> allPositions = new ArrayList<>();
        for (String term : terms) {
            TermData termData = doc.terms.get(term);
            if (termData == null || termData.positions.isEmpty()) {
                return false;
            }
            allPositions.add(termData.positions);
        }

        // Check if there's a sequence where terms appear in order
        for (int pos0 : allPositions.get(0)) {
            int currentPos = pos0;
            boolean sequenceFound = true;

            for (int i = 1; i < allPositions.size(); i++) {
                boolean foundNext = false;
                for (int pos : allPositions.get(i)) {
                    if (pos > currentPos && pos <= currentPos + 10) { // Allow small gaps
                        currentPos = pos;
                        foundNext = true;
                        break;
                    }
                }
                if (!foundNext) {
                    sequenceFound = false;
                    break;
                }
            }

            if (sequenceFound) {
                return true;
            }
        }

        return false;
    }

    private static Map<String, Double> calculateUrlAnchorScores(String[] queryWords, Map<String, DocumentInfo> docMap) {
        Map<String, Double> scores = new HashMap<>();

        for (Map.Entry<String, DocumentInfo> entry : docMap.entrySet()) {
            String url = entry.getKey();
            DocumentInfo doc = entry.getValue();

            double urlScore = 0.0;
            double anchorScore = 0.0;

            for (String word : queryWords) {
                TermData termData = doc.terms.get(word);
                if (termData == null)
                    continue;

                // Use actual TF values (not binary) - these are normalized frequencies
                urlScore += termData.urlTf;
                anchorScore += termData.anchorTf;
            }

            // Apply weights and normalize by query length
            double totalScore = (urlScore * URL_WEIGHT + anchorScore * ANCHOR_WEIGHT) / queryWords.length;
            scores.put(url, totalScore);
        }

        return normalizeScores(scores);
    }

    private static Map<String, Double> normalizeScores(Map<String, Double> scores) {
        if (scores.isEmpty())
            return scores;

        double max = scores.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        if (max == 0.0)
            max = 1.0;

        Map<String, Double> normalized = new HashMap<>();
        for (Map.Entry<String, Double> entry : scores.entrySet()) {
            normalized.put(entry.getKey(), entry.getValue() / max);
        }

        return normalized;
    }

    private static double normalizePageRank(double pagerank) {
        // Normalize PageRank to 0-1 range (assuming typical range is 0-10)
        return Math.min(pagerank / 10.0, 1.0);
    }

    public static Map<String, Double> getPageRanks(List<String> urls) {
        Map<String, Double> pageRanks = new HashMap<>();

        try {
            for (String url : urls) {
                // PageRank table uses hashed URLs as row keys
                String urlHash = Hasher.hash(url);
                Row pageRankEntry = kvs.getRow("pt-pageranks", urlHash);
                if (pageRankEntry != null) {
                    String rankStr = pageRankEntry.get("rank");
                    if (rankStr != null) {
                        try {
                            double rank = Double.parseDouble(rankStr);
                            pageRanks.put(url, rank);
                        } catch (NumberFormatException e) {
                            logger.warn("Invalid PageRank value for URL " + url + ": " + rankStr);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching PageRank values: " + e.getMessage());
        }

        return pageRanks;
    }

    public static String readSearchQuery(String filePath) {
        try {
            return Files.readString(Path.of(filePath));
        } catch (java.io.IOException e) {
            logger.error("Error reading search query file: " + e.getMessage());
            return null;
        }
    }

    public static Integer getCorpusSize() {
        try {
            if (kvs == null) {
                throw new IllegalStateException("KVSClient is not initialized.");
            }
            return kvs.count("pt-crawl");
        } catch (Exception e) {
            logger.error("Error getting corpus size: " + e.getMessage());
            return 0;
        }
    }
}
