package cis5550.tools;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

import cis5550.webserver.Request;
import cis5550.webserver.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lightweight spell-check server built on the project webserver.
 * Exposes:
 *   - GET /suggest?word=helo   -> JSON suggestions
 *   - GET /health              -> JSON status
 */
public class SpellCheckServer {
    private static final int PORT = 8080;
    private static final int MAX_DISTANCE = 2;
    private static final int CACHE_SIZE = 1000;
    private static final String DICT_URL = "https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english.txt";

    private static BKTree bkTree;
    private static Map<Integer, List<String>> lengthIndex;
    private static Map<String, String> cache;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Set<String> dictionary = loadDictionary();
        bkTree = new BKTree(dictionary);
        lengthIndex = buildLengthIndex(dictionary);
        cache = new LinkedHashMap<String, String>(CACHE_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > CACHE_SIZE;
            }
        };

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("SpellCheck initialized in " + elapsed + "ms with " + dictionary.size() + " words");

        port(PORT);
        registerRoutes();

        System.out.println("SpellCheck server on http://localhost:" + PORT);
        System.out.println("Try: http://localhost:" + PORT + "/suggest?word=helo");
    }

    private static void registerRoutes() {
        get("/health", SpellCheckServer::handleHealth);
        get("/suggest", SpellCheckServer::handleSuggest);
    }

    private static String handleHealth(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Origin", "*");
        return String.format("{\"status\":\"ok\",\"words\":%d,\"cached\":%d}", bkTree.size(), cache.size());
    }

    private static String handleSuggest(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Origin", "*");

        String word = req.queryParams("word");
        if (word == null) {
            res.status(400, "Bad Request");
            return "{\"error\":\"Missing 'word' parameter\"}";
        }

        try {
            word = URLDecoder.decode(word, StandardCharsets.UTF_8.name()).toLowerCase();
        } catch (Exception e) {
            res.status(400, "Bad Request");
            return "{\"error\":\"Invalid 'word' encoding\"}";
        }

        long start = System.nanoTime();
        List<Suggestion> suggestions = getSuggestions(word, 5);
        double micros = (System.nanoTime() - start) / 1000.0;
        return suggestionsToJson(word, suggestions, micros);
    }

    private static Set<String> loadDictionary() throws Exception {
        Set<String> dictionary = new HashSet<>();
        try {
            System.out.println("Loading dictionary from " + DICT_URL);
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new URL(DICT_URL).openStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String w = line.trim().toLowerCase();
                    if (!w.isEmpty()) {
                        dictionary.add(w);
                    }
                }
            }
            return dictionary;
        } catch (Exception e) {
            System.out.println("Failed to load from URL: " + e.getMessage());
        }

        File localFile = new File("words.txt");
        if (localFile.exists()) {
            System.out.println("Loading dictionary from local file: words.txt");
            try (BufferedReader reader = new BufferedReader(new FileReader(localFile, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String w = line.trim().toLowerCase();
                    if (!w.isEmpty()) {
                        dictionary.add(w);
                    }
                }
            }
            return dictionary;
        }

        System.out.println("Using embedded fallback dictionary");
        String[] fallback = {
                "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
                "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
                "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
                "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
                "hello", "world", "test", "example", "code", "java", "server", "spell"
        };
        dictionary.addAll(Arrays.asList(fallback));
        return dictionary;
    }

    private static Map<Integer, List<String>> buildLengthIndex(Set<String> dictionary) {
        Map<Integer, List<String>> index = new HashMap<>();
        for (String word : dictionary) {
            index.computeIfAbsent(word.length(), k -> new ArrayList<>()).add(word);
        }
        return index;
    }

    private static List<Suggestion> getSuggestions(String word, int maxSuggestions) {
        String cached = cache.get(word);
        if (cached != null) {
            return Collections.singletonList(new Suggestion(cached, 0));
        }

        List<Suggestion> quick = quickSearch(word);
        if (!quick.isEmpty()) {
            cache.put(word, quick.get(0).word);
            return quick.subList(0, Math.min(maxSuggestions, quick.size()));
        }

        List<Suggestion> results = bkTree.search(word, MAX_DISTANCE, maxSuggestions);
        if (!results.isEmpty()) {
            cache.put(word, results.get(0).word);
        }
        return results;
    }

    private static List<Suggestion> quickSearch(String word) {
        List<Suggestion> results = new ArrayList<>();
        if (bkTree.contains(word)) {
            results.add(new Suggestion(word, 0));
            return results;
        }

        int len = word.length();
        for (int targetLen = Math.max(1, len - MAX_DISTANCE); targetLen <= len + MAX_DISTANCE; targetLen++) {
            List<String> candidates = lengthIndex.get(targetLen);
            if (candidates == null) {
                continue;
            }
            if (len > 0) {
                char firstChar = word.charAt(0);
                for (String candidate : candidates) {
                    if (candidate.charAt(0) == firstChar) {
                        int dist = fastLevenshtein(word, candidate, MAX_DISTANCE);
                        if (dist <= MAX_DISTANCE) {
                            results.add(new Suggestion(candidate, dist));
                            if (results.size() >= 5) {
                                results.sort(Comparator.comparingInt(s -> s.distance));
                                return results;
                            }
                        }
                    }
                }
            }
        }
        results.sort(Comparator.comparingInt(s -> s.distance));
        return results;
    }

    private static int fastLevenshtein(String s1, String s2, int threshold) {
        int len1 = s1.length();
        int len2 = s2.length();
        if (Math.abs(len1 - len2) > threshold) {
            return threshold + 1;
        }
        if (len1 > len2) {
            String tmp = s1;
            s1 = s2;
            s2 = tmp;
            int tmpLen = len1;
            len1 = len2;
            len2 = tmpLen;
        }
        int[] prev = new int[len2 + 1];
        int[] curr = new int[len2 + 1];
        for (int j = 0; j <= len2; j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= len1; i++) {
            curr[0] = i;
            int min = curr[0];
            for (int j = 1; j <= len2; j++) {
                int cost = s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);
                min = Math.min(min, curr[j]);
            }
            if (min > threshold) {
                return threshold + 1;
            }
            int[] tmp = prev;
            prev = curr;
            curr = tmp;
        }
        return prev[len2];
    }

    private static String suggestionsToJson(String word, List<Suggestion> suggestions, double micros) {
        StringBuilder json = new StringBuilder();
        json.append("{\"word\":\"").append(word)
                .append("\",\"time_us\":").append(String.format("%.2f", micros))
                .append(",\"suggestions\":[");
        for (int i = 0; i < suggestions.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("{\"word\":\"").append(suggestions.get(i).word)
                    .append("\",\"distance\":").append(suggestions.get(i).distance).append("}");
        }
        json.append("]}");
        return json.toString();
    }

    private static class Suggestion {
        final String word;
        final int distance;

        Suggestion(String word, int distance) {
            this.word = word;
            this.distance = distance;
        }
    }

    /** BK-Tree implementation for efficient edit-distance search. */
    private static class BKTree {
        private Node root;
        private int size;

        BKTree(Set<String> words) {
            for (String word : words) {
                add(word);
            }
        }

        void add(String word) {
            if (root == null) {
                root = new Node(word);
                size++;
                return;
            }
            Node current = root;
            while (true) {
                int distance = fastLevenshtein(current.word, word, Integer.MAX_VALUE);
                if (distance == 0) {
                    return;
                }
                Node child = current.children.get(distance);
                if (child == null) {
                    current.children.put(distance, new Node(word));
                    size++;
                    return;
                }
                current = child;
            }
        }

        boolean contains(String word) {
            if (root == null) {
                return false;
            }
            return containsHelper(root, word);
        }

        private boolean containsHelper(Node node, String word) {
            if (node.word.equals(word)) {
                return true;
            }
            int distance = fastLevenshtein(node.word, word, Integer.MAX_VALUE);
            Node child = node.children.get(distance);
            return child != null && containsHelper(child, word);
        }

        List<Suggestion> search(String word, int maxDistance, int maxResults) {
            List<Suggestion> results = new ArrayList<>();
            if (root != null) {
                searchHelper(root, word, maxDistance, results, maxResults);
            }
            results.sort(Comparator.comparingInt(s -> s.distance));
            return results;
        }

        private void searchHelper(Node node, String word, int maxDistance, List<Suggestion> results, int maxResults) {
            if (results.size() >= maxResults) {
                return;
            }
            int distance = fastLevenshtein(node.word, word, maxDistance);
            if (distance <= maxDistance) {
                results.add(new Suggestion(node.word, distance));
                if (results.size() >= maxResults) {
                    return;
                }
            }
            int minDist = Math.max(1, distance - maxDistance);
            int maxDist = distance + maxDistance;
            for (int d = minDist; d <= maxDist; d++) {
                Node child = node.children.get(d);
                if (child != null) {
                    searchHelper(child, word, maxDistance, results, maxResults);
                    if (results.size() >= maxResults) {
                        return;
                    }
                }
            }
        }

        int size() {
            return size;
        }

        private static class Node {
            final String word;
            final Map<Integer, Node> children = new HashMap<>();

            Node(String word) {
                this.word = word;
            }
        }
    }
}
