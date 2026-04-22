package cis5550.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * "Google-like" Query Predictor.
 * Uses a Trie (Prefix Tree) to perform fast Autocomplete + Fuzzy Correction.
 * Prioritizes high-frequency words.
 */
public class SpellCheckerHelper {
    private static final int MAX_EDIT_DISTANCE = 2; // Tolerance for typos
    private static final int CACHE_SIZE = 1000;
    private static final String LOCAL_DICT = "src/cis5550/tools/dict3.csv";

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static Trie trie;
    private static Map<String, List<Suggestion>> cache;

    private SpellCheckerHelper() {}

    public static void init() throws Exception {
        if (initialized.get()) return;
        synchronized (SpellCheckerHelper.class) {
            if (initialized.get()) return;

            trie = new Trie();
            loadDictionaryAndPopulateTrie();

            cache = new LinkedHashMap<String, List<Suggestion>>(CACHE_SIZE, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<String, List<Suggestion>> eldest) {
                    return size() > CACHE_SIZE;
                }
            };
            initialized.set(true);
        }
    }

    public static List<Suggestion> suggest(String rawWord, int maxSuggestions) {
        if (rawWord == null) return Collections.emptyList();
        String prefix = rawWord.trim().toLowerCase();
        if (prefix.isEmpty()) return Collections.emptyList();
        if (!initialized.get()) throw new IllegalStateException("Not initialized");

        if (cache.containsKey(prefix)) {
            return cache.get(prefix);
        }

        // 1. Exact Prefix Search (The "Autocomplete" phase)
        // If I type "pro", find "program", "project", etc.
        Set<Suggestion> candidates = new HashSet<>(trie.searchPrefix(prefix, maxSuggestions));

        // 2. Fuzzy Search (The "Typo Correction" phase)
        // If exact prefix yields few results (or user made a typo like "prg"), search wider.
        if (candidates.size() < maxSuggestions) {
            candidates.addAll(trie.searchFuzzy(prefix, MAX_EDIT_DISTANCE));
        }

        // 3. Ranking: Sort by Frequency (Popularity) first, then Edit Distance
        List<Suggestion> results = new ArrayList<>(candidates);
        results.sort((a, b) -> {
            // Primary: Edit Distance (Exact matches first)
            int distCmp = Integer.compare(a.distance, b.distance);
            if (distCmp != 0) return distCmp;
            
            // Secondary: Frequency (Higher is better) using long compare
            int freqCmp = Long.compare(b.frequency, a.frequency); 
            if (freqCmp != 0) return freqCmp;

            // Tertiary: Length (Shorter words often preferred if freq is tied)
            return Integer.compare(a.word.length(), b.word.length());
        });

        if (results.size() > maxSuggestions) {
            results = results.subList(0, maxSuggestions);
        }

        cache.put(prefix, results);
        return results;
    }

    /** Check if a word exists exactly in the dictionary. */
    public static boolean containsExact(String word) {
        if (word == null || word.isEmpty()) return false;
        if (!initialized.get()) return false;
        return trie.contains(word.toLowerCase());
    }

    /**
     * Returns the best single-word correction; if none found, returns the original.
     */
    public static String bestCorrection(String word) {
        if (word == null || word.isEmpty()) return word;
        if (!initialized.get()) return word;
        if (containsExact(word)) return word;
        List<Suggestion> s = suggest(word, 1);
        if (s.isEmpty()) return word;
        return s.get(0).word;
    }

    /**
     * Correct each token in a phrase. Tokens are split on whitespace.
     * Returns the corrected phrase.
     */
    public static String correctPhrase(String phrase) {
        if (phrase == null || phrase.isEmpty()) return phrase;
        String[] parts = phrase.trim().split("\\s+");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = bestCorrection(parts[i]);
        }
        return String.join(" ", parts);
    }

    private static void loadDictionaryAndPopulateTrie() throws Exception {
        // Try multiple locations so it works both in dev and on server
        String[] candidates = {
            "/home/ec2-user/dict2.txt",
            "/home/ec2-user/searchengine/src/cis5550/tools/dict3.csv",
            "/home/ec2-user/searchengine/dict3.csv",
            "/home/ec2-user/dict3.csv",
            "dict3.csv",
            LOCAL_DICT,
            "dict2.txt",
            "dict.txt"
        };
        boolean loaded = false;

        for (String path : candidates) {
            File f = new File(path);
            if (!f.exists()) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(f, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim().toLowerCase();
                    if (line.isEmpty()) continue;

                    // If CSV: word,frequency — otherwise treat whole line as word with freq=1
                    String[] parts = line.split(",");
                    String word = parts[0];
                    long freq = 1L;
                    if (parts.length > 1) {
                        try {
                            freq = Long.parseLong(parts[1].trim());
                        } catch (NumberFormatException ignored) {
                            freq = 1L;
                        }
                    }
                    trie.insert(word, freq);
                }
                loaded = true;
                System.out.println("Loaded dictionary from: " + f.getAbsolutePath());
                break;
            }
        }

        if (!loaded) {
            // Fallback data
            System.out.println("Loaded fallback dictionary");
            String[] common = {"google", "java", "javascript", "json", "server", "socket", "search", "query", "question"};
            for (String s : common) trie.insert(s, 100);
        }
    }

    public static String suggestionsToJson(String word, List<Suggestion> suggestions, double micros) {
        StringBuilder json = new StringBuilder();
        json.append("{\"query\":\"").append(escape(word))
            .append("\",\"time_us\":").append(String.format("%.2f", micros))
            .append(",\"suggestions\":[");
        for (int i = 0; i < suggestions.size(); i++) {
            if (i > 0) json.append(",");
            Suggestion s = suggestions.get(i);
            json.append("{\"word\":\"").append(escape(s.word))
                .append("\",\"freq\":").append(s.frequency)
                .append(",\"dist\":").append(s.distance).append("}");
        }
        json.append("]}");
        return json.toString();
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
    
    public static String decodeWordParam(String raw) {
        if (raw == null) return null;
        try { return URLDecoder.decode(raw, StandardCharsets.UTF_8.name()); } 
        catch (Exception e) { return raw; }
    }

    // --- Data Structures ---

    public static class Suggestion {
        public final String word;
        public final int distance;
        public final long frequency;

        public Suggestion(String word, int distance, long frequency) {
            this.word = word;
            this.distance = distance;
            this.frequency = frequency;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Suggestion that = (Suggestion) o;
            return Objects.equals(word, that.word);
        }

        @Override
        public int hashCode() {
            return Objects.hash(word);
        }
    }

    /**
     * Trie (Prefix Tree) implementation optimized for fuzzy search.
     */
    private static class Trie {
        private final TrieNode root = new TrieNode();

        void insert(String word, long frequency) {
            TrieNode current = root;
            for (char c : word.toCharArray()) {
                current = current.children.computeIfAbsent(c, k -> new TrieNode());
            }
            current.isWord = true;
            current.frequency = frequency;
            current.fullWord = word;
        }

        /** Phase 1: Standard Autocomplete (Exact prefix) */
        List<Suggestion> searchPrefix(String prefix, int limit) {
            TrieNode node = root;
            for (char c : prefix.toCharArray()) {
                node = node.children.get(c);
                if (node == null) return Collections.emptyList();
            }
            // Collect all descendants from this point
            List<Suggestion> results = new ArrayList<>();
            collectDescendants(node, 0, results);
            return results;
        }

        private void collectDescendants(TrieNode node, int distance, List<Suggestion> results) {
            if (node.isWord) {
                results.add(new Suggestion(node.fullWord, distance, node.frequency));
            }
            for (TrieNode child : node.children.values()) {
                collectDescendants(child, distance, results);
            }
        }

        /** Phase 2: Fuzzy Search (Damerau-Levenshtein logic on the Trie) */
        List<Suggestion> searchFuzzy(String word, int maxDist) {
            List<Suggestion> results = new ArrayList<>();
            // Row 0 of Levenshtein matrix: 0, 1, 2, ...
            int[] currentRow = new int[word.length() + 1];
            for (int i = 0; i <= word.length(); i++) currentRow[i] = i;

            for (Map.Entry<Character, TrieNode> entry : root.children.entrySet()) {
                searchRecursive(entry.getValue(), entry.getKey(), word, currentRow, results, maxDist);
            }
            return results;
        }

        private void searchRecursive(TrieNode node, char charOnTrie, String targetWord, 
                                     int[] previousRow, List<Suggestion> results, int maxDist) {
            int columns = targetWord.length() + 1;
            int[] currentRow = new int[columns];
            currentRow[0] = previousRow[0] + 1;

            int minInRow = currentRow[0];

            for (int i = 1; i < columns; i++) {
                int insertCost = currentRow[i - 1] + 1;
                int deleteCost = previousRow[i] + 1;
                int replaceCost = previousRow[i - 1] + (targetWord.charAt(i - 1) == charOnTrie ? 0 : 1);

                currentRow[i] = Math.min(Math.min(insertCost, deleteCost), replaceCost);
                minInRow = Math.min(minInRow, currentRow[i]);
            }

            // If we are within valid edit distance and this node marks a word
            if (currentRow[columns - 1] <= maxDist && node.isWord) {
                results.add(new Suggestion(node.fullWord, currentRow[columns - 1], node.frequency));
            }

            // Pruning: if the best possible match in this branch is worse than maxDist, stop.
            if (minInRow <= maxDist) {
                for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
                    searchRecursive(entry.getValue(), entry.getKey(), targetWord, currentRow, results, maxDist);
                }
            }
        }

        boolean contains(String word) {
            TrieNode node = root;
            for (char c : word.toCharArray()) {
                node = node.children.get(c);
                if (node == null) return false;
            }
            return node.isWord;
        }
    }

    private static class TrieNode {
        final Map<Character, TrieNode> children = new HashMap<>();
        boolean isWord = false;
        long frequency = 0;
        String fullWord = null; // Caching the word at the node avoids backtracking reconstruction
    }
}