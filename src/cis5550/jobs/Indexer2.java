package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.tools.Logger;
import cis5550.tools.PorterStemmer;

import java.util.*;

public class Indexer2 {
    private static final Logger logger = Logger.getLogger(Indexer2.class);

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
                return url + "\t" + page;
            }).filter(Objects::nonNull)
                    .mapToPair(s -> {
                        int idx = s.indexOf('\t');
                        String url = s.substring(0, idx);
                        String page = s.substring(idx + 1);
                        return new FlamePair(url, page);
                    });
            logger.info("Loaded data from pt-crawl and converted to pairs");

            logger.info("Flat mapping data to word-url pairs (this may take time for large datasets)");
            FlamePairRDD wordUrlPairs = urlPageRDD.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();

                // Remove HTML tags, punctuation, control chars and convert to lower case
                String text = page.replaceAll("<[^>]+>", " ");
                text = text.replaceAll("[\\p{Punct}\\r\\n\\t]", " ");
                text = text.toLowerCase();

                // Split into words
                String[] words = text.split("\\s+");
                // Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));

                List<FlamePair> results = new ArrayList<>();
                int position = 1;
                Map<String, TermInfo> termInfoMap = new HashMap<>();
                for (String w : words) {
                    if (!w.isEmpty()) {
                        termInfoMap.computeIfAbsent(w, k -> new TermInfo()).positions.add(String.valueOf(position));
                        termInfoMap.get(w).totalFreq++;
                        position++;
                    }
                }
                int docLength = position - 1;
                if (docLength == 0) {
                    // Empty document, skip
                    return results;
                }

                // Process each term and create index entries
                for (Map.Entry<String, TermInfo> entry : termInfoMap.entrySet()) {
                    String w = entry.getKey();
                    TermInfo info = entry.getValue();

                    PorterStemmer stemmer = new PorterStemmer();
                    stemmer.add(w.toCharArray(), w.length());
                    stemmer.stem();
                    String stemmedWord = stemmer.toString();

                    double tf = 0.0;
                    if (docLength > 0) {
                        tf = (double) info.totalFreq / (double) docLength;
                    }
                    // logger.info("TermFreq for word " + w + " in URL " + url + ": " + tf);

                    // format: url|tf|positions, e.g., http://example.com|0.05|3 15 27
                    StringBuilder valueBuilder = new StringBuilder();
                    valueBuilder.append(url).append("|");
                    // Fixed: use double division to get proper TF value
                    valueBuilder.append(tf).append("|");
                    valueBuilder.append(String.join(" ", info.positions));

                    if (Objects.equals(w, stemmedWord)) {
                        results.add(new FlamePair(w, valueBuilder.toString()));
                    } else {
                        results.add(new FlamePair(stemmedWord, valueBuilder.toString()));
                        results.add(new FlamePair(w, valueBuilder.toString()));
                    }
                }
                return results;
            });

            logger.info("Starting foldByKey operation - aggregating word-document pairs");
            invertedIndex = wordUrlPairs.foldByKey("", (a, b) -> {
                if (a.isEmpty())
                    return b;
                // if (Arrays.asList(a.split(",")).contains(b)) return a; // avoid duplicates
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

    public static class TermInfo {
        public int totalFreq = 0;
        public int titleFreq = 0;
        public int headerFreq = 0;
        public int bodyFreq = 0;
        public List<String> positions = new ArrayList<>();
    }
}
