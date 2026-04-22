package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.tools.Logger;
import cis5550.tools.PorterStemmer;

import java.util.*;

public class IndexerRef {
    static Logger log = Logger.getLogger(Indexer.class);
    public static void run(FlameContext ctx, String[] args) {
        FlamePairRDD invertedIndex;
        try {
            FlamePairRDD urlPageRDD = ctx.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null) return null; // skip incomplete rows
                return url + "," + page;
            }).filter(s -> s != null)
            .mapToPair(s -> {
                int idx = s.indexOf(',');
                String url = s.substring(0, idx);
                String page = s.substring(idx + 1);
                return new FlamePair(url, page);
            });

            FlamePairRDD wordUrlPairs = urlPageRDD.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();

                // Remove HTML tags, punctuation, control chars and convert to lower case
                String text = page.replaceAll("<[^>]+>", " ");
                text = text.replaceAll("[\\p{Punct}\\r\\n\\t]", " ");
                text = text.toLowerCase();

                // Split into words & remove duplicates
                String[] words = text.split("\\s+");
                Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));

                List<FlamePair> results = new ArrayList<>();
                for (String w : uniqueWords) {
                    if (!w.isEmpty()) {
                        // EC3
                        PorterStemmer stemmer = new PorterStemmer();
                        stemmer.add(w.toCharArray(), w.length());
                        stemmer.stem();
                        String stemmedWord = stemmer.toString();
                        if (Objects.equals(w, stemmedWord)) {
                            results.add(new FlamePair(w, url));
                        } else {
                            results.add(new FlamePair(stemmedWord, url));
                            results.add(new FlamePair(w, url));
                        }
                    }
                }
                return results;
            });

            invertedIndex = wordUrlPairs.foldByKey("", (a, b) -> {
                if (a.isEmpty()) return b;
                if (Arrays.asList(a.split(",")).contains(b)) return a; // avoid duplicates
                return a + "," + b;
            });

            invertedIndex.saveAsTable("pt-index");
            ctx.output("Inverted index built successfully.");
        } catch (Exception e) {
            log.error("Exception in Indexer: ", e);
            ctx.output("Indexer failed: " + e.getMessage());
        }
    }
}
