package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PageRank {
    static Logger log = Logger.getLogger(PageRank.class);
    private static final Double DECAY_FACTOR = 0.85;
    private static Double CONVERGENCE_THRESHOLD = 0.01;
    private static Integer CONVERGENCE_PERCENTAGE = 100;

    public static void run(FlameContext ctx, String[] args) {
        log.info("========================================");
        log.info("Starting PageRank computation");
        log.info("========================================");

        if (args.length < 1) {
            log.error("Error: Expected convergence threshold as first argument");
            ctx.output("Error: Expected convergence threshold");
            return;
        }

        try {
            CONVERGENCE_THRESHOLD = Double.parseDouble(args[0]);
            log.info("Convergence threshold: " + CONVERGENCE_THRESHOLD);
        } catch (NumberFormatException e) {
            log.error("Invalid convergence threshold: " + args[0]);
            ctx.output("Error: Invalid convergence threshold");
            return;
        }

        if (args.length > 1) {
            try {
                Integer parsedPercentage = Integer.parseInt(args[1]);
                if (parsedPercentage < 0 || parsedPercentage > 100) {
                    log.error("Convergence percentage must be between 0 and 100, got: " + parsedPercentage);
                    ctx.output("Convergence percentage must be a valid value (0-100)");
                    return;
                }
                CONVERGENCE_PERCENTAGE = parsedPercentage;
                log.info("Convergence percentage: " + CONVERGENCE_PERCENTAGE + "%");
            } catch (NumberFormatException e) {
                log.error("Invalid convergence percentage: " + args[1]);
                ctx.output("Error: Invalid convergence percentage");
                return;
            }
        } else {
            log.info("Convergence percentage: " + CONVERGENCE_PERCENTAGE + "% (default)");
        }

        log.info("Decay factor: " + DECAY_FACTOR);
        log.info("");

        try {
            log.info("Loading pages from pt-crawl table...");
            FlamePairRDD stateTable = ctx.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null)
                    return null; // skip incomplete rows
                return url + "," + page;
            }).filter(s -> s != null)
                    .mapToPair(s -> {
                        int idx = s.indexOf(',');
                        String url = s.substring(0, idx);
                        String page = s.substring(idx + 1);
                        // extractUrls already returns normalized URLs, so no need to normalize again
                        List<String> urlLinks = Crawler.extractUrls(page, url).stream()
                                .map(Hasher::hash)
                                .toList();
                        return new FlamePair(Hasher.hash(url), "1.0,1.0," + String.join(",", urlLinks));
                    });

            // Get initial statistics
            List<FlamePair> allInitialPairs = stateTable.collect();
            int totalPages = allInitialPairs.size();
            log.info("Loaded " + totalPages + " pages from pt-crawl");

            // Count total links
            long totalLinks = 0;
            for (FlamePair pair : allInitialPairs) {
                String value = pair._2();
                String[] parts = value.split(",");
                int numLinks = Math.max(0, parts.length - 2);
                totalLinks += numLinks;
            }

            log.info("Total outgoing links: " + totalLinks);
            log.info("Average links per page: "
                    + (totalPages > 0 ? String.format("%.2f", (double) totalLinks / totalPages) : "0.00"));
            log.info("");

            // Perform PageRank iterations
            log.info("Starting PageRank iterations...");
            int iteration = 0;
            int maxIterations = 100; // Safety limit
            while (iteration < maxIterations) {
                iteration++;
                log.info("Iteration " + iteration + " starting...");

                long startTime = System.currentTimeMillis();
                stateTable = iteratePageRank(stateTable);
                long iterationTime = System.currentTimeMillis() - startTime;

                List<String> allChanges = stateTable.flatMap(pair -> {
                    String value = pair._2();
                    String[] parts = value.split(",");
                    if (parts.length < 2) {
                        return Collections.singletonList("0.0");
                    }
                    double absDiff = Math.abs(Double.parseDouble(parts[0]) - Double.parseDouble(parts[1]));
                    return Collections.singletonList(String.valueOf(absDiff));
                }).collect();

                int convergedPages = 0;
                double maxChange = 0.0;
                double avgChange = 0.0;

                for (String changeStr : allChanges) {
                    try {
                        double change = Double.parseDouble(changeStr);
                        if (change <= CONVERGENCE_THRESHOLD) {
                            convergedPages++;
                        }
                        maxChange = Math.max(maxChange, change);
                        avgChange += change;
                    } catch (NumberFormatException e) {
                        log.warn("Invalid change value: " + changeStr);
                    }
                }

                avgChange = allChanges.size() > 0 ? avgChange / allChanges.size() : 0.0;
                double convergedPercentage = (allChanges.size() > 0) ? (100.0 * convergedPages / allChanges.size())
                        : 0.0;

                log.info("Iteration " + iteration + " completed in " + iterationTime + "ms");
                log.info("  Converged pages: " + convergedPages + " / " + allChanges.size() + " ("
                        + String.format("%.2f", convergedPercentage) + "%)");
                log.info("  Max change: " + String.format("%.6f", maxChange));
                log.info("  Avg change: " + String.format("%.6f", avgChange));

                if (convergedPercentage >= CONVERGENCE_PERCENTAGE) {
                    log.info("");
                    log.info("✓ Convergence achieved! " + convergedPercentage + "% of pages converged (threshold: "
                            + CONVERGENCE_PERCENTAGE + "%)");
                    break;
                }

                if (iteration >= maxIterations) {
                    log.warn("Maximum iterations (" + maxIterations + ") reached. Stopping.");
                }

                log.info("");
            }

            log.info("PageRank computation completed in " + iteration + " iteration(s)");
            log.info("");

            log.info("Saving PageRank results to pt-pageranks table...");
            saveTable(stateTable, ctx);
            log.info("✓ PageRank results saved successfully");
            log.info("========================================");

            ctx.output("Completed PageRank successfully in " + iteration + " iterations.");

        } catch (Exception e) {
            log.error("Exception in PageRank computation: ", e);
            ctx.output("PageRank failed: " + e.getMessage());
        }
    }

    public static void saveTable(FlamePairRDD stateTable, FlameContext ctx) throws Exception {
        List<FlamePair> allPairs = stateTable.collect();
        int savedCount = 0;
        int errorCount = 0;
        double minRank = Double.MAX_VALUE;
        double maxRank = Double.MIN_VALUE;
        double totalRank = 0.0;

        for (FlamePair pair : allPairs) {
            String urlHash = pair._1();
            String value = pair._2();

            try {
                String[] parts = value.split(",");
                if (parts.length < 1) {
                    log.warn("Skipping invalid entry for URL hash: " + urlHash + " (no rank value)");
                    errorCount++;
                    continue;
                }

                double finalRank = Double.parseDouble(parts[0]);
                ctx.getKVS().put("pt-pageranks", urlHash, "rank", String.valueOf(finalRank));
                savedCount++;

                minRank = Math.min(minRank, finalRank);
                maxRank = Math.max(maxRank, finalRank);
                totalRank += finalRank;
            } catch (NumberFormatException e) {
                log.error("Error parsing rank for URL hash " + urlHash + ": " + e.getMessage());
                errorCount++;
            } catch (Exception e) {
                log.error("Error saving rank for URL hash " + urlHash + ": " + e.getMessage());
                errorCount++;
            }
        }

        log.info("Saved " + savedCount + " PageRank values");
        if (errorCount > 0) {
            log.warn("Encountered " + errorCount + " errors while saving");
        }
        if (savedCount > 0) {
            log.info("PageRank statistics:");
            log.info("  Min rank: " + String.format("%.6f", minRank));
            log.info("  Max rank: " + String.format("%.6f", maxRank));
            log.info("  Avg rank: " + String.format("%.6f", totalRank / savedCount));
        }
    }

    public static FlamePairRDD iteratePageRank(FlamePairRDD stateTable) throws Exception {
        try {
            log.debug("Computing transfer table...");
            FlamePairRDD transferTable = computeTransferTable(stateTable);

            log.debug("Aggregating transfer table...");
            FlamePairRDD transferTableAggregate = transferTable.foldByKey("0.0", (a, b) -> {
                try {
                    double sum = Double.parseDouble(a) + Double.parseDouble(b);
                    return String.valueOf(sum);
                } catch (NumberFormatException e) {
                    log.warn("Error parsing values in foldByKey: a=" + a + ", b=" + b);
                    return "0.0";
                }
            });

            log.debug("Joining state table with transfer table...");
            FlamePairRDD joined = stateTable.join(transferTableAggregate);

            log.debug("Updating PageRank values...");
            FlamePairRDD updatedState = joined.flatMapToPair(pair -> {
                String url = pair._1();
                String combined = pair._2();

                try {
                    List<String> parts = new ArrayList<>(Arrays.asList(combined.split(",")));
                    if (parts.size() < 2) {
                        log.warn("Invalid state for URL hash " + url + ": insufficient parts");
                        return Collections.emptyList();
                    }

                    // The last part is the aggregated transfer value
                    double aggregatedTransfer = Double.parseDouble(parts.get(parts.size() - 1));
                    double newRank = aggregatedTransfer + (1.0 - DECAY_FACTOR);

                    // Update: oldCurrentRank becomes newPreviousRank
                    parts.set(1, parts.get(0));
                    // Update: newRank becomes newCurrentRank
                    parts.set(0, String.valueOf(newRank));
                    // Remove the aggregated transfer value from the end
                    parts.remove(parts.size() - 1);

                    return Collections.singletonList(new FlamePair(url, String.join(",", parts)));
                } catch (NumberFormatException e) {
                    log.error("Error parsing values for URL hash " + url + ": " + e.getMessage());
                    return Collections.emptyList();
                } catch (Exception e) {
                    log.error("Unexpected error processing URL hash " + url + ": " + e.getMessage());
                    return Collections.emptyList();
                }
            });

            return updatedState;
        } catch (Exception e) {
            log.error("Exception in PageRank iteration: ", e);
            throw e;
        }
    }

    public static FlamePairRDD computeTransferTable(FlamePairRDD stateTable) throws Exception {
        return stateTable.flatMapToPair(pair -> {
            String urlHash = pair._1();
            String value = pair._2();
            List<FlamePair> results = new ArrayList<>();

            try {
                String[] parts = value.split(",");
                if (parts.length < 2) {
                    log.debug("Skipping URL hash " + urlHash + ": insufficient state data");
                    return results;
                }

                double rankCurrent = Double.parseDouble(parts[0]);
                int numLinks = parts.length - 2; // everything after currentRank,previousRank

                if (numLinks > 0) {
                    // Distribute PageRank to outgoing links
                    double share = DECAY_FACTOR * rankCurrent / numLinks;
                    for (int i = 2; i < parts.length; i++) {
                        String linkHash = parts[i].trim();
                        if (!linkHash.isEmpty()) {
                            results.add(new FlamePair(linkHash, String.valueOf(share)));
                        }
                    }
                } else {
                    // Page with no outgoing links - distribute to all pages (dangling node fix)
                    // This is handled by the (1 - DECAY_FACTOR) term in iteratePageRank
                    log.debug("URL hash " + urlHash + " has no outgoing links (dangling node)");
                }

                // Note: We don't add a self-transfer of 0.0 anymore
                // The join will handle pages that receive no transfers by defaulting to 0.0

            } catch (NumberFormatException e) {
                log.error("Failed to parse rank for URL hash " + urlHash + ": " + e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error processing URL hash " + urlHash + ": " + e.getMessage());
            }

            return results;
        });
    }
}
