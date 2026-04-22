package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDDImpl;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * PageRank Reverse Inlink Algorithm Implementation
 * 
 * This implementation uses a reverse approach compared to traditional PageRank:
 * 
 * KEY DIFFERENCES:
 * 1. Uses INLINKS from anchor text columns instead of parsing page content for outlinks
 *    - Scans "anchors:sourceUrl" columns in pt-crawl to find which pages link TO each page
 *    - No page parsing needed - uses data already extracted during crawl
 * 
 * 2. Single state table with delta instead of previousRank
 *    - Format: "currentRank,delta,inlink1,inlink2,..." instead of "currentRank,previousRank,link1,link2,..."
 *    - Eliminates previousRank field, reducing storage
 *    - Convergence check uses delta directly (abs(delta) <= threshold)
 * 
 * 3. Builds outlink counts by reversing inlink map
 *    - If page B has "anchors:A", then A has outlink to B
 *    - Count outlinks by iterating through all inlinks
 * 
 * 4. Reverse computation direction
 *    - Traditional: For each page, distribute rank to its outlinks
 *    - Reverse: For each page, sum contributions from its inlinks
 *    - More efficient when pages have fewer inlinks than outlinks
 * 
 * Benefits:
 * - No page parsing: Uses anchor columns (already extracted)
 * - Reduced storage: Eliminates previousRank field
 * - Efficient computation: Iterate through inlinks (typically fewer)
 * - Simpler convergence: Direct delta comparison
 */
public class PageRankReverse {
    static Logger log = Logger.getLogger(PageRankReverse.class);
    private static final Double DECAY_FACTOR = 0.85;
    private static Double CONVERGENCE_THRESHOLD = 0.01;
    private static Integer CONVERGENCE_PERCENTAGE = 100;

    /**
     * Suggest garbage collection if memory usage is high.
     * Only triggers GC if memory usage is above 70% to avoid unnecessary overhead.
     */
    private static void suggestGC(String context) {
        Runtime runtime = Runtime.getRuntime();
        long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long maxMB = runtime.maxMemory() / (1024 * 1024);
        double usagePercent = maxMB > 0 ? (100.0 * usedMB / maxMB) : 0.0;
        
        // Only suggest GC if memory usage is above 70%
        if (usagePercent > 70.0) {
            System.gc();
        }
    }

    /**
     * Safely destroy an RDD by deleting its underlying KVS table.
     * Wraps destroy() in try-catch to handle errors gracefully.
     */
    private static void destroyRDD(FlameRDD rdd, String context) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                log.warn("  [GC] Failed to destroy RDD (" + context + "): " + e.getMessage());
            }
        }
    }

    /**
     * Safely destroy a PairRDD by deleting its underlying KVS table.
     * Wraps destroy() in try-catch to handle errors gracefully.
     */
    private static void destroyRDD(FlamePairRDD rdd, String context) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                log.warn("  [GC] Failed to destroy PairRDD (" + context + "): " + e.getMessage());
            }
        }
    }

    public static void run(FlameContext ctx, String[] args) {
        log.info("========================================");
        log.info("Starting PageRank Reverse (Inlink-based) computation");
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
        log.info("Algorithm: Reverse inlink-based (uses anchor columns, no page parsing)");
        log.info("");

        // Track total job start time
        long jobStartTime = System.currentTimeMillis();

        try {
            log.info("========================================");
            log.info("Phase 1: Building inlink map from anchor columns");
            log.info("========================================");
            long phase1StartTime = System.currentTimeMillis();

            // Step 1: Scan pt-crawl for anchor columns to build inlink map
            // For each page, find all pages that link TO it (inlinks)
            log.info("  [Phase1] Scanning anchor columns to build inlink map...");
            FlameRDD inlinkData = ctx.fromTableParallel("pt-crawl", row -> {
                String targetUrl = row.get("url");
                if (targetUrl == null) {
                    return null; // skip rows without URL
                }
                
                try {
                    // Scan all columns starting with "anchors:" to find inlinks
                    List<String> inlinks = new ArrayList<>();
                    for (String col : row.columns()) {
                        if (col.startsWith("anchors:")) {
                            // Extract source URL from column name: "anchors:sourceUrl" -> sourceUrl
                            String sourceUrl = col.substring(8); // Remove "anchors:" prefix
                            if (sourceUrl != null && !sourceUrl.isEmpty()) {
                                // Hash the source URL for consistency
                                String hashedSource = Hasher.hash(sourceUrl);
                                inlinks.add(hashedSource);
                            }
                        }
                    }
                    
                    // Return "targetUrl::inlink1,inlink2,inlink3" (inlinks are hashed)
                    String inlinksStr = inlinks.isEmpty() ? "" : String.join(",", inlinks);
                    return targetUrl + "::" + inlinksStr;
                } catch (Exception e) {
                    log.warn("  [Phase1] Error processing inlinks for URL: " + 
                            (targetUrl.length() > 60 ? targetUrl.substring(0, 60) + "..." : targetUrl) + 
                            " | error: " + e.getMessage() + " - treating as page with no inlinks");
                    return targetUrl + "::"; // Empty inlinks
                }
            });

            // Step 2: Build outlink counts by reversing the inlink map
            // If page B has inlink from A, then A has outlink to B
            log.info("  [Phase1] Building outlink counts (reverse of inlink map)...");
            FlamePairRDD outlinkCounts = inlinkData.flatMapToPairParallel((inlinkStr, workerKvs, batchedKvs) -> {
                try {
                    String[] parts = inlinkStr.split("::", 2);
                    if (parts.length != 2) {
                        return Collections.emptyList();
                    }
                    
                    String inlinksStr = parts[1];
                    
                    List<FlamePair> results = new ArrayList<>();
                    
                    if (inlinksStr != null && !inlinksStr.isEmpty()) {
                        // For each inlink source, increment its outlink count
                        String[] inlinks = inlinksStr.split(",");
                        for (String hashedSource : inlinks) {
                            if (hashedSource != null && !hashedSource.trim().isEmpty()) {
                                // Emit: (sourceUrlHash, "1") to count outlinks
                                results.add(new FlamePair(hashedSource.trim(), "1"));
                            }
                        }
                    }
                    
                    return results;
                } catch (Exception e) {
                    log.warn("  [Phase1] Error building outlink counts: " + e.getMessage());
                    return Collections.emptyList();
                }
            }).foldByKeyParallel("0", (a, b) -> {
                // Aggregate outlink counts
                try {
                    int countA = Integer.parseInt(a);
                    int countB = Integer.parseInt(b);
                    return String.valueOf(countA + countB);
                } catch (NumberFormatException e) {
                    log.warn("  [Phase1] Error parsing outlink count: a=" + a + ", b=" + b);
                    return a; // Return accumulator on error
                }
            });

            // Step 3: Create state table with format "currentRank,delta,inlink1,inlink2,..."
            log.info("  [Phase1] Creating state table (format: currentRank,delta,inlinks)...");
            FlamePairRDD stateTable = inlinkData.mapToPairParallel((inlinkStr, workerKvs, batchedKvs) -> {
                try {
                    String[] parts = inlinkStr.split("::", 2);
                    if (parts.length != 2) {
                        log.warn("  [Phase1] Invalid inlink format: " + 
                                (inlinkStr != null && inlinkStr.length() > 100 ? inlinkStr.substring(0, 100) + "..." : inlinkStr));
                        return null;
                    }
                    
                    String targetUrl = parts[0];
                    String inlinksStr = parts[1]; // Already hashed, comma-separated inlinks (or empty)
                    
                    if (targetUrl == null || targetUrl.isEmpty()) {
                        log.warn("  [Phase1] WARNING: Found null/empty URL in mapToPairParallel");
                        return null;
                    }

                    String hashedTarget = Hasher.hash(targetUrl);
                    
                    // Format: "currentRank,delta,inlink1,inlink2,..."
                    // Initial: rank=1.0, delta=0.0
                    String stateValue;
                    if (inlinksStr == null || inlinksStr.isEmpty()) {
                        stateValue = "1.0,0.0"; // Page with no inlinks (dangling node or isolated)
                    } else {
                        stateValue = "1.0,0.0," + inlinksStr; // Inlinks already hashed and comma-separated
                    }
                    
                    return new FlamePair(hashedTarget, stateValue);
                } catch (Exception e) {
                    log.error("  [Phase1] Error creating state entry: " + e.getMessage());
                    try {
                        String[] errorParts = inlinkStr.split("::", 2);
                        if (errorParts.length > 0) {
                            String hashedUrl = Hasher.hash(errorParts[0]);
                            return new FlamePair(hashedUrl, "1.0,0.0"); // Default: no inlinks
                        }
                    } catch (Exception hashError) {
                        log.error("  [Phase1] Cannot hash URL, skipping: " + hashError.getMessage());
                    }
                    return null;
                }
            });

            long phase1Time = System.currentTimeMillis() - phase1StartTime;
            log.info("Phase 1 complete: State table and outlink counts created");

            log.info("Collecting initial statistics...");
            long statsStartTime = System.currentTimeMillis();
            List<FlamePair> allInitialPairs = stateTable.collect();
            long statsTime = System.currentTimeMillis() - statsStartTime;
            int totalPages = allInitialPairs.size();
            log.info(String.format(
                    "[PAGERANK_PHASE1_COMPLETE] Loaded %d pages | Took %dms (%.1fs) | Stats collection: %dms",
                    totalPages, phase1Time, phase1Time / 1000.0, statsTime));

            // Calculate initial statistics
            int totalInlinks = 0;
            int pagesWithInlinks = 0;
            int pagesWithoutInlinks = 0;
            int maxInlinks = 0;

            for (FlamePair pair : allInitialPairs) {
                String value = pair._2();
                String[] parts = value.split(",");
                if (parts.length > 2) {
                    int inlinkCount = parts.length - 2; // Subtract currentRank and delta
                    totalInlinks += inlinkCount;
                    pagesWithInlinks++;
                    maxInlinks = Math.max(maxInlinks, inlinkCount);
                } else {
                    pagesWithoutInlinks++;
                }
            }

            log.info("Initial PageRank statistics:");
            log.info("  Total pages: " + totalPages);
            log.info("  Total incoming links: " + totalInlinks);
            log.info("  Average inlinks per page: " + String.format("%.2f", totalPages > 0 ? (double) totalInlinks / totalPages : 0.0));
            log.info("  Pages with inlinks: " + pagesWithInlinks + " (" + String.format("%.1f", totalPages > 0 ? 100.0 * pagesWithInlinks / totalPages : 0.0) + "%)");
            log.info("  Pages without inlinks: " + pagesWithoutInlinks + " (" + String.format("%.1f", totalPages > 0 ? 100.0 * pagesWithoutInlinks / totalPages : 0.0) + "%)");
            log.info("  Maximum inlinks to single page: " + maxInlinks);
            log.info("");

            allInitialPairs = null;

            // Clear Phase 1 intermediate data structures
            destroyRDD(inlinkData, "inlinkData (Phase 1)");
            inlinkData = null;
            suggestGC("After Phase 1 completion");

            // Perform PageRank iterations
            log.info("========================================");
            log.info("Phase 2: PageRank Iterations (Reverse Inlink-based)");
            log.info("========================================");
            log.info("Starting PageRank iterations...");
            log.info("  Convergence threshold: " + CONVERGENCE_THRESHOLD);
            log.info("  Convergence percentage: " + CONVERGENCE_PERCENTAGE + "%");
            log.info("  Algorithm: For each page, sum contributions from its inlinks");
            log.info("");

            int iteration = 0;
            int maxIterations = 100; // Safety limit
            long totalIterationTime = 0;

            while (iteration < maxIterations) {
                iteration++;
                log.info("----------------------------------------");
                log.info("Iteration " + iteration + " starting...");
                log.info("----------------------------------------");

                long iterationStartTime = System.currentTimeMillis();
                // Store reference to old stateTable before replacing it
                FlamePairRDD oldStateTable = stateTable;
                stateTable = iteratePageRank(stateTable, outlinkCounts);
                // Destroy old stateTable - it's been replaced by the new one
                destroyRDD(oldStateTable, "oldStateTable (iteration " + iteration + ")");
                oldStateTable = null;
                long iterationTime = System.currentTimeMillis() - iterationStartTime;
                totalIterationTime += iterationTime;

                log.info("Computing convergence statistics...");
                long convergenceStartTime = System.currentTimeMillis();
                
                // Compute convergence stats using delta directly (no previousRank needed!)
                log.info("  [Convergence] Using delta directly (no previousRank comparison needed)");
                FlamePairRDD convergenceStats = ((FlamePairRDDImpl) stateTable).flatMapToPairParallel((pair, workerKvs, batchedKvs) -> {
                    try {
                        String value = pair._2();
                        
                        // Always emit stats, even for invalid pages
                        if (value == null) {
                            return Collections.singletonList(new FlamePair("__stats__", "0,1,999.0,999.0"));
                        }
                        
                        String[] parts = value.split(",");
                        if (parts.length < 2) {
                            // Invalid state - count as not converged
                            return Collections.singletonList(new FlamePair("__stats__", "0,1,999.0,999.0"));
                        }
                        
                        try {
                            double delta = Double.parseDouble(parts[1]);
                            double absDelta = Math.abs(delta);
                            boolean converged = absDelta <= CONVERGENCE_THRESHOLD;
                            
                            // Emit stats: key="__stats__", value="convergedCount,totalCount,maxDelta,sumDelta"
                            String statsValue = (converged ? "1" : "0") + "," + "1," + 
                                               String.valueOf(absDelta) + "," + String.valueOf(absDelta);
                            return Collections.singletonList(new FlamePair("__stats__", statsValue));
                        } catch (NumberFormatException e) {
                            // Unparseable ranks - count as not converged
                            return Collections.singletonList(new FlamePair("__stats__", "0,1,999.0,999.0"));
                        }
                    } catch (Exception e) {
                        // Any error - count as not converged
                        return Collections.singletonList(new FlamePair("__stats__", "0,1,999.0,999.0"));
                    }
                }).foldByKeyParallel("0,0,0.0,0.0", (acc, val) -> {
                    // Aggregate: "convergedCount,totalCount,maxDelta,sumDelta"
                    try {
                        String[] accParts = acc.split(",");
                        String[] valParts = val.split(",");
                        
                        int convergedCount = Integer.parseInt(accParts[0]) + Integer.parseInt(valParts[0]);
                        int totalCount = Integer.parseInt(accParts[1]) + Integer.parseInt(valParts[1]);
                        double maxDelta = Math.max(Double.parseDouble(accParts[2]), Double.parseDouble(valParts[2]));
                        double sumDelta = Double.parseDouble(accParts[3]) + Double.parseDouble(valParts[3]);
                        
                        return convergedCount + "," + totalCount + "," + maxDelta + "," + sumDelta;
                    } catch (Exception e) {
                        log.warn("  [Convergence] Error aggregating stats: " + e.getMessage());
                        return acc;
                    }
                });

                // Collect all stats rows and aggregate them
                List<FlamePair> statsList = convergenceStats.collect();
                
                // Aggregate stats from all partitions
                int convergedPages = 0;
                int totalPagesForConvergence = 0;
                double maxDelta = 0.0;
                double sumDelta = 0.0;
                int statsRowsFound = 0;
                int invalidPagesCount = 0;
                
                for (FlamePair statsPair : statsList) {
                    if (statsPair._1().equals("__stats__")) {
                        statsRowsFound++;
                        try {
                            String statsValue = statsPair._2();
                            String[] statsParts = statsValue.split(",");
                            
                            if (statsParts.length >= 4) {
                                convergedPages += Integer.parseInt(statsParts[0]);
                                totalPagesForConvergence += Integer.parseInt(statsParts[1]);
                                
                                double deltaValue = Double.parseDouble(statsParts[2]);
                                double sumValue = Double.parseDouble(statsParts[3]);
                                
                                // Check if this is an invalid page (999.0 is our fallback marker)
                                if (deltaValue >= 100.0) {
                                    invalidPagesCount += Integer.parseInt(statsParts[1]);
                                } else {
                                    // Valid page - include in maxDelta and sumDelta
                                    maxDelta = Math.max(maxDelta, deltaValue);
                                    sumDelta += sumValue;
                                }
                            }
                        } catch (Exception e) {
                            log.warn("  [Convergence] Error parsing stats from partition: " + e.getMessage());
                        }
                    }
                }
                
                // Log diagnostic info
                if (statsRowsFound == 0) {
                    log.warn("  [Convergence] WARNING: No __stats__ rows found!");
                } else {
                    log.info("  [Convergence] Found " + statsRowsFound + " partition-level stats rows");
                    if (invalidPagesCount > 0) {
                        log.warn("  [Convergence] WARNING: " + invalidPagesCount + " pages have invalid state - counted as not converged");
                    }
                }
                
                double avgDelta = totalPagesForConvergence > 0 ? sumDelta / totalPagesForConvergence : 0.0;
                double convergedPercentage = totalPagesForConvergence > 0 ? (100.0 * convergedPages / totalPagesForConvergence) : 0.0;
                
                long convergenceTime = System.currentTimeMillis() - convergenceStartTime;
                log.info("  [Convergence] Aggregated stats from workers in " + convergenceTime + "ms");
                log.info("  [Convergence] Stats: " + convergedPages + " converged / " + totalPagesForConvergence + " total pages");

                log.info(String.format("[PAGERANK_ITERATION_%d] Completed in %dms (%.1fs) | Convergence check: %dms",
                        iteration, iterationTime, iterationTime / 1000.0, convergenceTime));
                log.info("  Iteration time breakdown:");
                log.info("    - PageRank computation: " + (iterationTime - convergenceTime) + "ms");
                log.info("    - Convergence check: " + convergenceTime + "ms");
                log.info("  Convergence statistics:");
                log.info("    - Converged pages: " + convergedPages + " / " + totalPagesForConvergence + " ("
                        + String.format("%.2f", convergedPercentage) + "%)");
                log.info("    - Max delta: " + String.format("%.6f", maxDelta));
                log.info("    - Avg delta: " + String.format("%.6f", avgDelta));
                log.info("    - Total iteration time so far: " + totalIterationTime + "ms ("
                        + String.format("%.1f", totalIterationTime / 1000.0) + "s)");
                log.info("    - Avg time per iteration: "
                        + String.format("%.1f", totalIterationTime / (double) iteration) + "ms");

                // Clear convergence stats RDD
                destroyRDD(convergenceStats, "convergenceStats");
                convergenceStats = null;

                if (convergedPercentage >= CONVERGENCE_PERCENTAGE) {
                    log.info("");
                    log.info("✓ Convergence achieved! " + String.format("%.2f", convergedPercentage) +
                            "% of pages converged (threshold: " + CONVERGENCE_PERCENTAGE + "%)");
                    break;
                }

                if (iteration >= maxIterations) {
                    log.warn("Maximum iterations (" + maxIterations + ") reached. Stopping.");
                }

                // Suggest GC every 5 iterations
                if (iteration % 5 == 0) {
                    suggestGC("After iteration " + iteration);
                }

                log.info("");
            }

            long phase2EndTime = System.currentTimeMillis();
            long phase2Time = phase2EndTime - phase1StartTime - phase1Time;
            log.info("========================================");
            log.info("Phase 2 complete: PageRank iterations finished");
            log.info(String.format("[PAGERANK_PHASE2_COMPLETE] Total iterations: %d | Iteration time: %dms (%.1fs) | " +
                    "Phase 2 total time: %dms (%.1fs) | Avg per iteration: %.1fms | Total job time so far: %dms (%.1fs)",
                    iteration, totalIterationTime, totalIterationTime / 1000.0,
                    phase2Time, phase2Time / 1000.0,
                    totalIterationTime / (double) iteration,
                    phase2EndTime - jobStartTime,
                    (phase2EndTime - jobStartTime) / 1000.0));
            log.info("========================================");
            log.info("");

            // Clear Phase 2 data before Phase 3
            suggestGC("After Phase 2 completion (before Phase 3)");

            log.info("========================================");
            log.info("Phase 3: Saving PageRank results");
            log.info("========================================");
            long phase3StartTime = System.currentTimeMillis();
            log.info("Saving PageRank results to pt-pageranks table...");
            saveTable(stateTable, ctx);
            long phase3Time = System.currentTimeMillis() - phase3StartTime;
            log.info(String.format("[PAGERANK_PHASE3_COMPLETE] Save completed in %dms (%.1fs)",
                    phase3Time, phase3Time / 1000.0));
            log.info("✓ PageRank results saved successfully");
            log.info("========================================");
            log.info("");

            // Final summary
            long totalTime = System.currentTimeMillis() - jobStartTime;
            log.info("========================================");
            log.info("PageRank Reverse Computation Summary");
            log.info("========================================");
            log.info(String.format("Total time: %dms (%.1fs)", totalTime, totalTime / 1000.0));
            log.info(String.format("  Phase 1 (Build inlink map): %dms (%.1fs) - %.1f%%",
                    phase1Time, phase1Time / 1000.0, 100.0 * phase1Time / totalTime));
            log.info(String.format("  Phase 2 (Iterations): %dms (%.1fs) - %.1f%%",
                    totalIterationTime, totalIterationTime / 1000.0, 100.0 * totalIterationTime / totalTime));
            log.info(String.format("  Phase 3 (Save results): %dms (%.1fs) - %.1f%%",
                    phase3Time, phase3Time / 1000.0, 100.0 * phase3Time / totalTime));
            log.info("  Total iterations: " + iteration);
            log.info("========================================");

            // Clear stateTable - no longer needed after saving
            destroyRDD(stateTable, "stateTable (final cleanup)");
            destroyRDD(outlinkCounts, "outlinkCounts (final cleanup)");
            stateTable = null;
            outlinkCounts = null;
            suggestGC("After job completion (final cleanup)");

            ctx.output("Completed PageRank Reverse successfully in " + iteration + " iterations. Total time: " +
                    String.format("%.1f", totalTime / 1000.0) + "s");

        } catch (Exception e) {
            log.error("Exception in PageRank Reverse computation: ", e);
            ctx.output("PageRank Reverse failed: " + e.getMessage());
        }
    }

    public static void saveTable(FlamePairRDD stateTable, FlameContext ctx) throws Exception {
        log.info("Collecting PageRank results for saving...");
        long collectStartTime = System.currentTimeMillis();
        List<FlamePair> allPairs = stateTable.collect();
        long collectTime = System.currentTimeMillis() - collectStartTime;
        log.info("Collected " + allPairs.size() + " PageRank entries in " + collectTime + "ms");

        int savedCount = 0;
        int errorCount = 0;
        double minRank = Double.MAX_VALUE;
        double maxRank = Double.MIN_VALUE;
        double totalRank = 0.0;

        log.info("Saving PageRank values to pt-pageranks table...");
        long saveStartTime = System.currentTimeMillis();

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
                log.error("  [Save] Error saving rank for URL hash " + urlHash + ": " + e.getMessage());
                errorCount++;
            }

            // Log progress every 1000 entries
            if ((savedCount + errorCount) % 1000 == 0) {
                log.info("  [Save] Progress: " + (savedCount + errorCount) + " / " + allPairs.size() +
                        " entries processed ("
                        + String.format("%.1f", 100.0 * (savedCount + errorCount) / allPairs.size()) + "%)");
            }
        }

        long saveTime = System.currentTimeMillis() - saveStartTime;

        log.info("Save operation complete:");
        log.info("  Saved: " + savedCount + " PageRank values");
        log.info("  Errors: " + errorCount);
        log.info("  Save time: " + saveTime + "ms (" + String.format("%.1f", saveTime / 1000.0) + "s)");
        log.info("  Avg time per entry: " + String.format("%.2f", savedCount > 0 ? saveTime / (double) savedCount : 0.0)
                + "ms");

        if (errorCount > 0) {
            log.warn("  Encountered " + errorCount + " errors while saving");
        }
        if (savedCount > 0) {
            log.info("PageRank value statistics:");
            log.info("  Min rank: " + String.format("%.6f", minRank));
            log.info("  Max rank: " + String.format("%.6f", maxRank));
            log.info("  Avg rank: " + String.format("%.6f", totalRank / savedCount));
            log.info("  Rank range: " + String.format("%.6f", maxRank - minRank));
        }
    }

    /**
     * Iterate PageRank using reverse inlink approach.
     * For each page, sum contributions from its inlinks.
     * 
     * @param stateTable State table with format "currentRank,delta,inlink1,inlink2,..."
     * @param outlinkCounts Table mapping page hash -> outlink count (for computing transfers)
     * @return Updated state table
     */
    public static FlamePairRDD iteratePageRank(FlamePairRDD stateTable, FlamePairRDD outlinkCounts) throws Exception {
        try {
            // Get table names for direct KVS lookup
            String stateTableName = ((FlamePairRDDImpl) stateTable).getTableName();
            String outlinkTableName = ((FlamePairRDDImpl) outlinkCounts).getTableName();
            
            log.info("    [Iteration] Step 1: Computing transfers using inlinks (reverse direction)...");
            long step1Start = System.currentTimeMillis();
            
            // For each page, compute new rank by summing contributions from its inlinks
            FlamePairRDD updatedState = ((FlamePairRDDImpl) stateTable).flatMapToPairParallel((pair, workerKvs, batchedKvs) -> {
                String urlHash = pair._1();
                String stateValue = pair._2(); // "currentRank,delta,inlink1,inlink2,..."
                
                try {
                    // Parse state
                    String[] parts = stateValue.split(",");
                    if (parts.length < 2) {
                        log.warn("  [Iteration] Invalid state for URL hash " + urlHash + ": insufficient parts");
                        double fallbackRank = (1.0 - DECAY_FACTOR);
                        return Collections.singletonList(new FlamePair(urlHash, 
                            String.valueOf(fallbackRank) + ",0.0"));
                    }
                    
                    double currentRank = Double.parseDouble(parts[0]);
                    
                    // Sum contributions from inlinks
                    double sumTransfers = 0.0;
                    if (parts.length > 2) {
                        // Has inlinks - compute contributions from each
                        for (int i = 2; i < parts.length; i++) {
                            String inlinkHash = parts[i].trim();
                            if (inlinkHash.isEmpty()) {
                                continue;
                            }
                            
                            try {
                                // Get inlink source's current rank
                                byte[] sourceRankBytes = workerKvs.get(stateTableName, inlinkHash, "value");
                                if (sourceRankBytes == null) {
                                    continue; // Source not found, skip
                                }
                                
                                String sourceState = new String(sourceRankBytes);
                                String[] sourceParts = sourceState.split(",");
                                if (sourceParts.length < 1) {
                                    continue;
                                }
                                
                                double sourceRank = Double.parseDouble(sourceParts[0]);
                                
                                // Get inlink source's outlink count
                                byte[] outlinkCountBytes = workerKvs.get(outlinkTableName, inlinkHash, "value");
                                int outlinkCount = 1; // Default to 1 if not found (avoid division by zero)
                                
                                if (outlinkCountBytes != null) {
                                    try {
                                        outlinkCount = Integer.parseInt(new String(outlinkCountBytes));
                                        if (outlinkCount == 0) {
                                            outlinkCount = 1; // Avoid division by zero
                                        }
                                    } catch (NumberFormatException e) {
                                        // Use default
                                    }
                                }
                                
                                // Compute transfer: sourceRank / outlinkCount * DECAY_FACTOR
                                double transfer = (sourceRank / outlinkCount) * DECAY_FACTOR;
                                sumTransfers += transfer;
                            } catch (Exception e) {
                                log.warn("  [Iteration] Error processing inlink " + inlinkHash + " for " + urlHash + ": " + e.getMessage());
                                // Continue with other inlinks
                            }
                        }
                    }
                    // Pages with no inlinks get only the teleportation term
                    
                    // Compute new rank
                    double newRank = sumTransfers + (1.0 - DECAY_FACTOR);
                    
                    // Compute delta
                    double newDelta = newRank - currentRank;
                    
                    // Update state: "newRank,newDelta,inlink1,inlink2,..."
                    parts[0] = String.valueOf(newRank);
                    parts[1] = String.valueOf(newDelta);
                    
                    return Collections.singletonList(new FlamePair(urlHash, String.join(",", parts)));
                } catch (NumberFormatException e) {
                    log.error("  [Iteration] Error parsing values for URL hash " + urlHash + ": " + e.getMessage());
                    double fallbackRank = (1.0 - DECAY_FACTOR);
                    return Collections.singletonList(new FlamePair(urlHash, 
                        String.valueOf(fallbackRank) + ",0.0"));
                } catch (Exception e) {
                    log.error("  [Iteration] Unexpected error processing URL hash " + urlHash + ": " + e.getMessage());
                    double fallbackRank = (1.0 - DECAY_FACTOR);
                    return Collections.singletonList(new FlamePair(urlHash, 
                        String.valueOf(fallbackRank) + ",0.0"));
                }
            });
            
            long step1Time = System.currentTimeMillis() - step1Start;
            log.info("    [Iteration] Step 1 complete: Transfers computed in " + step1Time + "ms");
            log.info("    [Iteration] Total iteration time: " + step1Time + "ms");

            return updatedState;
        } catch (Exception e) {
            log.error("Exception in PageRank iteration: ", e);
            throw e;
        }
    }
}

