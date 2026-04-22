// package cis5550.jobs;

// import cis5550.flame.FlameContext;
// import cis5550.flame.FlameRDD;
// import cis5550.kvs.Row;
// import cis5550.tools.Hasher;
// import cis5550.tools.Logger;
// import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
// import org.jsoup.nodes.Element;
// import org.jsoup.select.Elements;

// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import java.util.Map;

// /**
//  * PageRankSinglepass - Optimized two-step PageRank computation
//  * 
//  * STEP 1: Build state table in fromTableParallel (page never leaves worker)
//  *   - Format: "#outlinks,curr_score,prev_score,inlink1,inlink2,..."
//  * 
//  * STEP 2: Iterate until convergence (repeated)
//  *   - Batch read inlinks from state table
//  *   - Calculate new score: (1-d) + d * sum(inlink_score / inlink_outlinks)
//  *   - Write updated state to KVS
//  *   - Emit convergence status
//  */
// public class PageRankSinglepass {
//     static Logger log = Logger.getLogger(PageRankSinglepass.class);
//     private static final Double DECAY_FACTOR = 0.85;
//     private static Double CONVERGENCE_THRESHOLD = 0.01;
//     private static Integer CONVERGENCE_PERCENTAGE = 100;

//     private static void suggestGC() {
//         Runtime runtime = Runtime.getRuntime();
//         long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
//         long maxMB = runtime.maxMemory() / (1024 * 1024);
//         double usagePercent = maxMB > 0 ? (100.0 * usedMB / maxMB) : 0.0;
//         if (usagePercent > 70.0) {
//             System.gc();
//         }
//     }

//     private static final String STATE_TABLE = "pt-pagerank-state";

//     private static String unescapeHtml4(String input) {
//         if (input == null) return null;
//         return input.replace("&#39;", "'")
//                 .replace("&quot;", "\"")
//                 .replace("&gt;", ">")
//                 .replace("&lt;", "<")
//                 .replace("&amp;", "&");
//     }

//     public static void run(FlameContext ctx, String[] args) {
//         log.info("========================================");
//         log.info("Starting PageRankSinglepass computation");
//         log.info("========================================");

//         if (args.length < 1) {
//             ctx.output("Error: Expected convergence threshold as first argument");
//             return;
//         }

//         try {
//             CONVERGENCE_THRESHOLD = Double.parseDouble(args[0]);
//             log.info("Convergence threshold: " + CONVERGENCE_THRESHOLD);
//         } catch (NumberFormatException e) {
//             ctx.output("Error: Invalid convergence threshold");
//             return;
//         }

//         if (args.length > 1) {
//             try {
//                 Integer parsedPercentage = Integer.parseInt(args[1]);
//                 if (parsedPercentage < 0 || parsedPercentage > 100) {
//                     ctx.output("Convergence percentage must be a valid value (0-100)");
//                     return;
//                 }
//                 CONVERGENCE_PERCENTAGE = parsedPercentage;
//             } catch (NumberFormatException e) {
//                 ctx.output("Error: Invalid convergence percentage");
//                 return;
//             }
//         }

//         log.info("Convergence percentage: " + CONVERGENCE_PERCENTAGE + "%");
//         log.info("Decay factor: " + DECAY_FACTOR);

//         long jobStartTime = System.currentTimeMillis();

//         try {
//             // ========================================
//             // STEP 1: Build state table (single pass)
//             // Page content processed locally, never sent through RDD
//             // ========================================
//             log.info("========================================");
//             log.info("STEP 1: Building state table");
//             log.info("========================================");
//             long step1Start = System.currentTimeMillis();

//             int totalPages = buildStateTable(ctx);

//             long step1Time = System.currentTimeMillis() - step1Start;
//             log.info("Step 1 complete in " + step1Time + "ms");
//             log.info("State table: " + STATE_TABLE);
//             log.info("Total pages: " + totalPages);
//             suggestGC();

//             // ========================================
//             // STEP 2: Iterate until convergence
//             // ========================================
//             log.info("========================================");
//             log.info("STEP 2: PageRank Iterations");
//             log.info("========================================");

//             int iteration = 0;
//             int maxIterations = 100;
//             long totalIterationTime = 0;

//             while (iteration < maxIterations) {
//                 iteration++;
//                 log.info("--- Iteration " + iteration + " ---");
//                 long iterStart = System.currentTimeMillis();




                
//                 FlameRDD convergenceFlags = ctx.fromTableParallel(STATE_TABLE, row -> {
//                     return row.key(); // Just return the key to process
//                 }).flatMapParallel((urlHash, workerKvs, batchedKvs) -> {
//                     try {
//                         // Read current state from KVS
//                         Row myRow = workerKvs.getRow(STATE_TABLE, urlHash);
//                         if (myRow == null) {
//                             return Collections.singletonList("0");
//                         }
//                         String stateValue = myRow.get("value");
//                         if (stateValue == null) {
//                             return Collections.singletonList("0");
//                         }

//                         String[] parts = stateValue.split(",");
//                         if (parts.length < 3) {
//                             return Collections.singletonList("0");
//                         }

//                         int numOutlinks = Integer.parseInt(parts[0]);
//                         double currScore = Double.parseDouble(parts[1]);

//                         // Extract inlink hashes
//                         List<String> inlinkHashes = new ArrayList<>();
//                         for (int i = 3; i < parts.length; i++) {
//                             String h = parts[i].trim();
//                             if (!h.isEmpty()) inlinkHashes.add(h);
//                         }

//                         // Batch read inlinks, compute new score
//                         double sumContributions = 0.0;
//                         if (!inlinkHashes.isEmpty()) {
//                             Map<String, Row> inlinkRows = workerKvs.bulkGetRows(STATE_TABLE, inlinkHashes);
//                             for (String inlinkHash : inlinkHashes) {
//                                 Row inlinkRow = inlinkRows.get(inlinkHash);
//                                 if (inlinkRow != null) {
//                                     String inlinkState = inlinkRow.get("value");
//                                     if (inlinkState != null) {
//                                         String[] inlinkParts = inlinkState.split(",");
//                                         if (inlinkParts.length >= 2) {
//                                             int inlinkOutlinks = Integer.parseInt(inlinkParts[0]);
//                                             double inlinkScore = Double.parseDouble(inlinkParts[1]);
//                                             if (inlinkOutlinks > 0) {
//                                                 sumContributions += DECAY_FACTOR * inlinkScore / inlinkOutlinks;
//                                             }
//                                         }
//                                     }
//                                 }
//                             }
//                         }

//                         double newScore = (1.0 - DECAY_FACTOR) + sumContributions;

//                         // Build new state and write to KVS in place
//                         StringBuilder newState = new StringBuilder();
//                         newState.append(numOutlinks).append(",").append(newScore).append(",").append(currScore);
//                         for (String inlink : inlinkHashes) {
//                             newState.append(",").append(inlink);
//                         }
//                         batchedKvs.put(STATE_TABLE, urlHash, "value", newState.toString());

//                         // Return 1 if converged, 0 if not
//                         boolean converged = Math.abs(newScore - currScore) <= CONVERGENCE_THRESHOLD;
//                         return Collections.singletonList(converged ? "1" : "0");

//                     } catch (Exception e) {
//                         return Collections.singletonList("0");
//                     }
//                 });

//                 // Collect and count convergence
//                 List<String> flags = convergenceFlags.collect();
//                 int convergedPages = 0;
//                 for (String flag : flags) {
//                     if ("1".equals(flag)) convergedPages++;
//                 }
//                 int totalCount = flags.size();

//                 long iterTime = System.currentTimeMillis() - iterStart;
//                 totalIterationTime += iterTime;

//                 double convergedPercent = totalCount > 0 ? (100.0 * convergedPages / totalCount) : 0.0;
//                 log.info(String.format("  Iteration %d: %d/%d converged (%.2f%%) in %dms",
//                         iteration, convergedPages, totalCount, convergedPercent, iterTime));

//                 if (convergedPercent >= CONVERGENCE_PERCENTAGE) {
//                     log.info("✓ Convergence achieved!");
//                     break;
//                 }

//                 if (iteration % 5 == 0) suggestGC();
//             }

//             // ========================================
//             // STEP 3: Save results
//             // ========================================
//             log.info("========================================");
//             log.info("STEP 3: Saving results");
//             log.info("========================================");
//             long step3Start = System.currentTimeMillis();
            
//             long step3Time = System.currentTimeMillis() - step3Start;

//             long totalTime = System.currentTimeMillis() - jobStartTime;
//             log.info("========================================");
//             log.info("Summary: " + iteration + " iterations in " + totalTime + "ms");
//             log.info("  Step 1 (build): " + step1Time + "ms");
//             log.info("  Step 2 (iterate): " + totalIterationTime + "ms");
//             log.info("  Step 3 (save): " + step3Time + "ms");
//             log.info("========================================");

//             ctx.output("PageRank completed in " + iteration + " iterations (" + totalTime + "ms)");

//         } catch (Exception e) {
//             log.error("PageRank failed: ", e);
//             ctx.output("PageRank failed: " + e.getMessage());
//         }
//     }

//     /**
//      * STEP 1: Build state table from pt-crawl
//      * - Processes page content locally (never sent through RDD)
//      * - Extracts outlink count from HTML
//      * - Extracts inlinks from anchor columns
//      * - Writes directly to pt-pagerank-state table
//      * - Returns count of pages processed
//      */
//     public static int buildStateTable(FlameContext ctx) throws Exception {
//         log.info("Building state table in " + STATE_TABLE + "...");

//         // Process everything in fromTableParallel - page never leaves the worker
//         FlameRDD processedKeys = ctx.fromTableParallel("pt-crawl", row -> {
//             String url = row.get("url");
//             String page = row.get("page");
//             if (url == null || page == null) return null;

//             try {
//                 String hashedUrl = Hasher.hash(url);

//                 // Count outlinks by parsing HTML locally (page stays here)
//                 int numOutlinks = 0;
//                 try {
//                     String unescapedPage = unescapeHtml4(page);
//                     Document doc = Jsoup.parse(unescapedPage, url);
//                     Elements links = doc.select("a[href]");
//                     for (Element link : links) {
//                         String href = link.attr("href");
//                         if (href != null && !href.isEmpty()) {
//                             try {
//                                 String absoluteUrl = Crawler.normalizeUrl(href, url);
//                                 if (absoluteUrl != null) {
//                                     numOutlinks++;
//                                 }
//                             } catch (Exception e) {
//                                 // Skip invalid URL
//                             }
//                         }
//                     }
//                 } catch (Exception e) {
//                     // Parsing failed, keep numOutlinks = 0
//                 }

//                 // Extract inlinks from anchor columns
//                 StringBuilder inlinks = new StringBuilder();
//                 for (String col : row.columns()) {
//                     if (col.startsWith("anchors:")) {
//                         String parentUrl = col.substring(8);
//                         if (parentUrl != null && !parentUrl.isEmpty()) {
//                             try {
//                                 String parentHash = Hasher.hash(parentUrl);
//                                 if (inlinks.length() > 0) inlinks.append(",");
//                                 inlinks.append(parentHash);
//                             } catch (Exception e) {
//                                 // Skip invalid URL
//                             }
//                         }
//                     }
//                 }

//                 // Build state value
//                 String stateValue = numOutlinks + ",1.0,1.0";
//                 if (inlinks.length() > 0) {
//                     stateValue += "," + inlinks.toString();
//                 }

//                 // Write directly to state table
//                 ctx.getKVS().put(STATE_TABLE, hashedUrl, "value", stateValue);

//                 return hashedUrl; // Return key to count
//             } catch (Exception e) {
//                 return null;
//             }
//         });

//         return processedKeys.count();
//     }

    

// }