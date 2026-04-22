package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import java.util.*;

/**
 * Distributed analytics on crawler logs using Flame.
 * Provides insights into crawl performance, errors, and patterns.
 */
public class LogAnalytics {

    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("OK");

        String operation = args.length > 0 ? args[0] : "summary";

        // Load pt-logs table as RDD
        FlameRDD logsRDD = ctx.fromTable("pt-logs", row -> {
            // Convert row to structured format: "url|status|round|worker|domain"
            String url = row.get("url");
            String status = row.get("currentStatus");
            String round = row.get("lastRound");
            String worker = row.get("lastWorkerID");
            String domain = row.get("domain");

            if (url == null || status == null) {
                return null;
            }

            return String.join("|", url, status, round != null ? round : "0",
                    worker != null ? worker : "unknown",
                    domain != null ? domain : "unknown");
        });

        switch (operation) {
            case "summary":
                generateSummary(ctx, logsRDD);
                break;
            case "errors":
                analyzeErrors(ctx, logsRDD);
                break;
            case "domains":
                analyzeDomains(ctx, logsRDD);
                break;
            case "workers":
                analyzeWorkers(ctx, logsRDD);
                break;
            case "rounds":
                analyzeRounds(ctx, logsRDD);
                break;
            default:
                ctx.output("Unknown operation: " + operation);
        }
    }

    //Generate overall summary statistic
    private static void generateSummary(FlameContext ctx, FlameRDD logsRDD) throws Exception {
        // Count total logs
        int total = logsRDD.count();

        // Count by status
        FlamePairRDD byStatus = logsRDD.mapToPair(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 2) {
                return new FlamePair(parts[1], "1"); // status -> count
            }
            return null;
        });

        FlamePairRDD statusCounts = byStatus.foldByKey("0", (a, b) -> {
            return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
        });

        // Save results
        statusCounts.saveAsTable("analytics-status-counts");

        ctx.output("\n Crawler Analytics Summary \n");
        ctx.output("Total URLs: " + total + "\n");
        ctx.output("\nStatus Breakdown:\n");

        List<FlamePair> statuses = statusCounts.collect();
        for (FlamePair pair : statuses) {
            ctx.output("  " + pair._1() + ": " + pair._2() + "\n");
        }
    }

    //Analyze errors by type and domain
    private static void analyzeErrors(FlameContext ctx, FlameRDD logsRDD) throws Exception {
        // Filter for errors only
        FlameRDD errors = logsRDD.filter(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 2) {
                String status = parts[1];
                return status.contains("FAIL") || status.contains("ERROR") ||
                        status.contains("TIMEOUT");
            }
            return false;
        });

        // Group errors by domain
        FlamePairRDD errorsByDomain = errors.mapToPair(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                String status = parts[1];
                String domain = parts[4];
                return new FlamePair(domain, status);
            }
            return null;
        });

        // Count errors per domain
        FlamePairRDD errorCounts = errorsByDomain.flatMapToPair(pair -> {
            return Collections.singletonList(new FlamePair(pair._1(), "1"));
        }).foldByKey("0", (a, b) -> {
            return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
        });

        errorCounts.saveAsTable("analytics-errors-by-domain");

        ctx.output("\n Error Analysis \n");
        ctx.output("Total Errors: " + errors.count() + "\n");
        ctx.output("\nErrors by Domain:\n");

        List<FlamePair> results = errorCounts.collect();
        // Sort by count descending
        results.sort((a, b) -> Integer.compare(
                Integer.parseInt(b._2()),
                Integer.parseInt(a._2())
        ));

        for (int i = 0; i < Math.min(20, results.size()); i++) {
            FlamePair pair = results.get(i);
            ctx.output("  " + pair._1() + ": " + pair._2() + " errors\n");
        }
    }

    /**
     * Analyze crawl distribution across domains
     */
    private static void analyzeDomains(FlameContext ctx, FlameRDD logsRDD) throws Exception {
        // Group by domain
        FlamePairRDD byDomain = logsRDD.mapToPair(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                return new FlamePair(parts[4], "1"); // domain -> count
            }
            return null;
        });

        FlamePairRDD domainCounts = byDomain.foldByKey("0", (a, b) -> {
            return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
        });

        domainCounts.saveAsTable("analytics-domain-counts");

        ctx.output("\n Domain Analysis \n");

        List<FlamePair> results = domainCounts.collect();
        results.sort((a, b) -> Integer.compare(
                Integer.parseInt(b._2()),
                Integer.parseInt(a._2())
        ));

        ctx.output("Top 20 Domains by URL Count:\n");
        for (int i = 0; i < Math.min(20, results.size()); i++) {
            FlamePair pair = results.get(i);
            ctx.output("  " + pair._1() + ": " + pair._2() + " URLs\n");
        }
    }

    //Analyze work distribution across workers
    private static void analyzeWorkers(FlameContext ctx, FlameRDD logsRDD) throws Exception {
        FlamePairRDD byWorker = logsRDD.mapToPair(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 4) {
                return new FlamePair(parts[3], "1"); // worker -> count
            }
            return null;
        });

        FlamePairRDD workerCounts = byWorker.foldByKey("0", (a, b) -> {
            return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
        });

        workerCounts.saveAsTable("analytics-worker-counts");

        ctx.output("\n Worker Analysis \n");

        List<FlamePair> results = workerCounts.collect();
        for (FlamePair pair : results) {
            ctx.output("  " + pair._1() + ": " + pair._2() + " URLs processed\n");
        }
    }

    //Analyze crawl progression by rounds
    private static void analyzeRounds(FlameContext ctx, FlameRDD logsRDD) throws Exception {
        FlamePairRDD byRound = logsRDD.mapToPair(line -> {
            String[] parts = line.split("\\|");
            if (parts.length >= 3) {
                return new FlamePair(parts[2], "1"); // round -> count
            }
            return null;
        });

        FlamePairRDD roundCounts = byRound.foldByKey("0", (a, b) -> {
            return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
        });

        roundCounts.saveAsTable("analytics-round-counts");

        ctx.output("\n Round Analysis \n");

        List<FlamePair> results = roundCounts.collect();
        results.sort((a, b) -> Integer.compare(
                Integer.parseInt(a._1()),
                Integer.parseInt(b._1())
        ));

        for (FlamePair pair : results) {
            ctx.output("  Round " + pair._1() + ": " + pair._2() + " URLs\n");
        }
    }
}