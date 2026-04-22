package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KVS Read Speed Test
 * 
 * Simulates the crawler's child URL existence check pattern:
 * - Sample 1% of URLs from pt-crawl
 * - Take up to 5000 URLs
 * - For each URL, do 50 KVS getRow lookups (simulating child URL checks)
 * - Measure and report timing statistics
 * 
 * Usage: Submit this job to Flame
 */
public class KVSReadSpeedTest {
    
    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("=== KVS Read Speed Test ===\n");
        
        int maxUrls = 5000;
        int lookupsPerUrl = 50;
        double sampleRate = 0.01; // 1%
        
        if (args.length > 0) {
            try {
                maxUrls = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                ctx.output("Invalid maxUrls, using default: " + maxUrls + "\n");
            }
        }
        if (args.length > 1) {
            try {
                lookupsPerUrl = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                ctx.output("Invalid lookupsPerUrl, using default: " + lookupsPerUrl + "\n");
            }
        }
        
        ctx.output("Config: maxUrls=" + maxUrls + ", lookupsPerUrl=" + lookupsPerUrl + ", sampleRate=" + (sampleRate * 100) + "%\n");
        
        final int finalMaxUrls = maxUrls;
        final int finalLookupsPerUrl = lookupsPerUrl;
        
        long startTime = System.currentTimeMillis();
        
        // Step 1: Sample 1% of URLs from pt-crawl
        ctx.output("Step 1: Sampling URLs from pt-crawl...\n");
        
        FlameRDD sampledUrls = ctx.fromTableParallel("pt-crawl", row -> {
            String url = row.get("url");
            if (url == null || url.isEmpty()) {
                return null;
            }
            // 1% sampling using coin toss
            if (Math.random() >= sampleRate) {
                return null;
            }
            return url;
        });
        
        long sampledCount = sampledUrls.count();
        ctx.output("Sampled " + sampledCount + " URLs (1% of pt-crawl)\n");
        
        if (sampledCount == 0) {
            ctx.output("ERROR: No URLs sampled. Is pt-crawl empty?\n");
            return;
        }
        
        // Step 2: Take up to maxUrls
        List<String> urlList = sampledUrls.take(maxUrls).stream().toList();
        int actualUrls = urlList.size();
        ctx.output("Using " + actualUrls + " URLs for testing\n");
        
        // Step 3: Create RDD from the URL list
        FlameRDD testUrls = ctx.parallelize(new ArrayList<>(urlList));
        
        // Step 4: Run KVS lookups and measure time
        ctx.output("\nStep 2: Running " + (actualUrls * finalLookupsPerUrl) + " KVS lookups...\n");
        
        long lookupStartTime = System.currentTimeMillis();
        
        // Use flatMapParallel to run lookups across workers
        FlameRDD timingResults = testUrls.flatMapParallel((url, kvs, batchedKvs) -> {
            List<String> results = new ArrayList<>();
            
            // Generate random-ish keys to lookup (simulating child URLs)
            List<String> keysToLookup = new ArrayList<>();
            for (int i = 0; i < finalLookupsPerUrl; i++) {
                // Hash the URL + index to get a key (like we do for child URLs)
                String fakeChildUrl = url + "/child" + i;
                String hashedKey = Hasher.hash(fakeChildUrl);
                keysToLookup.add(hashedKey);
            }
            
            // Measure lookup times
            long[] times = new long[finalLookupsPerUrl];
            int hits = 0;
            int misses = 0;
            
            for (int i = 0; i < finalLookupsPerUrl; i++) {
                long start = System.nanoTime();
                Row row = kvs.getRow("pt-crawl", keysToLookup.get(i));
                long elapsed = System.nanoTime() - start;
                times[i] = elapsed;
                
                if (row != null && row.get("url") != null) {
                    hits++;
                } else {
                    misses++;
                }
            }
            
            // Calculate stats
            long totalNs = 0;
            long minNs = Long.MAX_VALUE;
            long maxNs = 0;
            for (long t : times) {
                totalNs += t;
                minNs = Math.min(minNs, t);
                maxNs = Math.max(maxNs, t);
            }
            double avgMs = (totalNs / (double) finalLookupsPerUrl) / 1_000_000.0;
            double minMs = minNs / 1_000_000.0;
            double maxMs = maxNs / 1_000_000.0;
            double totalMs = totalNs / 1_000_000.0;
            
            // Return timing result as CSV: url,count,hits,misses,totalMs,avgMs,minMs,maxMs
            String result = String.format("%d,%d,%d,%.2f,%.2f,%.2f,%.2f",
                finalLookupsPerUrl, hits, misses, totalMs, avgMs, minMs, maxMs);
            results.add(result);
            
            return results;
        });
        
        // Step 5: Collect and aggregate results
        List<String> allResults = timingResults.collect();
        
        long lookupEndTime = System.currentTimeMillis();
        long totalLookupTimeMs = lookupEndTime - lookupStartTime;
        
        // Aggregate stats
        double totalTimeMs = 0;
        double minAvgMs = Double.MAX_VALUE;
        double maxAvgMs = 0;
        double globalMinMs = Double.MAX_VALUE;
        double globalMaxMs = 0;
        int totalHits = 0;
        int totalMisses = 0;
        int totalLookups = 0;
        
        for (String result : allResults) {
            String[] parts = result.split(",");
            if (parts.length >= 7) {
                int count = Integer.parseInt(parts[0]);
                int hits = Integer.parseInt(parts[1]);
                int misses = Integer.parseInt(parts[2]);
                double total = Double.parseDouble(parts[3]);
                double avg = Double.parseDouble(parts[4]);
                double min = Double.parseDouble(parts[5]);
                double max = Double.parseDouble(parts[6]);
                
                totalLookups += count;
                totalHits += hits;
                totalMisses += misses;
                totalTimeMs += total;
                minAvgMs = Math.min(minAvgMs, avg);
                maxAvgMs = Math.max(maxAvgMs, avg);
                globalMinMs = Math.min(globalMinMs, min);
                globalMaxMs = Math.max(globalMaxMs, max);
            }
        }
        
        double overallAvgMs = totalLookups > 0 ? totalTimeMs / totalLookups : 0;
        
        // Output results
        ctx.output("\n=== Results ===\n");
        ctx.output(String.format("URLs tested: %d\n", actualUrls));
        ctx.output(String.format("Lookups per URL: %d\n", finalLookupsPerUrl));
        ctx.output(String.format("Total lookups: %d\n", totalLookups));
        ctx.output(String.format("Cache hits: %d (%.1f%%)\n", totalHits, 100.0 * totalHits / Math.max(1, totalLookups)));
        ctx.output(String.format("Cache misses: %d (%.1f%%)\n", totalMisses, 100.0 * totalMisses / Math.max(1, totalLookups)));
        ctx.output("\n--- Timing ---\n");
        ctx.output(String.format("Wall clock time: %.2f seconds\n", totalLookupTimeMs / 1000.0));
        ctx.output(String.format("Sum of lookup times: %.2f seconds\n", totalTimeMs / 1000.0));
        ctx.output(String.format("Avg per lookup: %.3f ms\n", overallAvgMs));
        ctx.output(String.format("Min per lookup: %.3f ms\n", globalMinMs));
        ctx.output(String.format("Max per lookup: %.3f ms\n", globalMaxMs));
        ctx.output(String.format("Min avg (best worker): %.3f ms\n", minAvgMs));
        ctx.output(String.format("Max avg (worst worker): %.3f ms\n", maxAvgMs));
        
        // Estimate for crawler
        ctx.output("\n--- Crawler Estimate ---\n");
        int avgChildUrls = 50; // typical number of child URLs per page
        double timePerPage = avgChildUrls * overallAvgMs;
        double pagesPerSecond = 1000.0 / timePerPage;
        ctx.output(String.format("Estimated KVS read time per page: %.1f ms\n", timePerPage));
        ctx.output(String.format("Max pages/sec (if KVS-bound): %.1f\n", pagesPerSecond));
        ctx.output(String.format("For 100k pages: %.1f minutes (KVS reads only)\n", 100000 / pagesPerSecond / 60));
        
        long endTime = System.currentTimeMillis();
        ctx.output(String.format("\nTotal test time: %.2f seconds\n", (endTime - startTime) / 1000.0));
    }
}

