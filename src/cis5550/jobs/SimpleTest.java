package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

public class SimpleTest {
    public static void run(FlameContext ctx, String[] args) {
        ctx.output("OK");
        
        try {
            // Count frontier URLs with exploring=0
            // Iterator<Row> frontierRows = kvs.scan("pt-frontier-urls");
            // int frontierCount = 0;
            // while (frontierRows.hasNext()) {
            //     Row row = frontierRows.next();
            //     String exploring = row.get("exploring");
            //     if ("0".equals(exploring)) {
            //         frontierCount++;
            //     }
            // }
            // ctx.output("Count of frontier URLs with exploring=0: " + frontierCount);
            
            // // Extract URLs from pt-crawl using fromTable (like Indexer does)
            FlameRDD urlRDD = ctx.fromTableParallel("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                if (url == null || page == null) {
                    return null; // skip rows without URL
                }
                return url;
            });
            
            // Count non-null URLs without materializing the whole list
            long nonNullCount = urlRDD.count();
            ctx.output("Non-null URLs in pt-crawl: " + nonNullCount);
            System.out.println("Non-null URLs in pt-crawl: " + nonNullCount);
            
            // Collect a small sample for inspection
            // List<String> urls = urlRDD.collect();
            // int sampleSize = Math.min(10, urls.size());
            // ctx.output("Sample URLs (first " + sampleSize + "):");
            // for (int i = 0; i < sampleSize; i++) {
            //     ctx.output("  " + (i+1) + ". " + urls.get(i));
            // }
            
        } catch (Exception e) {
            ctx.output("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

