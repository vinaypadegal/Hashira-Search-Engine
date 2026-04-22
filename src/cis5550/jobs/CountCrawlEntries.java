package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.util.Iterator;

public class CountCrawlEntries {
    public static void main(String[] args) throws Exception {
        String coordinator = args.length > 0 ? args[0] : "localhost:8000";
        KVSClient kvs = new KVSClient(coordinator);

        System.out.println("Counting entries in pt-crawl with both 'page' and 'url' columns present and non-null...");
        System.out.println("");

        Iterator<Row> rows = kvs.scan("pt-crawl");
        int totalRows = 0;
        int validRows = 0;
        int missingUrl = 0;
        int missingPage = 0;
        int missingBoth = 0;

        while (rows.hasNext()) {
            Row row = rows.next();
            totalRows++;
            
            String url = row.get("url");
            String page = row.get("page");
            
            if (url != null && page != null) {
                validRows++;
            } else {
                if (url == null && page == null) {
                    missingBoth++;
                } else if (url == null) {
                    missingUrl++;
                } else {
                    missingPage++;
                }
            }
            
            // Progress indicator for large tables
            if (totalRows % 10000 == 0) {
                System.out.print(".");
                System.out.flush();
            }
        }

        System.out.println("");
        System.out.println("");
        System.out.println("Results:");
        System.out.println("  Total rows in pt-crawl: " + totalRows);
        System.out.println("  Rows with both 'url' and 'page' (non-null): " + validRows);
        System.out.println("  Rows missing 'url': " + missingUrl);
        System.out.println("  Rows missing 'page': " + missingPage);
        System.out.println("  Rows missing both: " + missingBoth);
        System.out.println("");
        System.out.println("✓ Count complete");
    }
}

