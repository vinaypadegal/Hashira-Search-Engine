// src/cis5550/tools/VerifyPageRank.java
package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.util.Iterator;

public class VerifyPageRank {
    public static void main(String[] args) throws Exception {
        String coordinator = args.length > 0 ? args[0] : "localhost:8000";
        KVSClient kvs = new KVSClient(coordinator);

        System.out.println("Verifying PageRank results...");
        System.out.println("");

        Iterator<Row> rows = kvs.scan("pt-pageranks");
        int count = 0;
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int belowThreshold = 0;

        while (rows.hasNext()) {
            Row row = rows.next();
            String rankStr = row.get("rank");
            if (rankStr != null) {
                try {
                    double rank = Double.parseDouble(rankStr);
                    count++;
                    sum += rank;
                    min = Math.min(min, rank);
                    max = Math.max(max, rank);

                    if (rank < 0.15) {
                        belowThreshold++;
                        System.out.println("WARNING: Rank below 0.15: " + rank + " (hash: " + row.key() + ")");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid rank value: " + rankStr);
                }
            }
        }

        System.out.println("");
        System.out.println("Verification Results:");
        System.out.println("  Total pages: " + count);
        System.out.println("  Sum of ranks: " + String.format("%.6f", sum));
        System.out.println("  Expected sum: " + count + " (should be close)");
        System.out.println("  Difference: " + String.format("%.6f", Math.abs(sum - count)));
        System.out.println("  Percent error: " + String.format("%.2f", Math.abs(sum - count) / count * 100.0) + "%");
        System.out.println("  Min rank: " + String.format("%.6f", min));
        System.out.println("  Max rank: " + String.format("%.6f", max));
        System.out.println("  Avg rank: " + String.format("%.6f", sum / count));
        System.out.println("  Ranks below 0.15: " + belowThreshold);

        // Validation
        boolean isValid = true;
        if (Math.abs(sum - count) / count > 0.05) {
            System.out.println("");
            System.out.println("❌ FAILED: Sum differs from page count by more than 5%");
            isValid = false;
        }
        if (min < 0.15) {
            System.out.println("");
            System.out.println("❌ FAILED: Found ranks below 0.15 (teleportation term)");
            isValid = false;
        }
        if (belowThreshold > 0) {
            System.out.println("");
            System.out.println("⚠️  WARNING: " + belowThreshold + " pages have rank < 0.15");
        }

        if (isValid && belowThreshold == 0) {
            System.out.println("");
            System.out.println("✓ PageRank verification PASSED");
        }
    }
}