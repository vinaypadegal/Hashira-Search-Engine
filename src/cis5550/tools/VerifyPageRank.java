package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.util.Iterator;

public class VerifyPageRank {
    public static void main(String[] args) throws Exception {
        String coordinator = args.length > 0 ? args[0] : "localhost:8000";
        KVSClient kvs = new KVSClient(coordinator);
        
        System.out.println("========================================");
        System.out.println("PageRank Verification");
        System.out.println("========================================");
        System.out.println("KVS Coordinator: " + coordinator);
        System.out.println("");
        
        Iterator<Row> rows = kvs.scan("pt-pageranks");
        int count = 0;
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int belowThreshold = 0;
        int exactlyThreshold = 0;
        
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
                        System.out.println("WARNING: Rank below 0.15: " + String.format("%.6f", rank) + 
                                         " (hash: " + row.key() + ")");
                    } else if (Math.abs(rank - 0.15) < 0.0001) {
                        exactlyThreshold++;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid rank value: " + rankStr + " (hash: " + row.key() + ")");
                }
            }
        }
        
        System.out.println("");
        System.out.println("Verification Results:");
        System.out.println("  Total pages: " + count);
        System.out.println("  Sum of ranks: " + String.format("%.6f", sum));
        System.out.println("  Expected sum: " + count + " (should be close)");
        System.out.println("  Difference: " + String.format("%.6f", Math.abs(sum - count)));
        if (count > 0) {
            System.out.println("  Percent error: " + String.format("%.2f", Math.abs(sum - count) / count * 100.0) + "%");
        }
        System.out.println("  Min rank: " + String.format("%.6f", min));
        System.out.println("  Max rank: " + String.format("%.6f", max));
        if (count > 0) {
            System.out.println("  Avg rank: " + String.format("%.6f", sum / count));
        }
        System.out.println("  Ranks below 0.15: " + belowThreshold);
        System.out.println("  Ranks exactly 0.15: " + exactlyThreshold);
        
        // Validation
        boolean isValid = true;
        String errors = "";
        
        if (count > 0 && Math.abs(sum - count) / count > 0.05) {
            errors += "  ❌ Sum differs from page count by more than 5%\n";
            isValid = false;
        }
        if (min < 0.15) {
            errors += "  ❌ Found ranks below 0.15 (teleportation term)\n";
            isValid = false;
        }
        
        System.out.println("");
        if (!isValid) {
            System.out.println("FAILED CHECKS:");
            System.out.print(errors);
        }
        
        if (belowThreshold > 0) {
            System.out.println("⚠️  WARNING: " + belowThreshold + " pages have rank < 0.15");
        }
        
        if (isValid && belowThreshold == 0) {
            System.out.println("✓ PageRank verification PASSED");
            System.out.println("  - Sum is within 5% of page count");
            System.out.println("  - All ranks >= 0.15");
        } else if (isValid) {
            System.out.println("⚠️  PageRank verification PASSED with warnings");
        } else {
            System.out.println("❌ PageRank verification FAILED");
        }
        System.out.println("========================================");
    }
}

