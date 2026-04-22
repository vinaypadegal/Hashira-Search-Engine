package cis5550.test;

import cis5550.flame.*;
import java.util.*;
import cis5550.kvs.*;
import java.nio.file.*;
import java.util.stream.Collectors;
import java.text.DecimalFormat;

public class KvsBenchmark {
    /*
     * This Flame Job simply makes a number of calls to putRow() and
     * subsequently to getRow(). It will give you a sense of how long it
     * actually takes to read/write (a critical primitive in most of your
     * components) to/from your storage layer. You can further experiment by
     * changing the number of KVS nodes you have running, adding profiling
     * support, and/or deploying it to AWS.
     *
     * In order to run this program, you will first need to jar it, after which
     * you can initialize your desired number of Flame and KVS workers and then
     * just run the program. Note that you are encouraged to modify this program
     * to better suit your testing needs, e.g., making it look more like a
     * crawler or indexer. This file is just a starting point for performance
     * tuning your project.
     *
     * Possible List of commands to run this program:
     * <Initialize all workers/coordinators>
     *
     * javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar \
     *       -d out src/cis5550/test/kvsBenchmark.java
     * jar cvf kvsBenchmark.jar -C out .
     *
     * java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar \
     *      cis5550.flame.FlameSubmit localhost:9000 kvsBenchmark.jar \
     *      cis5550.test.KvsBenchmark ./pt-crawl-example
     */
    // Class to hold operation data
    static class OperationData {
        String rowKey;
        long fileSize;
        long putTime; // in nanoseconds
        long getTime; // in nanoseconds

        OperationData(long fileSize, long putTime, String rowKey) {
            this.fileSize = fileSize;
            this.putTime = putTime;
            this.rowKey = rowKey;
        }

        void setGetTime(long getTime) {
            this.getTime = getTime;
        }
    }

    // Direct KVS benchmark (original method - for comparison)
    static void benchmarkPutRowDirect(KVSClient kvs, String table,
                                      List<OperationData> contentData,
                                      List<Path> contentFiles) throws Exception{
        System.out.println("Starting direct putRow() operations for '" + table +
                "' table...");

        for (Path filePath : contentFiles) {
            // Read file into byte[] (not timed)
            byte[] content = Files.readAllBytes(filePath);
            long fileSize = content.length;

            String rowKey = filePath.getFileName().toString();
            Row row = new Row(rowKey);
            row.put("content", content);

            // Time the putRow() operation
            long startTime = System.nanoTime();
            kvs.putRow(table, row);
            long totalTime = System.nanoTime() - startTime;

            // Store the data
            contentData.add(new OperationData(fileSize, totalTime, rowKey));
        }

        System.out.println("Completed direct putRow() operations for '" + table +
                "' table.");
    }

    // Flame-based benchmark using fromTable (tests Flame worker optimizations)
    static void benchmarkPutRowFlame(FlameContext ctx, String table,
                                     List<OperationData> contentData,
                                     List<Path> contentFiles) throws Exception {
        System.out.println("Starting Flame-based putRow() operations for '" + table +
                "' table (using fromTable + mapToPair to test worker optimizations)...");

        // First, write files to a temporary table using direct KVS (not timed)
        // Format: rowKey (filename) -> "value" column with content
        String tempTable = "temp-" + table + "-" + System.currentTimeMillis();
        KVSClient kvs = ctx.getKVS();
        
        for (Path filePath : contentFiles) {
            byte[] content = Files.readAllBytes(filePath);
            String rowKey = filePath.getFileName().toString();
            Row row = new Row(rowKey);
            // Store content as string for Flame operations
            row.put("value", new String(content, "UTF-8"));
            kvs.putRow(tempTable, row);
        }

        // Now use Flame fromTable + mapToPair to write (this goes through Flame workers!)
        // mapToPair uses the optimized batching we implemented (100 rows per batch)
        long startTime = System.nanoTime();
        
        ctx.fromTable(tempTable, row -> {
            // Return the value column - fromTable will process this through workers
            String rowKey = row.key();
            String value = row.get("value");
            // Return "rowKey\tvalue" so we can preserve the original row key
            return rowKey + "\t" + value;
        }).mapToPair(s -> {
            // Parse the data and create pairs
            // This mapToPair operation uses the optimized batched writes in workers
            if (s != null && s.contains("\t")) {
                int tabIdx = s.indexOf("\t");
                String rowKey = s.substring(0, tabIdx);
                String content = s.substring(tabIdx + 1);
                // Use original row key so we can read it back correctly
                return new FlamePair(rowKey, content);
            }
            return null;
        }).saveAsTable(table);
        
        long totalTime = System.nanoTime() - startTime;

        // Calculate average time per file
        long avgTime = totalTime / Math.max(1, contentFiles.size());
        for (Path filePath : contentFiles) {
            byte[] content = Files.readAllBytes(filePath);
            long fileSize = content.length;
            String rowKey = filePath.getFileName().toString();
            contentData.add(new OperationData(fileSize, avgTime, rowKey));
        }

        // Clean up temp table
        try {
            kvs.delete(tempTable);
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        System.out.println("Completed Flame-based putRow() operations for '" + table +
                "' table (used optimized mapToPair batching - 100 rows per batch).");
    }

    // Direct KVS benchmark (original method - for comparison)
    static void benchmarkGetRowDirect(KVSClient kvs, String table,
                                      List<OperationData> contentData) {
        System.out.println("Starting direct getRow() operations for '" + table +
                "' table...");
        for (OperationData data : contentData) {
            String rowKey = data.rowKey;

            // Ensure the rowKey is not null or empty before using it
            if (rowKey == null || rowKey.isEmpty()) {
                System.err.println("Skipping getRow() operation: row key " +
                        rowKey + " is null or empty.");
                continue;
            }

            try {
                // Time the getRow() operation
                long startTime = System.nanoTime();
                Row retrievedRow = kvs.getRow(table, rowKey);
                long totalTime = System.nanoTime() - startTime;

                data.setGetTime(totalTime);

                if (retrievedRow == null) {
                    System.err.println("Row not found for key: " + rowKey);
                }
            } catch (Exception e) {
                System.err.println("Exception while performing getRow() " +
                        "for key: " + rowKey);
                e.printStackTrace();
            }
        }

        System.out.println("Completed direct getRow() operations for '" + table +
                "' table.");
    }

    // Flame-based benchmark using fromTable (tests Flame worker optimizations)
    static void benchmarkGetRowFlame(FlameContext ctx, String table,
                                     List<OperationData> contentData) throws Exception {
        System.out.println("Starting Flame-based getRow() operations for '" + table +
                "' table (using fromTable to test worker optimizations)...");

        // Use fromTable to read all rows (this goes through Flame workers!)
        // PairRDD structure: rowKey -> columns (each column is a value from a pair)
        // We need to collect all values from all columns for each row
        long startTime = System.nanoTime();
        
        List<String> results = ctx.fromTable(table, row -> {
            String rowKey = row.key();
            // PairRDD stores values as columns, so we need to iterate columns
            // For our benchmark, each row should have one column with the content
            for (String column : row.columns()) {
                String value = row.get(column);
                if (value != null) {
                    return rowKey + "\t" + value;
                }
            }
            return null;
        }).collect();
        
        long totalTime = System.nanoTime() - startTime;

        // Calculate average time per file
        long avgTime = totalTime / Math.max(1, contentData.size());
        for (OperationData data : contentData) {
            data.setGetTime(avgTime);
        }

        System.out.println("Completed Flame-based getRow() operations for '" + table +
                "' table (read " + results.size() + " rows through optimized fromTable).");
    }

    public static void run(FlameContext ctx, String args[]) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: KvsBenchmark <directory_path> [mode]");
            System.err.println("  mode: 'direct' (default) or 'flame'");
            System.err.println("  'direct' - Direct KVS calls (coordinator -> KVS worker)");
            System.err.println("  'flame' - Flame operations (coordinator -> Flame worker -> KVS worker)");
            return;
        }

        String directoryPath = args[0];
        String mode = args.length > 1 ? args[1] : "direct";
        boolean useFlame = "flame".equalsIgnoreCase(mode);
        
        KVSClient kvs = ctx.getKVS();

        System.out.println("=== KVS Benchmark ===");
        System.out.println("Mode: " + (useFlame ? "Flame operations (tests worker optimizations)" : "Direct KVS calls"));
        System.out.println("Directory: " + directoryPath);
        System.out.println();

        // Read all files in the directory
        List<Path> filePaths = Files.list(Paths.get(directoryPath))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        int totalFiles = filePaths.size();
        int half = totalFiles / 2;

        List<Path> contentFiles = filePaths.subList(0, half);
        List<Path> ptContentFiles = filePaths.subList(half, totalFiles);

        // Data structures to hold operation data
        List<OperationData> contentData = new ArrayList<>();
        List<OperationData> ptContentData = new ArrayList<>();

        if (useFlame) {
            // Use Flame operations to test worker optimizations
            System.out.println("Testing Flame worker optimizations (batching, etc.)...");
            System.out.println();
            
            // Benchmark putRow() for 'content' table (in-memory) using Flame
            benchmarkPutRowFlame(ctx, "content", contentData, contentFiles);

            // Benchmark putRow() for 'pt-content' table (persistent) using Flame
            benchmarkPutRowFlame(ctx, "pt-content", ptContentData, ptContentFiles);

            // Benchmark getRow() for 'content' table (in-memory) using Flame
            benchmarkGetRowFlame(ctx, "content", contentData);

            // Benchmark getRow() for 'pt-content' table (persistent) using Flame
            benchmarkGetRowFlame(ctx, "pt-content", ptContentData);
        } else {
            // Use direct KVS calls (original benchmark)
            System.out.println("Testing direct KVS calls (coordinator -> KVS worker)...");
            System.out.println();
            
            // Benchmark putRow() for 'content' table (in-memory)
            benchmarkPutRowDirect(kvs, "content", contentData, contentFiles);

            // Benchmark putRow() for 'pt-content' table (persistent)
            benchmarkPutRowDirect(kvs, "pt-content", ptContentData, ptContentFiles);

            // Benchmark getRow() for 'content' table (in-memory)
            benchmarkGetRowDirect(kvs, "content", contentData);

            // Benchmark getRow() for 'pt-content' table (persistent)
            benchmarkGetRowDirect(kvs, "pt-content", ptContentData);
        }

        // Output results
        DecimalFormat df = new DecimalFormat("#.##");
        System.out.println();
        System.out.println("=== Results ===");
        processResults("content", contentData, df);
        processResults("pt-content", ptContentData, df);
        System.out.println("Benchmark completed.");
        
        if (useFlame) {
            System.out.println();
            System.out.println("Note: Flame mode tests the optimized worker operations.");
            System.out.println("Compare these results with 'direct' mode to see the impact of batching.");
        }
    }

    private static void processResults(String tableName,
                                       List<OperationData> dataList,
                                       DecimalFormat df) {
        long totalPutTime = 0;
        long totalGetTime = 0;
        long totalFileSize = 0;

        for (OperationData data : dataList) {
            totalPutTime += data.putTime;
            totalGetTime += data.getTime;
            totalFileSize += data.fileSize;
        }

        double averagePutTime = totalPutTime / (double) dataList.size();
        double averageGetTime = totalGetTime / (double) dataList.size();
        double averageFileSize = totalFileSize / (double) dataList.size();

        System.out.println("Results for '" + tableName + "' table:");
        System.out.printf("Number of files: %d\n", dataList.size());
        System.out.printf("Average file size: %.2f bytes\n",
                averageFileSize);
        System.out.printf("Average putRow() time: %.2f ms\n",
                averagePutTime / 1e6);
        System.out.printf("Average getRow() time: %.2f ms\n",
                averageGetTime / 1e6);

        System.out.println("Estimated time for operations:");

        double estimatedPutTime10k = averagePutTime * 10000;
        double estimatedGetTime10k = averageGetTime * 10000;
        System.out.printf("Estimated time for 10,000 putRow(): %.2f s\n", estimatedPutTime10k / 1e9);
        System.out.printf("Estimated time for 10,000 getRow(): %.2f s\n",
                estimatedGetTime10k / 1e9);

        double estimatedPutTime100k = averagePutTime * 100000;
        double estimatedGetTime100k = averageGetTime * 100000;
        System.out.printf("Estimated time for 100,000 putRow(): %.2f s\n", estimatedPutTime100k / 1e9);
        System.out.printf("Estimated time for 100,000 getRow(): %.2f s\n",
                estimatedGetTime100k / 1e9);

        double estimatedPutTime1M = averagePutTime * 1000000;
        double estimatedGetTime1M = averageGetTime * 1000000;
        System.out.printf("Estimated time for 1,000,000 putRow(): %.2f s\n", estimatedPutTime1M / 1e9);
        System.out.printf("Estimated time for 1,000,000 getRow(): %.2f s\n",
                estimatedGetTime1M / 1e9);

        System.out.println();
    }

    public static void main(String args[]) throws Exception {
        run(null, args);
    }
}