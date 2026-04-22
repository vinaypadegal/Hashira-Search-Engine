package cis5550.flame;

import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import static cis5550.webserver.Server.*;
import java.io.*;
import java.util.*;

//NOTE: NOT QUITE SURE IF THE HASHING IS CORRECT 

class Worker extends cis5550.generic.Worker {
    private static final Logger logger = Logger.getLogger(Worker.class);

	public static void main(String args[]) {
    // Configure DNS cache BEFORE any network operations
    java.security.Security.setProperty("networkaddress.cache.ttl", "300");       // 5 min positive cache
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "60"); // 1 min negative cache
    
    if (args.length != 2) {
    	logger.error("Syntax: Worker <port> <coordinatorIP:port>");
    	// System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    // Original sequential flatMap endpoint
    post("/rdd/flatMap", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
        rows = kvs.scan(inputTable);
      } else {
        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      int counter = 0;
      while (rows.hasNext()) {
        Row row = rows.next();
        if (row == null) continue;
        
        String rowKey = row.key();
        if (rowKey == null) {
          logger.warn("Skipping row with null key in /rdd/flatMap");
          continue;
        }
        
        String value = row.get("value");

        if (value != null) {
          Iterable<String> result = lambda.op(value);

          if (result != null) {
            for (String outputValue : result) {
              String uniqueKey = Hasher.hash(rowKey + "-" + (counter++));
              kvs.put(outputTable, uniqueKey, "value", outputValue);
            }
          }
        }
      }

      return "OK";
    });

    // NEW: Parallel flatMap endpoint using ParallelLambdaExecutor + BatchedKVSClient
    // Uses producer-consumer pattern with bounded queue and batching
    // Parallel flatMap with context - provides shared BatchedKVSClient for internal writes
    post("/rdd/flatMapParallel", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToIterableWithContext lambda = 
          (FlameRDD.StringToIterableWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
        rows = kvs.scan(inputTable);
      } else {
        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
      long startTime = System.currentTimeMillis();
      
      try (ParallelLambdaExecutor<String, String> executor = 
               new ParallelLambdaExecutor<>(kvs, outputTable)) {
          
          // Start bottleneck monitoring (logs every 10s to crawler.log)
          cis5550.tools.CrawlerLogger.startBottleneckMonitoring(executor, executor.getSharedWriter());
          
          executor.execute(
              rows,
              row -> row.get("value"),
              (value, sourceRowKey, collector) -> {
                  if (value != null) {
                      // Lambda gets access to shared BatchedKVSClient via collector
                      Iterable<String> result = lambda.op(
                          value, 
                          collector.getKVSClient(),      // For reads
                          collector.getSharedWriter()   // Shared batched writer for writes
                      );
                      if (result != null) {
                          for (String outputValue : result) {
                              String uniqueKey = Hasher.hash(sourceRowKey + "-" + counter.getAndIncrement());
                              collector.emit(uniqueKey, outputValue);
                          }
                      }
                  }
              }
          );
          
          // Stop bottleneck monitoring
          cis5550.tools.CrawlerLogger.stopBottleneckMonitoring();
          
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("[PARALLEL_FLATMAP_WITH_CONTEXT_COMPLETE] outputTable=" + outputTable + 
                      ", rowsProcessed=" + executor.getRowsProcessed() + 
                      ", outputsProduced=" + executor.getOutputsProduced() + 
                      ", timeMs=" + elapsed);
      }

      return "OK";
    });

    post("/rdd/mapToPair", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
        rows = kvs.scan(inputTable);
      } else {
        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      while (rows.hasNext()) {
        Row row = rows.next();
        if (row == null) {
          logger.warn("Skipping null row in /rdd/mapToPair");
          continue;
        }
        
        String rowKey = row.key();
        if (rowKey == null) {
          logger.warn("Skipping row with null key in /rdd/mapToPair");
          continue;
        }
        
        String value = row.get("value");

        if (value != null) {
          FlamePair pair = lambda.op(value);

          if (pair != null) {
            String pairKey = pair._1();
            String pairValue = pair._2();
            
            if (pairKey == null) {
              logger.warn("Skipping pair with null key from lambda output");
              continue;
            }

            kvs.put(outputTable, pairKey, rowKey, pairValue);
          }
        }
      }

      return "OK";
    });

    post("/pairRDD/foldByKey", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String zeroElement = request.queryParams("zeroElement");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
        rows = kvs.scan(inputTable);
      } else {
        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      while (rows.hasNext()) {
        Row row = rows.next();
        if (row == null) continue;
        
        String rowKey = row.key();
        if (rowKey == null) {
          logger.warn("Skipping row with null key in /pairRDD/foldByKey");
          continue;
        }

        String accumulator = zeroElement;

        for (String column : row.columns()) {
          String value = row.get(column);
          if (value != null) {
            accumulator = lambda.op(accumulator, value);
          }
        }
        kvs.put(outputTable, rowKey, "value", accumulator);
      }

      return "OK";
    });

    // Parallel version of foldByKey with batched I/O for better performance
    post("/pairRDD/foldByKeyParallel", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String zeroElement = request.queryParams("zeroElement");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      try (BatchedKVSClient batchedKvs = new BatchedKVSClient(kvs)) {
        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
        } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        // Process rows and aggregate by key using batched writes
        while (rows.hasNext()) {
          Row row = rows.next();
          if (row == null) continue;
          
          String rowKey = row.key();
          if (rowKey == null) {
            logger.warn("Skipping row with null key in /pairRDD/foldByKeyParallel");
            continue;
          }

          // Partition-level aggregation: fold all values for this key
          String accumulator = zeroElement;

          for (String column : row.columns()) {
            String value = row.get(column);
            if (value != null) {
              accumulator = lambda.op(accumulator, value);
            }
          }
          
          // Use batched write instead of direct put
          batchedKvs.put(outputTable, rowKey, "value", accumulator);
        }
      }

      return "OK";
    });

    // Original sequential fromTable endpoint
    post("/fromTable", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String zeroElement = request.queryParams("zeroElement");

      logger.info("/fromTable called: inputTable=" + inputTable + ", fromKey=" + fromKey + ", toKeyExclusive=" + toKeyExclusive);

      try {
        byte[] lambdaBytes = request.bodyAsBytes();

        FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        logger.info("Starting scan of table: " + inputTable + " (key range: [" + (fromKey != null ? fromKey : "null") + ", " + (toKeyExclusive != null ? toKeyExclusive : "null") + "))");
        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
        } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        int counter = 0;
        int processedRows = 0;
        while (rows.hasNext()) {
            Row row = rows.next();
            if (row == null) {
                logger.warn("Skipping null row in /fromTable");
                continue;
            }
            processedRows++;
            
            if (processedRows % 100 == 0) {
                logger.debug("Processed " + processedRows + " rows from " + inputTable);
            }

            // Apply the lambda - it gets the entire Row object
            String result = lambda.op(row);

            // Only store non-null results
            if (result != null) {
                // Generate unique key for the output table
                String rowKey = row.key();
                if (rowKey == null) {
                    logger.warn("Skipping row with null key in /fromTable");
                    continue;
                }
                String uniqueKey = Hasher.hash(rowKey + "-" + (counter++));
                kvs.put(outputTable, uniqueKey, "value", result);
            }
        }

        logger.info("/fromTable completed: processed " + processedRows + " rows, wrote " + counter + " results");
        return "OK";
      } catch (Exception e) {
        logger.error("/fromTable FAILED for table " + inputTable + ": " + e.getMessage(), e);
        throw e;
      }
    });

    // NEW: Parallel fromTable endpoint using ParallelLambdaExecutor + BatchedKVSClient
    // Uses producer-consumer pattern with bounded queue and batching
    post("/fromTableParallel", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      logger.info("[PARALLEL_FROMTABLE_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                  ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                  (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

      try {
        byte[] lambdaBytes = request.bodyAsBytes();
        FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
        } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        // Thread-safe counter for unique key generation
        java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
        long startTime = System.currentTimeMillis();
        
        // Use ParallelLambdaExecutor with Row as input type (not just String value)
        try (ParallelLambdaExecutor<Row, String> executor = 
                 new ParallelLambdaExecutor<>(kvs, outputTable)) {
            
            executor.execute(
                rows,
                // Extract input: pass the entire Row object (not just value)
                row -> row,
                // Lambda that processes Row and emits output
                (row, sourceRowKey, collector) -> {
                    // Apply the lambda - it gets the entire Row object
                    String result = lambda.op(row);
                    
                    // Only store non-null results
                    if (result != null) {
                        String uniqueKey = Hasher.hash(sourceRowKey + "-" + counter.getAndIncrement());
                        collector.emit(uniqueKey, result);
                    }
                }
            );
            
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("[PARALLEL_FROMTABLE_COMPLETE] outputTable=" + outputTable + 
                        ", rowsProcessed=" + executor.getRowsProcessed() + 
                        ", outputsProduced=" + executor.getOutputsProduced() + 
                        ", timeMs=" + elapsed);
        }

        return "OK";
      } catch (Exception e) {
        logger.error("[PARALLEL_FROMTABLE_FAILED] inputTable=" + inputTable + 
                     ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                     (toKeyExclusive != null ? toKeyExclusive : "null") + 
                     "): " + e.getMessage(), e);
        response.status(500, "Internal Server Error");
        return "500 Internal Server Error: " + e.getMessage();
      }
    });

    post("/rdd/flatMapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToPairIterable lambda =
          (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);
        BatchedKVSClient writer = new BatchedKVSClient(kvs);

        logger.info("/rdd/flatMapToPair: inputTable=" + inputTable + ", keyRange=[" + 
                    (fromKey != null ? fromKey : "null") + ", " + 
                    (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        int processedRows = 0;
        int totalPairs = 0;
        String currentUrl = null;
        long startTime = System.currentTimeMillis();
        
        try {
            while (rows.hasNext()) {
                Row row = rows.next();
                processedRows++;
                
                // Progress logging every 100 documents
                if (processedRows % 100 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    logger.info("Processing row " + processedRows + " from " + inputTable + 
                                " (" + processedRows * 1000L / Math.max(1, elapsed) + " rows/sec)" +
                                (currentUrl != null ? " (current: " + currentUrl + ")" : ""));
                }
              
              String value = row.get("value");
              if (value != null) {
                  // Extract URL for progress logging (for Indexer2 context)
                  if (value.contains("\t")) {
                      int tabIdx = value.indexOf('\t');
                      if (tabIdx > 0) {
                          currentUrl = value.substring(0, tabIdx);
                      }
                  }
                  
                  Iterable<FlamePair> pairs = lambda.op(value);

                  if (pairs != null) {
                      for (FlamePair pair : pairs) {
                          if (pair != null) {
                              String pairKey = pair._1();
                              String pairValue = pair._2();
                              
                              // Use batched writer
                              writer.put(outputTable, pairKey, "value", pairValue);
                              totalPairs++;
                          }
                      }
                  }
              }
          }
          
          // Final flush and close
          writer.flushAndWait();
          writer.close();
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("/rdd/flatMapToPair completed: processed " + processedRows + " rows, " + 
                      totalPairs + " pairs written in " + writer.getTotalBatches() + " batches " +
                      "(" + (elapsed / 1000.0) + "s, " + 
                      (processedRows > 0 ? (processedRows * 1000L / elapsed) : 0) + " rows/sec)");
          
      } catch (Exception e) {
          logger.error("/rdd/flatMapToPair failed: " + e.getMessage(), e);
          try {
              writer.close();
          } catch (Exception e2) {
              logger.error("Error during writer cleanup: " + e2.getMessage(), e2);
          }
          throw e;
      }

      return "OK";
    });

    post("/pairRDD/flatMap", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        byte[] lambdaBytes = request.bodyAsBytes();
        FlamePairRDD.PairToStringIterable lambda =
            (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        int counter = 0;
        while (rows.hasNext()) {
            Row row = rows.next();
            if (row == null) continue;
            
            String rowKey = row.key();
            if (rowKey == null) {
                logger.warn("Skipping row with null key in /pairRDD/flatMap");
                continue;
            }

            for (String column : row.columns()) {
                String value = row.get(column);
                if (value != null) {
                    FlamePair pair = new FlamePair(rowKey, value);
                    Iterable<String> results = lambda.op(pair);

                    if (results != null) {
                        for (String result : results) {
                            if (result != null) {
                                String uniqueKey = Hasher.hash(rowKey + "-" + column + "-" + (counter++));
                                kvs.put(outputTable, uniqueKey, "value", result);
                            }
                        }
                    }
                }
            }
        }

        return "OK";
    });

    post("/pairRDD/flatMapToPair", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        byte[] lambdaBytes = request.bodyAsBytes();
        FlamePairRDD.PairToPairIterable lambda =
            (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);
        BatchedKVSClient writer = new BatchedKVSClient(kvs);

        logger.info("/pairRDD/flatMapToPair: inputTable=" + inputTable + ", keyRange=[" + 
                    (fromKey != null ? fromKey : "null") + ", " + 
                    (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        int processedRows = 0;
        int totalPairs = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            while (rows.hasNext()) {
                Row row = rows.next();
                if (row == null) {
                    logger.warn("Skipping null row in /pairRDD/flatMapToPair");
                    continue;
                }
                processedRows++;
                
                String rowKey = row.key();
                if (rowKey == null) {
                    logger.warn("Skipping row with null key in /pairRDD/flatMapToPair");
                    continue;
                }
                
                // Progress logging every 100 rows
                if (processedRows % 100 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    logger.info("Processing row " + processedRows + " from " + inputTable + 
                                " (key: " + rowKey + ", " + 
                                (processedRows * 1000L / Math.max(1, elapsed)) + " rows/sec)");
                }

                // PairRDD: each row represents pairs with same key
                for (String column : row.columns()) {
                    String value = row.get(column);
                    if (value != null) {
                        FlamePair pair = new FlamePair(rowKey, value);
                        Iterable<FlamePair> results = lambda.op(pair);

                        if (results != null) {
                            for (FlamePair resultPair : results) {
                                if (resultPair != null) {
                                    String newKey = resultPair._1();
                                    String newValue = resultPair._2();
                                    
                                    if (newKey == null) {
                                        logger.warn("Skipping pair with null key from lambda output in /pairRDD/flatMapToPair");
                                        continue;
                                    }
                                    
                                    // Use batched writer
                                    writer.put(outputTable, newKey, "value", newValue);
                                    totalPairs++;
                                }
                            }
                        }
                    }
                }
            }
            
            // Final flush and close
            writer.flushAndWait();
            writer.close();
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("/pairRDD/flatMapToPair completed: processed " + processedRows + " rows, " + 
                        totalPairs + " pairs written in " + writer.getTotalBatches() + " batches " +
                        "(" + (elapsed / 1000.0) + "s, " + 
                        (processedRows > 0 ? (processedRows * 1000L / elapsed) : 0) + " rows/sec)");
                        
        } catch (Exception e) {
            logger.error("/pairRDD/flatMapToPair failed: " + e.getMessage(), e);
            try {
                writer.close();
            } catch (Exception e2) {
                logger.error("Error during writer cleanup: " + e2.getMessage(), e2);
            }
            throw e;
        }

        return "OK";
    });

    post("/rdd/distinct", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      while (rows.hasNext()) {
          Row row = rows.next();
          String value = row.get("value");

          if (value != null) {
              kvs.put(outputTable, value, "value", value);
          }
      }

      return "OK";
    });

    post("/pairRDD/join", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String otherTable = request.queryParams("otherTable");  
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        while (rows.hasNext()) {
            Row row1 = rows.next();
            String key = row1.key();

            Row row2 = kvs.getRow(otherTable, key);

            if (row2 != null) {
                for (String col1 : row1.columns()) {
                    String value1 = row1.get(col1);

                    if (value1 != null) {
                        for (String col2 : row2.columns()) {
                            String value2 = row2.get(col2);

                            if (value2 != null) {
                                String joinedValue = value1 + "," + value2;
                                String uniqueColumn = Hasher.hash(col1) + "_" + Hasher.hash(col2);
                                kvs.put(outputTable, key, uniqueColumn, joinedValue);
                            }
                        }
                    }
                }
            }
        }

        return "OK";
    });

    post("/rdd/fold", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");
        String zeroElement = request.queryParams("zeroElement");

        byte[] lambdaBytes = request.bodyAsBytes();
        FlamePairRDD.TwoStringsToString lambda =
            (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        String accumulator = zeroElement;

        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");

            if (value != null) {
                accumulator = lambda.op(accumulator, value);
            }
        }

        return accumulator;
    });

    post("/rdd/filter", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToBoolean lambda =
          (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      int counter = 0;
      while (rows.hasNext()) {
          Row row = rows.next();
          if (row == null) continue;
          
          String rowKey = row.key();
          if (rowKey == null) {
              logger.warn("Skipping row with null key in /rdd/filter");
              continue;
          }
          
          String value = row.get("value");

          if (value != null) {
              boolean keepValue = lambda.op(value);

              if (keepValue) {
                  String uniqueKey = Hasher.hash(rowKey + "-" + (counter++));
                  kvs.put(outputTable, uniqueKey, "value", value);
              }
          }
      }

      return "OK";
    });

    post("/rdd/mapPartitions", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.IteratorToIterator lambda =
          (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      Iterator<String> valueIterator = new Iterator<String>() {
          public boolean hasNext() {
              return rows.hasNext();
          }

          public String next() {
              Row row = rows.next();
              return row.get("value");
          }
      };

      Iterator<String> resultIterator = lambda.op(valueIterator);

      int counter = 0;
      while (resultIterator.hasNext()) {
          String value = resultIterator.next();
          if (value != null) {
              String uniqueKey = Hasher.hash(inputTable + "-" + fromKey + "-" + (counter++));
              kvs.put(outputTable, uniqueKey, "value", value);
          }
      }

      return "OK";
    });

    post("/pairRDD/cogroup", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String otherTable = request.queryParams("otherTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Set<String> allKeys = new HashSet<String>();

      Iterator<Row> rows1;
      if (fromKey == null && toKeyExclusive == null) {
          rows1 = kvs.scan(inputTable);
      } else {
          rows1 = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }
      while (rows1.hasNext()) {
          allKeys.add(rows1.next().key());
      }

      Iterator<Row> rows2;
      if (fromKey == null && toKeyExclusive == null) {
          rows2 = kvs.scan(otherTable);
      } else {
          rows2 = kvs.scan(otherTable, fromKey, toKeyExclusive);
      }
      while (rows2.hasNext()) {
          allKeys.add(rows2.next().key());
      }

      for (String key : allKeys) {
          List<String> valuesFromTable1 = new ArrayList<String>();
          List<String> valuesFromTable2 = new ArrayList<String>();

          Row row1 = kvs.getRow(inputTable, key);
          if (row1 != null) {
              for (String col : row1.columns()) {
                  String val = row1.get(col);
                  if (val != null) {
                      valuesFromTable1.add(val);
                  }
              }
          }

          Row row2 = kvs.getRow(otherTable, key);
          if (row2 != null) {
              for (String col : row2.columns()) {
                  String val = row2.get(col);
                  if (val != null) {
                      valuesFromTable2.add(val);
                  }
              }
          }

          String list1 = String.join(",", valuesFromTable1);
          String list2 = String.join(",", valuesFromTable2);
          String cogroupValue = "[" + list1 + "],[" + list2 + "]";

          kvs.put(outputTable, key, "value", cogroupValue);
      }

      return "OK";
    });

    // Parallel version of cogroup with batched I/O and optimized key processing
    // Instead of collecting all keys first, processes keys as we scan them
    post("/pairRDD/cogroupParallel", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String otherTable = request.queryParams("otherTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      try (BatchedKVSClient batchedKvs = new BatchedKVSClient(kvs)) {
        // Use a map to track keys we've seen and their values
        // This avoids collecting all keys first and allows parallel processing
        Map<String, List<String>> table1Values = new HashMap<>();
        Map<String, List<String>> table2Values = new HashMap<>();

        // Scan first table and collect values for keys in our partition
        Iterator<Row> rows1;
        if (fromKey == null && toKeyExclusive == null) {
            rows1 = kvs.scan(inputTable);
        } else {
            rows1 = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }
        
        while (rows1.hasNext()) {
            Row row = rows1.next();
            if (row == null) continue;
            String key = row.key();
            if (key == null) continue;
            
            List<String> values = new ArrayList<>();
            for (String col : row.columns()) {
                String val = row.get(col);
                if (val != null) {
                    values.add(val);
                }
            }
            table1Values.put(key, values);
        }

        // Scan second table and collect values for keys in our partition
        Iterator<Row> rows2;
        if (fromKey == null && toKeyExclusive == null) {
            rows2 = kvs.scan(otherTable);
        } else {
            rows2 = kvs.scan(otherTable, fromKey, toKeyExclusive);
        }
        
        while (rows2.hasNext()) {
            Row row = rows2.next();
            if (row == null) continue;
            String key = row.key();
            if (key == null) continue;
            
            List<String> values = new ArrayList<>();
            for (String col : row.columns()) {
                String val = row.get(col);
                if (val != null) {
                    values.add(val);
                }
            }
            table2Values.put(key, values);
        }

        // Combine all unique keys from both tables
        Set<String> allKeys = new HashSet<>(table1Values.keySet());
        allKeys.addAll(table2Values.keySet());

        // Process each key and write using batched I/O
        for (String key : allKeys) {
            List<String> values1 = table1Values.getOrDefault(key, new ArrayList<>());
            List<String> values2 = table2Values.getOrDefault(key, new ArrayList<>());

            String list1 = String.join(",", values1);
            String list2 = String.join(",", values2);
            String cogroupValue = "[" + list1 + "],[" + list2 + "]";

            // Use batched write instead of direct put
            batchedKvs.put(outputTable, key, "value", cogroupValue);
        }
      }

      return "OK";
    });

    // ==================== PARALLEL VERSIONS ====================
    
    // Parallel flatMapToPair with context - uses ParallelLambdaExecutor for CPU-heavy lambdas
    post("/rdd/flatMapToPairParallel", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      logger.info("[PARALLEL_FLATMAPTOPAIR_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                  ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                  (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToPairIterableWithContext lambda =
          (FlameRDD.StringToPairIterableWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
      long startTime = System.currentTimeMillis();
      
      try (ParallelLambdaExecutor<String, String> executor = 
               new ParallelLambdaExecutor<>(kvs, outputTable)) {
          
          executor.execute(
              rows,
              row -> row.get("value"),
              (value, sourceRowKey, collector) -> {
                  if (value != null) {
                      Iterable<FlamePair> pairs = lambda.op(
                          value,
                          collector.getKVSClient(),
                          collector.getSharedWriter()
                      );
                      if (pairs != null) {
                          for (FlamePair pair : pairs) {
                              if (pair != null) {
                                  String pairKey = pair._1();
                                  String pairValue = pair._2();
                                  if (pairKey != null) {
                                      // For PairRDD output: row key is the pair key, column is unique
                                      String uniqueCol = Hasher.hash(sourceRowKey + "-" + counter.getAndIncrement());
                                      collector.emit(pairKey, uniqueCol, pairValue);
                                  }
                              }
                          }
                      }
                  }
              }
          );
          
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("[PARALLEL_FLATMAPTOPAIR_COMPLETE] outputTable=" + outputTable + 
                      ", rowsProcessed=" + executor.getRowsProcessed() + 
                      ", outputsProduced=" + executor.getOutputsProduced() + 
                      ", timeMs=" + elapsed);
      }

      return "OK";
    });
    
    // Parallel mapToPair with context - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/rdd/mapToPairParallel", (request,response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      logger.info("[PARALLEL_MAPTOPAIR_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                  ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                  (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToPairWithContext lambda = (FlameRDD.StringToPairWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
        rows = kvs.scan(inputTable);
      } else {
        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      long startTime = System.currentTimeMillis();
      
      try (ParallelLambdaExecutor<String, String> executor = 
               new ParallelLambdaExecutor<>(kvs, outputTable)) {
          
          executor.execute(
              rows,
              row -> row.get("value"),
              (value, sourceRowKey, collector) -> {
                  if (value != null) {
                      FlamePair pair = lambda.op(
                          value,
                          collector.getKVSClient(),
                          collector.getSharedWriter()
                      );
                      if (pair != null) {
                          String pairKey = pair._1();
                          String pairValue = pair._2();
                          if (pairKey != null) {
                              // For PairRDD: row key is pair key, column is source row key
                              collector.emit(pairKey, sourceRowKey, pairValue);
                          }
                      }
                  }
              }
          );
          
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("[PARALLEL_MAPTOPAIR_COMPLETE] outputTable=" + outputTable + 
                      ", rowsProcessed=" + executor.getRowsProcessed() + 
                      ", outputsProduced=" + executor.getOutputsProduced() + 
                      ", timeMs=" + elapsed);
      }

      return "OK";
    });
    
    // Parallel filter with context - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/rdd/filterParallel", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      logger.info("[PARALLEL_FILTER_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                  ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                  (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

      byte[] lambdaBytes = request.bodyAsBytes();
      FlameRDD.StringToBooleanWithContext lambda =
          (FlameRDD.StringToBooleanWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
      long startTime = System.currentTimeMillis();
      
      try (ParallelLambdaExecutor<String, String> executor = 
               new ParallelLambdaExecutor<>(kvs, outputTable)) {
          
          executor.execute(
              rows,
              row -> row.get("value"),
              (value, sourceRowKey, collector) -> {
                  if (value != null) {
                      boolean keepValue = lambda.op(
                          value,
                          collector.getKVSClient(),
                          collector.getSharedWriter()
                      );
                      if (keepValue) {
                          String uniqueKey = Hasher.hash(sourceRowKey + "-" + counter.getAndIncrement());
                          collector.emit(uniqueKey, value);
                      }
                  }
              }
          );
          
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("[PARALLEL_FILTER_COMPLETE] outputTable=" + outputTable + 
                      ", rowsProcessed=" + executor.getRowsProcessed() + 
                      ", outputsProduced=" + executor.getOutputsProduced() + 
                      ", timeMs=" + elapsed);
      }

      return "OK";
    });
    
    // Parallel distinct - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/rdd/distinctParallel", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      logger.info("[PARALLEL_DISTINCT_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                  ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                  (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> rows;
      if (fromKey == null && toKeyExclusive == null) {
          rows = kvs.scan(inputTable);
      } else {
          rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
      }

      long startTime = System.currentTimeMillis();
      
      try (ParallelLambdaExecutor<String, String> executor = 
               new ParallelLambdaExecutor<>(kvs, outputTable)) {
          
          executor.execute(
              rows,
              row -> row.get("url"),
              (value, sourceRowKey, collector) -> {
                  if (value != null) {
                      // Use value as both key and value for deduplication
                      collector.emit(value, value);
                  }
              }
          );
          
          long elapsed = System.currentTimeMillis() - startTime;
          logger.info("[PARALLEL_DISTINCT_COMPLETE] outputTable=" + outputTable + 
                      ", rowsProcessed=" + executor.getRowsProcessed() + 
                      ", outputsProduced=" + executor.getOutputsProduced() + 
                      ", timeMs=" + elapsed);
      }

      return "OK";
    });
    
    // Parallel pairRDD flatMap with context - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/pairRDD/flatMapParallel", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        logger.info("[PARALLEL_PAIRRDD_FLATMAP_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                    ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                    (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

        byte[] lambdaBytes = request.bodyAsBytes();
        FlamePairRDD.PairToStringIterableWithContext lambda =
            (FlamePairRDD.PairToStringIterableWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
        long startTime = System.currentTimeMillis();
        
        // For PairRDD, each row may have multiple columns (key-value pairs with same key)
        // We need to process each column as a separate pair
        try (ParallelLambdaExecutor<Row, String> executor = 
                 new ParallelLambdaExecutor<>(kvs, outputTable)) {
            
            // Start bottleneck monitoring (logs every 10s to crawler.log)
            cis5550.tools.CrawlerLogger.startBottleneckMonitoring(executor, executor.getSharedWriter());
            
            executor.execute(
                rows,
                row -> row,  // Pass entire row
                (row, sourceRowKey, collector) -> {
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        if (value != null) {
                            FlamePair pair = new FlamePair(sourceRowKey, value);
                            Iterable<String> results = lambda.op(
                                pair,
                                collector.getKVSClient(),
                                collector.getSharedWriter()
                            );
                            
                            if (results != null) {
                                for (String result : results) {
                                    if (result != null) {
                                        String uniqueKey = Hasher.hash(sourceRowKey + "-" + column + "-" + counter.getAndIncrement());
                                        collector.emit(uniqueKey, result);
                                    }
                                }
                            }
                        }
                    }
                }
            );
            
            // Stop bottleneck monitoring
            cis5550.tools.CrawlerLogger.stopBottleneckMonitoring();
            
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("[PARALLEL_PAIRRDD_FLATMAP_COMPLETE] outputTable=" + outputTable + 
                        ", rowsProcessed=" + executor.getRowsProcessed() + 
                        ", outputsProduced=" + executor.getOutputsProduced() + 
                        ", timeMs=" + elapsed);
        }

        return "OK";
    });
    
    // Parallel pairRDD flatMapToPair with context - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/pairRDD/flatMapToPairParallel", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        logger.info("[PARALLEL_PAIRRDD_FLATMAPTOPAIR_START] inputTable=" + inputTable + ", outputTable=" + outputTable + 
                    ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                    (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

        byte[] lambdaBytes = request.bodyAsBytes();
        FlamePairRDD.PairToPairIterableWithContext lambda =
            (FlamePairRDD.PairToPairIterableWithContext) Serializer.byteArrayToObject(lambdaBytes, myJAR);

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(0);
        long startTime = System.currentTimeMillis();
        
        try (ParallelLambdaExecutor<Row, String> executor = 
                 new ParallelLambdaExecutor<>(kvs, outputTable)) {
            
            // Start bottleneck monitoring (logs every 10s to crawler.log)
            cis5550.tools.CrawlerLogger.startBottleneckMonitoring(executor, executor.getSharedWriter());
            
            executor.execute(
                rows,
                row -> row,  // Pass entire row
                (row, sourceRowKey, collector) -> {
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        if (value != null) {
                            FlamePair pair = new FlamePair(sourceRowKey, value);
                            Iterable<FlamePair> results = lambda.op(
                                pair,
                                collector.getKVSClient(),
                                collector.getSharedWriter()
                            );
                            
                            if (results != null) {
                                for (FlamePair resultPair : results) {
                                    if (resultPair != null) {
                                        String newKey = resultPair._1();
                                        String newValue = resultPair._2();
                                        if (newKey != null) {
                                            String uniqueCol = Hasher.hash(sourceRowKey + "-" + column + "-" + counter.getAndIncrement());
                                            collector.emit(newKey, uniqueCol, newValue);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            );
            
            // Stop bottleneck monitoring
            cis5550.tools.CrawlerLogger.stopBottleneckMonitoring();
            
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("[PARALLEL_PAIRRDD_FLATMAPTOPAIR_COMPLETE] outputTable=" + outputTable + 
                        ", rowsProcessed=" + executor.getRowsProcessed() + 
                        ", outputsProduced=" + executor.getOutputsProduced() + 
                        ", timeMs=" + elapsed);
        }

        return "OK";
    });
    
    // Parallel pairRDD join - uses ParallelLambdaExecutor + BatchedKVSClient
    post("/pairRDD/joinParallel", (request, response) -> {
        String inputTable = request.queryParams("inputTable");
        String otherTable = request.queryParams("otherTable");  
        String outputTable = request.queryParams("outputTable");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        logger.info("[PARALLEL_JOIN_START] inputTable=" + inputTable + ", otherTable=" + otherTable + 
                    ", outputTable=" + outputTable + 
                    ", keyRange=[" + (fromKey != null ? fromKey : "null") + ", " + 
                    (toKeyExclusive != null ? toKeyExclusive : "null") + ")");

        KVSClient kvs = new KVSClient(kvsCoordinator);

        Iterator<Row> rows;
        if (fromKey == null && toKeyExclusive == null) {
            rows = kvs.scan(inputTable);
        } else {
            rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        long startTime = System.currentTimeMillis();
        
        try (ParallelLambdaExecutor<Row, String> executor = 
                 new ParallelLambdaExecutor<>(kvs, outputTable)) {
            
            // Capture otherTable for use in lambda
            final String otherTableFinal = otherTable;
            
            executor.execute(
                rows,
                row -> row,  // Pass entire row
                (row1, key, collector) -> {
                    // Lookup matching row from other table
                    Row row2 = collector.getKVSClient().getRow(otherTableFinal, key);
                    
                    if (row2 != null) {
                        for (String col1 : row1.columns()) {
                            String value1 = row1.get(col1);
                            if (value1 != null) {
                                for (String col2 : row2.columns()) {
                                    String value2 = row2.get(col2);
                                    if (value2 != null) {
                                        String joinedValue = value1 + "," + value2;
                                        String uniqueColumn = Hasher.hash(col1) + "_" + Hasher.hash(col2);
                                        collector.emit(key, uniqueColumn, joinedValue);
                                    }
                                }
                            }
                        }
                    }
                }
            );
            
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("[PARALLEL_JOIN_COMPLETE] outputTable=" + outputTable + 
                        ", rowsProcessed=" + executor.getRowsProcessed() + 
                        ", outputsProduced=" + executor.getOutputsProduced() + 
                        ", timeMs=" + elapsed);
        }

        return "OK";
    });

	}
}
