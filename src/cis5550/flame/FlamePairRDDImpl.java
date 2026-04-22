package cis5550.flame;

import cis5550.kvs.Row;
import cis5550.tools.Logger;
import java.util.*;

public class FlamePairRDDImpl implements FlamePairRDD {

  private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);
  
  private String tableName;

  public FlamePairRDDImpl(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<FlamePair> collect() throws Exception {
    List<FlamePair> result = new ArrayList<>();

    Iterator<Row> rows = Coordinator.kvs.scan(tableName);
    while (rows.hasNext()) {
      Row row = rows.next();
      String rowKey = row.key();

      for (String column : row.columns()) {
        String value = row.get(column);
        if (value != null) {
          result.add(new FlamePair(rowKey, value));
        }
      }
    }

    return result;
  }

  public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/pairRDD/foldByKey", tableName, serializedLambda, zeroElement);

    return new FlamePairRDDImpl(outputTable);
  }

  public void saveAsTable(String tableNameArg) throws Exception {
    logger.info("saveAsTable called: renaming " + tableName + " to " + tableNameArg);
    
    // Rename the table in KVS (this renames on all workers)
    Coordinator.kvs.rename(tableName, tableNameArg);
    
    // Update this PairRDD to track the new name
    this.tableName = tableNameArg;
    
    logger.info("saveAsTable completed: table renamed to " + tableNameArg);
  }

  @Override
  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/pairRDD/flatMap",  
          tableName,
          serializedLambda,
          null
      );

      return new FlameRDDImpl(outputTable);  
  }
  
  /**
   * Parallel version of flatMap using ParallelLambdaExecutor + BatchedKVSClient.
   * Provides shared BatchedKVSClient for internal writes pooled across all threads.
   * Use this for CPU-intensive lambda operations on large datasets.
   */
  @Override
  public FlameRDD flatMapParallel(PairToStringIterableWithContext lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/pairRDD/flatMapParallel",  
          tableName,
          serializedLambda,
          null
      );

      return new FlameRDDImpl(outputTable);  
  }

  public void destroy() throws Exception {
    if (tableName != null) {
      String tableToDelete = tableName;
      logger.info("destroy() called: deleting table " + tableToDelete);
      try {
        Coordinator.kvs.delete(tableToDelete);
        tableName = null; // Mark as destroyed
        logger.info("destroy() completed: table " + tableToDelete + " deleted");
      } catch (Exception e) {
        logger.error("destroy() failed: " + e.getMessage(), e);
        throw e;
      }
    }
  }

  @Override
  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/pairRDD/flatMapToPair",  
          tableName,
          serializedLambda,
          null
      );

      return new FlamePairRDDImpl(outputTable); 
  }
  
  /**
   * Parallel version of flatMapToPair using ParallelLambdaExecutor + BatchedKVSClient.
   * Provides shared BatchedKVSClient for internal writes pooled across all threads.
   * Use this for CPU-intensive lambda operations on large datasets.
   */
  @Override
  public FlamePairRDD flatMapToPairParallel(PairToPairIterableWithContext lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/pairRDD/flatMapToPairParallel",  
          tableName,
          serializedLambda,
          null
      );

      return new FlamePairRDDImpl(outputTable); 
  }

  // basically copy pasta from invoke operation
  @Override
  public FlamePairRDD join(FlamePairRDD other) throws Exception {

      String otherTableName = ((FlamePairRDDImpl) other).getTableName();
      String outputTable = Coordinator.context.generateTableName();

      cis5550.kvs.KVSClient kvsClient = Coordinator.kvs;
      cis5550.tools.Partitioner partitioner = new cis5550.tools.Partitioner();

      int numKVSWorkers = kvsClient.numWorkers();
      Vector<String> workerIDs = new Vector<>();

      for (int i = 0; i < numKVSWorkers; i++) {
          String workerID = kvsClient.getWorkerID(i);
          workerIDs.add(workerID);
      }

      Collections.sort(workerIDs);

      for (int i = 0; i < workerIDs.size(); i++) {
          String workerID = workerIDs.get(i);
          String fromKey = workerID;
          String toKey = (i < workerIDs.size() - 1) ? workerIDs.get(i + 1) : null;

          String address = null;
          for (int j = 0; j < numKVSWorkers; j++) {
              if (kvsClient.getWorkerID(j).equals(workerID)) {
                  address = kvsClient.getWorkerAddress(j);
                  break;
              }
          }

          partitioner.addKVSWorker(address, fromKey, toKey);
      }

      if (workerIDs.size() > 0) {
          String lastWorkerID = workerIDs.get(workerIDs.size() - 1);
          String address = null;
          for (int j = 0; j < numKVSWorkers; j++) {
              if (kvsClient.getWorkerID(j).equals(lastWorkerID)) {
                  address = kvsClient.getWorkerAddress(j);
                  break;
              }
          }
          partitioner.addKVSWorker(address, null, workerIDs.get(0));
      }

      List<String> flameWorkers = Coordinator.getWorkersString();
      for (String worker : flameWorkers) {
          partitioner.addFlameWorker(worker);
      }

      Vector<cis5550.tools.Partitioner.Partition> partitions = partitioner.assignPartitions();

      Thread[] threads = new Thread[partitions.size()];
      String[] results = new String[partitions.size()];
      int[] statusCodes = new int[partitions.size()];

      for (int i = 0; i < partitions.size(); i++) {
          final int idx = i;
          final cis5550.tools.Partitioner.Partition partition = partitions.get(i);

          threads[i] = new Thread("Join operation #" + (i + 1)) {
              public void run() {
                  try {
                      String url = "http://" + partition.assignedFlameWorker + "/pairRDD/join";
                      url += "?inputTable=" + java.net.URLEncoder.encode(tableName, "UTF-8");
                      url += "&otherTable=" + java.net.URLEncoder.encode(otherTableName, "UTF-8");  // ← EXTRA PARAMETER
                      url += "&outputTable=" + java.net.URLEncoder.encode(outputTable, "UTF-8");
                      url += "&kvsCoordinator=" + java.net.URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");

                      if (partition.fromKey != null) {
                          url += "&fromKey=" + java.net.URLEncoder.encode(partition.fromKey, "UTF-8");
                      }
                      if (partition.toKeyExclusive != null) {
                          url += "&toKeyExclusive=" + java.net.URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                      }

                      cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("POST", url, null);
                      statusCodes[idx] = response.statusCode();
                      results[idx] = new String(response.body());
                  } catch (Exception e) {
                      statusCodes[idx] = -1;
                      results[idx] = "Exception: " + e.getMessage();
                      e.printStackTrace();
                  }
              }
          };
          threads[i].start();
      }

      for (int i = 0; i < threads.length; i++) {
          threads[i].join();
      }

      for (int i = 0; i < statusCodes.length; i++) {
          if (statusCodes[i] != 200) {
              throw new Exception("Join operation failed on worker " + partitions.get(i).assignedFlameWorker +
                                " (status " + statusCodes[i] + "): " + results[i]);
          }
      }

      return new FlamePairRDDImpl(outputTable);
  }
  
  /**
   * Parallel version of join using ParallelLambdaExecutor + BatchedKVSClient.
   * Use this for large datasets with many keys.
   */
  public FlamePairRDD joinParallel(FlamePairRDD other) throws Exception {
      String otherTableName = ((FlamePairRDDImpl) other).getTableName();
      String outputTable = Coordinator.context.generateTableName();

      cis5550.kvs.KVSClient kvsClient = Coordinator.kvs;
      cis5550.tools.Partitioner partitioner = new cis5550.tools.Partitioner();

      int numKVSWorkers = kvsClient.numWorkers();
      Vector<String> workerIDs = new Vector<>();

      for (int i = 0; i < numKVSWorkers; i++) {
          String workerID = kvsClient.getWorkerID(i);
          workerIDs.add(workerID);
      }

      Collections.sort(workerIDs);

      for (int i = 0; i < workerIDs.size(); i++) {
          String workerID = workerIDs.get(i);
          String fromKey = workerID;
          String toKey = (i < workerIDs.size() - 1) ? workerIDs.get(i + 1) : null;

          String address = null;
          for (int j = 0; j < numKVSWorkers; j++) {
              if (kvsClient.getWorkerID(j).equals(workerID)) {
                  address = kvsClient.getWorkerAddress(j);
                  break;
              }
          }

          partitioner.addKVSWorker(address, fromKey, toKey);
      }

      if (workerIDs.size() > 0) {
          String lastWorkerID = workerIDs.get(workerIDs.size() - 1);
          String address = null;
          for (int j = 0; j < numKVSWorkers; j++) {
              if (kvsClient.getWorkerID(j).equals(lastWorkerID)) {
                  address = kvsClient.getWorkerAddress(j);
                  break;
              }
          }
          partitioner.addKVSWorker(address, null, workerIDs.get(0));
      }

      List<String> flameWorkers = Coordinator.getWorkersString();
      for (String worker : flameWorkers) {
          partitioner.addFlameWorker(worker);
      }

      Vector<cis5550.tools.Partitioner.Partition> partitions = partitioner.assignPartitions();

      Thread[] threads = new Thread[partitions.size()];
      String[] results = new String[partitions.size()];
      int[] statusCodes = new int[partitions.size()];

      for (int i = 0; i < partitions.size(); i++) {
          final int idx = i;
          final cis5550.tools.Partitioner.Partition partition = partitions.get(i);

          threads[i] = new Thread("JoinParallel operation #" + (i + 1)) {
              public void run() {
                  try {
                      String url = "http://" + partition.assignedFlameWorker + "/pairRDD/joinParallel";
                      url += "?inputTable=" + java.net.URLEncoder.encode(tableName, "UTF-8");
                      url += "&otherTable=" + java.net.URLEncoder.encode(otherTableName, "UTF-8");
                      url += "&outputTable=" + java.net.URLEncoder.encode(outputTable, "UTF-8");
                      url += "&kvsCoordinator=" + java.net.URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");

                      if (partition.fromKey != null) {
                          url += "&fromKey=" + java.net.URLEncoder.encode(partition.fromKey, "UTF-8");
                      }
                      if (partition.toKeyExclusive != null) {
                          url += "&toKeyExclusive=" + java.net.URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                      }

                      cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("POST", url, null);
                      statusCodes[idx] = response.statusCode();
                      results[idx] = new String(response.body());
                  } catch (Exception e) {
                      statusCodes[idx] = -1;
                      results[idx] = "Exception: " + e.getMessage();
                      e.printStackTrace();
                  }
              }
          };
          threads[i].start();
      }

      for (int i = 0; i < threads.length; i++) {
          threads[i].join();
      }

      for (int i = 0; i < statusCodes.length; i++) {
          if (statusCodes[i] != 200) {
              throw new Exception("JoinParallel operation failed on worker " + partitions.get(i).assignedFlameWorker +
                                " (status " + statusCodes[i] + "): " + results[i]);
          }
      }

      return new FlamePairRDDImpl(outputTable);
  }

  //TODO: possibel refactor
  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
    String otherTableName = ((FlamePairRDDImpl) other).getTableName();
    String outputTable = Coordinator.context.generateTableName();

    cis5550.kvs.KVSClient kvsClient = Coordinator.kvs;
    cis5550.tools.Partitioner partitioner = new cis5550.tools.Partitioner();

    int numKVSWorkers = kvsClient.numWorkers();
    Vector<String> workerIDs = new Vector<>();

    for (int i = 0; i < numKVSWorkers; i++) {
        String workerID = kvsClient.getWorkerID(i);
        workerIDs.add(workerID);
    }

    Collections.sort(workerIDs);

    for (int i = 0; i < workerIDs.size(); i++) {
        String workerID = workerIDs.get(i);
        String fromKey = workerID;
        String toKey = (i < workerIDs.size() - 1) ? workerIDs.get(i + 1) : null;

        String address = null;
        for (int j = 0; j < numKVSWorkers; j++) {
            if (kvsClient.getWorkerID(j).equals(workerID)) {
                address = kvsClient.getWorkerAddress(j);
                break;
            }
        }

        partitioner.addKVSWorker(address, fromKey, toKey);
    }

    if (workerIDs.size() > 0) {
        String lastWorkerID = workerIDs.get(workerIDs.size() - 1);
        String address = null;
        for (int j = 0; j < numKVSWorkers; j++) {
            if (kvsClient.getWorkerID(j).equals(lastWorkerID)) {
                address = kvsClient.getWorkerAddress(j);
                break;
            }
        }
        partitioner.addKVSWorker(address, null, workerIDs.get(0));
    }

    List<String> flameWorkers = Coordinator.getWorkersString();
    for (String worker : flameWorkers) {
        partitioner.addFlameWorker(worker);
    }

    Vector<cis5550.tools.Partitioner.Partition> partitions = partitioner.assignPartitions();

    Thread[] threads = new Thread[partitions.size()];
    String[] results = new String[partitions.size()];
    int[] statusCodes = new int[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
        final int idx = i;
        final cis5550.tools.Partitioner.Partition partition = partitions.get(i);

        threads[i] = new Thread("Cogroup operation #" + (i + 1)) {
            public void run() {
                try {
                    String url = "http://" + partition.assignedFlameWorker + "/pairRDD/cogroup";
                    url += "?inputTable=" + java.net.URLEncoder.encode(tableName, "UTF-8");
                    url += "&otherTable=" + java.net.URLEncoder.encode(otherTableName, "UTF-8");
                    url += "&outputTable=" + java.net.URLEncoder.encode(outputTable, "UTF-8");
                    url += "&kvsCoordinator=" + java.net.URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");

                    if (partition.fromKey != null) {
                        url += "&fromKey=" + java.net.URLEncoder.encode(partition.fromKey, "UTF-8");
                    }
                    if (partition.toKeyExclusive != null) {
                        url += "&toKeyExclusive=" + java.net.URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                    }

                    cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("POST", url, null);
                    statusCodes[idx] = response.statusCode();
                    results[idx] = new String(response.body());
                } catch (Exception e) {
                    statusCodes[idx] = -1;
                    results[idx] = "Exception: " + e.getMessage();
                    e.printStackTrace();
                }
            }
        };
        threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
        threads[i].join();
    }

    for (int i = 0; i < statusCodes.length; i++) {
        if (statusCodes[i] != 200) {
            throw new Exception("Cogroup operation failed on worker " + partitions.get(i).assignedFlameWorker +
                              " (status " + statusCodes[i] + "): " + results[i]);
        }
    }

    return new FlamePairRDDImpl(outputTable);
  }

  /**
   * Parallel version of foldByKey with batched I/O for better performance.
   * Uses BatchedKVSClient to reduce network overhead and improve throughput.
   */
  public FlamePairRDD foldByKeyParallel(String zeroElement, TwoStringsToString lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/pairRDD/foldByKeyParallel", tableName, serializedLambda, zeroElement);

    return new FlamePairRDDImpl(outputTable);
  }

  /**
   * Parallel version of cogroup with batched I/O and optimized key processing.
   * Processes keys in parallel batches instead of collecting all keys first,
   * reducing memory usage and improving performance on large datasets.
   */
  public FlamePairRDD cogroupParallel(FlamePairRDD other) throws Exception {
    String otherTableName = ((FlamePairRDDImpl) other).getTableName();
    String outputTable = Coordinator.context.generateTableName();

    cis5550.kvs.KVSClient kvsClient = Coordinator.kvs;
    cis5550.tools.Partitioner partitioner = new cis5550.tools.Partitioner();

    int numKVSWorkers = kvsClient.numWorkers();
    Vector<String> workerIDs = new Vector<>();

    for (int i = 0; i < numKVSWorkers; i++) {
        String workerID = kvsClient.getWorkerID(i);
        workerIDs.add(workerID);
    }

    Collections.sort(workerIDs);

    for (int i = 0; i < workerIDs.size(); i++) {
        String workerID = workerIDs.get(i);
        String fromKey = workerID;
        String toKey = (i < workerIDs.size() - 1) ? workerIDs.get(i + 1) : null;

        String address = null;
        for (int j = 0; j < numKVSWorkers; j++) {
            if (kvsClient.getWorkerID(j).equals(workerID)) {
                address = kvsClient.getWorkerAddress(j);
                break;
            }
        }

        partitioner.addKVSWorker(address, fromKey, toKey);
    }

    if (workerIDs.size() > 0) {
        String lastWorkerID = workerIDs.get(workerIDs.size() - 1);
        String address = null;
        for (int j = 0; j < numKVSWorkers; j++) {
            if (kvsClient.getWorkerID(j).equals(lastWorkerID)) {
                address = kvsClient.getWorkerAddress(j);
                break;
            }
        }
        partitioner.addKVSWorker(address, null, workerIDs.get(0));
    }

    List<String> flameWorkers = Coordinator.getWorkersString();
    
    // Health check: filter out unhealthy Flame workers before partition assignment
    List<String> healthyFlameWorkers = new ArrayList<>();
    for (String worker : flameWorkers) {
        boolean isHealthy = false;
        try {
            // Simple health check: try to connect to worker
            cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("GET", "http://" + worker + "/", null);
            if (response.statusCode() == 200 || response.statusCode() == 404) {
                isHealthy = true;
            }
        } catch (Exception e) {
            // Worker is unreachable
            logger.warn("Flame worker " + worker + " is unreachable, skipping: " + e.getMessage());
        }
        
        if (isHealthy) {
            healthyFlameWorkers.add(worker);
        }
    }
    
    if (healthyFlameWorkers.size() == 0) {
        throw new Exception("No healthy Flame workers available for cogroupParallel!");
    }
    
    if (healthyFlameWorkers.size() < flameWorkers.size()) {
        logger.warn("Only " + healthyFlameWorkers.size() + " of " + flameWorkers.size() + " Flame workers are healthy for cogroupParallel");
    }
    
    for (String worker : healthyFlameWorkers) {
        partitioner.addFlameWorker(worker);
    }

    Vector<cis5550.tools.Partitioner.Partition> partitions = partitioner.assignPartitions();

    Thread[] threads = new Thread[partitions.size()];
    String[] results = new String[partitions.size()];
    int[] statusCodes = new int[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
        final int idx = i;
        final cis5550.tools.Partitioner.Partition partition = partitions.get(i);

        threads[i] = new Thread("CogroupParallel operation #" + (i + 1)) {
            public void run() {
                try {
                    String url = "http://" + partition.assignedFlameWorker + "/pairRDD/cogroupParallel";
                    url += "?inputTable=" + java.net.URLEncoder.encode(tableName, "UTF-8");
                    url += "&otherTable=" + java.net.URLEncoder.encode(otherTableName, "UTF-8");
                    url += "&outputTable=" + java.net.URLEncoder.encode(outputTable, "UTF-8");
                    url += "&kvsCoordinator=" + java.net.URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");

                    if (partition.fromKey != null) {
                        url += "&fromKey=" + java.net.URLEncoder.encode(partition.fromKey, "UTF-8");
                    }
                    if (partition.toKeyExclusive != null) {
                        url += "&toKeyExclusive=" + java.net.URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                    }

                    cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("POST", url, null);
                    statusCodes[idx] = response.statusCode();
                    results[idx] = new String(response.body());
                } catch (Exception e) {
                    statusCodes[idx] = -1;
                    results[idx] = "Exception: " + e.getMessage();
                    e.printStackTrace();
                }
            }
        };
        threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
        threads[i].join();
    }

    for (int i = 0; i < statusCodes.length; i++) {
        if (statusCodes[i] != 200) {
            throw new Exception("CogroupParallel operation failed on worker " + partitions.get(i).assignedFlameWorker +
                              " (status " + statusCodes[i] + "): " + results[i]);
        }
    }

    return new FlamePairRDDImpl(outputTable);
  }

}
