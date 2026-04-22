package cis5550.flame;

import cis5550.kvs.Row;
import java.util.*;

public class FlameRDDImpl implements FlameRDD {

  private String tableName;

  public FlameRDDImpl(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public List<String> collect() throws Exception {
    List<String> result = new ArrayList<>();

    Iterator<Row> rows = Coordinator.kvs.scan(tableName);
    while (rows.hasNext()) {
      Row row = rows.next();
      String value = row.get("value");
      if (value != null) {
        result.add(value);
      }
    }

    return result;
  }

  @Override
  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/rdd/flatMap", tableName, serializedLambda, null);

    return new FlameRDDImpl(outputTable);
  }

  /**
   * Parallel version of flatMap using ParallelLambdaExecutor + BatchedKVSIO.
   * 
   * This version uses:
   * - Producer-consumer pattern with bounded queue for backpressure
   * - Parallel lambda execution on multiple threads
   * - Worker-grouped batching for efficient KVS writes
   * - Time and size-based flush triggers
   * - Provides shared BatchedKVSClient for internal writes pooled across all threads
   * 
   * Use this for CPU-intensive lambda operations on large datasets.
   */
  @Override
  public FlameRDD flatMapParallel(StringToIterableWithContext lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/rdd/flatMapParallel", tableName, serializedLambda, null);

    return new FlameRDDImpl(outputTable);
  }

  @Override
  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/rdd/mapToPair", tableName, serializedLambda, null);

    return new FlamePairRDDImpl(outputTable);
  }
  
  /**
   * Parallel version of mapToPair using ParallelLambdaExecutor + BatchedKVSClient.
   * Provides shared BatchedKVSClient for internal writes pooled across all threads.
   * Use this for CPU-intensive lambda operations on large datasets.
   */
  @Override
  public FlamePairRDD mapToPairParallel(StringToPairWithContext lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation("/rdd/mapToPairParallel", tableName, serializedLambda, null);

    return new FlamePairRDDImpl(outputTable);
  }

  @Override
  public FlameRDD intersection(FlameRDD r) throws Exception {
    List<String> thisElements = this.collect();
    List<String> otherElements = r.collect();

    Set<String> thisSet = new HashSet<>(thisElements);
    Set<String> otherSet = new HashSet<>(otherElements);

    thisSet.retainAll(otherSet);

    String outputTable = Coordinator.context.generateTableName();

    int counter = 0;
    for (String element : thisSet) {
      String uniqueKey = cis5550.tools.Hasher.hash(outputTable + "-" + (counter++));
      Coordinator.kvs.put(outputTable, uniqueKey, "value", element);
    }

    return new FlameRDDImpl(outputTable);
  }

  @Override
  public FlameRDD sample(double f) throws Exception {
    List<String> elements = this.collect();

    Random random = new Random();

    String outputTable = Coordinator.context.generateTableName();

    int counter = 0;
    for (String element : elements) {
      if (random.nextDouble() < f) {
        String uniqueKey = cis5550.tools.Hasher.hash(outputTable + "-" + (counter++));
        Coordinator.kvs.put(outputTable, uniqueKey, "value", element);
      }
    }

    return new FlameRDDImpl(outputTable);
  }

  @Override
  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
    List<String> elements = this.collect();

    Map<String, List<String>> groups = new HashMap<>();
    for (String element : elements) {
      String key = lambda.op(element);
      if (key != null) {
        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(element);
      }
    }

    String outputTable = Coordinator.context.generateTableName();

    for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
      String groupKey = entry.getKey();
      List<String> groupValues = entry.getValue();

      String commaSeparatedValues = String.join(",", groupValues);

      Coordinator.kvs.put(outputTable, groupKey, "value", commaSeparatedValues);
    }

    return new FlamePairRDDImpl(outputTable);
  }

  @Override
  public int count() throws Exception {
    return Coordinator.kvs.count(tableName);
  }

  @Override
  public void saveAsTable(String tableNameArg) throws Exception {
    Coordinator.kvs.rename(tableName, tableNameArg);
    tableName = tableNameArg;
  }


  //this could be coordinator level but doing it in a distributed fashion is more scalable
  @Override
  public FlameRDD distinct() throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(
          (FlameRDD.StringToString) value -> value //just passing identity in
      );

      String outputTable = Coordinator.context.invokeOperation(
          "/rdd/distinct",
          tableName,
          serializedLambda,
          null
      );

      return new FlameRDDImpl(outputTable);
  }
  
  /**
   * Parallel version of distinct using ParallelLambdaExecutor + BatchedKVSClient.
   * More efficient for large datasets.
   */
  public FlameRDD distinctParallel() throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(
          (FlameRDD.StringToString) value -> value //just passing identity in
      );

      String outputTable = Coordinator.context.invokeOperation(
          "/rdd/distinctParallel",
          tableName,
          serializedLambda,
          null
      );

      return new FlameRDDImpl(outputTable);
  }

  @Override
  public void destroy() throws Exception {
    if (tableName != null) {
      try {
        Coordinator.kvs.delete(tableName);
        tableName = null; // Mark as destroyed
      } catch (Exception e) {
        throw e;
      }
    }
  }

  @Override
  public Vector<String> take(int num) throws Exception {
    Vector<String> result = new Vector<>();
    Iterator<Row> rows = Coordinator.kvs.scan(tableName);

    int taken = 0;
    while (rows.hasNext() && taken < num) {
        Row row = rows.next();
        String value = row.get("value");
        if (value != null) {
            result.add(value);
            taken++;
        }
    }
    return result;
  }

  // TODO: a lot of copy pasta here from invokeOperation. Should consider doing some refactoring
  @Override
  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

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
      String[] workerAccumulators = new String[partitions.size()];  
      int[] statusCodes = new int[partitions.size()];

      for (int i = 0; i < partitions.size(); i++) {
          final int idx = i;
          final cis5550.tools.Partitioner.Partition partition = partitions.get(i);

          threads[i] = new Thread("Fold operation #" + (i + 1)) {
              public void run() {
                  try {
                      String url = "http://" + partition.assignedFlameWorker + "/rdd/fold";
                      url += "?inputTable=" + java.net.URLEncoder.encode(tableName, "UTF-8");
                      url += "&kvsCoordinator=" + java.net.URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");
                      url += "&zeroElement=" + java.net.URLEncoder.encode(zeroElement, "UTF-8");

                      if (partition.fromKey != null) {
                          url += "&fromKey=" + java.net.URLEncoder.encode(partition.fromKey, "UTF-8");
                      }
                      if (partition.toKeyExclusive != null) {
                          url += "&toKeyExclusive=" + java.net.URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                      }

                      cis5550.tools.HTTP.Response response = cis5550.tools.HTTP.doRequest("POST", url, serializedLambda);
                      statusCodes[idx] = response.statusCode();
                      workerAccumulators[idx] = new String(response.body());  
                  } catch (Exception e) {
                      statusCodes[idx] = -1;
                      workerAccumulators[idx] = null;
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
              throw new Exception("Fold operation failed on worker " + partitions.get(i).assignedFlameWorker +
                                " (status " + statusCodes[i] + ")");
          }
      }

      String finalAccumulator = zeroElement;
      for (String workerResult : workerAccumulators) {
          if (workerResult != null) {
              finalAccumulator = lambda.op(finalAccumulator, workerResult);
          }
      }

      return finalAccumulator;
  }

  @Override
  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/rdd/flatMapToPair",  
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
  public FlamePairRDD flatMapToPairParallel(StringToPairIterableWithContext lambda) throws Exception {
      byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

      String outputTable = Coordinator.context.invokeOperation(
          "/rdd/flatMapToPairParallel",  
          tableName,
          serializedLambda,
          null
      );

      return new FlamePairRDDImpl(outputTable);  
  }

  @Override
  public FlameRDD filter(StringToBoolean lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation(
        "/rdd/filter",
        tableName,
        serializedLambda,
        null
    );

    return new FlameRDDImpl(outputTable);
  }
  
  /**
   * Parallel version of filter using ParallelLambdaExecutor + BatchedKVSClient.
   * Provides shared BatchedKVSClient for internal writes pooled across all threads.
   * Use this for CPU-intensive predicate operations on large datasets.
   */
  @Override
  public FlameRDD filterParallel(StringToBooleanWithContext lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation(
        "/rdd/filterParallel",
        tableName,
        serializedLambda,
        null
    );

    return new FlameRDDImpl(outputTable);
  }

  @Override
  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
    byte[] serializedLambda = cis5550.tools.Serializer.objectToByteArray(lambda);

    String outputTable = Coordinator.context.invokeOperation(
        "/rdd/mapPartitions",
        tableName,
        serializedLambda,
        null
    );

    return new FlameRDDImpl(outputTable);
  }
}
