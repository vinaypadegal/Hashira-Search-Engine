package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import java.net.*;
import java.util.*;
import java.util.Map;

public class FlameContextImpl implements FlameContext {

  private static final Logger logger = Logger.getLogger(FlameContextImpl.class);
  
  private String jarName;
  private KVSClient kvsClient;
  private StringBuilder outputBuffer;
  private int tableCounter;
  private long jobID;

  public FlameContextImpl(String jarName) {
    this.jarName = jarName;
    this.kvsClient = Coordinator.kvs;
    this.outputBuffer = new StringBuilder();
    this.tableCounter = 0;
    this.jobID = System.currentTimeMillis();
  }

  public KVSClient getKVS() {
    return kvsClient;
  }

  public void output(String s) {
    outputBuffer.append(s);
  }

  public String getOutput() {
    return outputBuffer.toString();
  }

  public String getJarName() {
    return jarName;
  }

  public String generateTableName() {
    return "table-" + jobID + "-" + (tableCounter++);
  }

  public String invokeOperation(String operationPath, String inputTable, byte[] lambda, String zeroElement) throws Exception {
    String outputTable = generateTableName();

    Partitioner partitioner = new Partitioner();

    int numKVSWorkers = kvsClient.numWorkers();
    Vector<String> workerIDs = new Vector<>();

    for (int i = 0; i < numKVSWorkers; i++) {
      String workerID = kvsClient.getWorkerID(i);
      String workerAddress = kvsClient.getWorkerAddress(i);
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
    // Log worker URLs in a more readable format
    List<String> workerUrls = flameWorkers.stream().map(w -> "http://" + w).toList();
    logger.info("Distributing operation " + operationPath + " to " + flameWorkers.size() + " Flame worker(s): " + workerUrls);
    for (String worker : flameWorkers) {
      partitioner.addFlameWorker(worker);
    }

    Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
    logger.info("Created " + partitions.size() + " partitions");
    
    // Calculate statistics
    Map<String, Integer> workerCounts = new HashMap<>();
    int sameIPCount = 0;
    int differentIPCount = 0;
    for (Partitioner.Partition p : partitions) {
      workerCounts.put(p.assignedFlameWorker, workerCounts.getOrDefault(p.assignedFlameWorker, 0) + 1);
      String kvsIP = p.kvsWorker.split(":")[0];
      String flameIP = p.assignedFlameWorker.split(":")[0];
      if (kvsIP.equals(flameIP)) {
        sameIPCount++;
      } else {
        differentIPCount++;
      }
    }
    
    // Summary logging
    logger.info("Partition assignment: " + sameIPCount + " same-IP / " + partitions.size() + 
               " total (" + (partitions.size() > 0 ? String.format("%.1f", sameIPCount * 100.0 / partitions.size()) : "0") + "%)");

    Thread[] threads = new Thread[partitions.size()];
    String[] results = new String[partitions.size()];
    int[] statusCodes = new int[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
      final int idx = i;
      final Partitioner.Partition partition = partitions.get(i);
      
      logger.info("Assigning partition " + (i+1) + " to worker: " + partition.assignedFlameWorker + 
                         " (keys: " + partition.fromKey + " to " + partition.toKeyExclusive + ")");

      threads[i] = new Thread("Operation " + operationPath + " #" + (i + 1)) {
        public void run() {
          try {
            String url = "http://" + partition.assignedFlameWorker + operationPath;
            logger.info("Sending POST request to: " + url);
            url += "?inputTable=" + URLEncoder.encode(inputTable, "UTF-8");
            url += "&outputTable=" + URLEncoder.encode(outputTable, "UTF-8");
            url += "&kvsCoordinator=" + URLEncoder.encode(kvsClient.getCoordinator(), "UTF-8");

            if (partition.fromKey != null) {
              url += "&fromKey=" + URLEncoder.encode(partition.fromKey, "UTF-8");
            }
            if (partition.toKeyExclusive != null) {
              url += "&toKeyExclusive=" + URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
            }

            if (zeroElement != null) {
              url += "&zeroElement=" + URLEncoder.encode(zeroElement, "UTF-8");
            }

            HTTP.Response response = HTTP.doRequest("POST", url, lambda);
            statusCodes[idx] = response.statusCode();
            results[idx] = new String(response.body());
            logger.info("Worker " + partition.assignedFlameWorker + " completed with status " + statusCodes[idx]);
          } catch (Exception e) {
            statusCodes[idx] = -1;
            results[idx] = "Exception: " + e.getMessage();
            logger.error("Worker " + partition.assignedFlameWorker + " FAILED: " + e.getMessage(), e);
          }
        }
      };
      threads[i].start();
    }

    logger.info("Waiting for all " + threads.length + " partitions to complete...");
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
      logger.debug("Partition " + (i+1) + " thread finished");
    }

    logger.info("All partitions completed, checking status codes...");
    for (int i = 0; i < statusCodes.length; i++) {
      if (statusCodes[i] != 200) {
        logger.error("Partition " + (i+1) + " on worker " + partitions.get(i).assignedFlameWorker + 
                          " failed with status " + statusCodes[i] + ": " + results[i]);
        throw new Exception("Operation failed on worker " + partitions.get(i).assignedFlameWorker +
                          " (status " + statusCodes[i] + "): " + results[i]);
      }
    }
    logger.info("All partitions completed successfully");

    return outputTable;
  }

  public FlameRDD parallelize(List<String> list) throws Exception {
    String tableName = generateTableName();

    // Build all rows first, then bulk write
    List<Row> rows = new ArrayList<>();
    int index = 0;
    for (String value : list) {
      String rowKey = Hasher.hash(String.valueOf(index));
      Row row = new Row(rowKey);
      row.put("value", value);
      rows.add(row);
      index++;
    }
    
    // Bulk write all rows at once (much faster than individual puts)
    if (!rows.isEmpty()) {
      kvsClient.bulkPutRows(tableName, rows);
    }

    return new FlameRDDImpl(tableName);
  }

  @Override
  public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
    byte[] lambdaBytes = Serializer.objectToByteArray(lambda);
    String outputTableName = invokeOperation("/fromTable", tableName, lambdaBytes, null);
    return new FlameRDDImpl(outputTableName);
  }

  @Override
  public FlameRDD fromTableParallel(String tableName, RowToString lambda) throws Exception {
    byte[] lambdaBytes = Serializer.objectToByteArray(lambda);
    String outputTableName = invokeOperation("/fromTableParallel", tableName, lambdaBytes, null);
    return new FlameRDDImpl(outputTableName);
  }
}
