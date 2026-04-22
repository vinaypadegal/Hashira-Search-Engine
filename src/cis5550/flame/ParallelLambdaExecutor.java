package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Parallel Lambda Executor following standard practices (Spark/Flink style).
 * 
 * Architecture:
 * 1. Single-threaded scan (KVS iterator is not thread-safe)
 * 2. Bounded queue for backpressure
 * 3. Fixed thread pool for lambda execution
 * 4. Results flow to BatchedKVSIO for efficient writes
 * 
 * This pattern maximizes CPU utilization while keeping I/O efficient.
 * 
 * Usage:
 * ```
 * try (ParallelLambdaExecutor<String, String> executor = 
 *          new ParallelLambdaExecutor<>(kvs, outputTable, numThreads)) {
 *     
 *     executor.execute(
 *         rows,                          // Iterator<Row> from scan
 *         row -> row.get("value"),       // Extract input from row
 *         (input, collector) -> {        // Lambda that produces outputs
 *             for (String s : lambda.op(input)) {
 *                 collector.accept(generateKey(), s);
 *             }
 *         }
 *     );
 * }
 * ```
 */
public class ParallelLambdaExecutor<I, O> implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ParallelLambdaExecutor.class);
    
    // Configuration
    private static final int DEFAULT_QUEUE_SIZE = 1000;      // Bounded queue size
    private static final int DEFAULT_THREAD_COUNT = 
        Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
    
    private final KVSClient kvs;
    private final String outputTable;
    private final BatchedKVSClient sharedWriter;  // Single shared writer for all writes (output + internal)
    private final ExecutorService lambdaPool;
    private final BlockingQueue<WorkItem<I>> workQueue;
    private final int numThreads;
    
    // Statistics
    private final AtomicLong rowsProcessed = new AtomicLong(0);
    private final AtomicLong outputsProduced = new AtomicLong(0);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    
    // State
    private volatile Throwable workerException = null;
    private final CountDownLatch workersComplete;
    
    // Work item wrapper
    private static class WorkItem<I> {
        final String rowKey;
        final I input;
        final boolean poison; // Sentinel to signal workers to stop
        
        WorkItem(String rowKey, I input) {
            this.rowKey = rowKey;
            this.input = input;
            this.poison = false;
        }
        
        // Poison pill constructor
        private WorkItem() {
            this.rowKey = null;
            this.input = null;
            this.poison = true;
        }
        
        static <I> WorkItem<I> poison() {
            return new WorkItem<>();
        }
    }
    
    /**
     * Output collector - lambda uses this to emit results.
     * Thread-safe wrapper around BatchedKVSClient.
     * Also provides access to shared BatchedKVSClient for internal writes.
     */
    public interface OutputCollector {
        void emit(String rowKey, String value) throws IOException;
        void emit(String rowKey, String column, String value) throws IOException;
        
        /**
         * Get the shared BatchedKVSClient for internal writes.
         * All lambda threads share this client - writes are pooled and batched automatically.
         * DO NOT call flushAndClose() - the executor handles that.
         */
        BatchedKVSClient getSharedWriter();
        
        /**
         * Get the underlying KVSClient for reads (not batched).
         */
        KVSClient getKVSClient();
    }
    
    /**
     * Lambda interface that takes input and collector
     */
    @FunctionalInterface
    public interface LambdaWithCollector<I> {
        void apply(I input, String sourceRowKey, OutputCollector collector) throws Exception;
    }
    
    public ParallelLambdaExecutor(KVSClient kvs, String outputTable) {
        this(kvs, outputTable, DEFAULT_THREAD_COUNT, DEFAULT_QUEUE_SIZE);
    }
    
    public ParallelLambdaExecutor(KVSClient kvs, String outputTable, int numThreads) {
        this(kvs, outputTable, numThreads, DEFAULT_QUEUE_SIZE);
    }
    
    public ParallelLambdaExecutor(KVSClient kvs, String outputTable, int numThreads, int queueSize) {
        this.kvs = kvs;
        this.outputTable = outputTable;
        this.sharedWriter = new BatchedKVSClient(kvs);  // Single writer for all tables
        this.numThreads = numThreads;
        this.lambdaPool = Executors.newFixedThreadPool(numThreads);
        this.workQueue = new LinkedBlockingQueue<>(queueSize);
        this.workersComplete = new CountDownLatch(numThreads);
    }
    
    /**
     * Execute lambda in parallel over rows.
     * 
     * @param rows Iterator over input rows (from KVS scan)
     * @param inputExtractor Function to extract input value from Row
     * @param lambda The lambda function that processes input and emits outputs
     */
    public void execute(
            Iterator<Row> rows,
            Function<Row, I> inputExtractor,
            LambdaWithCollector<I> lambda
    ) throws Exception {
        
        // Create output collector - uses single shared writer for all writes
        OutputCollector collector = new OutputCollector() {
            @Override
            public void emit(String rowKey, String value) throws IOException {
                sharedWriter.put(outputTable, rowKey, "value", value);
                outputsProduced.incrementAndGet();
            }
            
            @Override
            public void emit(String rowKey, String column, String value) throws IOException {
                sharedWriter.put(outputTable, rowKey, column, value);
                outputsProduced.incrementAndGet();
            }
            
            @Override
            public BatchedKVSClient getSharedWriter() {
                return sharedWriter;  // Single writer for all tables
            }
            
            @Override
            public KVSClient getKVSClient() {
                return kvs;  // For reads
            }
        };
        
        // Start worker threads
        for (int i = 0; i < numThreads; i++) {
            lambdaPool.submit(() -> workerLoop(lambda, collector));
        }
        
        // Producer: single-threaded scan, feed to queue
        long startTime = System.currentTimeMillis();
        int producedCount = 0;
        
        try {
            while (rows.hasNext()) {
                // Check for worker exceptions
                if (workerException != null) {
                    throw new RuntimeException("Worker failed", workerException);
                }
                
                Row row = rows.next();
                if (row == null) continue;
                
                String rowKey = row.key();
                if (rowKey == null) {
                    logger.warn("Skipping row with null key");
                    continue;
                }
                
                I input = inputExtractor.apply(row);
                if (input == null) continue;
                
                // Put to queue (blocks if full - backpressure)
                workQueue.put(new WorkItem<>(rowKey, input));
                producedCount++;
                
                // Log progress
                if (producedCount % 1000 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    logger.info("Producer: " + producedCount + " rows queued (" + 
                               (producedCount * 1000L / Math.max(1, elapsed)) + " rows/sec), " +
                               "queue size: " + workQueue.size() + ", active workers: " + activeWorkers.get());
                }
            }
        } finally {
            // Send poison pills to stop workers
            for (int i = 0; i < numThreads; i++) {
                try {
                    workQueue.put(WorkItem.poison());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        // Wait for all workers to complete
        workersComplete.await();
        
        // Check for worker exceptions
        if (workerException != null) {
            throw new RuntimeException("Worker failed", workerException);
        }
        
        // Flush and wait for all batched writes
        try {
            sharedWriter.flushAndWait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while flushing writer", e);
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("ParallelLambdaExecutor complete: " + rowsProcessed.get() + " rows processed, " +
                    outputsProduced.get() + " outputs produced in " + totalTime + "ms " +
                    "(" + (rowsProcessed.get() * 1000L / Math.max(1, totalTime)) + " rows/sec)" +
                    ", writer stats: puts=" + sharedWriter.getTotalPuts() + 
                    ", flushed=" + sharedWriter.getTotalRowsFlushed() + 
                    ", batches=" + sharedWriter.getTotalBatches());
    }
    
    /**
     * Worker loop - takes items from queue, applies lambda, emits to collector
     */
    private void workerLoop(LambdaWithCollector<I> lambda, OutputCollector collector) {
        try {
            while (true) {
                WorkItem<I> item = workQueue.take();
                
                // Check for poison pill
                if (item.poison) {
                    break;
                }
                
                activeWorkers.incrementAndGet();
                try {
                    lambda.apply(item.input, item.rowKey, collector);
                    rowsProcessed.incrementAndGet();
                } catch (Exception e) {
                    logger.error("Lambda execution failed: " + e.getMessage(), e);
                    workerException = e;
                    break;
                } finally {
                    activeWorkers.decrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workersComplete.countDown();
        }
    }
    
    @Override
    public void close() throws Exception {
        lambdaPool.shutdownNow();
        sharedWriter.close();
    }
    
    // Statistics
    public long getRowsProcessed() { return rowsProcessed.get(); }
    public long getOutputsProduced() { return outputsProduced.get(); }
    public int getActiveWorkers() { return activeWorkers.get(); }
    public int getQueueSize() { return workQueue.size(); }
    public int getNumThreads() { return numThreads; }
    
    /**
     * Get shared writer for stats access.
     */
    public BatchedKVSClient getSharedWriter() { return sharedWriter; }
}

