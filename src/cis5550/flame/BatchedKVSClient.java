package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batched KVS Client for use inside Flame job lambdas.
 * 
 * Pools put() requests and sends them in batches to reduce network overhead.
 * Thread-safe for use across parallel lambda executions.
 * 
 * Features:
 * - Groups writes by table and destination KVS worker
 * - Flushes by size (N rows) or time (T ms)
 * - Async HTTP execution to KVS
 * - Drop-in replacement for direct KVSClient.put() calls
 * 
 * Usage in lambda:
 * ```
 * // Create once per partition (or share across lambdas)
 * BatchedKVSClient batchedKvs = new BatchedKVSClient(kvs);
 * 
 * // Use like regular kvs.put()
 * batchedKvs.put("table", "row", "col", "value");
 * 
 * // MUST call at end
 * batchedKvs.flushAndClose();
 * ```
 */
public class BatchedKVSClient implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(BatchedKVSClient.class);
    
    // Configuration - tune based on workload
    private static final int BATCH_SIZE = 400;           // Rows per batch before flush
    private static final long FLUSH_INTERVAL_MS = 200;   // Max time before flush
    private static final int MAX_CONCURRENT_WRITES = 8;  // Parallel HTTP requests
    
    private final KVSClient kvs;
    
    // Buffer: table -> (rowKey -> Row)
    // We accumulate columns for the same row before flushing
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tableBuffers;
    
    // Track total rows per table for flush threshold
    private final ConcurrentHashMap<String, AtomicLong> tableRowCounts;
    
    // Fine-grained locks per table
    private final ConcurrentHashMap<String, Object> tableLocks;
    
    // Async write execution
    private final ExecutorService writeExecutor;
    // Use ConcurrentLinkedQueue for lock-free, thread-safe tracking of pending writes
    private final ConcurrentLinkedQueue<Future<?>> pendingWrites;
    
    // Scheduled flush for time-based batching
    private final ScheduledExecutorService flushScheduler;
    private final ScheduledFuture<?> flushTask;
    
    // Statistics
    private final AtomicLong totalPuts = new AtomicLong(0);
    private final AtomicLong totalRowsFlushed = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    
    // State
    private volatile boolean closed = false;
    
    public BatchedKVSClient(KVSClient kvs) {
        this.kvs = kvs;
        this.tableBuffers = new ConcurrentHashMap<>();
        this.tableRowCounts = new ConcurrentHashMap<>();
        this.tableLocks = new ConcurrentHashMap<>();
        this.writeExecutor = Executors.newFixedThreadPool(MAX_CONCURRENT_WRITES);
        this.pendingWrites = new ConcurrentLinkedQueue<>();
        
        // Schedule periodic flush
        this.flushScheduler = Executors.newSingleThreadScheduledExecutor();
        this.flushTask = flushScheduler.scheduleAtFixedRate(
            this::flushAllTablesAsync,
            FLUSH_INTERVAL_MS,
            FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Batched put operation.
     * Buffers the write and flushes when batch size or time threshold is reached.
     */
    public void put(String tableName, String rowKey, String column, String value) throws IOException {
        put(tableName, rowKey, column, value.getBytes());
    }
    
    /**
     * Batched put operation (byte[] value).
     */
    public void put(String tableName, String rowKey, String column, byte[] value) throws IOException {
        if (closed) {
            throw new IllegalStateException("BatchedKVSClient is closed");
        }
        
        if (tableName == null || rowKey == null || column == null) {
            throw new IllegalArgumentException("tableName, rowKey, and column cannot be null");
        }
        
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        
        synchronized (lock) {
            ConcurrentHashMap<String, Row> buffer = tableBuffers.computeIfAbsent(
                tableName, k -> new ConcurrentHashMap<>());
            
            // Get or create row in buffer
            // computeIfAbsent will return existing row if present, or create new one
            Row row = buffer.computeIfAbsent(rowKey, k -> {
                tableRowCounts.computeIfAbsent(tableName, t -> new AtomicLong(0)).incrementAndGet();
                return new Row(rowKey);
            });
            
            // Add column to row (merges with existing columns in buffer)
            row.put(column, value);
            totalPuts.incrementAndGet();
            
            // Check if we should flush this table
            AtomicLong rowCount = tableRowCounts.get(tableName);
            if (rowCount != null && rowCount.get() >= BATCH_SIZE) {
                logger.debug("Flush trigger (size) table=" + tableName + " rows=" + rowCount.get());
                flushTableAsync(tableName, "size");
            }
        }
    }
    
    /**
     * Batched putRow operation.
     * Buffers the entire row and flushes when batch threshold is reached.
     */
    public void putRow(String tableName, Row row) throws IOException {
        if (closed) {
            throw new IllegalStateException("BatchedKVSClient is closed");
        }
        
        if (tableName == null || row == null || row.key() == null) {
            throw new IllegalArgumentException("tableName and row cannot be null");
        }
        
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());
        
        synchronized (lock) {
            ConcurrentHashMap<String, Row> buffer = tableBuffers.computeIfAbsent(
                tableName, k -> new ConcurrentHashMap<>());
            
            // Merge with existing row if present, or add new
            Row existing = buffer.get(row.key());
            if (existing != null) {
                // Merge columns
                for (String col : row.columns()) {
                    existing.put(col, row.getBytes(col));
                }
            } else {
                buffer.put(row.key(), row);
                tableRowCounts.computeIfAbsent(tableName, t -> new AtomicLong(0)).incrementAndGet();
            }
            
            totalPuts.addAndGet(row.columns().size());
            
            // Check if we should flush this table
            AtomicLong rowCount = tableRowCounts.get(tableName);
            if (rowCount != null && rowCount.get() >= BATCH_SIZE) {
                logger.debug("Flush trigger (size) table=" + tableName + " rows=" + rowCount.get());
                flushTableAsync(tableName, "size");
            }
        }
    }
    
    /**
     * Flush a specific table asynchronously
     */
    private void flushTableAsync(String tableName, String reason) {
        Object lock = tableLocks.get(tableName);
        if (lock == null) return;
        
        List<Row> toFlush;
        synchronized (lock) {
            ConcurrentHashMap<String, Row> buffer = tableBuffers.get(tableName);
            if (buffer == null || buffer.isEmpty()) return;
            
            toFlush = new ArrayList<>(buffer.values());
            buffer.clear();
            
            AtomicLong rowCount = tableRowCounts.get(tableName);
            if (rowCount != null) rowCount.set(0);
        }
        if (toFlush.isEmpty()) return;
        
        final List<Row> rows = toFlush;
        Future<?> future = writeExecutor.submit(() -> {
            try {
                // Use bulk put to send all rows in one HTTP request per worker
                kvs.bulkPutRows(tableName, rows);
                totalRowsFlushed.addAndGet(rows.size());
                totalBatches.incrementAndGet();
                logger.info("Flushed " + rows.size() + " rows to table " + tableName +
                    " reason=" + reason +
                    " bufferedAfter=" + tableRowCounts.getOrDefault(tableName, new AtomicLong(0)).get());
            } catch (IOException e) {
                logger.error("Failed to flush " + rows.size() + " rows to " + tableName + ": " + e.getMessage(), e);
                throw new RuntimeException("Batch write failed", e);
            }
        });
        
        pendingWrites.add(future);
        cleanupCompletedFutures();
    }
    
    /**
     * Flush all tables asynchronously (called by timer)
     */
    private void flushAllTablesAsync() {
        if (closed) return;
        
        for (String tableName : tableBuffers.keySet()) {
            logger.debug("Flush trigger (timer) table=" + tableName + " rows=" +
                tableRowCounts.getOrDefault(tableName, new AtomicLong(0)).get());
            flushTableAsync(tableName, "timer");
        }
    }
    
    /**
     * Clean up completed futures.
     * Uses removeIf() which is thread-safe with ConcurrentLinkedQueue.
     */
    private void cleanupCompletedFutures() {
        pendingWrites.removeIf(future -> {
            if (future.isDone()) {
                try {
                    future.get(); // Check for exceptions
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Write task interrupted: " + e.getMessage(), e);
                } catch (ExecutionException | CancellationException e) {
                    logger.error("Write task failed: " + e.getMessage(), e);
                } 
                return true; // Remove from list
            }
            return false;
        });
    }
    
    /**
     * Passthrough for reads - no batching on reads currently
     */
    public Row getRow(String tableName, String rowKey) throws IOException {
        return kvs.getRow(tableName, rowKey);
    }
    
    /**
     * Passthrough for reads
     */
    public byte[] get(String tableName, String rowKey, String column) throws IOException {
        return kvs.get(tableName, rowKey, column);
    }
    
    /**
     * Flush all remaining data and wait for completion.
     * MUST be called before close or end of lambda.
     */
    public void flushAndWait() throws IOException, InterruptedException {
        // Stop scheduled flushes
        flushTask.cancel(false);
        
        // Flush all tables synchronously
        for (String tableName : tableBuffers.keySet()) {
            Object lock = tableLocks.get(tableName);
            if (lock == null) continue;
            
            List<Row> toFlush;
            synchronized (lock) {
                ConcurrentHashMap<String, Row> buffer = tableBuffers.get(tableName);
                if (buffer == null || buffer.isEmpty()) continue;
                
                toFlush = new ArrayList<>(buffer.values());
                buffer.clear();
            }
            
            if (!toFlush.isEmpty()) {
                logger.info("Flush trigger (shutdown) table=" + tableName + " rows=" + toFlush.size());
                kvs.bulkPutRows(tableName, toFlush);
                totalRowsFlushed.addAndGet(toFlush.size());
                totalBatches.incrementAndGet();
            }
        }
        
        // Wait for pending async writes
        cleanupCompletedFutures();
        for (Future<?> future : pendingWrites) {
            try {
                future.get(60, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.error("Write task timed out");
                future.cancel(true);
            } catch (ExecutionException e) {
                logger.error("Write task failed: " + e.getCause().getMessage());
            }
        }
        pendingWrites.clear();

        logger.info("BatchedKVSClient flushed: puts=" + totalPuts.get() + 
                    ", rowsFlushed=" + totalRowsFlushed.get() + 
                    ", batches=" + totalBatches.get());
    }
    
    @Override
    public void close() throws Exception {
        if (closed) return;

        // IMPORTANT: Flush all buffered writes before closing
        flushAndWait();

        closed = true;
        
        flushAndWait();
        
        flushTask.cancel(true);
        flushScheduler.shutdownNow();
        writeExecutor.shutdown();

        if (!writeExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
            writeExecutor.shutdownNow();
        }
    }
    
    /**
     * Convenience method: flush and close in one call.
     * @deprecated Use close() directly - it now flushes before closing.
     */
    @Deprecated
    public void flushAndClose() throws Exception {
        close();
    }
    
    // Statistics
    public long getTotalPuts() { return totalPuts.get(); }
    public long getTotalRowsFlushed() { return totalRowsFlushed.get(); }
    public long getTotalBatches() { return totalBatches.get(); }
    
    /**
     * Get current buffer sizes for monitoring/debugging.
     * Returns total buffered rows across all tables.
     */
    public int getBufferedRowCount() {
        int total = 0;
        for (AtomicLong count : tableRowCounts.values()) {
            total += count.get();
        }
        return total;
    }
    
    /**
     * Get number of pending async writes.
     */
    public int getPendingWriteCount() {
        cleanupCompletedFutures();
        return pendingWrites.size();
    }
    
    /**
     * Get number of tables being buffered.
     */
    public int getBufferedTableCount() {
        return tableBuffers.size();
    }
}

