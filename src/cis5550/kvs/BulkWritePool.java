package cis5550.kvs;

import cis5550.tools.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
// ThreadPoolExecutor is in java.util.concurrent.* - used for stats

/**
 * Internal write pool for KVS Worker.
 * Buffers incoming bulk writes and flushes them efficiently.
 * 
 * Features:
 * - Buffers writes per table
 * - Flushes by size (N rows) or time (T ms)
 * - Async disk/memory writes
 * - Thread-safe for concurrent bulk requests
 */
public class BulkWritePool {
    private static final Logger logger = Logger.getLogger(BulkWritePool.class);
    
    // Configuration
    private static final int FLUSH_SIZE = 100;           // Rows per flush
    private static final long FLUSH_INTERVAL_MS = 50;    // Max time before flush
    private static final int WRITE_THREADS = 4;          // Parallel write threads
    
    // Buffer: table -> list of rows
    private final ConcurrentHashMap<String, List<Row>> tableBuffers;
    private final ConcurrentHashMap<String, Object> tableLocks;
    
    // Write executor
    private final ExecutorService writeExecutor;
    private final ScheduledExecutorService flushScheduler;
    private final ScheduledFuture<?> flushTask;
    
    // The actual write function (provided by KVS Worker)
    private final BiConsumer<String, Row> rowWriter;
    
    // Statistics
    private final AtomicLong totalRowsBuffered = new AtomicLong(0);
    private final AtomicLong totalRowsFlushed = new AtomicLong(0);
    private final AtomicLong totalFlushes = new AtomicLong(0);
    
    private volatile boolean shutdown = false;
    
    public BulkWritePool(BiConsumer<String, Row> rowWriter) {
        this.rowWriter = rowWriter;
        this.tableBuffers = new ConcurrentHashMap<>();
        this.tableLocks = new ConcurrentHashMap<>();
        this.writeExecutor = Executors.newFixedThreadPool(WRITE_THREADS);
        
        // Schedule periodic flush
        this.flushScheduler = Executors.newSingleThreadScheduledExecutor();
        this.flushTask = flushScheduler.scheduleAtFixedRate(
            this::flushAllTables,
            FLUSH_INTERVAL_MS,
            FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        
        logger.info("BulkWritePool initialized: flushSize=" + FLUSH_SIZE + 
                    ", flushIntervalMs=" + FLUSH_INTERVAL_MS + 
                    ", writeThreads=" + WRITE_THREADS);
    }
    
    /**
     * Add rows to the buffer for a table.
     * Will trigger flush if buffer exceeds threshold.
     */
    public void addRows(String table, List<Row> rows) {
        if (shutdown) {
            throw new IllegalStateException("BulkWritePool is shutdown");
        }
        
        if (rows == null || rows.isEmpty()) {
            return;
        }
        
        Object lock = tableLocks.computeIfAbsent(table, k -> new Object());
        
        synchronized (lock) {
            List<Row> buffer = tableBuffers.computeIfAbsent(table, k -> new ArrayList<>());
            buffer.addAll(rows);
            totalRowsBuffered.addAndGet(rows.size());
            
            // Flush if buffer is large enough
            if (buffer.size() >= FLUSH_SIZE) {
                List<Row> toFlush = new ArrayList<>(buffer);
                buffer.clear();
                flushAsync(table, toFlush);
            }
        }
    }
    
    /**
     * Add a single row to the buffer.
     */
    public void addRow(String table, Row row) {
        addRows(table, Collections.singletonList(row));
    }
    
    /**
     * Flush rows asynchronously
     */
    private void flushAsync(String table, List<Row> rows) {
        if (rows.isEmpty()) return;
        
        writeExecutor.submit(() -> {
            try {
                for (Row row : rows) {
                    rowWriter.accept(table, row);
                }
                totalRowsFlushed.addAndGet(rows.size());
                totalFlushes.incrementAndGet();
                logger.debug("Flushed " + rows.size() + " rows to table " + table);
            } catch (Exception e) {
                logger.error("Failed to flush " + rows.size() + " rows to table " + table + ": " + e.getMessage(), e);
            }
        });
    }
    
    /**
     * Flush all table buffers (called by timer)
     */
    private void flushAllTables() {
        if (shutdown) return;
        
        for (String table : tableBuffers.keySet()) {
            Object lock = tableLocks.get(table);
            if (lock == null) continue;
            
            List<Row> toFlush = null;
            synchronized (lock) {
                List<Row> buffer = tableBuffers.get(table);
                if (buffer != null && !buffer.isEmpty()) {
                    toFlush = new ArrayList<>(buffer);
                    buffer.clear();
                }
            }
            
            if (toFlush != null && !toFlush.isEmpty()) {
                flushAsync(table, toFlush);
            }
        }
    }
    
    /**
     * Flush all remaining data synchronously and shutdown.
     */
    public void flushAndShutdown() {
        shutdown = true;
        flushTask.cancel(false);
        
        // Flush all remaining buffers synchronously
        for (String table : tableBuffers.keySet()) {
            Object lock = tableLocks.get(table);
            if (lock == null) continue;
            
            List<Row> toFlush;
            synchronized (lock) {
                List<Row> buffer = tableBuffers.get(table);
                if (buffer == null || buffer.isEmpty()) continue;
                toFlush = new ArrayList<>(buffer);
                buffer.clear();
            }
            
            // Write synchronously
            for (Row row : toFlush) {
                try {
                    rowWriter.accept(table, row);
                    totalRowsFlushed.incrementAndGet();
                } catch (Exception e) {
                    logger.error("Failed to write row during shutdown: " + e.getMessage(), e);
                }
            }
            totalFlushes.incrementAndGet();
        }
        
        // Shutdown executors
        flushScheduler.shutdownNow();
        writeExecutor.shutdown();
        try {
            if (!writeExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                writeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            writeExecutor.shutdownNow();
        }
        
        logger.info("BulkWritePool shutdown: buffered=" + totalRowsBuffered.get() + 
                    ", flushed=" + totalRowsFlushed.get() + 
                    ", flushes=" + totalFlushes.get());
    }
    
    // Statistics
    public long getTotalRowsBuffered() { return totalRowsBuffered.get(); }
    public long getTotalRowsFlushed() { return totalRowsFlushed.get(); }
    public long getTotalFlushes() { return totalFlushes.get(); }
    
    /**
     * Get current number of rows waiting in buffers.
     */
    public int getCurrentBufferedRows() {
        int total = 0;
        for (List<Row> buffer : tableBuffers.values()) {
            total += buffer.size();
        }
        return total;
    }
    
    /**
     * Get number of tables being buffered.
     */
    public int getBufferedTableCount() {
        return tableBuffers.size();
    }
    
    /**
     * Get pending write tasks in executor queue (approximate).
     */
    public int getPendingWriteTasks() {
        if (writeExecutor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) writeExecutor).getQueue().size();
        }
        return -1; // Unknown
    }
    
    /**
     * Get active write threads.
     */
    public int getActiveWriteThreads() {
        if (writeExecutor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) writeExecutor).getActiveCount();
        }
        return -1; // Unknown
    }
}

