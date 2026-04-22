package cis5550.tools;

import cis5550.flame.BatchedKVSClient;
import cis5550.flame.ParallelLambdaExecutor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CrawlerLogger {
    private final String workerID;
    private int currentRound;
    private static PrintWriter logFile;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Object logFileLock = new Object();
    private static boolean logFileInitialized = false;
    private static ScheduledExecutorService flushScheduler;
    
    // Bottleneck stats monitoring
    private static ScheduledExecutorService statsScheduler;
    private static ScheduledFuture<?> statsTask;
    private static volatile ParallelLambdaExecutor<?, ?> monitoredExecutor;
    private static volatile BatchedKVSClient monitoredBatchClient;

    public CrawlerLogger(BatchedKVSClient batchedKvs, String workerID) {
        this.workerID = workerID;
        this.currentRound = 0;
        
        // Initialize shared file logging (only once per JVM)
        // Uses append mode, so it will append to existing file if process restarts
        synchronized (logFileLock) {
            if (!logFileInitialized) {
                try {
                    File logsDir = new File("logs");
                    if (!logsDir.exists()) {
                        logsDir.mkdirs();
                    }
                    String logFileName = "logs/crawler.log";
                    // false = NO auto-flush (buffered writes for better performance)
                    logFile = new PrintWriter(new FileWriter(logFileName, true), false);
                    logFileInitialized = true;
                    
                    // Start periodic flush thread (every 5 seconds)
                    flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                        Thread t = new Thread(r, "crawler-log-flush");
                        t.setDaemon(true);
                        return t;
                    });
                    flushScheduler.scheduleAtFixedRate(() -> {
                        synchronized (logFileLock) {
                            if (logFile != null) {
                                logFile.flush();
                            }
                        }
                    }, 5, 5, TimeUnit.SECONDS);
                    
                    // Also flush on shutdown
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        synchronized (logFileLock) {
                            if (logFile != null) {
                                logFile.flush();
                                logFile.close();
                            }
                        }
                    }));
                } catch (IOException e) {
                    // If file logging fails, continue without it
                    logFile = null;
                    logFileInitialized = true;
                }
            }
        }
    }
    
    private void logToFile(String event, String url, String details) {
        synchronized (logFileLock) {
            if (logFile != null) {
                String timestamp = dateFormat.format(new Date());
                String logLine = String.format("[%s] [Worker:%s] [Round:%d] [%s] %s - %s", 
                    timestamp, workerID, currentRound, event, url, details != null ? details : "");
                logFile.println(logLine);
                // No flush here - buffered writes, periodic flush handles it
            }
        }
    }

    public void setRound(int round) {
        this.currentRound = round;
    }

    // ============================================================
    // Public logging methods - NO Row creation (KVS writes disabled)
    // ============================================================

    public void queued(String url, String parentUrl) {
        logToFile("QUEUED", url, parentUrl != null ? "parent: " + parentUrl : null);
    }

    public void blacklisted(String url) {
        logToFile("BLACKLISTED", url, null);
    }

    public void robotsDisallowed(String url) {
        logToFile("ROBOTS_DISALLOWED", url, null);
    }

    public void crawlerWait(String url) {
        logToFile("CRAWLER_WAIT", url, null);
    }

    public void headFail(String url, String error) {
        logToFile("HEAD_FAIL", url, "error: " + error);
    }

    public void headSuccess(String url, int responseCode, String contentType) {
        logToFile("HEAD_SUCCESS", url, "code: " + responseCode + (contentType != null ? ", type: " + contentType : ""));
    }

    public void headErrorCode(String url, int responseCode) {
        logToFile("HEAD_ERROR_CODE", url, "code: " + responseCode);
    }

    public void redirectFail(String url, String error) {
        logToFile("REDIRECT_FAIL", url, "error: " + error);
    }

    public void redirectSuccess(String url, String redirectUrl) {
        logToFile("REDIRECT_SUCCESS", url, "redirect: " + redirectUrl);
    }

    public void getFail(String url, String error) {
        logToFile("GET_FAIL", url, "error: " + error);
    }

    public void getSuccess(String url, int responseCode, String contentType) {
        logToFile("GET_SUCCESS", url, "code: " + responseCode + (contentType != null ? ", type: " + contentType : ""));
    }

    // --------------------------------------------------------------------
    // Lightweight timing logs (file log only)
    // --------------------------------------------------------------------

    public void headFetchStart(String url) {
        logToFile("HEAD_START", url, null);
    }

    public void headFetchEnd(String url, int responseCode, long millis) {
        logToFile("HEAD_DONE", url, "code: " + responseCode + ", ms: " + millis);
    }

    public void getFetchStart(String url) {
        logToFile("GET_START", url, null);
    }

    public void getFetchEnd(String url, int responseCode, Long bytes, long millis) {
        String details = "code: " + responseCode + ", ms: " + millis;
        if (bytes != null) {
            details += ", bytes: " + bytes;
        }
        logToFile("GET_DONE", url, details);
    }

    /**
     * Mark a URL as skipped (not queued for a future round).
     * Used for cases like already-crawled URLs, invalid hosts, etc.
     */
    public void skipped(String url, String reason) {
        logToFile("SKIPPED", url, reason != null ? "reason: " + reason : null);
    }

    /**
     * Log when a page is successfully stored to pt-crawl with page content.
     */
    public void pageStored(String url, int contentLength) {
        logToFile("PAGE_STORED", url, "contentLength: " + contentLength);
    }

    /**
     * Force flush the log file (useful before checking logs)
     */
    public static void flush() {
        synchronized (logFileLock) {
            if (logFile != null) {
                logFile.flush();
            }
        }
    }
    
    // ============================================================
    // BOTTLENECK STATS MONITORING
    // Search for [BOTTLENECK_STATS] in log file to find these entries
    // ============================================================
    
    /**
     * Start periodic bottleneck stats logging (every 10 seconds).
     * Call this once at the start of your job.
     * 
     * @param executor The ParallelLambdaExecutor to monitor (can be null)
     * @param batchClient The BatchedKVSClient to monitor (can be null)
     */
    public static void startBottleneckMonitoring(ParallelLambdaExecutor<?, ?> executor, BatchedKVSClient batchClient) {
        monitoredExecutor = executor;
        monitoredBatchClient = batchClient;
        
        synchronized (logFileLock) {
            if (statsScheduler == null) {
                statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "bottleneck-stats-logger");
                    t.setDaemon(true);
                    return t;
                });
            }
            
            // Cancel existing task if any
            if (statsTask != null) {
                statsTask.cancel(false);
            }
            
            // Schedule stats logging every 10 seconds
            statsTask = statsScheduler.scheduleAtFixedRate(
                CrawlerLogger::logBottleneckStats,
                10, 10, TimeUnit.SECONDS
            );
        }
        
        logRaw("[BOTTLENECK_STATS] Monitoring started");
    }
    
    /**
     * Stop bottleneck stats monitoring.
     */
    public static void stopBottleneckMonitoring() {
        synchronized (logFileLock) {
            if (statsTask != null) {
                statsTask.cancel(false);
                statsTask = null;
            }
        }
        monitoredExecutor = null;
        monitoredBatchClient = null;
        logRaw("[BOTTLENECK_STATS] Monitoring stopped");
    }
    
    /**
     * Log all bottleneck stats now.
     * Called periodically by the scheduler.
     */
    private static void logBottleneckStats() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("[BOTTLENECK_STATS] === Periodic Stats Report ===\n");
            
            // 1. HTTP Request Stats (reads + writes)
            logHTTPStats(sb);
            
            // 2. Connection Cache Stats
            logConnectionCacheStats(sb);
            
            // 3. Batch KVS Client Stats  
            logBatchKVSStats(sb);
            
            // 4. Lambda Executor Stats
            logLambdaExecutorStats(sb);
            
            logRaw(sb.toString().trim());
        } catch (Exception e) {
            logRaw("[BOTTLENECK_STATS] Error collecting stats: " + e.getMessage());
        }
    }
    
    private static void logHTTPStats(StringBuilder sb) {
        try {
            sb.append("[BOTTLENECK_STATS] [HTTP_REQUESTS] ");
            sb.append("total=").append(HTTP.getTotalRequests());
            sb.append(", gets=").append(HTTP.getTotalGetRequests());
            sb.append(", puts=").append(HTTP.getTotalPutRequests());
            sb.append(", in_flight=").append(HTTP.getInFlightRequests());
            sb.append(", peak_in_flight=").append(HTTP.getPeakInFlight());
            sb.append(", avg_latency_ms=").append(String.format("%.2f", HTTP.getAverageLatencyMs()));
            sb.append(", errors=").append(HTTP.getTotalErrors());
            
            // Warnings
            int inFlight = HTTP.getInFlightRequests();
            double avgLatency = HTTP.getAverageLatencyMs();
            if (inFlight > 20) {
                sb.append(" [WARNING: HIGH_IN_FLIGHT]");
            }
            if (avgLatency > 100) {
                sb.append(" [WARNING: HIGH_LATENCY]");
            }
            if (HTTP.getTotalErrors() > 0) {
                sb.append(" [WARNING: ERRORS]");
            }
            sb.append("\n");
        } catch (Exception e) {
            sb.append("[BOTTLENECK_STATS] [HTTP_REQUESTS] Error: ").append(e.getMessage()).append("\n");
        }
    }
    
    private static void logConnectionCacheStats(StringBuilder sb) {
        try {
            long hits = HTTP.getCacheHits();
            long misses = HTTP.getCacheMisses();
            long total = hits + misses;
            double hitRate = total > 0 ? (100.0 * hits / total) : 0;
            
            sb.append("[BOTTLENECK_STATS] [CONN_CACHE] ");
            sb.append("cache_hits=").append(hits);
            sb.append(", cache_misses=").append(misses);
            sb.append(", hit_rate=").append(String.format("%.1f%%", hitRate));
            
            // Warning if cache hit rate is low (connections aren't being reused)
            if (total > 100 && hitRate < 50) {
                sb.append(" [WARNING: LOW_HIT_RATE]");
            }
            sb.append("\n");
        } catch (Exception e) {
            sb.append("[BOTTLENECK_STATS] [CONN_CACHE] Error: ").append(e.getMessage()).append("\n");
        }
    }
    
    private static void logBatchKVSStats(StringBuilder sb) {
        BatchedKVSClient client = monitoredBatchClient;
        if (client == null) {
            return; // No batch client to monitor
        }
        
        try {
            sb.append("[BOTTLENECK_STATS] [BATCH_KVS] ");
            sb.append("buffered_rows=").append(client.getBufferedRowCount());
            sb.append(", buffered_tables=").append(client.getBufferedTableCount());
            sb.append(", pending_writes=").append(client.getPendingWriteCount());
            sb.append(", total_puts=").append(client.getTotalPuts());
            sb.append(", total_flushed=").append(client.getTotalRowsFlushed());
            sb.append(", total_batches=").append(client.getTotalBatches());
            
            // Warning if buffer is growing
            int buffered = client.getBufferedRowCount();
            int pending = client.getPendingWriteCount();
            if (buffered > 1000) {
                sb.append(" [WARNING: HIGH_BUFFER]");
            }
            if (pending > 4) {
                sb.append(" [WARNING: HIGH_PENDING]");
            }
            sb.append("\n");
        } catch (Exception e) {
            sb.append("[BOTTLENECK_STATS] [BATCH_KVS] Error: ").append(e.getMessage()).append("\n");
        }
    }
    
    private static void logLambdaExecutorStats(StringBuilder sb) {
        ParallelLambdaExecutor<?, ?> executor = monitoredExecutor;
        if (executor == null) {
            return; // No executor to monitor
        }
        
        try {
            sb.append("[BOTTLENECK_STATS] [LAMBDA_EXEC] ");
            sb.append("active_workers=").append(executor.getActiveWorkers());
            sb.append("/").append(executor.getNumThreads());
            sb.append(", queue_size=").append(executor.getQueueSize());
            sb.append(", rows_processed=").append(executor.getRowsProcessed());
            sb.append(", outputs_produced=").append(executor.getOutputsProduced());
            
            // Warning if queue is filling up
            int queueSize = executor.getQueueSize();
            if (queueSize > 800) {
                sb.append(" [WARNING: QUEUE_BACKPRESSURE]");
            }
            
            // Warning if no workers active
            int active = executor.getActiveWorkers();
            if (active == 0 && queueSize > 0) {
                sb.append(" [WARNING: WORKERS_IDLE]");
            }
            sb.append("\n");
            
            // Also log the shared writer stats if available
            BatchedKVSClient sharedWriter = executor.getSharedWriter();
            if (sharedWriter != null && monitoredBatchClient != sharedWriter) {
                sb.append("[BOTTLENECK_STATS] [LAMBDA_WRITER] ");
                sb.append("buffered_rows=").append(sharedWriter.getBufferedRowCount());
                sb.append(", pending_writes=").append(sharedWriter.getPendingWriteCount());
                sb.append(", total_puts=").append(sharedWriter.getTotalPuts());
                sb.append("\n");
            }
        } catch (Exception e) {
            sb.append("[BOTTLENECK_STATS] [LAMBDA_EXEC] Error: ").append(e.getMessage()).append("\n");
        }
    }
    
    /**
     * Log raw message (for stats output).
     */
    private static void logRaw(String message) {
        synchronized (logFileLock) {
            if (logFile != null) {
                String timestamp = dateFormat.format(new Date());
                logFile.println("[" + timestamp + "] " + message);
            }
        }
    }
    
    /**
     * Manually log bottleneck stats once (for debugging).
     * Use: CrawlerLogger.logBottleneckStatsNow();
     */
    public static void logBottleneckStatsNow() {
        logBottleneckStats();
        flush();
    }
}