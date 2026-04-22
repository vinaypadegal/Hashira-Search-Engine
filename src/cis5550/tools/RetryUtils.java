package cis5550.tools;

import java.io.IOException;
import java.util.Random;

/**
 * Utility class for retrying operations with exponential backoff and jitter.
 * 
 * Features:
 * - Exponential backoff to reduce load on failing services
 * - Jitter to prevent thundering herd problem
 * - Configurable retry attempts and delays
 * - Distinguishes between retryable and non-retryable exceptions
 */
public class RetryUtils {
    
    @FunctionalInterface
    public interface RetryableOperation<T> {
        T execute() throws IOException;
    }
    
    @FunctionalInterface
    public interface RetryableVoidOperation {
        void execute() throws IOException;
    }
    
    private static final Random random = new Random();
    
    /**
     * Retry configuration
     */
    public static class RetryConfig {
        public final int maxRetries;
        public final long initialDelayMs;
        public final double backoffMultiplier;
        public final double jitterFactor; // 0.0 to 1.0, adds randomness to delay
        
        public RetryConfig(int maxRetries, long initialDelayMs, double backoffMultiplier, double jitterFactor) {
            this.maxRetries = maxRetries;
            this.initialDelayMs = initialDelayMs;
            this.backoffMultiplier = backoffMultiplier;
            this.jitterFactor = Math.max(0.0, Math.min(1.0, jitterFactor)); // Clamp to [0, 1]
        }
        
        public static RetryConfig defaultConfig() {
            return new RetryConfig(3, 100, 2.0, 0.3); // 3 retries, 100ms base, 2x backoff, 30% jitter
        }
    }
    
    /**
     * Execute an operation with retry logic.
     * 
     * @param operation The operation to execute (returns result)
     * @param config Retry configuration
     * @param operationName Name for logging purposes
     * @return Result of the operation
     * @throws IOException If all retries are exhausted
     */
    public static <T> T executeWithRetry(RetryableOperation<T> operation, RetryConfig config, String operationName) throws IOException {
        IOException lastException = null;
        
        for (int attempt = 0; attempt <= config.maxRetries; attempt++) {
            try {
                T result = operation.execute();
                if (attempt > 0) {
                    System.err.println("RetryUtils: " + operationName + " succeeded after " + (attempt + 1) + " attempts");
                }
                return result;
            } catch (IOException e) {
                lastException = e;
                if (attempt < config.maxRetries) {
                    long delayMs = calculateDelay(attempt, config);
                    System.err.println("RetryUtils: " + operationName + " failed (attempt " + (attempt + 1) + "/" + (config.maxRetries + 1) + "): " + 
                                     e.getMessage() + ". Retrying in " + delayMs + "ms...");
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Retry interrupted for " + operationName, ie);
                    }
                } else {
                    // Final attempt failed
                    throw new IOException(operationName + " failed after " + (config.maxRetries + 1) + " attempts: " + e.getMessage(), lastException);
                }
            }
        }
        
        // Should never reach here, but compiler needs it
        throw new IOException(operationName + " failed after " + (config.maxRetries + 1) + " attempts", lastException);
    }
    
    /**
     * Execute a void operation with retry logic.
     */
    public static void executeWithRetry(RetryableVoidOperation operation, RetryConfig config, String operationName) throws IOException {
        executeWithRetry(() -> {
            operation.execute();
            return null;
        }, config, operationName);
    }
    
    /**
     * Calculate delay with exponential backoff and jitter.
     * Jitter prevents synchronized retries when multiple clients fail simultaneously.
     */
    private static long calculateDelay(int attempt, RetryConfig config) {
        // Exponential backoff: base * (multiplier ^ attempt)
        long baseDelay = (long) (config.initialDelayMs * Math.pow(config.backoffMultiplier, attempt));
        
        // Add jitter: random value between -jitterFactor and +jitterFactor
        double jitter = (random.nextDouble() * 2 - 1) * config.jitterFactor; // [-jitterFactor, +jitterFactor]
        long jitterAmount = (long) (baseDelay * jitter);
        
        long delay = baseDelay + jitterAmount;
        return Math.max(1, delay); // Ensure at least 1ms delay
    }
}
