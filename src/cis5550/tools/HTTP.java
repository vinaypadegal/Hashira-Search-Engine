package cis5550.tools;

import java.util.*;
import java.net.*;
import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.X509Certificate;

/**
 * HTTP client with simple bounded connection cache.
 * Uses a lightweight cache with size limits per host - no complex pooling overhead.
 * 
 * Includes request statistics tracking for bottleneck monitoring.
 */
public class HTTP {
  
  // ============================================================
  // CONNECTION CACHE CONFIGURATION
  // ============================================================
  
  // Maximum idle connections to cache per host
  // This does NOT limit concurrent connections - just how many idle ones we keep
  private static final int MAX_CACHED_PER_HOST = 50;
  
  // Simple cache: serverID -> list of idle sockets
  private static Map<String, Vector<Socket>> cachedConnections;
  
  // ============================================================
  // REQUEST STATISTICS (for bottleneck monitoring)
  // ============================================================
  
  // Request counts by method
  private static final AtomicLong totalGetRequests = new AtomicLong(0);
  private static final AtomicLong totalPutRequests = new AtomicLong(0);
  private static final AtomicLong totalOtherRequests = new AtomicLong(0);
  
  // In-flight tracking
  private static final AtomicInteger inFlightRequests = new AtomicInteger(0);
  private static final AtomicInteger peakInFlight = new AtomicInteger(0);
  
  // Latency tracking (for running average)
  private static final AtomicLong totalLatencyNanos = new AtomicLong(0);
  private static final AtomicLong latencyCount = new AtomicLong(0);
  
  // Error tracking
  private static final AtomicLong totalErrors = new AtomicLong(0);
  
  // Cache stats
  private static final AtomicLong cacheHits = new AtomicLong(0);
  private static final AtomicLong cacheMisses = new AtomicLong(0);
  
  // Stats getters
  public static long getTotalGetRequests() { return totalGetRequests.get(); }
  public static long getTotalPutRequests() { return totalPutRequests.get(); }
  public static long getTotalRequests() { return totalGetRequests.get() + totalPutRequests.get() + totalOtherRequests.get(); }
  public static int getInFlightRequests() { return inFlightRequests.get(); }
  public static int getPeakInFlight() { return peakInFlight.get(); }
  public static long getTotalErrors() { return totalErrors.get(); }
  public static long getCacheHits() { return cacheHits.get(); }
  public static long getCacheMisses() { return cacheMisses.get(); }
  
  /**
   * Get average latency in milliseconds.
   */
  public static double getAverageLatencyMs() {
    long count = latencyCount.get();
    if (count == 0) return 0;
    return (totalLatencyNanos.get() / count) / 1_000_000.0;
  }
  
  /**
   * Reset all statistics (useful for per-job tracking).
   */
  public static void resetStats() {
    totalGetRequests.set(0);
    totalPutRequests.set(0);
    totalOtherRequests.set(0);
    inFlightRequests.set(0);
    peakInFlight.set(0);
    totalLatencyNanos.set(0);
    latencyCount.set(0);
    totalErrors.set(0);
    cacheHits.set(0);
    cacheMisses.set(0);
  }
  
  /**
   * Create a new socket connection.
   */
  private static Socket openSocket(String protocol, String host, int port) {
    try {
      if (protocol.equals("https")) {
        TrustManager[] trustAllCerts = { new X509TrustManager() {
          public X509Certificate[] getAcceptedIssuers() { return null; }
          public void checkClientTrusted(X509Certificate[] certs, String authType) { }
          public void checkServerTrusted(X509Certificate[] certs, String authType) { }
        } };
        SSLContext sc;
        try { 
          sc = SSLContext.getInstance("SSL"); 
          sc.init(null, trustAllCerts, new SecureRandom());
          return sc.getSocketFactory().createSocket(host, port);
        } catch (NoSuchAlgorithmException nsae) {
        } catch (KeyManagementException kme) {
        }
      } else if (protocol.equals("http")) {
        return new Socket(host, port);
      }
    } catch (Exception e) {
      // Connection failed - return null
    }

    return null;
  }
  
  public static class Response {
    byte body[];
    Map<String,String> headers;
    int statusCode;

    public Response(byte bodyArg[], Map<String,String> headersArg, int statusCodeArg) {
      body = bodyArg;
      headers = headersArg;
      statusCode = statusCodeArg;
    }

    public byte[] body() {
      return body;
    }

    public int statusCode() {
      return statusCode;
    }

    public Map<String,String> headers() {
      return headers;
    }
  }

  public static Response doRequest(String method, String urlArg, byte uploadOrNull[]) throws IOException {
    return doRequestWithTimeout(method, urlArg, uploadOrNull, -1, false);
  }

  public static Response doRequestWithTimeout(String method, String urlArg, byte uploadOrNull[], int timeoutMillis, boolean isHeadRequest) throws IOException {
    // Track request start
    long startNanos = System.nanoTime();
    int currentInFlight = inFlightRequests.incrementAndGet();
    
    // Update peak in-flight (best effort, not perfectly atomic but good enough for monitoring)
    int peak = peakInFlight.get();
    while (currentInFlight > peak) {
      if (peakInFlight.compareAndSet(peak, currentInFlight)) break;
      peak = peakInFlight.get();
    }
    
    // Track by method
    if ("GET".equalsIgnoreCase(method)) {
      totalGetRequests.incrementAndGet();
    } else if ("PUT".equalsIgnoreCase(method)) {
      totalPutRequests.incrementAndGet();
    } else {
      totalOtherRequests.incrementAndGet();
    }
    
    try {
      return doRequestInternal(method, urlArg, uploadOrNull, timeoutMillis, isHeadRequest);
    } catch (IOException e) {
      totalErrors.incrementAndGet();
      throw e;
    } finally {
      // Track latency and decrement in-flight
      long elapsedNanos = System.nanoTime() - startNanos;
      totalLatencyNanos.addAndGet(elapsedNanos);
      latencyCount.incrementAndGet();
      inFlightRequests.decrementAndGet();
    }
  }
  
  private static Response doRequestInternal(String method, String urlArg, byte uploadOrNull[], int timeoutMillis, boolean isHeadRequest) throws IOException {
    String protocol = "http";
    int pos = urlArg.indexOf("://");
    if (pos >= 0) {
      protocol = urlArg.substring(0, pos);
      urlArg = urlArg.substring(pos+3);
    }
    pos = urlArg.indexOf('/');
    if (pos < 0)
      return null;

    String host = urlArg.substring(0, pos), path = urlArg.substring(pos);
    int port = (protocol.equals("https")) ? 443 : 80;
    pos = host.indexOf(":");
    if (pos > 0) {
      String sport = host.substring(pos+1);
      try { port = Integer.valueOf(sport).intValue(); } catch (NumberFormatException nfe) {}
      host = host.substring(0, pos);
    }

    String serverID = protocol+"-"+host+"-"+port;

    while (true) {
      boolean usingCached = false;
      Socket sock = null;
      
      // Try to get a cached connection
      if ((cachedConnections != null) && (cachedConnections.get(serverID) != null)) {
        synchronized(cachedConnections) {
          Vector<Socket> cached = cachedConnections.get(serverID);
          if (cached != null && cached.size() > 0) {
            sock = cached.remove(0);
          }
        }
        if (sock != null) {
          usingCached = true;
          cacheHits.incrementAndGet();
        }
      }
      
      // Create new connection if no cached one available
      if (sock == null) {
        sock = openSocket(protocol, host, port);
        cacheMisses.incrementAndGet();
      }
      
      if (sock == null)
        throw new IOException("Cannot connect to server "+host+":"+port);

      try {
        if (timeoutMillis > 0)
          sock.setSoTimeout(timeoutMillis);

        OutputStream out = sock.getOutputStream();
        String request = method+" "+path+" HTTP/1.1\r\nHost: "+host+"\r\n";
        if (uploadOrNull != null)
          request = request + "Content-Length: "+uploadOrNull.length+"\r\n";
        request = request + "Connection: keep-alive\r\n\r\n";
        out.write(request.getBytes());
        if (uploadOrNull != null)
          out.write(uploadOrNull);
        out.flush();
      } catch (IOException ioe) {
        // Connection failed during write - close and retry if using cached
        try { sock.close(); } catch (Exception e) {}
        if (usingCached) {
          // Retry with a fresh connection
          continue;
        }
        throw new IOException("Connection to "+host+":"+port+" failed while writing the request");
      }

  
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      boolean readingHeaders = true;
      int contentLength = -1;
      Map<String,String> headers = new HashMap<String,String>();
      byte buf[] = new byte[100000];
      int inBuf = 0;
      int statusCode = -1;

      try {
        InputStream in = sock.getInputStream();
        while (true) {
          int n = in.read(buf, inBuf, buf.length - inBuf);
          if (n<0)
            break;

          inBuf += n;
          if (readingHeaders) {
            int matchPtr = 0;
            for (int i=0; i<inBuf; i++) {
              if (buf[i] == 10)
                matchPtr ++;
               else if (buf[i] != 13)
                matchPtr = 0;

              if (matchPtr == 2) {
                buffer.write(buf, 0, i);
                ByteArrayInputStream bais = new ByteArrayInputStream(buffer.toByteArray());
                BufferedReader hdr = new BufferedReader(new InputStreamReader(bais));
                String statusLine[] = hdr.readLine().split(" ");
                statusCode = Integer.valueOf(statusLine[1]);

                while (true) {
                  String s = hdr.readLine();
                  if (s.equals(""))
                    break;
                  String[] p2 = s.split(":", 2);
                  if (p2.length == 2) {
                    String headerName = p2[0];
                    headers.put(headerName.toLowerCase(), p2[1].trim());
                    if (headerName.toLowerCase().equals("content-length"))
                      contentLength = Integer.parseInt(p2[1].trim());
                  }
                }
                buffer.reset();
                System.arraycopy(buf, i+1, buf, 0, inBuf-(i+1));
                inBuf -= (i+1);
                readingHeaders = false;
                break;
              }
            }
          }

          if (!readingHeaders) {
            int toCopy = ((contentLength>=0) && (inBuf > contentLength)) ? contentLength : inBuf;
            buffer.write(buf, 0, toCopy);
            System.arraycopy(buf, toCopy, buf, 0, inBuf-toCopy);
            inBuf -= toCopy;

            // Only break if:
            // 1. It's a HEAD request (no body expected), OR
            // 2. Content-Length was set AND we've read that many bytes
            // If Content-Length is -1 (not set), keep reading until EOF (n<0)
            if (isHeadRequest || (contentLength >= 0 && buffer.size() >= contentLength))
              break;
          }
        }
      } catch (Exception e) {
        // Connection failed during read - close and retry if using cached
        try { sock.close(); } catch (Exception e2) {}
        if (usingCached) {
          // Retry with a fresh connection
          continue;
        }
        throw new IOException("Connection to "+host+":"+port+" failed while reading the response ("+e+")");
      }

      // Check if we got a valid response (EOF before headers means stale connection)
      if (statusCode == -1) {
        // Connection was closed before we could read headers - this is a stale connection
        try { sock.close(); } catch (Exception e) {}
        if (usingCached) {
          // Retry with a fresh connection
          continue;
        }
        throw new IOException("Connection to "+host+":"+port+" closed before response headers received");
      }

      // Determine if connection should be cached for reuse
      String connectionHeader = headers.get("connection");
      boolean shouldCache = connectionHeader == null || !connectionHeader.toLowerCase().equals("close");
      
      if (shouldCache) {
        // Try to cache the connection for reuse
        if (cachedConnections == null) {
          cachedConnections = new HashMap<String,Vector<Socket>>();
        }
        synchronized(cachedConnections) {
          if (cachedConnections.get(serverID) == null) {
            cachedConnections.put(serverID, new Vector<Socket>());
          }
          Vector<Socket> cached = cachedConnections.get(serverID);
          // Only cache if under the limit
          if (cached.size() < MAX_CACHED_PER_HOST) {
            cached.add(sock);
          } else {
            // Cache full - close the connection
            try { sock.close(); } catch (Exception e) {}
          }
        }
      } else {
        // Server requested connection close
        try { sock.close(); } catch (Exception e) {}
      }

      return new Response(buffer.toByteArray(), headers, statusCode);
    }
  }
}
