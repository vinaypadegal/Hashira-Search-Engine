package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.*;

/**
 * Dynamic web server implementation based on Spark Java Framework.
 *
 * Supports (HW1-HW3):
 * - Dynamic routing with path parameters (/user/:id)
 * - Static file serving
 * - HTTP and HTTPS (dual ports)
 * - Session management with expiration
 * - Multiple virtual hosts (EC1 from HW2)
 * - SNI-based multi-host HTTPS (EC from HW3)
 * - Before/after filters (EC3 from HW2)
 * - HTTP redirection (EC2 from HW2)
 * - Conditional requests (HW1 EC2)
 * - Range requests (HW1 EC3)
 * - Secure cookies (HW3 EC)
 *
 */
public class Server {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static final String SERVER_NAME = "AshayHW3Server";
    public static final int NUM_WORKERS = 1000;
    
    // Idle timeout for keep-alive connections (30 seconds)
    // Longer timeout allows more connection reuse, reducing TCP handshake overhead
    private static final int IDLE_TIMEOUT_MS = 30000;

    // RFC 1123 date format for HTTP
    private static final SimpleDateFormat HTTP_DATE_FORMAT = new SimpleDateFormat(
            "EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US
    );

    static {
        HTTP_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    // Ports
    private int port = 80;
    private int securePortNo = -1;

    // Multi-host routing (HW2 EC1)
    private Map<String, Map<String, Route>> getRoutes = new HashMap<>();
    private Map<String, Map<String, Route>> postRoutes = new HashMap<>();
    private Map<String, Map<String, Route>> putRoutes = new HashMap<>();
    private Map<String, String> hostStaticFiles = new HashMap<>();
    private String currentHost = null;

    // Filters (HW2 EC3)
    private List<Filter> beforeFilters = new ArrayList<>();
    private List<Filter> afterFilters = new ArrayList<>();

    // HW3: Session management
    static final Map<String, SessionImpl> sessions = new HashMap<>();
    private final static SecureRandom secureRandom = new SecureRandom();
    private final static String optionsCookie =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";

    // HW3 EC: Multi-host SSL with SNI
    private Map<String, HostConfig> hostConfigs = new HashMap<>();
    private SSLContext defaultSSLContext = null;  // For default/no-SNI requests
    // Use explicit path to avoid differences when running under sudo/root
    private String defaultKeystorePath = "/home/ec2-user/keystore.jks";
    private String defaultKeystorePassword = "000000";

    // Singleton
    private static Server instance = null;
    private static boolean isRunning = false;

    /**
     * Configuration for a virtual host with its own SSL certificate.
     */
    private static class HostConfig {
        String keystorePath;
        String password;
        SSLContext sslContext;

        HostConfig(String path, String pwd) throws Exception {
            this.keystorePath = path;
            this.password = pwd;
            this.sslContext = loadSSLContext(path, pwd);
        }

        private static SSLContext loadSSLContext(String keystorePath, String password)
                throws Exception {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(keystorePath), password.toCharArray());

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, password.toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), null, null);

            return sslContext;
        }
    }

    /**
     * HW2 EC1: Sets current host context (no SSL).
     */
    public static void host(String hostname) {
        if (instance == null) {
            instance = new Server();
        }
        instance.currentHost = hostname;
    }

    /**
     * HW3 EC: Sets current host context with custom SSL certificate.
     *
     * @param hostname      The hostname (e.g., "example.com")
     * @param keystorePath  Path to JKS keystore for this host
     * @param password      Keystore password
     */
    public static void host(String hostname, String keystorePath, String password) {
        if (instance == null) {
            instance = new Server();
        }
        instance.currentHost = hostname;

        try {
            HostConfig config = new HostConfig(keystorePath, password);
            instance.hostConfigs.put(hostname, config);
            logger.info("Registered SSL certificate for host: " + hostname);
        } catch (Exception e) {
            logger.error("Failed to load SSL certificate for " + hostname + ": " + e.getMessage());
            throw new RuntimeException("Failed to configure SSL for " + hostname, e);
        }
    }

    public static void port(int p) {
        if (instance == null) {
            instance = new Server();
        }
        instance.port = p;
    }

    /**
     * HW3: Sets the HTTPS port.
     */
    public static void securePort(int portNo) {
        if (instance == null) {
            instance = new Server();
        }
        instance.securePortNo = portNo;
    }

    public static void get(String path, Route route) {
        if (instance == null) {
            instance = new Server();
        }
        String host = instance.currentHost;
        instance.getRoutes.putIfAbsent(host, new HashMap<>());
        instance.getRoutes.get(host).put(path, route);
        startServerIfNeeded();
    }

    public static void post(String path, Route route) {
        if (instance == null) {
            instance = new Server();
        }
        String host = instance.currentHost;
        instance.postRoutes.putIfAbsent(host, new HashMap<>());
        instance.postRoutes.get(host).put(path, route);
        startServerIfNeeded();
    }

    public static void put(String path, Route route) {
        if (instance == null) {
            instance = new Server();
        }
        String host = instance.currentHost;
        instance.putRoutes.putIfAbsent(host, new HashMap<>());
        instance.putRoutes.get(host).put(path, route);
        startServerIfNeeded();
    }

    public static void before(Filter filter) {
        if (instance == null) {
            instance = new Server();
        }
        instance.beforeFilters.add(filter);
    }

    public static void after(Filter filter) {
        if (instance == null) {
            instance = new Server();
        }
        instance.afterFilters.add(filter);
    }

    public static class staticFiles {
        public static void location(String directory) {
            if (instance == null) {
                instance = new Server();
            }
            String host = instance.currentHost;
            instance.hostStaticFiles.put(host, directory);
            startServerIfNeeded();
        }
    }

    private static void startServerIfNeeded() {
        if (!isRunning) {
            isRunning = true;
            new Thread(() -> instance.run()).start();
        }
    }

    //Session Management

    /**
     * Generates cryptographically random session ID.
     * 64 characters, 20 picked = 20 * 6 bits = 120 bits of entropy.
     */
    public static String generateSessionId(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = secureRandom.nextInt(optionsCookie.length());
            sb.append(optionsCookie.charAt(index));
        }
        return sb.toString();
    }

    /**
     * Background thread to expire old sessions.
     */
    private void sessionCleanupLoop() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);  // Check every 10 seconds
                } catch (InterruptedException e) {
                    logger.error("Session cleanup interrupted: " + e.getMessage());
                }

                long currentTime = System.currentTimeMillis();
                synchronized (sessions) {
                    sessions.entrySet().removeIf(entry -> {
                        SessionImpl session = entry.getValue();
                        int maxInterval = session.getMaxActiveInterval();
                        if (maxInterval > 0) {
                            long inactiveDuration = (currentTime - session.lastAccessedTime()) / 1000;
                            if (inactiveDuration > maxInterval) {
                                logger.info("Session expired: " + session.id());
                                return true;
                            }
                        }
                        return false;
                    });
                }
            }
        }, "SessionCleanup").start();
    }

    // Generates a simple HTTP error response
    public static String errorResponse(int code, String message) {
        String statusLine = "HTTP/1.1 " + code + " " + message + "\r\n";
        String body = code + " " + message;
        String headers = "Content-Type: text/plain\r\n" +
                "Server: " + SERVER_NAME + "\r\n" +
                "Content-Length: " + body.length() + "\r\n\r\n";
        return statusLine + headers + body;
    }

    // Determines content type based on file extension
    private static String getContentType(String filePath) {
        if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) return "image/jpeg";
        if (filePath.endsWith(".txt")) return "text/plain";
        if (filePath.endsWith(".html")) return "text/html";
        return "application/octet-stream";
    }

    // Sends a raw HTTP response string to the client
    public static void sendResponse(Socket clientSocket, String response) {
        try {
            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
            output.print(response);
            output.flush();
        } catch (IOException e) {
            logger.error("Error sending response: " + e.getMessage());
        }
    }

    // Reads and discards the request body
    private static void parseBody(InputStream input, int contentLength) throws IOException {
        int remaining = contentLength;
        byte[] buffer = new byte[8192];
        while (remaining > 0) {
            int toRead = Math.min(remaining, buffer.length);
            int bytesRead = input.read(buffer, 0, toRead);
            if (bytesRead == -1) break;
            remaining -= bytesRead;
        }
    }

    // Matches a path against registered routes with path parameters
    public static MatchVO matchRouteWithParams(Map<String, Route> routesMap, String path) {
        if (routesMap == null) return null;

        for (Map.Entry<String, Route> entry : routesMap.entrySet()) {
            String pattern = entry.getKey();
            String[] patternParts = pattern.split("/");
            String[] pathParts = path.split("/");

            if (patternParts.length != pathParts.length) continue;

            Map<String, String> params = new HashMap<>();
            boolean isMatch = true;

            for (int i = 0; i < patternParts.length; i++) {
                if (patternParts[i].startsWith(":")) {
                    // URL-decode path parameters to handle encoded column names and other values
                    String decodedValue = pathParts[i];
                    try {
                        decodedValue = URLDecoder.decode(pathParts[i], "UTF-8");
                    } catch (IllegalArgumentException e) {
                        // If decoding fails, use the original value
                        decodedValue = pathParts[i];
                    } catch (java.io.UnsupportedEncodingException e) {
                        // UTF-8 should always be supported, but handle it just in case
                        decodedValue = pathParts[i];
                    }
                    params.put(patternParts[i].substring(1), decodedValue);
                } else if (!patternParts[i].equals(pathParts[i])) {
                    isMatch = false;
                    break;
                }
            }

            if (isMatch) return new MatchVO(params, entry.getValue());
        }
        return null;
    }

    // Gets routes for a specific host and method
    private static Map<String, Route> getRoutesForHost(String method, String hostname) {
        Map<String, Map<String, Route>> methodRoutes;
        if (method.equals("GET")) methodRoutes = instance.getRoutes;
        else if (method.equals("POST")) methodRoutes = instance.postRoutes;
        else if (method.equals("PUT")) methodRoutes = instance.putRoutes;
        else return new HashMap<>();

        return methodRoutes.getOrDefault(hostname, new HashMap<>());
    }

    // Parses query parameters from URL and body
    public static Map<String, String> getQueryAndBodyParams(String path, String body, String contentType) {
        Map<String, String> params = new HashMap<>();

        if (path.contains("?")) {
            String queryString = path.substring(path.indexOf("?") + 1);
            parseParams(queryString, params);
        }

        // Only parse body if Content-Type is application/x-www-form-urlencoded
        if (contentType != null &&
            contentType.equals("application/x-www-form-urlencoded") &&
            body != null && !body.isEmpty()) {
            parseParams(body, params);
        }

        return params;
    }

    // Parses a URL-encoded parameter string into a map
    private static void parseParams(String paramString, Map<String, String> params) {
        String[] pairs = paramString.split("&");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length >= 1) {
                try {
                    //String key = URLDecoder.decode(keyValue[0], java.nio.charset.StandardCharsets.UTF_8); //--> this was causing PairRDD.foldByKey to fail, but now its working????
                    String key = keyValue[0];  // Jar does NOT decode the key
                    String value = keyValue.length == 2 ?
                            URLDecoder.decode(keyValue[1], java.nio.charset.StandardCharsets.UTF_8) : "";
                    params.put(key, value);
                } catch (IllegalArgumentException e) {
                    // Skip malformed parameters like the jar does
                }
            }
        }
    }

    //Static File Response
    public static void sendFileResponse(Socket clientSocket, String baseDir, String path,
                                        String method, Date ifModifiedSince, String rangeHeader) {
        if (path.contains("..")) {
            sendResponse(clientSocket, errorResponse(403, "Forbidden"));
            logger.error("Directory traversal attempt: " + path);
            return;
        }

        String filePath = path.startsWith("/") ? baseDir + path : baseDir + "/" + path;
        File file = new File(filePath);

        if (!file.exists() || file.isDirectory()) {
            sendResponse(clientSocket, errorResponse(404, "Not Found"));
            return;
        }
        if (!file.canRead()) {
            sendResponse(clientSocket, errorResponse(403, "Forbidden"));
            return;
        }

        long fileLength = file.length();
        long lastModified = file.lastModified();
        Date lastModifiedDate = new Date(lastModified);

        // HW1 EC2: 304 Not Modified
        if (ifModifiedSince != null) {
            long ifModifiedSinceSeconds = ifModifiedSince.getTime() / 1000;
            long lastModifiedSeconds = lastModified / 1000;

            if (lastModifiedSeconds <= ifModifiedSinceSeconds) {
                try {
                    PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                    output.print("HTTP/1.1 304 Not Modified\r\n" +
                            "Server: " + SERVER_NAME + "\r\n" +
                            "Content-Length: 0\r\n" +
                            "Last-Modified: " + formatDate(lastModifiedDate) + "\r\n\r\n");
                    output.flush();
                } catch (IOException e) {
                    logger.error("Error sending 304: " + e.getMessage());
                }
                return;
            }
        }

        // HW1 EC3: Range requests
        long rangeStart = 0;
        long rangeEnd = fileLength - 1;
        boolean isRangeRequest = false;

        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
            isRangeRequest = true;
            String rangeSpec = rangeHeader.substring(6).trim();

            try {
                if (rangeSpec.startsWith("-")) {
                    long suffix = Long.parseLong(rangeSpec.substring(1));
                    rangeStart = Math.max(0, fileLength - suffix);
                } else if (rangeSpec.endsWith("-")) {
                    rangeStart = Long.parseLong(rangeSpec.substring(0, rangeSpec.length() - 1));
                } else {
                    String[] parts = rangeSpec.split("-");
                    rangeStart = Long.parseLong(parts[0]);
                    rangeEnd = Long.parseLong(parts[1]);
                }

                if (rangeStart < 0 || rangeStart >= fileLength ||
                        rangeEnd >= fileLength || rangeStart > rangeEnd) {
                    isRangeRequest = false;
                    rangeStart = 0;
                    rangeEnd = fileLength - 1;
                }
            } catch (Exception e) {
                logger.error("Invalid range: " + rangeHeader);
                isRangeRequest = false;
            }
        }

        long contentLength = rangeEnd - rangeStart + 1;

        try {
            OutputStream outStream = clientSocket.getOutputStream();
            PrintWriter output = new PrintWriter(outStream, true);

            String statusLine = isRangeRequest ?
                    "HTTP/1.1 206 Partial Content\r\n" : "HTTP/1.1 200 OK\r\n";

            StringBuilder headers = new StringBuilder();
            headers.append("Content-Type: ").append(getContentType(filePath)).append("\r\n");
            headers.append("Server: ").append(SERVER_NAME).append("\r\n");
            headers.append("Content-Length: ").append(contentLength).append("\r\n");
            headers.append("Last-Modified: ").append(formatDate(lastModifiedDate)).append("\r\n");

            if (isRangeRequest) {
                headers.append("Content-Range: bytes ")
                        .append(rangeStart).append("-").append(rangeEnd)
                        .append("/").append(fileLength).append("\r\n");
            }

            headers.append("\r\n");

            output.print(statusLine + headers.toString());
            output.flush();

            if (!method.equals("HEAD")) {
                try (FileInputStream fileInput = new FileInputStream(file)) {
                    if (rangeStart > 0) {
                        fileInput.skip(rangeStart);
                    }

                    byte[] buffer = new byte[8192];
                    long remaining = contentLength;
                    int bytesRead;

                    while (remaining > 0 &&
                            (bytesRead = fileInput.read(buffer, 0,
                                    (int) Math.min(buffer.length, remaining))) != -1) {
                        outStream.write(buffer, 0, bytesRead);
                        remaining -= bytesRead;
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error sending file: " + e.getMessage());
        }
    }

    private static String formatDate(Date date) {
        synchronized (HTTP_DATE_FORMAT) {
            return HTTP_DATE_FORMAT.format(date);
        }
    }

    //Request Handler
    public static void handleClient(Socket clientSocket, String baseDir) {
        InputStream input;
        try {
            // Set idle timeout - connection closes if no data for 30 seconds
            clientSocket.setSoTimeout(IDLE_TIMEOUT_MS);
            input = clientSocket.getInputStream();
        } catch (IOException e) {
            logger.error("Error getting input stream: " + e.getMessage());
            return;
        }

        try {
            while (true) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int nRead;
                boolean headersEnded = false;
                boolean gotData = false;

                try {
                    while ((nRead = input.read()) != -1) {
                        gotData = true;
                        buffer.write(nRead);

                        byte[] currentBytes = buffer.toByteArray();
                        int len = currentBytes.length;

                        if (len >= 4 &&
                                currentBytes[len - 4] == '\r' && currentBytes[len - 3] == '\n' &&
                                currentBytes[len - 2] == '\r' && currentBytes[len - 1] == '\n') {
                            headersEnded = true;
                            break;
                        }
                    }
                } catch (SocketTimeoutException e) {
                    // Idle timeout - close connection gracefully
                    logger.debug("Connection idle timeout, closing");
                    break;
                }

                if (!headersEnded) {
                    if (gotData) {
                        logger.error("Incomplete headers (" + buffer.size() + " bytes)");
                    }
                    break;
                }

                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer.toByteArray());
                BufferedReader in = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                String requestLine = in.readLine();
                if (requestLine == null || requestLine.isEmpty()) {
                    sendResponse(clientSocket, errorResponse(400, "Bad Request"));
                    continue;
                }

                String[] parts = requestLine.split(" ");
                if (parts.length != 3) {
                    sendResponse(clientSocket, errorResponse(400, "Bad Request"));
                    continue;
                }

                String method = parts[0];
                String path = parts[1];
                String version = parts[2];

                String hostHeader = null;
                int contentLength = 0;
                Map<String, String> reqHeaders = new HashMap<>();

                String inputLine;
                while ((inputLine = in.readLine()) != null && !inputLine.isEmpty()) {
                    int idx = inputLine.indexOf(":");
                    if (idx > 0) {
                        String headerName = inputLine.substring(0, idx).trim().toLowerCase(Locale.ROOT);
                        String headerValue = inputLine.substring(idx + 1).trim();
                        reqHeaders.put(headerName, headerValue);

                        if (headerName.equals("host")) {
                            hostHeader = headerValue;
                        } else if (headerName.equals("content-length")) {
                            try {
                                contentLength = Integer.parseInt(headerValue);
                            } catch (NumberFormatException e) {
                                contentLength = 0;
                            }
                        }
                    }
                }

                if (!version.equals("HTTP/1.1")) {
                    sendResponse(clientSocket, errorResponse(505, "HTTP Version Not Supported"));
                    continue;
                }

                if (hostHeader == null) {
                    sendResponse(clientSocket, errorResponse(400, "Bad Request"));
                    continue;
                }

                // Extract hostname (strip port)
                String hostname = hostHeader.contains(":") ?
                        hostHeader.substring(0, hostHeader.indexOf(":")) : hostHeader;

                // Parse cookie for session
                String cookieHeader = reqHeaders.get("cookie");
                String sessionId = null;
                if (cookieHeader != null) {
                    String[] cookies = cookieHeader.split(";");
                    for (String cookie : cookies) {
                        String[] keyValue = cookie.trim().split("=", 2);
                        if (keyValue.length == 2 && keyValue[0].trim().equals("SessionID")) {
                            sessionId = keyValue[1].trim();
                            break;
                        }
                    }
                }

                SessionImpl session = null;
                if (sessionId != null) {
                    synchronized (sessions) {
                        session = sessions.get(sessionId);
                        if (session != null && !session.isValid()) {
                            sessions.remove(sessionId);
                            session = null;
                        }
                    }
                }

                // Try dynamic routes
                String pathWithoutQuery = path.contains("?") ?
                        path.substring(0, path.indexOf("?")) : path;

                Map<String, Route> routesMap = getRoutesForHost(method, hostname);
                MatchVO matchVO = matchRouteWithParams(routesMap, pathWithoutQuery);

                if (matchVO == null && hostname != null) {
                    routesMap = getRoutesForHost(method, null);
                    matchVO = matchRouteWithParams(routesMap, pathWithoutQuery);
                }

                if (matchVO != null) {
                    // Dynamic route matched
                    byte[] requestBodyBytes = null;
                    if (contentLength > 0) {
                        requestBodyBytes = new byte[contentLength];
                        int totalRead = 0;
                        while (totalRead < contentLength) {
                            int bytesRead = input.read(requestBodyBytes, totalRead,
                                    contentLength - totalRead);
                            if (bytesRead == -1) break;
                            totalRead += bytesRead;
                        }
                    }

                    String bodyString = requestBodyBytes != null ? new String(requestBodyBytes) : "";
                    String contentType = reqHeaders.get("content-type");
                    Map<String, String> queryParams = getQueryAndBodyParams(path, bodyString, contentType);

                    Request req = new RequestImpl(method, path, version, reqHeaders,
                            queryParams, matchVO.params,
                            (java.net.InetSocketAddress) clientSocket.getRemoteSocketAddress(),
                            requestBodyBytes, instance, session);
                    ResponseImpl resp = new ResponseImpl(clientSocket.getOutputStream());

                    try {
                        // Before filters
                        for (Filter filter : instance.beforeFilters) {
                            filter.handle(req, resp);
                            if (resp.isHalted()) {
                                resp.write(resp.getBody());
                                continue;
                            }
                        }

                        // Route handler
                        Object result = matchVO.route.handle(req, resp);

                        // After filters
                        for (Filter filter : instance.afterFilters) {
                            filter.handle(req, resp);
                        }

                        // Set session cookie if new session created
                        if (((RequestImpl) req).isNewSessionCreated()) {
                            // HW3 EC: Detect HTTPS and set Secure flag
                            boolean isSecure = clientSocket instanceof SSLSocket;
                            resp.setSessionId(req.session().id(), isSecure);
                        }

                        // Send response
                        if (!resp.isCommitted()) {
                            if (result != null) {
                                resp.body(result.toString());
                            }
                            byte[] finalBody = resp.getBody();
                            resp.write(finalBody != null ? finalBody : new byte[0]);
                        }

                    } catch (Exception e) {
                        if (!resp.isCommitted()) {
                            resp.status(500, "Internal Server Error");
                            resp.body("500 Internal Server Error: " + e.getMessage());
                            try {
                                resp.write(resp.getBody());
                            } catch (Exception ex) {
                                logger.error("Failed to send 500: " + ex.getMessage());
                            }
                        } else {
                            logger.error("Exception after write(): " + e.getMessage());
                        }
                    }
                    
                    // Break out of keep-alive loop if connection should close
                    // (only for streaming responses without Content-Length)
                    if (resp.shouldCloseConnection()) {
                        break;
                    }

                } else {
                    // Try static files
                    String staticDir = instance.hostStaticFiles.get(hostname);
                    if (staticDir == null) {
                        staticDir = instance.hostStaticFiles.get(null);
                    }

                    if (staticDir != null) {
                        Date ifModifiedSince = null;
                        String rangeHeader = null;

                        String imsHeader = reqHeaders.get("if-modified-since");
                        if (imsHeader != null) {
                            try {
                                synchronized (HTTP_DATE_FORMAT) {
                                    ifModifiedSince = HTTP_DATE_FORMAT.parse(imsHeader);
                                }
                            } catch (ParseException e) {
                                logger.error("Invalid If-Modified-Since: " + imsHeader);
                            }
                        }

                        rangeHeader = reqHeaders.get("range");

                        sendFileResponse(clientSocket, staticDir, path, method,
                                ifModifiedSince, rangeHeader);
                    } else {
                        sendResponse(clientSocket, errorResponse(404, "Not Found"));
                    }
                }
            }
        } catch (IOException e) {
            logger.error("I/O error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error("Error closing socket: " + e.getMessage());
            }
        }
    }

    /**
     * HW3 EC: Server loop for HTTPS with SNI support.
     */
    private void serverLoopHTTPS(ServerSocket serverSocket, BlockingQueue<Socket> sockQ) {
        logger.info("HTTPS server started on port " + securePortNo + " with SNI support");

        while (true) {
            try {
                // Accept connection on regular socket
                Socket rawSocket = serverSocket.accept();

                // Use SNI inspector to determine hostname
                SNIInspector inspector = new SNIInspector();
                inspector.parseConnection(rawSocket);

                SNIHostName sniHostName = inspector.getHostName();
                String hostname = sniHostName != null ? sniHostName.getAsciiName() : null;

                logger.info("HTTPS connection - SNI hostname: " +
                        (hostname != null ? hostname : "(none)"));

                // Select appropriate SSL context
                SSLContext sslContext;
                if (hostname != null && hostConfigs.containsKey(hostname)) {
                    sslContext = hostConfigs.get(hostname).sslContext;
                    logger.info("Using certificate for: " + hostname);
                } else {
                    sslContext = defaultSSLContext;
                    logger.info("Using default certificate");
                }

                // Create SSL socket with replayed ClientHello
                InputStream replayStream = inspector.getInputStream();
                SSLSocketFactory factory = sslContext.getSocketFactory();
                Socket sslSocket = factory.createSocket(rawSocket, replayStream, true);

                // Add to worker queue
                sockQ.add(sslSocket);

            } catch (Exception e) {
                logger.error("Error in HTTPS loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Regular HTTP server loop.
     */
    private void serverLoopHTTP(ServerSocket serverSocket, BlockingQueue<Socket> sockQ) {
        logger.info("HTTP server started on port " + port);

        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                sockQ.add(clientSocket);
            } catch (IOException e) {
                logger.error("Error in HTTP loop: " + e.getMessage());
            }
        }
    }

    /**
     * Main run method - starts HTTP and HTTPS servers
     */
    private void run() {
        BlockingQueue<Socket> sockQ = new LinkedBlockingQueue<>();

        // Start worker threads
        for (int i = 0; i < NUM_WORKERS; i++) {
            new Worker(null, sockQ).start();
        }

        // Start session cleanup
        sessionCleanupLoop();

        try {
            // Start HTTP server
            ServerSocket httpServerSocket = new ServerSocket(port);
            new Thread(() -> serverLoopHTTP(httpServerSocket, sockQ), "HTTP-Server").start();

            // Start HTTPS server if configured
            if (securePortNo != -1) {
                // Load default SSL context
                defaultSSLContext = HostConfig.loadSSLContext(
                        defaultKeystorePath, defaultKeystorePassword);
                logger.info("Loaded default SSL certificate from " + defaultKeystorePath);

                // Create regular ServerSocket (NOT SSLServerSocket)
                // handle TLS manually with SNI inspector
                ServerSocket httpsServerSocket = new ServerSocket(securePortNo);
                new Thread(() -> serverLoopHTTPS(httpsServerSocket, sockQ), "HTTPS-Server").start();
            }

        } catch (Exception e) {
            logger.error("Error starting server: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}