package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParserSimple;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrawlerRef {
    private static final double DEFAULT_DELAY_SECONDS = 1.0;
    private static final String USER_AGENT = "cis5550-crawler";
    public static void run(FlameContext ctx, String[] args) {
        if (args.length < 1) {
            ctx.output("Error: Expected seed URL");
            return;
        }

        String seed = normalizeUrl(args[0], "");
        String blacklistTable = (args.length >= 2) ? args[1] : null;
        System.out.println("Starting crawler with seed " + seed +
                (blacklistTable != null ? (" and blacklist " + blacklistTable) : ""));

        if (isBlacklisted(seed, ctx.getKVS(), blacklistTable)) {
            ctx.output("Seed URL is Blacklisted. Exiting.");
            return;
        }

        try {
            FlameRDD urlQueue = ctx.parallelize(Collections.singletonList(seed));
            while (urlQueue.count() > 0) {
                urlQueue = urlQueue.flatMap(url -> processUrl(url, blacklistTable, ctx));
                Thread.sleep(1000);
            }
            ctx.output("OK");
        } catch (Exception e) {
            System.out.println("Some error occurred: " + e);
        }
    }


    public static List<String> processUrl(String urlString, String blacklistTable, FlameContext ctx) throws IOException {
        List<String> nextUrls = new ArrayList<>();
        KVSClient kvs = ctx.getKVS();

        // If url entry already present, ignore it and move on (to avoid cycles)
        if (kvs.get("pt-crawl", Hasher.hash(urlString), "url") != null) {
            return nextUrls;
        }

        HttpURLConnection conn = null;
        int responseCode;
        String contentType;
        String contentLength;

        try {
            String[] parts = URLParserSimple.parseURL(urlString);
            String protocol = parts[0];
            String host = parts[1];
            String port = parts[2];
            if (host == null) return nextUrls; // skip malformed

            // Robots.txt check
            String robotsTxt = getOrFetchRobotsTxt(kvs, protocol, host, port);
            Map<String, RobotsTxtParser.RobotsRules> rulesMap = RobotsTxtParser.parse(robotsTxt);
            boolean allowed = RobotsTxtParser.isAllowed(urlString, rulesMap);
            if (!allowed) {
                System.out.println("Blocked by robots.txt: " + urlString + " (" + Hasher.hash(urlString) + ")");
                return nextUrls;
            }

            // Crawl delay enforcement
            double delaySec = RobotsTxtParser.getCrawlDelay(rulesMap);
            long now = System.currentTimeMillis();
            byte[] lastAccessBytes = kvs.get("hosts", host, "lastAccess");
            String lastAccessStr = lastAccessBytes != null ? new String(lastAccessBytes, StandardCharsets.UTF_8) : null;
            long lastAccess = (lastAccessStr != null) ? Long.parseLong(lastAccessStr) : 0;

            if (now - lastAccess < delaySec * 1000) {
                // Too soon; skip and requeue for later
                System.out.printf("Rate-limited host %s (%.2f sec delay)\n", host, delaySec);
                nextUrls.add(urlString);
                return nextUrls;
            }

            // Update host access time
            kvs.put("hosts", host, "lastAccess", Long.toString(now));

            // Make HEAD request
            URL url = new URL(urlString);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");
            conn.setInstanceFollowRedirects(false);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.connect();

            responseCode = conn.getResponseCode();
            contentType = conn.getHeaderField("Content-Type");
            contentLength = conn.getHeaderField("Content-Length");

            String rowKey = Hasher.hash(urlString);

            // If HEAD -> 200 OK and content-type is text/html, fetch body via GET
            if (responseCode == 200 && contentType != null && contentType.toLowerCase().contains("text/html")) {
                conn.disconnect();
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setInstanceFollowRedirects(false);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                conn.connect();

                responseCode = conn.getResponseCode();

                // If GET request is successful
                if (responseCode == 200) {
                    InputStream in = conn.getInputStream();
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    byte[] tmp = new byte[4096];
                    int n;
                    while ((n = in.read(tmp)) != -1)
                        buffer.write(tmp, 0, n);
                    in.close();

                    byte[] pageBytes = buffer.toByteArray();
                    String page = new String(pageBytes, StandardCharsets.ISO_8859_1);

                    // EC1: Check if page contents already seen
                    String hashedPage = Hasher.hash(page);
                    Row pageCacheRow = kvs.getRow("page-cache", hashedPage);
                    if (pageCacheRow != null && pageCacheRow.get("url") != null && !Objects.equals(pageCacheRow.get("url"), urlString)) {
                        String otherUrl = pageCacheRow.get("url");
                        kvs.put("pt-crawl", rowKey, "canonicalURL", otherUrl);
                    } else {
                        pageCacheRow = new Row(hashedPage);
                        pageCacheRow.put("url", urlString);
                        kvs.putRow("page-cache", pageCacheRow);
                        kvs.put("pt-crawl", rowKey, "page", page);

                        // Extract normalized URLs and add anchor entries
                        // nextUrls = extractUrls(page, urlString);
                        Map<String, StringBuilder> anchorTextMap = extractUrlsWithAnchors(page, urlString);
                        for (Map.Entry<String, StringBuilder> entry : anchorTextMap.entrySet()) {
                            String childUrl = entry.getKey();

                            // Check if URL is blacklisted, if yes then ignore
                            if (isBlacklisted(childUrl, kvs, blacklistTable)) {
                                System.out.println("Ignoring " + childUrl + " as it is blacklisted");
                                continue;
                            }

                            // Check if URL is blocked by robots.txt, if yes then ignore
                            if (!RobotsTxtParser.isAllowedByRobots(childUrl, kvs)) {
                                System.out.println("Ignoring " + childUrl + " as it is blocked by robots.txt");
                                continue;
                            }

                            nextUrls.add(childUrl);

                            String allAnchors = entry.getValue().toString().trim();
                            if (allAnchors.isEmpty())
                                continue;

                            // EC3: Adding anchor entries
                            String columnName = "anchors:" + urlString;
                            kvs.put("pt-crawl", Hasher.hash(childUrl), columnName, allAnchors);
                        }
                    }
                }
            } else if (responseCode >= 300 && responseCode <= 308) {
                // Redirects
                String redirectUrl = conn.getHeaderField("Location");
                nextUrls.add(normalizeUrl(redirectUrl, urlString));
            }

            kvs.put("pt-crawl", rowKey, "url", urlString);
            kvs.put("pt-crawl", rowKey, "responseCode", Integer.toString(responseCode));

            if (contentType != null) kvs.put("pt-crawl", rowKey, "contentType", contentType);
            if (contentLength != null) kvs.put("pt-crawl", rowKey, "length", contentLength);

            System.out.println("Stored: " + urlString + " (" + responseCode + ")");
        } catch (Exception e) {
            System.out.println("Crawler failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) conn.disconnect();
        }

        return nextUrls;
    }

    public static List<String> extractUrls(String html, String parentUrl) {
        List<String> urls = new ArrayList<>();
        if (html == null || html.isEmpty())
            return urls;

        // Regex for <a ...> tags (case-insensitive)
        Pattern tagPattern = Pattern.compile("<\\s*a\\s+[^>]*>", Pattern.CASE_INSENSITIVE);
        Matcher tagMatcher = tagPattern.matcher(html);

        while (tagMatcher.find()) {
            String tag = tagMatcher.group(); // e.g. <a href="...">

            // Skip closing tags
            if (tag.startsWith("</") || tag.contains("</"))
                continue;

            // Split by spaces, first token is tag name
            String[] parts = tag.split("\\s+");
            if (parts.length == 0)
                continue;

            String tagName = parts[0].replace("<", "").toLowerCase();
            if (!tagName.equals("a"))
                continue;

            // Look for href attribute using regex
            Pattern hrefPattern = Pattern.compile("href\\s*=\\s*['\"]([^'\"]+)['\"]", Pattern.CASE_INSENSITIVE);
            Matcher hrefMatcher = hrefPattern.matcher(tag);

            if (hrefMatcher.find()) {
                String url = hrefMatcher.group(1).trim();
                urls.add(normalizeUrl(url, parentUrl));
            }
        }

        return urls;
    }

    public static Map<String, StringBuilder> extractUrlsWithAnchors(String html, String parentUrl) {
        // Map from normalized URL → combined anchor texts
        Map<String, StringBuilder> anchorTextMap = new HashMap<>();

        if (html == null || html.isEmpty())
            return anchorTextMap;

        // Regex for anchor tags with href and optional inner text
        Pattern anchorPattern = Pattern.compile(
                "<\\s*a\\s+[^>]*href\\s*=\\s*['\"]([^'\"]+)['\"][^>]*>(.*?)</a\\s*>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher anchorMatcher = anchorPattern.matcher(html);

        while (anchorMatcher.find()) {
            String href = anchorMatcher.group(1).trim();
            String anchorText = anchorMatcher.group(2).replaceAll("<[^>]+>", "").trim(); // remove nested tags

            if (href.isEmpty())
                continue;

            // Normalize the URL relative to the parent page
            String normalized = normalizeUrl(href, parentUrl);
            if (normalized == null)
                continue;

            // Collect anchor text if present
            if (!anchorText.isEmpty()) {
                anchorTextMap
                        .computeIfAbsent(normalized, k -> new StringBuilder())
                        .append(anchorText).append(" ");
            }
        }

        return anchorTextMap;
    }

    public static String normalizeUrl(String foundUrl, String baseUrl) {
        if (foundUrl == null || foundUrl.isEmpty()) return null;

        // Remove fragments (#xyz)
        int hashIndex = foundUrl.indexOf('#');
        if (hashIndex != -1)
            foundUrl = foundUrl.substring(0, hashIndex);
        if (foundUrl.isEmpty()) return null;

        try {
            // Case 1: Found URL is absolute (has http:// or https://)
            if (foundUrl.startsWith("http://") || foundUrl.startsWith("https://")) {
                String[] parts = URLParserSimple.parseURL(foundUrl);
                String proto = parts[0];
                String host  = parts[1];
                String port  = parts[2];
                String path  = parts[3];

                if (proto == null || host == null) return null;
                if (port == null) {
                    port = proto.equals("https") ? "443" : "80";
                }
                if (path == null || path.isEmpty()) {
                    path = "/";
                }

                return proto + "://" + host + ":" + port + path;
            }

            // Parse base URL with provided parser
            String[] baseParts = URLParserSimple.parseURL(baseUrl);
            String baseProto = baseParts[0];
            String baseHost  = baseParts[1];
            String basePort  = baseParts[2];
            String basePath  = baseParts[3];

            // Add default port if missing
            if (basePort == null) {
                basePort = baseProto.equals("https") ? "443" : "80";
            }

            // Case 2: Starts with "/" → absolute path on same host
            if (foundUrl.startsWith("/")) {
                return baseProto + "://" + baseHost + ":" + basePort + foundUrl;
            }

            // Case 3: Relative path (e.g. "../", "foo.html", etc.)
            String baseDir = basePath;
            if (!baseDir.endsWith("/")) {
                int lastSlash = baseDir.lastIndexOf('/');
                baseDir = (lastSlash >= 0) ? baseDir.substring(0, lastSlash + 1) : "/";
            }

            // Combine and normalize relative paths (.., .)
            List<String> parts = new ArrayList<>(Arrays.asList(baseDir.split("/")));
            List<String> relParts = Arrays.asList(foundUrl.split("/"));
            for (String p : relParts) {
                if (p.equals("..")) {
                    if (parts.size() > 1) parts.remove(parts.size() - 1);
                } else if (!p.equals(".") && !p.isEmpty()) {
                    parts.add(p);
                }
            }
            String normalizedPath = String.join("/", parts);
            if (!normalizedPath.startsWith("/")) normalizedPath = "/" + normalizedPath;

            // Recombine full URL
            String normalized = baseProto + "://" + baseHost + ":" + basePort + normalizedPath;

            // Filter unwanted URLs (protocol or file extension)
            if (!normalized.startsWith("http://") && !normalized.startsWith("https://"))
                return null;

            String lower = normalized.toLowerCase();
            if (lower.matches(".*\\.(jpg|jpeg|png|gif|pdf|css|js|ico|mp4|zip|tar|gz)$"))
                return null;

            return normalized;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static String getOrFetchRobotsTxt(KVSClient kvs, String protocol, String host, String port) throws IOException {
        // Try cache first
        byte[] robotsTxtBytes = kvs.get("hosts", host, "robotsTxt");
        String robotsTxt = robotsTxtBytes != null ? new String(robotsTxtBytes, StandardCharsets.UTF_8) : null;
        if (robotsTxt != null) {
            return robotsTxt;
        }

        // Not cached — fetch it
        String robotsUrl = protocol + "://" + host + ":" + port + "/robots.txt"; //
        StringBuilder sb = new StringBuilder();
        int code = -1;

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(robotsUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);
            conn.setRequestProperty("User-Agent", USER_AGENT);
            code = conn.getResponseCode();

            if (code == 200) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                }
            }
        } catch (IOException e) {
            // If robots.txt not found or fails, treat as empty
        }

        String content = (code == 200) ? sb.toString() : "";
        kvs.put("hosts", host, "robotsTxt", content); // cache it
        return content;
    }

    public static boolean isBlacklisted(String url, KVSClient kvs, String blacklistTable) {
        if (blacklistTable == null) return false;

        try {
            Iterator<Row> rows = kvs.scan(blacklistTable);
            while (rows.hasNext()) {
                Row r = rows.next();
                String pattern = r.get("pattern");
                if (pattern == null) continue;

                // Convert wildcard to regex: * → .* (escape regex special chars first)
                String regex = pattern
                        .replace(".", "\\.")
                        .replace("?", "\\?")
                        .replace("*", ".*");

                if (url.matches(regex)) {
                    System.out.println("Skipping blacklisted URL: " + url + " (matched " + pattern + ")");
                    return true;
                }
            }
        } catch (Exception e) {
            System.err.println("Blacklist check failed: " + e.getMessage());
        }

        return false;
    }

    public class RobotsTxtParser {

        public static class RobotsRules {
            List<String> allows = new ArrayList<>();
            List<String> disallows = new ArrayList<>();
            Double crawlDelay = null;
        }

        // Parses entire robots.txt into user-agent -> rules map
        public static Map<String, RobotsRules> parse(String robotsTxt) {
            Map<String, RobotsRules> rulesMap = new HashMap<>();
            if (robotsTxt == null) return rulesMap;

            String currentUA = null;
            RobotsRules currentRules = null;

            for (String rawLine : robotsTxt.split("\n")) {
                String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String lower = line.toLowerCase();

                if (lower.startsWith("user-agent:")) {
                    currentUA = line.split(":", 2)[1].trim().toLowerCase();
                    currentRules = rulesMap.computeIfAbsent(currentUA, k -> new RobotsRules());
                } else if (currentUA != null && currentRules != null) {
                    if (lower.startsWith("allow:")) {
                        currentRules.allows.add(line.split(":", 2)[1].trim());
                    } else if (lower.startsWith("disallow:")) {
                        currentRules.disallows.add(line.split(":", 2)[1].trim());
                    } else if (lower.startsWith("crawl-delay:")) {
                        try {
                            currentRules.crawlDelay = Double.parseDouble(line.split(":", 2)[1].trim());
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }

            return rulesMap;
        }

        // Determine which rule group applies — either specific crawler or wildcard
        private static RobotsRules getApplicableRules(Map<String, RobotsRules> map) {
            if (map.containsKey(USER_AGENT)) {
                return map.get(USER_AGENT);
            } else if (map.containsKey("*")) {
                return map.get("*");
            } else {
                return new RobotsRules(); // default: allow all, default delay
            }
        }

        // Checks if a URL is allowed under these rules
        public static boolean isAllowed(String url, Map<String, RobotsRules> map) {
            RobotsRules rules = getApplicableRules(map);

            String path;
            try {
                path = new URL(url).getPath();
            } catch (MalformedURLException e) {
                return true;
            }

            String matchedRule = null;
            boolean allowed = true;

            // First match counts, longest prefix wins (RFC 9309)
            for (String prefix : rules.disallows) {
                if (path.startsWith(prefix) && (matchedRule == null || prefix.length() > matchedRule.length())) {
                    matchedRule = prefix;
                    allowed = false;
                }
            }

            for (String prefix : rules.allows) {
                if (path.startsWith(prefix) && (matchedRule == null || prefix.length() > matchedRule.length())) {
                    matchedRule = prefix;
                    allowed = true;
                }
            }

            return allowed;
        }

        private static boolean isAllowedByRobots(String urlString, KVSClient kvs) {
            try {
                String[] parts = URLParserSimple.parseURL(urlString);
                String host = parts[1];
                if (host == null) return false;

                // Retrieve cached robots.txt (don’t fetch again)
                byte[] robotsBytes = kvs.get("hosts", host, "robotsTxt");
                if (robotsBytes == null) return true;
                String robotsTxt = new String(robotsBytes, StandardCharsets.UTF_8);

                Map<String, RobotsTxtParser.RobotsRules> rulesMap = RobotsTxtParser.parse(robotsTxt);
                return RobotsTxtParser.isAllowed(urlString, rulesMap);

            } catch (Exception e) {
                // If any parsing error, fail-safe: treat as allowed
                return true;
            }
        }

        // Get crawl delay (default if not specified)
        public static double getCrawlDelay(Map<String, RobotsRules> map) {
            RobotsRules rules = getApplicableRules(map);
            return (rules.crawlDelay != null) ? rules.crawlDelay : DEFAULT_DELAY_SECONDS;
        }
    }
}
