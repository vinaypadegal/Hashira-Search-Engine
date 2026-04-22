package cis5550.tools;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLParser {

  public static String[] parseURL(String url) {
    String result[] = new String[4];
    int slashslash = url.indexOf("//");
    if (slashslash>0) {
      result[0] = url.substring(0, slashslash-1);
      int nextslash = url.indexOf('/', slashslash+2);
      if (nextslash>=0) {
        result[1] = url.substring(slashslash+2, nextslash);
        result[3] = url.substring(nextslash);
      } else {
        result[1] = url.substring(slashslash+2);
        result[3] = "/";
      }
      int colonPos = result[1].indexOf(':');
      if (colonPos > 0) {
        result[2] = result[1].substring(colonPos+1);
        result[1] = result[1].substring(0, colonPos);
      }
    } else {
      result[3] = url;
    }

    return result;
  }

    

    public static String normalizeSeedURL(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }

        // Filter (works on encoded URL as-is)
        if (shouldFilterURL(url)) {
            return null;
        }

        // Filter query parameters (keep only allowed ones)
        url = filterQueryParams(url);

        // Step 4: Normalize - Remove fragment
        url = url.split("#")[0].trim();
        if (url.isEmpty()) {
            return null;
        }

        try {
            String[] parsed = parseURL(url);
            String protocol = parsed[0];
            String host = parsed[1];
            String portStr = parsed[2];
            String path = parsed[3];

            // Only handle http and https
            if (protocol == null || (!protocol.equals("http") && !protocol.equals("https"))) {
                return null;
            }

            // Add default port if missing
            int port;
            if (portStr == null || portStr.isEmpty()) {
                port = protocol.equals("http") ? 80 : 443;
            } else {
                port = Integer.parseInt(portStr);
            }

            // Ensure path starts with /
            if (path == null || path.isEmpty() || !path.startsWith("/")) {
                path = "/";
            }

            // Encode spaces in the path so that Java's URL/HTTP handling does not break
            // (e.g., "2021_DSWG Recommendations" -> "2021_DSWG%20Recommendations")
            path = path.replace(" ", "%20");

            return protocol + "://" + host + ":" + port + path;
        } catch (Exception e) {
            return null;
        }
    }
    
    public static List<String> normalizeURLs(List<String> rawUrls, String baseUrl) {
        List<String> normalized = new ArrayList<>();
        if (rawUrls == null || baseUrl == null) {
            return normalized;
        }
        
        // First normalize the base URL
        String normalizedBase = normalizeSeedURL(baseUrl);
        if (normalizedBase == null) {
            return normalized;
        }
        
        String[] baseParsed = parseURL(normalizedBase);
        String baseProtocol = baseParsed[0];
        String baseHost = baseParsed[1];
        String basePortStr = baseParsed[2];
        String basePath = baseParsed[3];
        
        int basePort;
        if (basePortStr == null || basePortStr.isEmpty()) {
            basePort = baseProtocol.equals("http") ? 80 : 443;
        } else {
            basePort = Integer.parseInt(basePortStr);
        }
        
        for (String url : rawUrls) {
            try {
                // Filter (works on encoded URL as-is)
                if (shouldFilterURL(url)) {
                    continue;
                }

                // Filter query parameters (keep only allowed ones)
                url = filterQueryParams(url);

                // Remove fragment
                url = url.split("#")[0].trim();
                if (url.isEmpty()) {
                    continue;
                }

                String normalizedUrl = null;

                // Check if it's an absolute URL (starts with protocol)
                if (url.contains("://")) {
                    String[] parsed = parseURL(url);
                    String protocol = parsed[0];
                    String host = parsed[1];
                    String portStr = parsed[2];
                    String path = parsed[3];
                    
                    // Only allow http and https
                    if (protocol == null || (!protocol.equals("http") && !protocol.equals("https"))) {
                        continue;
                    }
                    
                    // Add default port if missing
                    int port;
                    if (portStr == null || portStr.isEmpty()) {
                        port = protocol.equals("http") ? 80 : 443;
                    } else {
                        port = Integer.parseInt(portStr);
                    }
                    
                    // Ensure path starts with /
                    if (path == null || path.isEmpty() || !path.startsWith("/")) {
                        path = "/";
                    }

                    // Encode spaces in absolute URL paths
                    path = path.replace(" ", "%20");
                    
                    normalizedUrl = protocol + "://" + host + ":" + port + path;
                } else {
                    // Relative URL - resolve against base URL
                    String resolvedPath;
                    
                    if (url.startsWith("/")) {
                        // Absolute path on same host
                        resolvedPath = url;
                    } else {
                        // Relative path - resolve against base path
                        // Remove filename from base path if present
                        String baseDir = basePath;
                        if (!baseDir.endsWith("/")) {
                            int lastSlash = baseDir.lastIndexOf('/');
                            if (lastSlash >= 0) {
                                baseDir = baseDir.substring(0, lastSlash + 1);
                            } else {
                                baseDir = "/";
                            }
                        }
                        
                        // Handle .. and . in relative path
                        String[] parts = url.split("/");
                        List<String> resolvedParts = new ArrayList<>();
                        String[] baseParts = baseDir.split("/");
                        
                        // Add base path parts (skip empty first element)
                        for (int i = 1; i < baseParts.length; i++) {
                            if (!baseParts[i].isEmpty()) {
                                resolvedParts.add(baseParts[i]);
                            }
                        }
                        
                        // Process relative path parts
                        for (String part : parts) {
                            if (part.equals("..")) {
                                if (!resolvedParts.isEmpty()) {
                                    resolvedParts.remove(resolvedParts.size() - 1);
                                }
                            } else if (!part.equals(".") && !part.isEmpty()) {
                                resolvedParts.add(part);
                            }
                        }
                        
                        // Reconstruct path
                        resolvedPath = "/" + String.join("/", resolvedParts);
                    }

                    // Encode spaces in resolved relative paths
                    resolvedPath = resolvedPath.replace(" ", "%20");
                    
                    normalizedUrl = baseProtocol + "://" + baseHost + ":" + basePort + resolvedPath;
                }
                
                // Filter out unwanted URLs
                if (normalizedUrl == null) {
                    continue;
                }
                
                // Filter by protocol (should already be http/https, but double-check)
                if (!normalizedUrl.startsWith("http://") && !normalizedUrl.startsWith("https://")) {
                    continue;
                }
                
                // Filter by extension (jpg, jpeg, gif, png, txt)
                String lowerUrl = normalizedUrl.toLowerCase();
                if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") ||
                    lowerUrl.endsWith(".gif") || lowerUrl.endsWith(".png") ||
                    lowerUrl.endsWith(".txt")) {
                    continue;
                }

                normalized.add(normalizedUrl);
            } catch (Exception e) {
                // Skip invalid URLs
                continue;
            }
        }
        
        // Remove duplicates while preserving order
        return new ArrayList<>(new LinkedHashSet<>(normalized));
    }

    /**
     * Filters query parameters to keep only allowed ones with valid values
     * @param url The URL to filter (can be absolute or relative, with query string)
     * @return URL with filtered query parameters, sorted alphabetically
     */
    private static String filterQueryParams(String url) {
        // Extract query string
        int queryIndex = url.indexOf('?');
        if (queryIndex == -1) {
            return url; // No query params
        }

        String baseUrl = url.substring(0, queryIndex);
        String queryString = url.substring(queryIndex + 1);

        // Parse query parameters
        Map<String, String> params = new HashMap<>();
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length >= 1) {
                String key = keyValue[0].toLowerCase(); // Case insensitive
                String value = keyValue.length == 2 ? keyValue[1] : "";

                // Keep first occurrence if duplicate
                if (!params.containsKey(key)) {
                    params.put(key, value);
                }
            }
        }

        // Check if any search parameter exists
        boolean hasSearchParam = params.containsKey("q") || params.containsKey("query") ||
                                 params.containsKey("search") || params.containsKey("s") ||
                                 params.containsKey("term") || params.containsKey("keywords") ||
                                 params.containsKey("search_query");
        // TODO: Add 'k' for amazon domains only

        int maxPageValue = hasSearchParam ? 3 : 5;

        // Filter parameters based on rules
        Map<String, String> filtered = new HashMap<>();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // id: keep if length <= 25
            if (key.equals("id")) {
                if (value.length() <= 25) {
                    filtered.put(key, value);
                }
            }
            // Pagination: keep if value <= maxPageValue (3 with search, 5 without)
            else if (key.equals("page") || key.equals("p") ||
                     key.equals("page_num") || key.equals("pagenumber")) {
                try {
                    int pageNum = Integer.parseInt(value);
                    if (pageNum <= maxPageValue) {
                        filtered.put(key, value);
                    }
                } catch (NumberFormatException e) {
                    // Not a valid number, skip
                }
            }
            // Search terms: always keep
            else if (key.equals("q") || key.equals("query") || key.equals("search") ||
                     key.equals("s") || key.equals("term") || key.equals("keywords") ||
                     key.equals("search_query")) {
                filtered.put(key, value);
            }
            // Category: keep if length <= 18
            else if (key.equals("category") || key.equals("cat") || key.equals("c")) {
                if (value.length() <= 18) {
                    filtered.put(key, value);
                }
            }
            // Tags: keep if length <= 18
            else if (key.equals("tag") || key.equals("topic") || key.equals("label")) {
                if (value.length() <= 18) {
                    filtered.put(key, value);
                }
            }
            // All other parameters are dropped
        }

        // If no params remain, return URL without query string
        if (filtered.isEmpty()) {
            return baseUrl;
        }

        // Sort params alphabetically and reconstruct query string
        List<String> sortedKeys = new ArrayList<>(filtered.keySet());
        Collections.sort(sortedKeys);

        StringBuilder result = new StringBuilder(baseUrl);
        result.append('?');
        boolean first = true;
        for (String key : sortedKeys) {
            if (!first) {
                result.append('&');
            }
            String value = filtered.get(key);
            // Encode spaces in query parameter values
            if (value != null && !value.isEmpty()) {
                value = value.replace(" ", "%20");
            }
            result.append(key).append('=').append(value);
            first = false;
        }

        return result.toString();
    }

    /**
     * Checks if URL contains only valid characters
     * Decodes the URL first to check for non-ASCII characters (which would filter non-English pages)
     * Allowed: A-Z, a-z, 0-9, -, ., _, ~, :, /, ?, #, [, ], @, !, $, &, ', (, ), *, +, ,, ;, %, =
     * @param url The URL to check (should be encoded)
     * @return true if all characters are valid, false otherwise
     */
    private static boolean isValidUrlCharacters(String url) {
        // Decode the URL to check for non-ASCII characters
        // This catches URLs with Chinese, Cyrillic, Arabic, etc. that are percent-encoded
        String decodedUrl;
        try {
            decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            // Malformed encoding - let it fail during HTTP request
            return false; // Don't filter here, let it fail naturally
        }

        // Check characters in DECODED URL (to catch non-ASCII)
        for (int i = 0; i < decodedUrl.length(); i++) {
            char c = decodedUrl.charAt(i);

            // Check if character is in allowed set
            boolean valid = (c >= 'A' && c <= 'Z') ||  // A-Z
                           (c >= 'a' && c <= 'z') ||  // a-z
                           (c >= '0' && c <= '9') ||  // 0-9
                           c == '-' || c == '.' || c == '_' || c == '~' ||  // unreserved
                           c == ':' || c == '/' || c == '?' || c == '#' ||  // gen-delims
                           c == '[' || c == ']' || c == '@' ||              // gen-delims
                           c == '!' || c == '$' || c == '&' || c == '\'' || // sub-delims
                           c == '(' || c == ')' || c == '*' || c == '+' ||  // sub-delims
                           c == ',' || c == ';' || c == '=' || c == '%' ||  // sub-delims + percent
                           c == ' ';                                        // space (from decoding %20)

            if (!valid) {
                return false; // Found non-ASCII or invalid character
            }
        }
        return true;
    }

    /**
     * Filters out low-quality URLs based on various heuristics
     * @param url The URL to check (can be absolute or relative)
     * @return true if the URL should be filtered out, false otherwise
     */
    private static boolean shouldFilterURL(String url) {
        // 0. Filter URLs with invalid characters (must be first, before any processing)
        if (!isValidUrlCharacters(url)) {
            return true; // Filter out URLs with invalid characters
        }

        // Strip protocol a
        // nd domain to work with just the path+query portion
        // This makes the function work for both absolute and relative URLs
        // Example: "http://example.com/path?q=1" -> "/path?q=1"
        //          "relative/path?q=1" -> "relative/path?q=1"
        int protocolEnd = url.indexOf("://");
        if (protocolEnd != -1) {
            // Absolute URL - strip protocol and domain
            String afterProtocol = url.substring(protocolEnd + 3);
            int pathStart = afterProtocol.indexOf('/');
            if (pathStart != -1) {
                url = afterProtocol.substring(pathStart); // Now url = "/path?query..."
            } else {
                url = "/"; // Just domain with no path
            }
        }
        // Now url is either relative or just the path portion of an absolute URL

        // Normalize relative URLs to start with / for consistent pattern matching
        // This ensures all our regex patterns work uniformly
        if (!url.startsWith("/")) {
            url = "/" + url;
        }

        // 1. Filter out pages with multiple question marks (low quality indicator)
        int firstQuestion = url.indexOf('?');
        int lastQuestion = url.lastIndexOf('?');
        if (firstQuestion != -1 && firstQuestion != lastQuestion) {
            return true;
        }

        // 2. Filter out pages with matrix parameters (semicolon in path)
        // Matrix params appear before query string, e.g., /path;param=value?query=foo
        int semicolonIndex = url.indexOf(';');
        if (semicolonIndex != -1) {
            // Check if semicolon appears in the path (before query string or at all)
            int queryIndex = url.indexOf('?');
            if (queryIndex == -1 || semicolonIndex < queryIndex) {
                return true;
            }
        }

        // 3. Filter out pages with path-embedded parameters (e.g., /q=dog/limit=20)
        // Extract path portion only (before query string)
        String path = url;
        int queryStart = path.indexOf('?');
        if (queryStart != -1) {
            path = path.substring(0, queryStart);
        }

        // Check for = signs in path segments (indicates embedded parameters)
        // Pattern: /something=value/ where = appears between slashes
        if (path.matches(".*/[^/]*=[^/]*/.*")) {
            return true;
        }

        // 4. Filter out calendar URLs
        if (url.matches("^.*calendar.*$")) {
            return true;
        }

        // 5. Filter out URLs with repeating path segments
        // Examples: /foo/bar/foo/ or /abc/abc/
        if (url.matches("^.*?(/.+?/).*?\\1.*$|^.*?/(.+?/)\\2.*$")) {
            return true;
        }

        // 6. Filter out URLs where any path segment is longer than 100 characters
        // Split path by "/" and check each segment length
        String[] segments = path.split("/");
        for (String segment : segments) {
            if (segment.length() > 100) {
                return true;
            }
        }

        return false;
    }

    public static List<String> extractURLs(String page) {
        List<String> urls = new ArrayList<>();
        if (page == null) {
            return urls;
        }
        
        // Find all opening tags
        Pattern tagPattern = Pattern.compile("<\\s*([a-zA-Z0-9]+)([^>]*)>", Pattern.CASE_INSENSITIVE);
        Matcher tagMatcher = tagPattern.matcher(page);
        
        while (tagMatcher.find()) {
            String tagName = tagMatcher.group(1).toLowerCase();
            
            // Only process anchor tags
            if (!tagName.equals("a")) {
                continue;
            }
            
            // Search for href attribute in tag body
            String tagBody = tagMatcher.group(2);
            // Match href="..." or href='...' or href=... (without quotes)
            Pattern hrefPattern = Pattern.compile("href\\s*=\\s*['\"]?([^'\"\\s>]+)['\"]?", Pattern.CASE_INSENSITIVE);
            Matcher hrefMatcher = hrefPattern.matcher(tagBody);
            
            if (hrefMatcher.find()) {
                String url = hrefMatcher.group(1).trim();
                if (!url.isEmpty()) {
                    urls.add(url);
                }
            }
        }
        
        return urls;
    }
}
