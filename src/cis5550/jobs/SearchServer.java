package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.SpellCheckerHelper;
import cis5550.tools.SpellCheckerHelper.Suggestion;
import cis5550.tools.URLParser;
import static cis5550.webserver.Server.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SearchServer {
    private static Logger logger = Logger.getLogger(SearchServer.class);
    private static KVSClient kvs;
    private static String coordinatorAddress;
    private static final ExecutorService spellExecutor = Executors.newFixedThreadPool(2);

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: SearchServer <port> <kvsCoordinator>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        coordinatorAddress = args[1];
        kvs = new KVSClient(coordinatorAddress);

        Ranking.initialize(kvs);
        try {
            SpellCheckerHelper.init();
        } catch (Exception e) {
            logger.warn("Spell checker initialization failed: " + e.getMessage());
        }

        port(port);
        securePort(443);

        get("/", (req, res) -> {
            res.type("text/html");
            return getSearchPage();
        });

        post("/search", (req, res) -> {
            res.type("application/json");
            String query = req.queryParams("q");
            boolean debug = "true".equals(req.queryParams("debug"));
            String mode = req.queryParams("mode");
            
            // New flag to prevent auto-correction loop
            boolean forceOriginal = "true".equals(req.queryParams("force"));

            int offset = 0;
            int limit = 20;
            try {
                String offsetStr = req.queryParams("offset");
                String limitStr = req.queryParams("limit");
                if (offsetStr != null) offset = Integer.parseInt(offsetStr);
                if (limitStr != null) limit = Integer.parseInt(limitStr);
            } catch (NumberFormatException e) {}

            logger.info("Search query: " + query + " (offset: " + offset + ", force: " + forceOriginal + ")");

            if (query == null || query.trim().isEmpty()) {
                return "{\"error\": \"Query is required\"}";
            }

            String correctedQuery = query.trim();
            boolean correctedUsed = false;

            // Only run spell check if NOT forced
            if (!forceOriginal) {
                StringBuilder correctedSb = new StringBuilder();
                String[] words = query.trim().split("\\s+");
                for (String w : words) {
                    List<Suggestion> suggs = SpellCheckerHelper.suggest(w, 1);
                    if (!suggs.isEmpty()) {
                        correctedSb.append(suggs.get(0).word).append(" ");
                    } else {
                        correctedSb.append(w).append(" ");
                    }
                }
                correctedQuery = correctedSb.toString().trim();
                correctedUsed = !correctedQuery.equalsIgnoreCase(query.trim());
            }

            try {
                Map<String, Object> results;
                if ("image".equalsIgnoreCase(mode)) {
                    results = Ranking.searchImagesWithDetails(correctedQuery, debug);
                } else {
                    results = Ranking.searchWithDetails(correctedQuery, debug);
                }

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> allResults = (List<Map<String, Object>>) results.get("results");
                int totalResults = allResults.size();
                int endIndex = Math.min(offset + limit, totalResults);
                
                List<Map<String, Object>> paginatedResults = Collections.emptyList();
                if (offset < totalResults) {
                     paginatedResults = allResults.subList(offset, endIndex);
                }
                System.out.println("starting to bulk load meta info");
                // Enrich results with stored meta info (title/description) from pt-meta-info via bulk get
                try {
                    List<String> hashes = new ArrayList<>(paginatedResults.size());
                    List<String> urls = new ArrayList<>(paginatedResults.size());
                    for (Map<String, Object> r : paginatedResults) {
                        Object urlObj = r.get("url");
                        if (urlObj == null || urlObj.toString().isEmpty()) {
                            continue;
                        }
                        String urlStr = urlObj.toString();
                        // Use the same normalization scheme as crawler/meta extractor (explicit port)
                        String normalized = URLParser.normalizeSeedURL(urlStr);
                        String hashSource = normalized != null ? normalized : urlStr;
                        urls.add(hashSource);
                        hashes.add(Hasher.hash(hashSource));
                    }

                    // Try bulk get first, fallback to individual gets if bulk returns empty
                    Map<String, Row> metaRows = kvs.bulkGetRows("pt-meta-info", hashes);
                    if (metaRows.isEmpty() && !hashes.isEmpty()) {
                        // Fallback: try individual gets to verify data exists
                        logger.debug("Bulk get returned empty, trying individual gets for " + hashes.size() + " hashes");
                        for (int i = 0; i < hashes.size(); i++) {
                            try {
                                Row row = kvs.getRow("pt-meta-info", hashes.get(i));
                                if (row != null && row.columns().size() > 0) {
                                    metaRows.put(hashes.get(i), row);
                                }
                            } catch (Exception e) {
                                logger.debug("Individual get failed for hash " + hashes.get(i) + ": " + e.getMessage());
                            }
                            // if (i == 5) {break;}
                        }
                        logger.debug("Individual gets returned " + metaRows.size() + " rows");
                    }
                    for (int i = 0; i < urls.size(); i++) {
                        Row metaRow = metaRows.get(hashes.get(i));
                        if (metaRow == null) {
                            continue;
                        }
                        Map<String, Object> r = paginatedResults.get(i);
                        String title = metaRow.get("title");
                        String description = metaRow.get("description");
                        if (title != null && !title.isEmpty()) {
                            r.put("title", title);
                        }
                        if (description != null && !description.isEmpty()) {
                            String truncated = truncate(description, 200);
                            r.put("description", truncated);
                            // If ranking did not supply a snippet, use description as snippet
                            if (!r.containsKey("snippet") || r.get("snippet") == null || r.get("snippet").toString().isEmpty()) {
                                r.put("snippet", truncated);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to bulk load meta info: " + e.getMessage());
                }

                Map<String, Object> paginatedResponse = new HashMap<>();
                paginatedResponse.put("results", paginatedResults);
                paginatedResponse.put("query", results.get("query"));
                paginatedResponse.put("total", totalResults);
                paginatedResponse.put("offset", offset);
                paginatedResponse.put("limit", limit);
                paginatedResponse.put("hasMore", endIndex < totalResults);
                paginatedResponse.put("originalQuery", query);
                paginatedResponse.put("correctedQuery", correctedQuery);
                paginatedResponse.put("correctedUsed", correctedUsed);

                return formatJSON(paginatedResponse);
            } catch (Exception e) {
                e.printStackTrace();
                return "{\"error\": \"" + escapeJSON(e.getMessage()) + "\"}";
            }
        });

        get("/suggest", (req, res) -> {
            res.type("application/json");
            String wordParam = req.queryParams("word");
            if (wordParam == null || wordParam.trim().isEmpty()) {
                return "{\"word\":\"\",\"suggestions\":[]}";
            }
            String word = wordParam;
            try { word = URLDecoder.decode(word, StandardCharsets.UTF_8.name()); } catch (Exception e) {}
            
            long start = System.nanoTime();
            final String wordFinal = word;
            try {
                List<Suggestion> suggestions = CompletableFuture
                        .supplyAsync(() -> SpellCheckerHelper.suggest(wordFinal, 5), spellExecutor)
                        .get(800, TimeUnit.MILLISECONDS);
                double micros = (System.nanoTime() - start) / 1000.0;
                return SpellCheckerHelper.suggestionsToJson(wordFinal, suggestions, micros);
            } catch (Exception e) {
                return "{\"word\":\"" + escapeJSON(wordFinal) + "\",\"suggestions\":[]}";
            }
        });

        System.out.println("Search server started on port " + port);
    }

    private static String getSearchPage() {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Hashira Search</title>\n" +
                "    <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n" +
                "    <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\n" +
                "    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap\" rel=\"stylesheet\">\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --primary: #4285f4;\n" +
                "            --text-main: #202124;\n" +
                "            --text-secondary: #5f6368;\n" +
                "            --bg: #ffffff;\n" +
                "            --border: #dfe1e5;\n" +
                "            --shadow: 0 1px 6px rgba(32,33,36,.28);\n" +
                "        }\n" +
                "        * { margin: 0; padding: 0; box-sizing: border-box; }\n" +
                "        body {\n" +
                "            font-family: 'Inter', sans-serif;\n" +
                "            color: var(--text-main);\n" +
                "            background-color: var(--bg);\n" +
                "            min-height: 100vh;\n" +
                "            display: flex;\n" +
                "            flex-direction: column;\n" +
                "        }\n" +
                "        .app-container {\n" +
                "            display: flex;\n" +
                "            flex-direction: column;\n" +
                "            align-items: center;\n" +
                "            padding-top: 20vh;\n" +
                "            transition: all 0.4s cubic-bezier(0.25, 0.8, 0.25, 1);\n" +
                "        }\n" +
                "        .app-container.has-results {\n" +
                "            padding-top: 15px;\n" +
                "            padding-bottom: 15px;\n" +
                "            padding-left: 30px;\n" +
                "            border-bottom: 1px solid #ebebeb;\n" +
                "            background: white;\n" +
                "            position: sticky;\n" +
                "            top: 0;\n" +
                "            z-index: 100;\n" +
                "            box-shadow: 0 2px 4px rgba(0,0,0,0.05);\n" +
                "            flex-direction: row;\n" +
                "            justify-content: flex-start;\n" +
                "        }\n" +
                "        .logo-container {\n" +
                "            margin-bottom: 30px;\n" +
                "            transition: opacity 0.2s ease, transform 0.3s ease;\n" +
                "        }\n" +
                "        .logo {\n" +
                "            font-size: 4rem;\n" +
                "            font-weight: 700;\n" +
                "            letter-spacing: -2px;\n" +
                "            background: linear-gradient(90deg, #4285f4, #db4437, #f4b400, #4285f4);\n" +
                "            -webkit-background-clip: text;\n" +
                "            -webkit-text-fill-color: transparent;\n" +
                "            background-size: 200% auto;\n" +
                "            animation: shine 5s linear infinite;\n" +
                "            cursor: default;\n" +
                "            transition: all 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);\n" +
                "        }\n" +
                "        .logo:hover {\n" +
                "             background-image: linear-gradient(90deg, #FF00CC, #3333FF, #FF00CC);\n" +
                "             transform: scale(1.05) rotate(-2deg);\n" +
                "             text-shadow: 0 0 10px rgba(255,0,204,0.3);\n" +
                "             animation: shine 2s linear infinite;\n" +
                "        }\n" +
                "        .logo-small {\n" +
                "            display: none;\n" +
                "            font-size: 24px;\n" +
                "            font-weight: 600;\n" +
                "            letter-spacing: -1px;\n" +
                "            margin-right: 35px;\n" +
                "            cursor: pointer;\n" +
                "            background: linear-gradient(90deg, #4285f4, #db4437, #f4b400, #4285f4);\n" +
                "            -webkit-background-clip: text;\n" +
                "            -webkit-text-fill-color: transparent;\n" +
                "            background-size: 200% auto;\n" +
                "            white-space: nowrap;\n" +
                "            transition: transform 0.2s;\n" +
                "        }\n" +
                "        .logo-small:hover {\n" +
                "             background-image: linear-gradient(90deg, #FF00CC, #3333FF, #FF00CC);\n" +
                "             transform: scale(1.1);\n" +
                "        }\n" +
                "        .app-container.has-results .logo-container { display: none; }\n" +
                "        .app-container.has-results .logo-small { display: block; }\n" +
                "        @keyframes shine { to { background-position: 200% center; } }\n" +
                "        .search-wrapper {\n" +
                "            width: 100%;\n" +
                "            max-width: 584px;\n" +
                "            position: relative;\n" +
                "        }\n" +
                "        .app-container.has-results .search-wrapper {\n" +
                "            max-width: 690px;\n" +
                "            margin-bottom: 0;\n" +
                "        }\n" +
                "        .search-box {\n" +
                "            display: flex;\n" +
                "            align-items: center;\n" +
                "            background: #fff;\n" +
                "            border: 1px solid var(--border);\n" +
                "            border-radius: 24px;\n" +
                "            padding: 0 14px;\n" +
                "            height: 48px;\n" +
                "            transition: all 0.2s ease;\n" +
                "        }\n" +
                "        .search-box:hover, .search-box:focus-within {\n" +
                "            box-shadow: 0 1px 6px rgba(32,33,36,0.28), 0 0 0 2px rgba(66, 133, 244, 0.1);\n" +
                "            border-color: transparent;\n" +
                "        }\n" +
                "        .search-icon { color: #9aa0a6; margin-right: 12px; }\n" +
                "        .search-input { flex: 1; height: 100%; border: none; outline: none; font-size: 16px; font-family: inherit; }\n" +
                "        .search-btn { display: none; }\n" +
                "        .suggestions {\n" +
                "            position: absolute;\n" +
                "            top: 48px; left: 0; right: 0;\n" +
                "            background: white;\n" +
                "            border-radius: 0 0 24px 24px;\n" +
                "            box-shadow: 0 4px 6px rgba(32,33,36,0.28);\n" +
                "            padding-bottom: 10px;\n" +
                "            z-index: 10;\n" +
                "            display: none;\n" +
                "        }\n" +
                "        .search-box.has-suggestions { border-radius: 24px 24px 0 0; box-shadow: 0 1px 6px rgba(32,33,36,0.28); }\n" +
                "        .suggestion-item { padding: 8px 16px; display: flex; align-items: center; cursor: pointer; }\n" +
                "        .suggestion-item:hover { background-color: #f1f3f4; }\n" +
                "        .suggestion-icon { margin-right: 14px; color: #9aa0a6; }\n" +
                "        .search-options { margin-top: 20px; display: flex; gap: 20px; font-size: 13px; color: var(--text-secondary); }\n" +
                "        .app-container.has-results .search-options { display: none; }\n" +
                "        .results-container { width: 100%; max-width: 652px; margin-left: max(180px, 15%); padding: 20px 0; display: none; }\n" +
                "        @media (max-width: 1100px) { .results-container { margin-left: 20px; margin-right: 20px; max-width: 100%; } }\n" +
                "        .results-meta { color: var(--text-secondary); font-size: 14px; margin-bottom: 24px; }\n" +
                "        .result-item { margin-bottom: 30px; padding: 10px; border-radius: 8px; transition: transform 0.2s ease, background 0.2s ease; animation: fadein 0.5s ease; }\n" +
                "        .result-item:hover { transform: translateY(-2px); background: #fafafa; }\n" +
                "        @keyframes fadein { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }\n" +
                "        .result-url-display { font-size: 12px; color: var(--text-secondary); margin-bottom: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n" +
                "        .result-title { font-size: 20px; color: #1a0dab; text-decoration: none; line-height: 1.3; }\n" +
                "        .result-title:hover { text-decoration: underline; }\n" +
                "        .result-snippet { font-size: 14px; line-height: 1.58; color: #4d5156; margin-top: 4px; }\n" +
                "        .debug-toggle { display: flex; align-items: center; gap: 6px; cursor: pointer; user-select: none; }\n" +
                "        .debug-box { margin-top: 10px; background: #f8f9fa; border: 1px solid #dadce0; border-radius: 8px; font-family: monospace; font-size: 11px; display: none; }\n" +
                "        .debug-box.show { display: block; }\n" +
                "        .debug-header { padding: 8px 12px; background: #f1f3f4; border-bottom: 1px solid #dadce0; font-weight: 600; }\n" +
                "        .debug-content { padding: 10px; }\n" +
                "        .loader { border: 3px solid #f3f3f3; border-top: 3px solid #3498db; border-radius: 50%; width: 24px; height: 24px; animation: spin 1s linear infinite; margin: 20px auto; display: none; }\n" +
                "        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }\n" +
                "        .error-msg { color: #d93025; background: #fce8e6; padding: 15px; border-radius: 8px; margin-bottom: 20px; }\n" +
                "        .fun-error { text-align: center; padding: 40px; color: #5f6368; font-size: 18px; line-height: 1.6; background: #f8f9fa; border-radius: 12px; }\n" +
                "        .fun-emoji { font-size: 40px; display: block; margin-bottom: 10px; animation: bounce 1s infinite alternate; }\n" +
                "        @keyframes bounce { from { transform: translateY(0); } to { transform: translateY(-10px); } }\n" +
                "\n" +
                "        /* --- NEW: Correction UI Styles --- */\n" +
                "        .correction-area { margin-bottom: 25px; font-size: 16px; line-height: 1.6; }\n" +
                "        .correction-line { margin-bottom: 4px; }\n" +
                "        .correction-label { color: #d93025; font-style: italic; }\n" +
                "        .correction-link { color: #1a0dab; font-weight: bold; font-style: italic; text-decoration: none; }\n" +
                "        .correction-link:hover { text-decoration: underline; }\n" +
                "        .original-link { color: #1a0dab; text-decoration: none; }\n" +
                "        .original-link:hover { text-decoration: underline; }\n" +
                "        .original-wrapper { font-size: 14px; color: #5f6368; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"app-container\" id=\"appContainer\">\n" +
                "        <div class=\"logo-small\" onclick=\"window.location.reload()\">Hashira</div>\n" +
                "        <div class=\"logo-container\">\n" +
                "            <div class=\"logo\">Hashira</div>\n" +
                "        </div>\n" +
                "        <div class=\"search-wrapper\">\n" +
                "            <form class=\"search-form\" id=\"searchForm\">\n" +
                "                <div class=\"search-box\" id=\"searchBox\">\n" +
                "                    <span class=\"search-icon\">\n" +
                "                        <svg focusable=\"false\" xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\" width=\"20px\" height=\"20px\"><path fill=\"currentColor\" d=\"M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z\"></path></svg>\n" +
                "                    </span>\n" +
                "                    <input type=\"text\" class=\"search-input\" id=\"queryInput\" placeholder=\"Search...\" autocomplete=\"off\">\n" +
                "                    <button type=\"submit\" class=\"search-btn\">Search</button>\n" +
                "                </div>\n" +
                "                <div class=\"suggestions\" id=\"suggestions\"></div>\n" +
                "            </form>\n" +
                "            <div class=\"search-options\">\n" +
                "                <label class=\"debug-toggle\">\n" +
                "                    <input type=\"checkbox\" id=\"debugCheckbox\">\n" +
                "                    <span>Developer Mode</span>\n" +
                "                </label>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "    </div>\n" +
                "\n" +
                "    <div class=\"results-container\" id=\"resultsContainer\">\n" +
                "        <div id=\"correctionArea\"></div>\n" +
                "        <div id=\"resultsMeta\"></div>\n" +
                "        <div id=\"results\"></div>\n" +
                "        <div class=\"loader\" id=\"loadingMore\"></div>\n" +
                "    </div>\n" +
                "\n" +
                "    <script>\n" +
                "        let currentQuery = '', currentDebug = false, currentOffset = 0;\n" +
                "        let hasMore = false, isLoading = false, resultIndexCounter = 0;\n" +
                "        let suggestTimer;\n" +
                "        let forcedSearch = false;\n" +
                "        \n" +
                "        document.getElementById('searchForm').addEventListener('submit', async (e) => {\n" +
                "            e.preventDefault();\n" +
                "            const query = document.getElementById('queryInput').value.trim();\n" +
                "            if (!query) return;\n" +
                "            runSearch(query, false);\n" +
                "        });\n" +
                "        \n" +
                "        async function runSearch(query, force) {\n" +
                "            // UI Transition\n" +
                "            document.getElementById('appContainer').classList.add('has-results');\n" +
                "            document.getElementById('suggestions').style.display = 'none';\n" +
                "            document.getElementById('queryInput').value = query;\n" +
                "            \n" +
                "            currentQuery = query;\n" +
                "            forcedSearch = force;\n" +
                "            currentDebug = document.getElementById('debugCheckbox').checked;\n" +
                "            currentOffset = 0;\n" +
                "            hasMore = false;\n" +
                "            resultIndexCounter = 0;\n" +
                "            \n" +
                "            const resultsDiv = document.getElementById('results');\n" +
                "            document.getElementById('resultsContainer').style.display = 'block';\n" +
                "            resultsDiv.innerHTML = '';\n" +
                "            document.getElementById('correctionArea').innerHTML = '';\n" +
                "            document.getElementById('resultsMeta').innerHTML = '<div class=\"loader\" style=\"display:block\"></div>';\n" +
                "            \n" +
                "            try {\n" +
                "                const params = new URLSearchParams();\n" +
                "                params.append('q', query);\n" +
                "                if (currentDebug) params.append('debug', 'true');\n" +
                "                if (force) params.append('force', 'true');\n" +
                "                params.append('offset', '0');\n" +
                "                params.append('limit', '20');\n" +
                "                \n" +
                "                const response = await fetch('/search', { \n" +
                "                    method: 'POST', \n" +
                "                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
                "                    body: params.toString() \n" +
                "                });\n" +
                "                const data = await response.json();\n" +
                "                \n" +
                "                if (data.error) {\n" +
                "                    document.getElementById('resultsMeta').innerHTML = '<div class=\"error-msg\">' + data.error + '</div>';\n" +
                "                    return;\n" +
                "                }\n" +
                "                \n" +
                "                currentOffset = data.results.length;\n" +
                "                hasMore = data.hasMore;\n" +
                "                \n" +
                "                // --- CORRECTION UI LOGIC ---\n" +
                "                if (data.correctedUsed) {\n" +
                "                    const escCorrect = data.correctedQuery.replace(/'/g, \"\\\\'\");\n" +
                "                    const escOrig = data.originalQuery.replace(/'/g, \"\\\\'\");\n" +
                "                    const correctionHtml = `\n" +
                "                        <div class=\"correction-area\">\n" +
                "                            <div class=\"correction-line\">\n" +
                "                                <span class=\"correction-label\">Showing results for</span> \n" +
                "                                <a href=\"#\" class=\"correction-link\" onclick=\"runSearch('${escCorrect}', false)\">${data.correctedQuery}</a>\n" +
                "                            </div>\n" +
                "                            <div class=\"original-wrapper\">\n" +
                "                                <span>Search instead for</span>\n" +
                "                                <a href=\"#\" class=\"original-link\" onclick=\"runSearch('${escOrig}', true)\">${data.originalQuery}</a>\n" +
                "                            </div>\n" +
                "                        </div>`;\n" +
                "                    document.getElementById('correctionArea').innerHTML = correctionHtml;\n" +
                "                }\n" +
                "                \n" +
                "                // --- RESULTS HANDLING ---\n" +
                "                if (!data.results || data.results.length === 0) {\n" +
                "                    document.getElementById('resultsMeta').innerHTML = '';\n" +
                "                    resultsDiv.innerHTML = `<div class=\"fun-error\">\n" +
                "                        <span class=\"fun-emoji\">🥶</span>\n" +
                "                        <b>Chill out!</b><br>I don\\'t think we have crawled that yet.<br>\n" +
                "                        <span style=\"font-size:0.9em;color:#888\">Maybe try something easier like \"computer\" or \"java\"?</span>\n" +
                "                    </div>`;\n" +
                "                    return;\n" +
                "                }\n" +
                "                \n" +
                "                let statsHtml = '<div class=\"results-meta\">About ' + data.total + ' results (' + (Math.random() * 0.5 + 0.1).toFixed(2) + ' seconds)</div>';\n" +
                "                document.getElementById('resultsMeta').innerHTML = statsHtml;\n" +
                "                appendResults(data.results);\n" +
                "            } catch (error) {\n" +
                "                document.getElementById('resultsMeta').innerHTML = '<div class=\"error-msg\">Server Error</div>';\n" +
                "            }\n" +
                "        }\n" +
                "        \n" +
                "        function appendResults(results) {\n" +
                "            const target = document.getElementById('results');\n" +
                "            let html = '';\n" +
                "            results.forEach(result => {\n" +
                "                const idx = resultIndexCounter++;\n" +
                "                const displayUrl = result.url.length > 90 ? result.url.substring(0,90) + '...' : result.url;\n" +
                "                const snippet = result.snippet || result.description || 'No description available.';\n" +
                "                \n" +
                "                html += `<div class=\"result-item\">\n" +
                "                    <div class=\"result-url-display\">${displayUrl}</div>\n" +
                "                    <a href=\"${result.url}\" target=\"_blank\" class=\"result-title\">${result.title || result.url}</a>\n" +
                "                    <div class=\"result-snippet\">${snippet}</div>`;\n" +
                "                \n" +
                "                if (currentDebug && result.debug) {\n" +
                "                     html += `<button style=\"border:none;background:none;color:#70757a;cursor:pointer;font-size:12px;margin-top:5px\" onclick=\"document.getElementById('dbg-${idx}').classList.toggle('show')\">Show Metrics</button>\n" +
                "                     <div class=\"debug-box\" id=\"dbg-${idx}\"><div class=\"debug-header\">Score: ${result.finalScore.toFixed(4)}</div>\n" +
                "                     <div class=\"debug-content\">${JSON.stringify(result.debug).replace(/,/g, ', ')}</div></div>`;\n" +
                "                }\n" +
                "                html += `</div>`;\n" +
                "            });\n" +
                "            target.insertAdjacentHTML('beforeend', html);\n" +
                "        }\n" +
                "        \n" +
                "        // --- Autocomplete ---\n" +
                "        async function fetchSuggestions(fullValue) {\n" +
                "             const suggestionsDiv = document.getElementById('suggestions');\n" +
                "             const searchBox = document.getElementById('searchBox');\n" +
                "             const parts = fullValue.trim().split(/\\s+/);\n" +
                "             const last = parts[parts.length - 1] || '';\n" +
                "             if(!last) { suggestionsDiv.style.display='none'; searchBox.classList.remove('has-suggestions'); return; }\n" +
                "             \n" +
                "             try {\n" +
                "                 const res = await fetch('/suggest?word=' + encodeURIComponent(last));\n" +
                "                 const data = await res.json();\n" +
                "                 if(!data.suggestions || !data.suggestions.length) { suggestionsDiv.style.display='none'; searchBox.classList.remove('has-suggestions'); return; }\n" +
                "                 \n" +
                "                 suggestionsDiv.style.display='block';\n" +
                "                 searchBox.classList.add('has-suggestions');\n" +
                "                 suggestionsDiv.innerHTML = data.suggestions.slice(0,5).map(s => \n" +
                "                     `<div class=\"suggestion-item\" onclick=\"applySuggestion('${s.word}')\"><span class=\"suggestion-icon\">🔍</span>${s.word}</div>`\n" +
                "                 ).join('');\n" +
                "             } catch(e) {}\n" +
                "        }\n" +
                "        \n" +
                "        function applySuggestion(word) {\n" +
                "            const input = document.getElementById('queryInput');\n" +
                "            const parts = input.value.trim().split(/\\s+/);\n" +
                "            if(parts.length) parts[parts.length-1] = word;\n" +
                "            else parts.push(word);\n" +
                "            input.value = parts.join(' ');\n" +
                "            document.getElementById('suggestions').style.display='none';\n" +
                "            document.getElementById('searchBox').classList.remove('has-suggestions');\n" +
                "            input.focus();\n" +
                "        }\n" +
                "        \n" +
                "        document.getElementById('queryInput').addEventListener('input', (e) => {\n" +
                "            clearTimeout(suggestTimer);\n" +
                "            suggestTimer = setTimeout(() => fetchSuggestions(e.target.value), 150);\n" +
                "        });\n" +
                "        \n" +
                "        // Hide suggestions when Enter is pressed\n" +
                "        document.getElementById('queryInput').addEventListener('keydown', (e) => {\n" +
                "            if (e.key === 'Enter') {\n" +
                "                document.getElementById('suggestions').style.display = 'none';\n" +
                "                document.getElementById('searchBox').classList.remove('has-suggestions');\n" +
                "            }\n" +
                "        });\n" +
                "        \n" +
                "        // --- Scroll & Utils ---\n" +
                "        window.addEventListener('scroll', () => {\n" +
                "             if(isLoading || !hasMore) return;\n" +
                "             if((window.innerHeight + window.scrollY) >= document.body.offsetHeight - 500) {\n" +
                "                 loadMore();\n" +
                "             }\n" +
                "        });\n" +
                "        async function loadMore() {\n" +
                "             isLoading = true;\n" +
                "             document.getElementById('loadingMore').style.display='block';\n" +
                "             const params = new URLSearchParams();\n" +
                "             params.append('q', currentQuery);\n" +
                "             if(currentDebug) params.append('debug', 'true');\n" +
                "             if(forcedSearch) params.append('force', 'true');\n" +
                "             params.append('offset', currentOffset);\n" +
                "             const res = await fetch('/search', { \n" +
                "                 method: 'POST', \n" +
                "                 headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
                "                 body: params.toString()\n" +
                "             });\n" +
                "             const data = await res.json();\n" +
                "             if(data.results && data.results.length) {\n" +
                "                 appendResults(data.results);\n" +
                "                 currentOffset += data.results.length;\n" +
                "                 hasMore = data.hasMore;\n" +
                "             } else { hasMore = false; }\n" +
                "             isLoading = false;\n" +
                "             document.getElementById('loadingMore').style.display='none';\n" +
                "        }\n" +
                "    </script>\n" +
                "</body>\n" +
                "</html>";
    }

    private static String formatJSON(Map<String, Object> data) {
        StringBuilder json = new StringBuilder("{\n");
        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!first) json.append(",\n");
            first = false;
            json.append("  \"").append(escapeJSON(entry.getKey())).append("\": ").append(formatJSONValue(entry.getValue()));
        }
        json.append("\n}");
        return json.toString();
    }

    private static String formatJSONValue(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escapeJSON((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map) {
            StringBuilder sb = new StringBuilder("{\n");
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) sb.append(",\n");
                first = false;
                sb.append("    \"").append(escapeJSON(entry.getKey().toString())).append("\": ").append(formatJSONValue(entry.getValue()));
            }
            sb.append("\n  }");
            return sb.toString();
        }
        if (value instanceof List) {
            StringBuilder sb = new StringBuilder("[\n");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(",\n");
                first = false;
                sb.append("    ").append(formatJSONValue(item));
            }
            sb.append("\n  ]");
            return sb.toString();
        }
        return "\"" + escapeJSON(value.toString()) + "\"";
    }

    private static String escapeJSON(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    // Utility to trim long descriptions to a safe display length
    private static String truncate(String text, int maxLen) {
        if (text == null || text.length() <= maxLen) {
            return text;
        }
        return text.substring(0, Math.max(0, maxLen - 3)) + "...";
    }
}