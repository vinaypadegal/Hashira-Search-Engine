package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.FileNotFoundException;
import java.util.*;

public class IndexerTest extends GenericTest {

    void runSetup() {
        // No setup needed - uses existing crawl data
    }

    void prompt() {
        System.out.println("Make sure:");
        System.out.println("  1. KVS and Flame services are running");
        System.out.println("  2. You have crawled some data (pt-crawl table exists)");
        System.out.println("  3. indexer.jar is in the current directory");
        System.out.println("Then hit Enter to continue...");
        (new Scanner(System.in)).nextLine();
    }

    void cleanup() {
        // No cleanup needed
    }

    void runTests(Set<String> tests) throws Exception {
        System.out.printf("\n%-15s%-45sResult\n", "Test", "Description");
        System.out.println("--------------------------------------------------------------");

        // Test 1: Basic Indexer Functionality
        if (tests.contains("basic")) {
            testBasicIndexer();
        }

        // Test 2: HTML Tag Filtering
        if (tests.contains("html")) {
            testHtmlFiltering();
        }

        // Test 3: Stop Word Filtering
        if (tests.contains("stopwords")) {
            testStopWordFiltering();
        }

        // Test 4: Field-Specific Indexing
        if (tests.contains("fields")) {
            testFieldIndexing();
        }

        // Test 5: IDF Calculation
        if (tests.contains("idf")) {
            testIdfCalculation();
        }

        // Test 6: Document Metadata
        if (tests.contains("metadata")) {
            testDocumentMetadata();
        }

        // Test 7: Stemming
        if (tests.contains("stemming")) {
            testStemming();
        }

        System.out.println("--------------------------------------------------------------\n");
        if (numTestsFailed == 0) {
            System.out.println("All Indexer tests passed! ✓");
        } else {
            System.out.println(numTestsFailed + " test(s) failed.");
        }
        cleanup();
        closeOutputFile();
    }

    private void testBasicIndexer() throws Exception {
        try {
            startTest("basic", "Basic Indexer Functionality", 20);

            KVSClient kvs = new KVSClient("localhost:8000");
            
            // Create test data
            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/page1.html", "apple banana cherry");
            data.put("http://test.com/page2.html", "banana date elderberry");
            data.put("http://test.com/page3.html", "cherry fig grape");

            installTestCrawl(data);

            // Run indexer
            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed: " + FlameSubmit.getErrorResponse());
                return;
            }

            // Check pt-index table exists
            Iterator<Row> indexRows = kvs.scan("pt-index");
            if (!indexRows.hasNext()) {
                testFailed("pt-index table is empty");
                return;
            }

            // Verify basic words are indexed
            Row appleRow = kvs.getRow("pt-index", "apple");
            if (appleRow == null || appleRow.get("acc") == null) {
                testFailed("Word 'apple' not found in index");
                return;
            }

            Row bananaRow = kvs.getRow("pt-index", "banana");
            if (bananaRow == null || bananaRow.get("acc") == null) {
                testFailed("Word 'banana' not found in index");
                return;
            }

            String bananaPosting = bananaRow.get("acc");
            if (!bananaPosting.contains("page1.html") || !bananaPosting.contains("page2.html")) {
                testFailed("Banana posting list doesn't contain expected URLs");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in basic test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testHtmlFiltering() throws Exception {
        try {
            startTest("html", "HTML Tag Filtering", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/html.html", 
                "<html><body><p>Hello</p> <script>var x = 1;</script> <style>.class { color: red; }</style> World</body></html>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            Row helloRow = kvs.getRow("pt-index", "hello");
            Row worldRow = kvs.getRow("pt-index", "world");

            if (helloRow == null || worldRow == null) {
                testFailed("Expected words not found after HTML filtering");
                return;
            }

            // Check that script/style content is filtered
            Row scriptRow = kvs.getRow("pt-index", "var");
            Row styleRow = kvs.getRow("pt-index", "color");
            
            if (scriptRow != null || styleRow != null) {
                testFailed("Script/style content should be filtered but was indexed");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in HTML test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testStopWordFiltering() throws Exception {
        try {
            startTest("stopwords", "Stop Word Filtering", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/stop.html", "the quick brown fox jumps over the lazy dog");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            
            // Stop words should NOT be indexed
            Row theRow = kvs.getRow("pt-index", "the");
            Row overRow = kvs.getRow("pt-index", "over");
            
            if (theRow != null || overRow != null) {
                testFailed("Stop words 'the' or 'over' should not be indexed");
                return;
            }

            // Non-stop words should be indexed
            Row quickRow = kvs.getRow("pt-index", "quick");
            Row brownRow = kvs.getRow("pt-index", "brown");
            Row foxRow = kvs.getRow("pt-index", "fox");

            if (quickRow == null || brownRow == null || foxRow == null) {
                testFailed("Non-stop words should be indexed");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in stopword test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testFieldIndexing() throws Exception {
        try {
            startTest("fields", "Field-Specific Indexing", 20);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/fields.html", 
                "<html><head><title>Important Title</title>" +
                "<meta name='description' content='Meta Content'>" +
                "</head><body><h1>Heading One</h1><h2>Heading Two</h2>" +
                "<p>Body text here</p></body></html>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            Row importantRow = kvs.getRow("pt-index", "important");
            Row titleRow = kvs.getRow("pt-index", "title");

            if (importantRow == null || titleRow == null) {
                testFailed("Title words should be indexed");
                return;
            }

            // Check that posting lists contain field information
            String importantPosting = importantRow.get("acc");
            if (!importantPosting.contains("title")) {
                testFailed("Field information should be stored in posting list");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in field test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testIdfCalculation() throws Exception {
        try {
            startTest("idf", "IDF Calculation", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/idf1.html", "common word unique1");
            data.put("http://test.com/idf2.html", "common word unique2");
            data.put("http://test.com/idf3.html", "common word unique3");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            
            // Check pt-index-df table exists
            Row commonDfRow = kvs.getRow("pt-index-df", "common");
            Row unique1DfRow = kvs.getRow("pt-index-df", "unique1");

            if (commonDfRow == null || unique1DfRow == null) {
                testFailed("Document frequency table (pt-index-df) not found or incomplete");
                return;
            }

            String commonDf = commonDfRow.get("df");
            String unique1Df = unique1DfRow.get("df");

            if (commonDf == null || unique1Df == null) {
                testFailed("Document frequency values not stored");
                return;
            }

            // Common word should have higher DF than unique word
            int commonDfVal = Integer.parseInt(commonDf);
            int unique1DfVal = Integer.parseInt(unique1Df);

            if (commonDfVal <= unique1DfVal) {
                testFailed("Common word should have higher document frequency");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in IDF test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testDocumentMetadata() throws Exception {
        try {
            startTest("metadata", "Document Metadata Storage", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/meta1.html", "word1 word2 word3 word4 word5");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            String urlHash = Hasher.hash("http://test.com/meta1.html");
            Row metaRow = kvs.getRow("pt-doc-metadata", urlHash);

            if (metaRow == null) {
                testFailed("Document metadata table (pt-doc-metadata) not found");
                return;
            }

            String totalWords = metaRow.get("totalWords");
            String uniqueTerms = metaRow.get("uniqueTerms");

            if (totalWords == null || uniqueTerms == null) {
                testFailed("Metadata fields (totalWords, uniqueTerms) not stored");
                return;
            }

            int total = Integer.parseInt(totalWords);
            int unique = Integer.parseInt(uniqueTerms);

            if (total < 1 || unique < 1) {
                testFailed("Invalid metadata values");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in metadata test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testStemming() throws Exception {
        try {
            startTest("stemming", "Porter Stemming", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com/stem.html", "running jumped jumping runs");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "indexer.jar", 
                "cis5550.jobs.Indexer", new String[] {});
            
            if (output == null) {
                testFailed("Indexer submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            
            // Stemmed forms should be indexed
            Row runRow = kvs.getRow("pt-index", "run");
            Row jumpRow = kvs.getRow("pt-index", "jump");

            if (runRow == null || jumpRow == null) {
                testFailed("Stemmed forms should be indexed");
                return;
            }

            // Original forms should also be indexed
            Row runningRow = kvs.getRow("pt-index", "running");
            if (runningRow == null) {
                testFailed("Original forms should also be indexed");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in stemming test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void installTestCrawl(Map<String, String> data) throws Exception {
        KVSClient kvs = new KVSClient("localhost:8000");
        
        // Clear existing test data
        try {
            kvs.delete("pt-index");
            kvs.delete("pt-index-df");
            kvs.delete("pt-doc-metadata");
        } catch (Exception e) {
            // Tables might not exist, ignore
        }

        // Install test crawl data
        for (String url : data.keySet()) {
            String hash = Hasher.hash(url);
            kvs.put("pt-crawl", hash, "url", url);
            kvs.put("pt-crawl", hash, "page", data.get(url));
        }
    }

    public static void main(String[] args) throws Exception {
        Set<String> tests = new TreeSet<>();
        
        if (args.length == 0 || args[0].equals("all")) {
            tests.add("basic");
            tests.add("html");
            tests.add("stopwords");
            tests.add("fields");
            tests.add("idf");
            tests.add("metadata");
            tests.add("stemming");
        } else {
            for (String arg : args) {
                tests.add(arg);
            }
        }

        IndexerTest t = new IndexerTest();
        t.setExitUponFailure(true);
        t.outputToFile();
        t.prompt();
        t.runTests(tests);
    }
}

