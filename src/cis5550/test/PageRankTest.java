package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.FileNotFoundException;
import java.util.*;

public class PageRankTest extends GenericTest {

    void runSetup() {
        // No setup needed - uses existing crawl data
    }

    void prompt() {
        System.out.println("Make sure:");
        System.out.println("  1. KVS and Flame services are running");
        System.out.println("  2. You have crawled some data (pt-crawl table exists)");
        System.out.println("  3. pagerank.jar is in the current directory");
        System.out.println("Then hit Enter to continue...");
        (new Scanner(System.in)).nextLine();
    }

    void cleanup() {
        // No cleanup needed
    }

    void runTests(Set<String> tests) throws Exception {
        System.out.printf("\n%-15s%-45sResult\n", "Test", "Description");
        System.out.println("--------------------------------------------------------------");

        // Test 1: Basic PageRank Functionality
        if (tests.contains("basic")) {
            testBasicPageRank();
        }

        // Test 2: Convergence Testing
        if (tests.contains("convergence")) {
            testConvergence();
        }

        // Test 3: Percentage-Based Convergence
        if (tests.contains("percent")) {
            testPercentageConvergence();
        }

        // Test 4: Dangling Nodes
        if (tests.contains("dangling")) {
            testDanglingNodes();
        }

        // Test 5: Link Graph (3-node cycle)
        if (tests.contains("cycle")) {
            testThreeNodeCycle();
        }

        // Test 6: Larger Graph
        if (tests.contains("larger")) {
            testLargerGraph();
        }

        // Test 7: Link Count Metadata
        if (tests.contains("metadata")) {
            testLinkMetadata();
        }

        System.out.println("--------------------------------------------------------------\n");
        if (numTestsFailed == 0) {
            System.out.println("All PageRank tests passed! ✓");
        } else {
            System.out.println(numTestsFailed + " test(s) failed.");
        }
        cleanup();
        closeOutputFile();
    }

    private void testBasicPageRank() throws Exception {
        try {
            startTest("basic", "Basic PageRank Functionality", 20);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/page1.html",
                "Links to <a href=\"http://test.com:80/page2.html\">page 2</a>");
            data.put("http://test.com:80/page2.html",
                "Links to <a href=\"http://test.com:80/page1.html\">page 1</a>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.01" });

            if (output == null) {
                testFailed("PageRank submission failed: " + FlameSubmit.getErrorResponse());
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            
            // Check pt-pageranks table exists
            Iterator<Row> rankRows = kvs.scan("pt-pageranks");
            if (!rankRows.hasNext()) {
                testFailed("pt-pageranks table is empty");
                return;
            }

            // Verify ranks are stored
            String hash1 = Hasher.hash("http://test.com:80/page1.html");
            String hash2 = Hasher.hash("http://test.com:80/page2.html");

            Row row1 = kvs.getRow("pt-pageranks", hash1);
            Row row2 = kvs.getRow("pt-pageranks", hash2);

            if (row1 == null || row2 == null) {
                testFailed("Ranks not stored for all pages");
                return;
            }

            String rank1Str = row1.get("rank");
            String rank2Str = row2.get("rank");

            if (rank1Str == null || rank2Str == null) {
                testFailed("Rank values not stored");
                return;
            }

            double rank1 = Double.parseDouble(rank1Str);
            double rank2 = Double.parseDouble(rank2Str);

            if (rank1 <= 0 || rank2 <= 0) {
                testFailed("Ranks should be positive");
                return;
            }

            // In a 2-node cycle, ranks should be approximately equal
            if (Math.abs(rank1 - rank2) > 0.1) {
                testFailed("Two-node cycle should have similar ranks. Got: " + rank1 + " and " + rank2);
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in basic test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testConvergence() throws Exception {
        try {
            startTest("convergence", "Convergence Testing", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/c1.html",
                "Links to <a href=\"http://test.com:80/c2.html\">c2</a>");
            data.put("http://test.com:80/c2.html",
                "Links to <a href=\"http://test.com:80/c1.html\">c1</a> and <a href=\"http://test.com:80/c3.html\">c3</a>");
            data.put("http://test.com:80/c3.html",
                "Links to <a href=\"http://test.com:80/c1.html\">c1</a>");

            installTestCrawl(data);

            // Test with strict threshold
            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.001" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            Iterator<Row> rows = kvs.scan("pt-pageranks");
            
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }

            if (count != 3) {
                testFailed("Expected 3 pages in pageranks table, got " + count);
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in convergence test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testPercentageConvergence() throws Exception {
        try {
            startTest("percent", "Percentage-Based Convergence", 15);

            Map<String, String> data = new HashMap<>();
            for (int i = 1; i <= 5; i++) {
                int next = (i % 5) + 1;
                data.put("http://test.com:80/p" + i + ".html",
                    "Link to <a href=\"http://test.com:80/p" + next + ".html\">p" + next + "</a>");
            }

            installTestCrawl(data);

            // Test with 70% convergence threshold
            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.01", "70" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            // Should complete successfully with percentage threshold
            if (!output.contains("OK")) {
                testFailed("PageRank should output OK");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in percentage test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testDanglingNodes() throws Exception {
        try {
            startTest("dangling", "Dangling Node Handling", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/node1.html",
                "Links to <a href=\"http://test.com:80/node2.html\">node2</a>");
            data.put("http://test.com:80/node2.html",
                "No links"); // Dangling node
            data.put("http://test.com:80/node3.html",
                "Links to <a href=\"http://test.com:80/node1.html\">node1</a>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.01" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            
            String hash1 = Hasher.hash("http://test.com:80/node1.html");
            String hash2 = Hasher.hash("http://test.com:80/node2.html");
            String hash3 = Hasher.hash("http://test.com:80/node3.html");

            Row row2 = kvs.getRow("pt-pageranks", hash2);
            if (row2 == null) {
                testFailed("Dangling node should have a rank");
                return;
            }

            double rank2 = Double.parseDouble(row2.get("rank"));
            if (rank2 <= 0) {
                testFailed("Dangling node rank should be positive");
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in dangling test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testThreeNodeCycle() throws Exception {
        try {
            startTest("cycle", "Three-Node Cycle (HW9 Test)", 20);

            // This is the exact test from HW9
            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/page1.html",
                "This links to <a href=\"http://test.com:80/page2.html\">page 2</a>");
            data.put("http://test.com:80/page2.html",
                "A link to <a href=\"http://test.com:80/page1.html\">page 1</a> and <a href=\"http://test.com:80/page3.html\">page 3</a>");
            data.put("http://test.com:80/page3.html",
                "Linking back to <a href=\"http://test.com:80/page1.html\">page 1</a>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.001" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");

            HashMap<String, Double> expected = new HashMap<>();
            expected.put(Hasher.hash("http://test.com:80/page1.html"), 1.191681575822917);
            expected.put(Hasher.hash("http://test.com:80/page2.html"), 1.1637322274926893);
            expected.put(Hasher.hash("http://test.com:80/page3.html"), 0.644586196684393);

            String problems = compareRanksToExpected(expected, 0.01); // Slightly relaxed for floating point

            if (!problems.equals("")) {
                testFailed("Three-node cycle test failed:\n" + problems);
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in cycle test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testLargerGraph() throws Exception {
        try {
            startTest("larger", "Larger Graph (5 nodes)", 15);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/l1.html",
                "Link to <a href=\"http://test.com:80/l2.html\">l2</a> and <a href=\"http://test.com:80/l3.html\">l3</a>");
            data.put("http://test.com:80/l2.html",
                "Link to <a href=\"http://test.com:80/l4.html\">l4</a>");
            data.put("http://test.com:80/l3.html",
                "Link to <a href=\"http://test.com:80/l4.html\">l4</a> and <a href=\"http://test.com:80/l5.html\">l5</a>");
            data.put("http://test.com:80/l4.html",
                "Link to <a href=\"http://test.com:80/l1.html\">l1</a>");
            data.put("http://test.com:80/l5.html",
                "Link to <a href=\"http://test.com:80/l1.html\">l1</a>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.01" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            Iterator<Row> rows = kvs.scan("pt-pageranks");

            int count = 0;
            double totalRank = 0.0;
            while (rows.hasNext()) {
                Row row = rows.next();
                String rankStr = row.get("rank");
                if (rankStr != null) {
                    totalRank += Double.parseDouble(rankStr);
                    count++;
                }
            }

            if (count != 5) {
                testFailed("Expected 5 pages, got " + count);
                return;
            }

            // Ranks should sum to approximately N (where N = number of pages)
            // Due to normalization, they should sum close to 5
            if (Math.abs(totalRank - 5.0) > 1.0) {
                testFailed("Ranks should sum to approximately 5.0, got " + totalRank);
                return;
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in larger test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void testLinkMetadata() throws Exception {
        try {
            startTest("metadata", "Link Count Metadata", 10);

            Map<String, String> data = new HashMap<>();
            data.put("http://test.com:80/m1.html",
                "Link1 <a href=\"http://test.com:80/m2.html\">m2</a> Link2 <a href=\"http://test.com:80/m3.html\">m3</a>");
            data.put("http://test.com:80/m2.html", "No links");
            data.put("http://test.com:80/m3.html",
                "Link <a href=\"http://test.com:80/m1.html\">m1</a>");

            installTestCrawl(data);

            String output = FlameSubmit.submit("localhost:9000", "pagerank.jar",
                "cis5550.jobs.PageRank", new String[] { "0.01" });

            if (output == null) {
                testFailed("PageRank submission failed");
                return;
            }

            KVSClient kvs = new KVSClient("localhost:8000");
            String hash1 = Hasher.hash("http://test.com:80/m1.html");
            String hash2 = Hasher.hash("http://test.com:80/m2.html");

            Row row1 = kvs.getRow("pt-pageranks", hash1);
            Row row2 = kvs.getRow("pt-pageranks", hash2);

            if (row1 != null) {
                String outLinks = row1.get("outLinks");
                if (outLinks != null) {
                    int linkCount = Integer.parseInt(outLinks);
                    if (linkCount != 2) {
                        testFailed("m1 should have 2 outlinks, got " + linkCount);
                        return;
                    }
                }
            }

            if (row2 != null) {
                String outLinks = row2.get("outLinks");
                if (outLinks != null) {
                    int linkCount = Integer.parseInt(outLinks);
                    if (linkCount != 0) {
                        testFailed("m2 should have 0 outlinks (dangling), got " + linkCount);
                        return;
                    }
                }
            }

            testSucceeded();

        } catch (Exception e) {
            testFailed("Exception in metadata test: " + e.getMessage(), false);
            e.printStackTrace();
        }
    }

    private void installTestCrawl(Map<String, String> data) throws Exception {
        KVSClient kvs = new KVSClient("localhost:8000");

        // Clear existing test data
        try {
            kvs.delete("pt-pageranks");
        } catch (Exception e) {
            // Table might not exist, ignore
        }

        // Install test crawl data
        for (String url : data.keySet()) {
            String hash = Hasher.hash(url);
            kvs.put("pt-crawl", hash, "url", url);
            kvs.put("pt-crawl", hash, "page", data.get(url));
        }
    }

    private String compareRanksToExpected(HashMap<String, Double> expected, double maxDiff)
            throws Exception {
        KVSClient kvs = new KVSClient("localhost:8000");
        Iterator<Row> iter = kvs.scan("pt-pageranks", null, null);

        String problems = "";
        while (iter.hasNext()) {
            Row r = iter.next();
            if (!expected.containsKey(r.key())) {
                problems = problems + " * The 'pt-pageranks' table contains a row with key '" + r.key()
                        + "', but this wasn't a URL in our input data\n";
            } else if (r.get("rank") == null) {
                problems = problems + " * In the 'pt-pageranks' table, the row with key '" + r.key()
                        + "' doesn't have a 'rank' column\n";
            } else if ((Math.abs(expected.get(r.key()) - Double.valueOf(r.get("rank")))) > maxDiff) {
                problems = problems + " * The rank for page '" + r.key() + "' is " + r.get("rank")
                        + " in the 'pt-pageranks' table, but we expected '" + expected.get(r.key()) + ", +/-" + maxDiff
                        + "\n";
                expected.remove(r.key());
            } else {
                expected.remove(r.key());
            }
        }

        for (String s : expected.keySet()) {
            problems = problems + " * '" + s
                    + "' is a URL in our input data, but we couldn't find a row with that key in the 'pt-pageranks' table after running the job.\n";
        }

        return problems;
    }

    public static void main(String[] args) throws Exception {
        Set<String> tests = new TreeSet<>();

        if (args.length == 0 || args[0].equals("all")) {
            tests.add("basic");
            tests.add("convergence");
            tests.add("percent");
            tests.add("dangling");
            tests.add("cycle");
            tests.add("larger");
            tests.add("metadata");
        } else {
            for (String arg : args) {
                tests.add(arg);
            }
        }

        PageRankTest t = new PageRankTest();
        t.setExitUponFailure(true);
        t.outputToFile();
        t.prompt();
        t.runTests(tests);
    }
}

