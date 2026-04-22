package cis5550.test;

import cis5550.tools.URLParser;
import java.util.*;
import java.lang.reflect.*;

public class URLParserTest extends GenericTest {

  void runSetup() {
  }

  void prompt() {
    System.out.println("Testing URLParser filtering functionality...");
    System.out.println("Press Enter to continue.");
    (new Scanner(System.in)).nextLine();
  }

  void cleanup() {
  }

  // Helper method to access private shouldFilterURL method via reflection
  private boolean shouldFilterURL(String url) throws Exception {
    Method method = URLParser.class.getDeclaredMethod("shouldFilterURL", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, url);
  }

  void runTests(Set<String> tests) throws Exception {

    System.out.printf("\n%-15s%-50sResult\n", "Test", "Description");
    System.out.println("------------------------------------------------------------------------");

    if (tests.contains("multiple-question-marks")) try {
      startTest("multi-qm", "Filter URLs with multiple question marks", 5);
      String testUrl = "http://example.com/page?foo=bar?baz=qux";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with multiple question marks to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("multiple-question-marks-2")) try {
      startTest("multi-qm-2", "Filter URLs with ? in middle and end", 5);
      String testUrl = "http://site.org/search?q=test?&page=2?sort=asc";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with multiple question marks to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("multiple-question-marks-3")) try {
      startTest("multi-qm-3", "Filter URLs with many question marks", 5);
      String testUrl = "http://example.com/api?v=1?token=abc?debug=true?format=json";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with multiple question marks to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("matrix-params")) try {
      startTest("matrix", "Filter URLs with matrix parameters", 5);
      String testUrl = "http://example.com/path;jsessionid=123ABC";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with matrix parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("matrix-params-2")) try {
      startTest("matrix-2", "Filter URLs with matrix params in middle", 5);
      String testUrl = "http://site.com/users;id=456/profile;view=full";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with matrix parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("matrix-params-3")) try {
      startTest("matrix-3", "Filter URLs with matrix params before query", 5);
      String testUrl = "http://example.com/app;lang=en;region=us?search=test";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with matrix parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("path-embedded-params")) try {
      startTest("path-param", "Filter URLs with path-embedded parameters", 5);
      String testUrl = "http://example.com/search/q=dog/limit=20";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with path-embedded parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("path-embedded-params-2")) try {
      startTest("path-param-2", "Filter URLs with multiple path params", 5);
      String testUrl = "http://site.org/api/v=2/user=john/format=json/data";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with path-embedded parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("path-embedded-params-3")) try {
      startTest("path-param-3", "Filter URLs with key=value in path", 5);
      String testUrl = "http://example.com/page/sort=date/order=desc/results";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with path-embedded parameters to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("calendar")) try {
      startTest("calendar", "Filter calendar URLs", 5);
      String testUrl = "http://example.com/events/calendar/2024/march";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected calendar URL to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("calendar-2")) try {
      startTest("calendar-2", "Filter calendar at beginning of path", 5);
      String testUrl = "http://school.edu/calendar/academic/fall";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected calendar URL to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("calendar-3")) try {
      startTest("calendar-3", "Filter calendar in query string", 5);
      String testUrl = "http://site.com/view?type=calendar&year=2024";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected calendar URL to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("repeating-paths")) try {
      startTest("repeat", "Filter URLs with repeating path segments", 5);
      String testUrl = "http://example.com/foo/bar/foo/baz";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with repeating paths to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("repeating-paths-2")) try {
      startTest("repeat-2", "Filter URLs with consecutive repeating segments", 5);
      String testUrl = "http://site.com/users/users/profile";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with repeating paths to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("repeating-paths-3")) try {
      startTest("repeat-3", "Filter URLs with repeated segment with slash", 5);
      String testUrl = "http://example.com/api/v1/api/v1/endpoint";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with repeating paths to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normal-url")) try {
      startTest("normal", "Allow normal URLs to pass through", 5);
      String testUrl = "http://example.com/normal/path/page";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected normal URL to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normal-url-2")) try {
      startTest("normal-2", "Allow URLs with equals in query only", 5);
      String testUrl = "http://example.com/products?category=electronics";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected normal URL to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normal-url-3")) try {
      startTest("normal-3", "Allow URLs with deep paths", 5);
      String testUrl = "http://site.org/docs/api/v2/reference/guide/index.html";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected normal URL to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normal-query-param")) try {
      startTest("normal-qp", "Allow URLs with normal query parameters", 5);
      String testUrl = "http://example.com/search?q=test&lang=en";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected URL with normal query parameters to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normal-query-param-2")) try {
      startTest("normal-qp-2", "Allow URLs with multiple query params", 5);
      String testUrl = "http://example.com/api?format=json&version=2&auth=token123";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected URL with normal query parameters to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("domain-only")) try {
      startTest("domain", "Allow domain-only URLs (no path)", 5);
      String testUrl = URLParser.normalizeSeedURL("https://example.com");
      if (testUrl != null && testUrl.equals("https://example.com:443/"))
        testSucceeded();
      else
        testFailed("Expected domain-only URL to normalize to https://example.com:443/, got: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("domain-only-http")) try {
      startTest("domain-http", "Allow domain-only URLs with http", 5);
      String testUrl = URLParser.normalizeSeedURL("http://example.com");
      if (testUrl != null && testUrl.equals("http://example.com:80/"))
        testSucceeded();
      else
        testFailed("Expected domain-only URL to normalize to http://example.com:80/, got: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("long-segment")) try {
      startTest("long-seg", "Filter URLs with path segments > 100 chars", 5);
      // Create a 101-character segment
      String longSegment = "a".repeat(101);
      String testUrl = "http://example.com/path/" + longSegment + "/page";
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with 101-char path segment to be filtered out, but it wasn't: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("long-segment-2")) try {
      startTest("long-seg-2", "Allow URLs with exactly 100-char segments", 5);
      // Create a 100-character segment (should pass)
      String segment100 = "a".repeat(100);
      String testUrl = "http://example.com/path/" + segment100 + "/page";
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected URL with 100-char path segment to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("long-segment-3")) try {
      startTest("long-seg-3", "Filter URLs with very long segments", 5);
      // Create a 200-character segment
      String longSegment = "x".repeat(200);
      String testUrl = "http://site.org/normal/" + longSegment;
      boolean result = shouldFilterURL(testUrl);
      if (result == true)
        testSucceeded();
      else
        testFailed("Expected URL with 200-char path segment to be filtered out, but it wasn't");
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("long-query-ok")) try {
      startTest("long-query", "Allow URLs with long query params (not path)", 5);
      // Long query parameter should be OK (we only check path segments)
      String longQuery = "q=" + "search".repeat(30);
      String testUrl = "http://example.com/search?" + longQuery;
      boolean result = shouldFilterURL(testUrl);
      if (result == false)
        testSucceeded();
      else
        testFailed("Expected URL with long query param to NOT be filtered out, but it was: " + testUrl);
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("seed-url-filter")) try {
      startTest("seed-filter", "normalizeSeedURL filters bad absolute URLs", 10);

      // Test URLs that should be filtered by normalizeSeedURL
      String calendarUrl = URLParser.normalizeSeedURL("http://example.com/events/calendar/2024");
      String matrixUrl = URLParser.normalizeSeedURL("http://example.com/path;jsessionid=ABC123");
      String pathParamUrl = URLParser.normalizeSeedURL("http://example.com/search/q=test/limit=20");
      String repeatUrl = URLParser.normalizeSeedURL("http://example.com/users/users/profile");
      String longSegmentUrl = URLParser.normalizeSeedURL("http://example.com/path/" + "x".repeat(150) + "/page");

      // Test URL that should pass
      String validUrl = URLParser.normalizeSeedURL("http://example.com/normal/path");

      boolean allBadFiltered = (calendarUrl == null && matrixUrl == null &&
                                pathParamUrl == null && repeatUrl == null && longSegmentUrl == null);
      boolean goodPassed = (validUrl != null && validUrl.equals("http://example.com:80/normal/path"));

      if (allBadFiltered && goodPassed) {
        testSucceeded();
      } else {
        String msg = "Expected bad URLs to be filtered (return null) and good URL to pass.\n";
        msg += "  calendar: " + calendarUrl + " (should be null)\n";
        msg += "  matrix: " + matrixUrl + " (should be null)\n";
        msg += "  pathParam: " + pathParamUrl + " (should be null)\n";
        msg += "  repeat: " + repeatUrl + " (should be null)\n";
        msg += "  longSegment: " + longSegmentUrl + " (should be null)\n";
        msg += "  valid: " + validUrl + " (should be http://example.com:80/normal/path)";
        testFailed(msg);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-id")) try {
      startTest("qf-id", "Keep id param if <= 25 chars", 5);
      String url1 = URLParser.normalizeSeedURL("http://example.com/page?id=12345");
      String url2 = URLParser.normalizeSeedURL("http://example.com/page?id=" + "x".repeat(26));

      if (url1 != null && url1.contains("id=12345") &&
          url2 != null && !url2.contains("id=")) {
        testSucceeded();
      } else {
        testFailed("Expected id=12345 to be kept and 26-char id to be removed. Got: " + url1 + ", " + url2);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-page")) try {
      startTest("qf-page", "Keep page param if <= 5", 5);
      String url1 = URLParser.normalizeSeedURL("http://example.com/page?page=3");
      String url2 = URLParser.normalizeSeedURL("http://example.com/page?page=5");
      String url3 = URLParser.normalizeSeedURL("http://example.com/page?page=6");

      boolean pass = url1 != null && url1.contains("page=3") &&
                     url2 != null && url2.contains("page=5") &&
                     url3 != null && !url3.contains("page=");

      if (pass) {
        testSucceeded();
      } else {
        testFailed("Expected page=3 and page=5 kept, page=6 dropped. Got: " + url1 + ", " + url2 + ", " + url3);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-page-search")) try {
      startTest("qf-page-srch", "Page limit 3 with search params", 5);
      String url1 = URLParser.normalizeSeedURL("http://example.com/page?q=test&page=3");
      String url2 = URLParser.normalizeSeedURL("http://example.com/page?q=test&page=4");

      boolean pass = url1 != null && url1.contains("page=3") && url1.contains("q=test") &&
                     url2 != null && url2.contains("q=test") && !url2.contains("page=");

      if (pass) {
        testSucceeded();
      } else {
        testFailed("Expected page=3 with search kept, page=4 with search dropped. Got: " + url1 + ", " + url2);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-search")) try {
      startTest("qf-search", "Always keep search params", 5);
      String url = URLParser.normalizeSeedURL("http://example.com/search?q=test&query=foo");

      if (url != null && url.contains("q=test") && url.contains("query=foo")) {
        testSucceeded();
      } else {
        testFailed("Expected search params to be kept. Got: " + url);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-category")) try {
      startTest("qf-cat", "Keep category if <= 18 chars", 5);
      String url1 = URLParser.normalizeSeedURL("http://example.com/page?category=electronics");
      String url2 = URLParser.normalizeSeedURL("http://example.com/page?cat=books");
      String url3 = URLParser.normalizeSeedURL("http://example.com/page?category=" + "x".repeat(19));

      boolean pass = url1 != null && url1.contains("category=electronics") &&
                     url2 != null && url2.contains("cat=books") &&
                     url3 != null && !url3.contains("category=");

      if (pass) {
        testSucceeded();
      } else {
        testFailed("Expected short categories kept, 19-char dropped. Got: " + url1 + ", " + url2 + ", " + url3);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-tag")) try {
      startTest("qf-tag", "Keep tags if <= 18 chars", 5);
      String url1 = URLParser.normalizeSeedURL("http://example.com/page?tag=technology");
      String url2 = URLParser.normalizeSeedURL("http://example.com/page?tag=" + "x".repeat(19));

      boolean pass = url1 != null && url1.contains("tag=technology") &&
                     url2 != null && !url2.contains("tag=");

      if (pass) {
        testSucceeded();
      } else {
        testFailed("Expected short tag kept, 19-char dropped. Got: " + url1 + ", " + url2);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-sorted")) try {
      startTest("qf-sort", "Query params sorted alphabetically", 5);
      String url = URLParser.normalizeSeedURL("http://example.com/page?page=2&id=123&cat=tech");

      // Should be sorted: cat, id, page
      if (url != null && url.matches(".*\\?cat=tech&id=123&page=2$")) {
        testSucceeded();
      } else {
        testFailed("Expected params sorted alphabetically (cat, id, page). Got: " + url);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-case")) try {
      startTest("qf-case", "Case insensitive param names", 5);
      String url = URLParser.normalizeSeedURL("http://example.com/page?Page=2&ID=123");

      // Should normalize to lowercase
      if (url != null && url.contains("id=123") && url.contains("page=2")) {
        testSucceeded();
      } else {
        testFailed("Expected case-insensitive params normalized to lowercase. Got: " + url);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("query-filter-drop")) try {
      startTest("qf-drop", "Drop unrecognized query params", 5);
      String url = URLParser.normalizeSeedURL("http://example.com/page?id=123&random=value&junk=data");

      // Should keep only id
      if (url != null && url.contains("id=123") && !url.contains("random=") && !url.contains("junk=")) {
        testSucceeded();
      } else {
        testFailed("Expected only recognized params kept. Got: " + url);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("normalize-integration")) try {
      startTest("normalize", "Integration test with normalizeURLs()", 10);
      // Note: normalizeURLs strips query params/fragments BEFORE filtering,
      // so we need to test with full absolute URLs that will be problematic after normalization
      List<String> rawUrls = Arrays.asList(
        "page1.html",                                    // Should pass
        "http://example.com/search;jsessionid=ABC",     // Should be filtered (matrix param in absolute URL)
        "search/q=dog/limit=20/results",                // Should be filtered (path param)
        "events/calendar/2024",                         // Should be filtered (calendar)
        "users/users/profile",                          // Should be filtered (repeating consecutive segments)
        "valid-page.html",                              // Should pass
        "another-valid"                                 // Should pass
      );
      String baseUrl = "http://example.com:80/base/";
      List<String> normalized = URLParser.normalizeURLs(rawUrls, baseUrl);

      // Expected: only page1.html, valid-page.html, another-valid should pass
      int expectedCount = 3;
      if (normalized.size() == expectedCount) {
        boolean hasPage1 = normalized.stream().anyMatch(u -> u.contains("page1.html"));
        boolean hasValid = normalized.stream().anyMatch(u -> u.contains("valid-page.html"));
        boolean hasAnother = normalized.stream().anyMatch(u -> u.contains("another-valid"));
        boolean hasCalendar = normalized.stream().anyMatch(u -> u.contains("calendar"));
        boolean hasUsers = normalized.stream().anyMatch(u -> u.contains("users"));

        if (hasPage1 && hasValid && hasAnother && !hasCalendar && !hasUsers)
          testSucceeded();
        else
          testFailed("Expected specific URLs to pass/fail filtering. Got: " + normalized);
      } else {
        testFailed("Expected " + expectedCount + " normalized URLs after filtering, but got " + normalized.size() + ": " + normalized);
      }
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    System.out.println("------------------------------------------------------------------------\n");
    if (numTestsFailed == 0)
      System.out.println("Looks like URLParser filtering passed all selected tests. Congratulations!");
    else
      System.out.println(numTestsFailed+" test(s) failed.");

    cleanup();
    closeOutputFile();
  }

  public static void main(String args[]) throws Exception {

    /* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */

    Set<String> tests = new TreeSet<String>();
    boolean runSetup = true, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = true;

    if ((args.length > 0) && args[0].equals("auto")) {
      runSetup = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("setup")) {
      runSetup = true;
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && args[0].equals("cleanup")) {
      runSetup = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("URLParser test v1.0");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto")) {
      tests.add("multiple-question-marks");
      tests.add("multiple-question-marks-2");
      tests.add("multiple-question-marks-3");
      tests.add("matrix-params");
      tests.add("matrix-params-2");
      tests.add("matrix-params-3");
      tests.add("path-embedded-params");
      tests.add("path-embedded-params-2");
      tests.add("path-embedded-params-3");
      tests.add("calendar");
      tests.add("calendar-2");
      tests.add("calendar-3");
      tests.add("repeating-paths");
      tests.add("repeating-paths-2");
      tests.add("repeating-paths-3");
      tests.add("normal-url");
      tests.add("normal-url-2");
      tests.add("normal-url-3");
      tests.add("normal-query-param");
      tests.add("normal-query-param-2");
      tests.add("domain-only");
      tests.add("domain-only-http");
      tests.add("long-segment");
      tests.add("long-segment-2");
      tests.add("long-segment-3");
      tests.add("long-query-ok");
      tests.add("seed-url-filter");
      tests.add("query-filter-id");
      tests.add("query-filter-page");
      tests.add("query-filter-page-search");
      tests.add("query-filter-search");
      tests.add("query-filter-category");
      tests.add("query-filter-tag");
      tests.add("query-filter-sorted");
      tests.add("query-filter-case");
      tests.add("query-filter-drop");
      tests.add("normalize-integration");
    }

    for (int i=0; i<args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto") && !args[i].equals("setup") && !args[i].equals("cleanup"))
        tests.add(args[i]);

    URLParserTest t = new URLParserTest();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile)
      t.outputToFile();
    if (runSetup)
      t.runSetup();
    if (promptUser)
      t.prompt();
    if (runTests)
      t.runTests(tests);
    if (cleanup)
      t.cleanup();
  }
}
