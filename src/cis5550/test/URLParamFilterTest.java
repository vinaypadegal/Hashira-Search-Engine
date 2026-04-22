package cis5550.test;

import cis5550.tools.URLParser;
import java.lang.reflect.Method;

/**
 * Comprehensive test suite for URL parameter filtering
 * Tests the filterQueryParams function with real-world URL examples
 */
public class URLParamFilterTest {
    
    private static int testsPassed = 0;
    private static int testsFailed = 0;
    
    // Use reflection to access private filterQueryParams method
    private static String filterQueryParams(String url) throws Exception {
        Method method = URLParser.class.getDeclaredMethod("filterQueryParams", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, url);
    }
    
    private static void test(String name, String input, String expected, String description) {
        try {
            String result = filterQueryParams(input);
            boolean passed = result.equals(expected);
            
            if (passed) {
                System.out.println("✓ PASS: " + name);
                testsPassed++;
            } else {
                System.out.println("✗ FAIL: " + name);
                System.out.println("  Description: " + description);
                System.out.println("  Input:    " + input);
                System.out.println("  Expected: " + expected);
                System.out.println("  Got:      " + result);
                testsFailed++;
            }
        } catch (Exception e) {
            System.out.println("✗ ERROR: " + name + " - " + e.getMessage());
            e.printStackTrace();
            testsFailed++;
        }
    }
    
    public static void main(String[] args) {
        System.out.println("==========================================");
        System.out.println("URL Parameter Filter Test Suite");
        System.out.println("==========================================");
        System.out.println();
        
        // Test 1: E-commerce URLs with tracking parameters
        test("ecommerce-1", 
            "https://amazon.com/product?id=12345&ref=sr_1_1&pf_rd_r=ABC&pf_rd_p=xyz&utm_source=email&utm_campaign=sale",
            "https://amazon.com/product?id=12345",
            "E-commerce: Keep id, remove tracking params");
        
        test("ecommerce-2",
            "https://ebay.com/item?item_id=67890&hash=item123&var=456&utm_medium=cpc",
            "https://ebay.com/item",
            "E-commerce: Remove all unrecognized params");
        
        // Test 2: Search engine URLs
        test("search-1",
            "https://google.com/search?q=distributed+systems&page=2&start=20&hl=en&safe=active",
            "https://google.com/search?page=2&q=distributed+systems",
            "Search: Keep q and page (within limit), remove others");
        
        test("search-2",
            "https://bing.com/search?query=java&p=1&FORM=QSRE1&PC=U316",
            "https://bing.com/search?p=1&query=java",
            "Search: Keep query and p (page), remove FORM/PC");
        
        test("search-3",
            "https://duckduckgo.com/?q=test&ia=web&iax=images",
            "https://duckduckgo.com/?q=test",
            "Search: Keep q, remove ia/iax");
        
        // Test 3: Pagination tests
        test("pagination-1",
            "https://example.com/list?page=1&per_page=20&sort=name",
            "https://example.com/list?page=1",
            "Pagination: Keep page <= 5, remove per_page/sort");
        
        test("pagination-2",
            "https://example.com/list?page=5&p=3",
            "https://example.com/list?p=3&page=5",
            "Pagination: Keep both page and p within limits, sorted");
        
        test("pagination-3",
            "https://example.com/list?page=6&q=test",
            "https://example.com/list?q=test",
            "Pagination: Drop page > 5 when no search (limit is 5)");
        
        test("pagination-4",
            "https://example.com/list?page=3&q=test&p=4",
            "https://example.com/list?page=3&q=test",
            "Pagination: Drop p=4 when search exists (limit is 3), keep page=3");
        
        test("pagination-5",
            "https://example.com/list?page_num=2&pagenumber=1",
            "https://example.com/list?page_num=2&pagenumber=1",
            "Pagination: Keep page_num and pagenumber variants");
        
        // Test 4: Category and tag filtering
        test("category-1",
            "https://store.com/products?category=electronics&cat=phones&c=laptops",
            "https://store.com/products?c=laptops&cat=phones&category=electronics",
            "Category: Keep all category variants, sorted");
        
        test("category-2",
            "https://store.com/products?category=" + "x".repeat(18) + "&cat=short",
            "https://store.com/products?cat=short&category=" + "x".repeat(18),
            "Category: Keep 18-char category, keep short cat");
        
        test("category-3",
            "https://store.com/products?category=" + "x".repeat(19),
            "https://store.com/products",
            "Category: Drop category > 18 chars");
        
        test("tag-1",
            "https://blog.com/posts?tag=technology&topic=ai&label=machine-learning",
            "https://blog.com/posts?label=machine-learning&tag=technology&topic=ai",
            "Tags: Keep tag, topic, label variants, sorted");
        
        test("tag-2",
            "https://blog.com/posts?tag=" + "x".repeat(18) + "&topic=short",
            "https://blog.com/posts?tag=" + "x".repeat(18) + "&topic=short",
            "Tags: Keep 18-char tag, keep short topic");
        
        test("tag-3",
            "https://blog.com/posts?tag=" + "x".repeat(19),
            "https://blog.com/posts",
            "Tags: Drop tag > 18 chars");
        
        // Test 5: ID parameter tests
        test("id-1",
            "https://api.example.com/user?id=12345&name=john",
            "https://api.example.com/user?id=12345",
            "ID: Keep id <= 25 chars, remove name");
        
        test("id-2",
            "https://api.example.com/user?id=" + "x".repeat(25),
            "https://api.example.com/user?id=" + "x".repeat(25),
            "ID: Keep id exactly 25 chars");
        
        test("id-3",
            "https://api.example.com/user?id=" + "x".repeat(26),
            "https://api.example.com/user",
            "ID: Drop id > 25 chars");
        
        // Test 6: Multiple search parameter variants
        test("search-variants-1",
            "https://search.com?q=test&query=foo&search=bar&s=baz&term=qux&keywords=abc&search_query=xyz",
            "https://search.com?keywords=abc&q=test&query=foo&s=baz&search=bar&search_query=xyz&term=qux",
            "Search: Keep all search variants, sorted alphabetically");
        
        // Test 7: Complex real-world URLs
        test("realworld-1",
            "https://wikipedia.org/wiki/Java?oldid=123456&diff=prev&printable=yes&action=edit",
            "https://wikipedia.org/wiki/Java",
            "Wikipedia: Remove all params (none are allowed)");
        
        test("realworld-2",
            "https://github.com/search?q=java&type=repositories&ref=searchresults&p=2",
            "https://github.com/search?p=2&q=java",
            "GitHub: Keep q and p, remove type/ref");
        
        test("realworld-3",
            "https://youtube.com/watch?v=dQw4w9WgXcQ&list=PLrAXtmRdnEQy6nuLMH&index=1&t=30s",
            "https://youtube.com/watch",
            "YouTube: Remove all params (v, list, index, t not allowed)");
        
        test("realworld-4",
            "https://reddit.com/r/programming?q=java&sort=hot&t=all&limit=25",
            "https://reddit.com/r/programming?q=java",
            "Reddit: Keep q, remove sort/t/limit");
        
        test("realworld-5",
            "https://stackoverflow.com/questions?q=java&tab=newest&page=2&pagesize=15",
            "https://stackoverflow.com/questions?page=2&q=java",
            "StackOverflow: Keep q and page, remove tab/pagesize");
        
        // Test 8: Edge cases
        test("edge-1",
            "https://example.com/page",
            "https://example.com/page",
            "Edge: No query params, return as-is");
        
        test("edge-2",
            "https://example.com/page?",
            "https://example.com/page",
            "Edge: Empty query string, return base URL");
        
        test("edge-3",
            "https://example.com/page?foo",
            "https://example.com/page",
            "Edge: Param without value, drop it");
        
        test("edge-4",
            "https://example.com/page?foo=&bar=",
            "https://example.com/page",
            "Edge: Params with empty values, drop all");
        
        test("edge-5",
            "https://example.com/page?ID=123&PAGE=2&Q=test",
            "https://example.com/page?id=123&page=2&q=test",
            "Edge: Case insensitive, normalize to lowercase");
        
        // Test 9: Duplicate parameters (should keep first)
        test("duplicate-1",
            "https://example.com/page?id=123&id=456&id=789",
            "https://example.com/page?id=123",
            "Duplicate: Keep first occurrence of duplicate param");
        
        // Test 10: Mixed allowed and disallowed
        test("mixed-1",
            "https://shop.com/products?id=123&category=electronics&ref=google&utm_source=email&page=2&sort=price&tag=tech",
            "https://shop.com/products?category=electronics&id=123&page=2&tag=tech",
            "Mixed: Keep allowed (id, category, page, tag), remove tracking (ref, utm_source, sort)");
        
        test("mixed-2",
            "https://news.com/articles?q=breaking&page=1&category=world&author=john&date=2024&tag=politics&share=facebook",
            "https://news.com/articles?category=world&page=1&q=breaking&tag=politics",
            "Mixed: Keep search/category/page/tag, remove author/date/share");
        
        // Test 11: URL encoding
        test("encoding-1",
            "https://example.com/search?q=hello%20world&page=1",
            "https://example.com/search?page=1&q=hello%20world",
            "Encoding: Preserve URL-encoded values in params");
        
        // Test 12: Special characters in values
        test("special-chars-1",
            "https://example.com/page?id=abc-123_xyz&q=test+query&category=cat%2Fsub",
            "https://example.com/page?category=cat%2Fsub&id=abc-123_xyz&q=test+query",
            "Special chars: Preserve special characters in param values");
        
        // Test 13: Maximum page value edge cases
        test("max-page-1",
            "https://example.com/list?page=5&q=test",
            "https://example.com/list?q=test",
            "Max page: Drop page=5 with search (limit is 3 when search exists)");
        
        test("max-page-2",
            "https://example.com/list?page=5",
            "https://example.com/list?page=5",
            "Max page: Keep page=5 without search (limit is 5)");
        
        test("max-page-3",
            "https://example.com/list?page=6",
            "https://example.com/list",
            "Max page: Drop page=6 without search (limit is 5)");
        
        // Test 14: All params removed scenario
        test("all-removed-1",
            "https://example.com/page?ref=google&utm_source=email&fbclid=123&gclid=456",
            "https://example.com/page",
            "All removed: When all params are disallowed, return base URL");
        
        // Test 15: Real Amazon/eBay style URLs
        test("amazon-style-1",
            "https://amazon.com/dp/B08N5WRWNW?ref_=Oct_DLandingS_D_abc123&pf_rd_r=XYZ&pf_rd_p=123&psc=1",
            "https://amazon.com/dp/B08N5WRWNW",
            "Amazon: Remove all tracking params");
        
        test("ebay-style-1",
            "https://ebay.com/itm/123456789?hash=itemabc&var=456&chn=ps&mkevt=1&mkcid=1",
            "https://ebay.com/itm/123456789",
            "eBay: Remove all tracking params");
        
        // Print summary
        System.out.println();
        System.out.println("==========================================");
        System.out.println("Test Summary");
        System.out.println("==========================================");
        System.out.println("Total tests: " + (testsPassed + testsFailed));
        System.out.println("Passed: " + testsPassed);
        System.out.println("Failed: " + testsFailed);
        System.out.println("==========================================");
        
        if (testsFailed == 0) {
            System.out.println("🎉 All tests passed!");
            System.exit(0);
        } else {
            System.out.println("❌ Some tests failed. Review output above.");
            System.exit(1);
        }
    }
}

