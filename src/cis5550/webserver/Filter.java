package cis5550.webserver;

/**
 * Functional interface for before/after filters in the web server.
 *
 * Filters allow intercepting requests before they reach route handlers (before filters)
 * and after handlers complete (after filters).
 *
 * Common use cases:
 * - Authentication/authorization (before)
 * - Logging (before and after)
 * - CORS headers (after)
 * - Request validation (before)
 *
 * Example:
 * <pre>
 * Server.before((req, res) -> {
 *     if (!req.headers("auth-token").equals("secret")) {
 *         res.halt(401, "Unauthorized");
 *     }
 * });
 * </pre>
 *
 */
@FunctionalInterface
public interface Filter {
    /**
     * Handles the filter logic.
     *
     * @param request  The incoming HTTP request
     * @param response The HTTP response being constructed
     */
    void handle(Request request, Response response);
}