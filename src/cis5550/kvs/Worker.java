package cis5550.kvs;

import static cis5550.tools.KeyEncoder.encode;
import cis5550.tools.Logger;
import static cis5550.webserver.Server.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Worker extends cis5550.generic.Worker {
    static Logger logger = Logger.getLogger(Worker.class);
    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> data = new ConcurrentHashMap<>();
    static Integer portNumber = 8001;
    static String storageDirPath;
    
    // Bulk write pool for batched operations
    private static BulkWritePool bulkWritePool;
    
    // Stats logging
    private static PrintWriter statsLogFile;
    private static ScheduledExecutorService statsScheduler;
    private static final SimpleDateFormat statsDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Object statsLogLock = new Object();

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            logger.error("Invalid arguments. Usage: Worker <port> <storageDir> <coordinatorIp:port> [workerId]");
            System.exit(1);
        }

        try {
            portNumber = Integer.parseInt(args[0]);
            storageDirPath = args[1];
            String coordinator = args[2];

            logger.info("Starting KVS Worker on port " + portNumber + " with storage directory: " + storageDirPath);

            // Ensure storage directory exists
            Path storagePath = Paths.get(storageDirPath);
            Files.createDirectories(storagePath);
            logger.info("Storage directory created/verified: " + storageDirPath);

            File idFile = new File(storageDirPath, "id");
            String workerId;

            if (idFile.exists()) {
                workerId = new String(Files.readAllBytes(idFile.toPath())).trim();
                logger.info("Worker ID loaded from file: " + workerId);
            } else if (args.length == 4 && args[3] != null && !args[3].isEmpty()) {
                // Use provided worker ID and persist it if no id file yet
                workerId = args[3];
                try (FileWriter writer = new FileWriter(idFile)) {
                    writer.write(workerId);
                }
                logger.info("Using provided Worker ID: " + workerId + " (saved to " + idFile.getAbsolutePath() + ")");
            } else {
                workerId = generateRandomId(5);
                try (FileWriter writer = new FileWriter(idFile)) {
                    writer.write(workerId);
                }
                logger.info("Generated new Worker ID: " + workerId);
            }

            port(portNumber);
            cis5550.webserver.Server.after((req, res) -> {
                res.header("Access-Control-Allow-Origin", "*");
                res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS");
                res.header("Access-Control-Allow-Headers", "*");
            });
            
            // Initialize bulk write pool
            bulkWritePool = new BulkWritePool((table, row) -> {
                try {
                    synchronized (data) {
                        // The merge logic is now handled inside putRow (both in-memory and persistent)
                        // so we can just call putRow directly here.
                        putRow(table, row);
                    }
                } catch (Exception e) {
                    logger.error("BulkWritePool: Failed to write row to " + table + ": " + e.getMessage(), e);
                }
            });
            logger.info("BulkWritePool initialized");
            
            // Initialize bottleneck stats logging
            startStatsLogging(storageDirPath, workerId);
            
            startPingThread(coordinator, workerId, portNumber);
            logger.info("KVS Worker started successfully. Coordinator: " + coordinator);

            get("/", (req, res) -> createHomeTable());

            get("/view/:T", (req, res) -> {
                String tableName = req.params("T");
                String offset = req.queryParams("offset");
                try {
                    int offsetValue = offset == null ? 0 : Integer.parseInt(offset);
                    return viewTable(tableName, offsetValue);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid offset parameter: " + offset);
                    res.status(401, "Bad Request");
                    return "Invalid offset parameter";
                } catch (Exception e) {
                    logger.error("Error viewing table " + tableName + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error viewing table: " + e.getMessage();
                }
            });

            get("/tables", (req, res) -> {
                List<String> tables = getTables();
                res.header("Content-Type", "text/plain");
                return String.join("\n", tables);
            });

            get("/count/:T", (req, res) -> {
                String table = req.params("T");
                if (table != null) {
                    List<String> rowKeys = getRowKeys(table);
                    if (rowKeys == null) {
                        res.status(404, "Not Found");
                        return "Table not found";
                    } else {
                        return String.valueOf(rowKeys.size());
                    }
                } else {
                    res.status(401, "Bad Request");
                    return "Table parameter required";
                }
            });

            get("/data/:T/:R/:C", (req, res) -> {
                String table = req.params("T");
                String rowKey = req.params("R");
                String column = req.params("C");

                if (table == null || rowKey == null || column == null) {
                    logger.warn("GET /data/:T/:R/:C called with missing parameters. Table: " + table + ", RowKey: "
                            + rowKey + ", Column: " + column);
                    res.status(401, "Bad Request");
                    return "Table, row key and column parameters required";
                }

                try {
                    Row row = getRow(table, rowKey);
                    if (row == null) {
                        //logger.debug("Row not found: table=" + table + ", rowKey=" + rowKey);
                        res.status(404, "Not Found");
                        return "Specified row key does not exist in table";
                    }

                    String value = row.get(column);
                    if (value == null) {
                        logger.debug("Column not found: table=" + table + ", rowKey=" + rowKey + ", column=" + column);
                        res.status(404, "Not Found");
                        return "Specified column does not exist in table";
                    }

                    res.bodyAsBytes(value.getBytes(StandardCharsets.UTF_8));
                    return null;
                } catch (Exception e) {
                    logger.error("Error getting column value from table " + table + ", row " + rowKey + ", column "
                            + column + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error retrieving data: " + e.getMessage();
                }
            });

            get("/data/:T/:R", (req, res) -> {
                String table = req.params("T");
                String rowKey = req.params("R");

                if (table == null || rowKey == null) {
                    logger.warn(
                            "GET /data/:T/:R called with missing parameters. Table: " + table + ", RowKey: " + rowKey);
                    res.status(401, "Bad Request");
                    return "Table and row key parameters required";
                }

                try {
                    Row row = getRow(table, rowKey);
                    if (row == null) {
                        //logger.debug("Row not found: table=" + table + ", rowKey=" + rowKey);
                        res.status(404, "Not Found");
                        return "Table or row does not exist";
                    }

                    res.bodyAsBytes(row.toByteArray());
                    return null;
                } catch (Exception e) {
                    logger.error("Error getting row from table " + table + ", rowKey " + rowKey + ": " + e.getMessage(),
                            e);
                    res.status(500, "Internal Server Error");
                    return "Error retrieving row: " + e.getMessage();
                }
            });

            get("/data/:T", (req, res) -> {
                String table = req.params("T");
                String startRow = req.queryParams("startRow");
                String endRowExclusive = req.queryParams("endRowExclusive");

                if (table == null) {
                    logger.warn("GET /data/:T called without table parameter");
                    res.status(401, "Bad Request");
                    return "Table parameter required";
                }

                try {
                    List<String> rowKeys = getRowKeys(table);
                    if (rowKeys == null) {
                        logger.debug("Table not found: " + table);
                        res.status(404, "Not Found");
                        return "Table not found";
                    }

                    logger.debug("Streaming rows from table " + table + " (total rows: " + rowKeys.size() + ", startRow: "
                            + startRow + ", endRowExclusive: " + endRowExclusive + ")");

                    // Stream rows directly using res.write() - no buffering!
                    // Enable streaming mode to omit Content-Length (client reads until connection close)
                    res.type("application/octet-stream");
                    res.setStreamingMode(true);
                    int rowsWritten = 0;
                    byte[] newline = new byte[] { '\n' };
                    
                    for (String rowKey : rowKeys) {
                        if ((startRow == null || rowKey.compareTo(startRow) >= 0)
                                && (endRowExclusive == null || rowKey.compareTo(endRowExclusive) < 0)) {
                            Row row = getRow(table, rowKey);
                            if (row != null) {
                                res.write(row.toByteArray());
                                res.write(newline);
                                rowsWritten++;
                            }
                        }
                    }
                    // Final newline to signal end
                    res.write(newline);
                    logger.debug("Streamed " + rowsWritten + " rows from table " + table);
                    return null;
                } catch (Exception e) {
                    logger.error("Error streaming rows from table " + table + ": " + e.getMessage(), e);
                    // If we haven't committed yet, we can send error status
                    // If already streaming, just log and close
                    res.status(500, "Internal Server Error");
                    return "Error retrieving table data: " + e.getMessage();
                }
            });

            put("/data/:T/:R/:C", (req, res) -> {
                String table = req.params("T");
                String rowKey = req.params("R");
                String column = req.params("C");
                String ifColumn = req.queryParams("ifcolumn");
                String equalValue = req.queryParams("equal");

                if (table == null || rowKey == null || column == null) {
                    logger.warn("PUT /data/:T/:R/:C called with missing parameters. Table: " + table + ", RowKey: "
                            + rowKey + ", Column: " + column);
                    res.status(401, "Bad Request");
                    return "Table, row key and column parameters required";
                }

                try {
                    synchronized (data) {
                        Row row = getRow(table, rowKey);
                        if (row == null) {
                            row = new Row(rowKey);
                            //logger.debug("Creating new row: table=" + table + ", rowKey=" + rowKey);
                        }

                        // Conditional PUT check
                        if (ifColumn != null && equalValue != null) {
                            String ifColumnValue = row.get(ifColumn);
                            if (ifColumnValue == null || !Objects.equals(ifColumnValue, equalValue)) {
                                logger.debug(
                                        "Conditional PUT failed: table=" + table + ", rowKey=" + rowKey + ", ifColumn="
                                                + ifColumn + ", expected=" + equalValue + ", actual=" + ifColumnValue);
                                return "FAIL";
                            }
                        }

                        byte[] bodyBytes = req.bodyAsBytes();
                        row.put(column, bodyBytes);
                        putRow(table, row);
                       // logger.debug("Successfully wrote column: table=" + table + ", rowKey=" + rowKey + ", column="
                        //        + column + ", size=" + bodyBytes.length + " bytes");
                        return "OK";
                    }
                } catch (IOException e) {
                    logger.error("IO error writing to table " + table + ", rowKey " + rowKey + ", column " + column
                            + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error writing data: " + e.getMessage();
                } catch (Exception e) {
                    logger.error("Unexpected error in PUT /data/:T/:R/:C for table " + table + ", rowKey " + rowKey
                            + ", column " + column + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

            put("/data/:T", (req, res) -> {
                String table = req.params("T");

                if (table == null) {
                    logger.warn("PUT /data/:T called without table parameter");
                    res.status(401, "Bad Request");
                    return "Table parameter required";
                }

                try {
                    byte[] bodyBytes = req.bodyAsBytes();
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(bodyBytes);
                    Row row = Row.readFrom(inputStream);
                    if (row == null) {
                        logger.debug("PUT /data/:T received null row for table " + table);
                        return "OK";
                    }
                    putRow(table, row);
                    //logger.debug("Successfully wrote row: table=" + table + ", rowKey=" + row.key() + ", size="
                     //       + bodyBytes.length + " bytes");
                    return "OK";
                } catch (IOException e) {
                    logger.error("IO error writing row to table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error writing data: " + e.getMessage();
                } catch (Exception e) {
                    logger.error("Unexpected error in PUT /data/:T for table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

            // Bulk write endpoint - accepts multiple rows in one request
            // Format: multiple rows separated by newlines, each row in Row.toByteArray() format
            put("/data/:T/bulk", (req, res) -> {
                String table = req.params("T");

                if (table == null) {
                    logger.warn("PUT /data/:T/bulk called without table parameter");
                    res.status(401, "Bad Request");
                    return "Table parameter required";
                }

                try {
                    byte[] bodyBytes = req.bodyAsBytes();
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(bodyBytes);
                    
                    List<Row> rows = new ArrayList<>();
                    while (inputStream.available() > 0) {
                        Row row = Row.readFrom(inputStream);
                        if (row != null) {
                            rows.add(row);
                        }
                        // Note: Row.readFrom already consumes the newline separator when it
                        // encounters it in readStringSpace(), so no need to skip it here
                    }
                    
                    if (rows.isEmpty()) {
                        logger.debug("PUT /data/:T/bulk received no rows for table " + table);
                        return "OK:0";
                    }
                    
                    // Add to bulk write pool for batched processing
                    bulkWritePool.addRows(table, rows);
                    
                    logger.debug("Bulk write: queued " + rows.size() + " rows for table " + table);
                    return "OK:" + rows.size();
                } catch (IOException e) {
                    logger.error("IO error in bulk write to table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error writing data: " + e.getMessage();
                } catch (Exception e) {
                    logger.error("Unexpected error in PUT /data/:T/bulk for table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

            // Bulk read endpoint - accepts multiple row keys and returns all matching rows
            // Request body: newline-separated row keys
            // Response body: rows serialized with Row.toByteArray(), separated by newlines
            //                Missing rows are returned as empty rows (just the key, no columns)
            post("/data/:T/bulkGet", (req, res) -> {
                String table = req.params("T");

                if (table == null) {
                    logger.warn("POST /data/:T/bulkGet called without table parameter");
                    res.status(400, "Bad Request");
                    return "Table parameter required";
                }

                try {
                    String body = req.body();
                    if (body == null || body.isEmpty()) {
                        logger.debug("POST /data/:T/bulkGet received empty body for table " + table);
                        return new byte[0];
                    }

                    String[] keys = body.split("\n");
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();

                    for (String key : keys) {
                        if (key.isEmpty()) continue;
                        
                        Row row = null;
                        
                        // Check if table is persistent or in-memory
                        if (table.startsWith("pt-")) {
                            // Persistent table - read from disk
                            File tableDir = new File(storageDirPath, table);
                            if (tableDir.exists()) {
                                File rowFile = new File(tableDir, java.net.URLEncoder.encode(key, "UTF-8"));
                                if (rowFile.exists()) {
                                    try (FileInputStream fis = new FileInputStream(rowFile)) {
                                        row = Row.readFrom(fis);
                                    }
                                }
                            }
                        } else {
                            // In-memory table
                            ConcurrentHashMap<String, Row> tableData = data.get(table);
                            if (tableData != null) {
                                row = tableData.get(key);
                            }
                        }
                        
                        if (row != null) {
                            baos.write(row.toByteArray());
                        } else {
                            // Return empty row marker (just the key, no columns)
                            // This lets client know the key was checked but not found
                            new Row(key).toByteArray();  // Marker for "not found"
                            baos.write(new Row(key).toByteArray());
                        }
                        baos.write('\n');
                    }

                    res.type("application/octet-stream");
                    res.bodyAsBytes(baos.toByteArray());
                    return null;
                } catch (IOException e) {
                    logger.error("IO error in bulk read from table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error reading data: " + e.getMessage();
                } catch (Exception e) {
                    logger.error("Unexpected error in POST /data/:T/bulkGet for table " + table + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

            put("/rename/:T", (req, res) -> {
                String oldTable = req.params("T");
                String newTable = req.body();

                if (oldTable == null || newTable == null || newTable.isEmpty()) {
                    logger.warn("PUT /rename/:T called with invalid parameters. oldTable: " + oldTable + ", newTable: "
                            + newTable);
                    res.status(401, "Bad Request");
                    return "Table name and new table name required";
                }

                try {
                    // Table is in persistent storage
                    if (oldTable.startsWith("pt-")) {
                        if (!newTable.startsWith("pt-")) {
                            logger.warn("Cannot rename persistent table to non-persistent: " + oldTable + " -> "
                                    + newTable);
                            res.status(401, "Bad Request");
                            return "Cannot rename persistent table " + oldTable + " to non-persistent name " + newTable;
                        }

                        Path oldPath = FileSystems.getDefault().getPath(storageDirPath, oldTable);
                        Path newPath = FileSystems.getDefault().getPath(storageDirPath, newTable);

                        if (!Files.exists(oldPath) || !Files.isDirectory(oldPath)) {
                            logger.warn("Table not found for rename: " + oldTable);
                            res.status(404, "Not Found");
                            return "Table " + oldTable + " not found";
                        }

                        if (Files.exists(newPath)) {
                            logger.warn("Target table already exists for rename: " + newTable);
                            res.status(409, "Conflict");
                            return "Table " + newTable + " already exists";
                        }

                        Files.move(oldPath, newPath);
                        logger.info("Renamed persistent table: " + oldTable + " -> " + newTable);
                    } else {
                        // Table is in memory
                        if (!data.containsKey(oldTable)) {
                            logger.warn("In-memory table not found for rename: " + oldTable);
                            res.status(404, "Not Found");
                            return "Table " + oldTable + " not found";
                        }

                        ConcurrentHashMap<String, Row> rows = data.get(oldTable);

                        if (newTable.startsWith("pt-")) {
                            // Move to persistent storage
                            Path storageDir = Path.of(storageDirPath);
                            Path newPath = storageDir.resolve(newTable);
                            if (Files.exists(newPath)) {
                                logger.warn("Target persistent table already exists: " + newTable);
                                res.status(409, "Conflict");
                                return "Table " + newTable + " already exists";
                            }

                            logger.info("Moving in-memory table to persistent storage: " + oldTable + " -> " + newTable
                                    + " (" + rows.size() + " rows)");
                            for (Row row : rows.values()) {
                                putRow(newTable, row);
                            }
                            data.remove(oldTable);
                        } else {
                            // Move within memory
                            if (data.containsKey(newTable)) {
                                logger.warn("Target in-memory table already exists: " + newTable);
                                res.status(409, "Conflict");
                                return "Table " + newTable + " already exists";
                            }
                            data.put(newTable, rows);
                            data.remove(oldTable);
                            logger.info("Renamed in-memory table: " + oldTable + " -> " + newTable);
                        }
                    }
                    return "OK";
                } catch (IOException e) {
                    logger.error("IO error renaming table " + oldTable + " to " + newTable + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error renaming table: " + e.getMessage();
                } catch (Exception e) {
                    logger.error(
                            "Unexpected error renaming table " + oldTable + " to " + newTable + ": " + e.getMessage(),
                            e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

            put("/delete/:T", (req, res) -> {
                String tableName = req.params("T");

                if (tableName == null || tableName.isEmpty()) {
                    logger.warn("PUT /delete/:T called without table parameter");
                    res.status(401, "Bad Request");
                    return "Table parameter required";
                }

                try {
                    if (tableName.startsWith("pt-")) {
                        Path storageDir = Path.of(storageDirPath);
                        Path tablePath = storageDir.resolve(tableName);

                        if (!Files.exists(tablePath) || !Files.isDirectory(tablePath)) {
                            logger.warn("Persistent table not found for deletion: " + tableName);
                            res.status(404, "Not Found");
                            return "Table " + tableName + " not found";
                        }

                        int deletedCount = 0;
                        try (Stream<Path> walk = Files.walk(tablePath)) {
                            List<Path> pathsToDelete = walk.sorted(Comparator.reverseOrder()).toList();
                            for (Path path : pathsToDelete) {
                                try {
                                    Files.delete(path);
                                    deletedCount++;
                                } catch (IOException e) {
                                    logger.warn("Failed to delete " + path + ": " + e.getMessage());
                                }
                            }
                        }
                        logger.info(
                                "Deleted persistent table " + tableName + " (" + deletedCount + " files/directories)");
                        return "OK";
                    } else {
                        if (!data.containsKey(tableName)) {
                            logger.warn("In-memory table not found for deletion: " + tableName);
                            res.status(404, "Not Found");
                            return "Table " + tableName + " not found";
                        }

                        int rowCount = data.get(tableName).size();
                        data.remove(tableName);
                        logger.info("Deleted in-memory table " + tableName + " (" + rowCount + " rows)");
                        return "OK";
                    }
                } catch (IOException e) {
                    logger.error("IO error deleting table " + tableName + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Error deleting table: " + e.getMessage();
                } catch (Exception e) {
                    logger.error("Unexpected error deleting table " + tableName + ": " + e.getMessage(), e);
                    res.status(500, "Internal Server Error");
                    return "Unexpected error: " + e.getMessage();
                }
            });

        } catch (NumberFormatException e) {
            logger.error("Invalid port number: " + args[0], e);
            System.exit(1);
        } catch (IOException e) {
            logger.error("Fatal IO error during worker initialization: " + e.getMessage(), e);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Fatal error during worker initialization: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    public static String createHomeTable() {
        try {
            String html = """
                    <html>
                    <head>
                    <style>
                    table, th, td {
                      border: 1px solid black;
                      border-collapse: collapse;
                    }
                    th, td {
                      padding: 8px;
                    }
                    </style>
                    <title>CIS5550 HW5 Worker</title>
                    </head>
                    <body>
                        <table>
                        <tr>
                            <th>Table</th>
                            <th>Rows</th>
                        </tr>
                        """;

            List<String> allTables = getTables();
            logger.debug("Generating home table view with " + allTables.size() + " tables");
            for (String table : allTables) {
                try {
                    List<String> rowKeys = getRowKeys(table);
                    Integer rowCount = (rowKeys != null) ? rowKeys.size() : 0;
                    html += "<tr><td><a href='/view/" + table + "'>" + table + "</a></td><td>" + rowCount
                            + "</td></tr>";
                } catch (Exception e) {
                    logger.warn("Error getting row count for table " + table + ": " + e.getMessage());
                    html += "<tr><td>" + table + "</td><td>Error</td></tr>";
                }
            }
            html += "</table></body></html>";
            return html;
        } catch (Exception e) {
            logger.error("Error creating home table: " + e.getMessage(), e);
            return "<html><body>Error loading tables: " + e.getMessage() + "</body></html>";
        }
    }

    public static String viewTable(String tableName, Integer offset) {
        try {
            List<String> rowKeys = getRowKeys(tableName);
            if (rowKeys == null || rowKeys.isEmpty()) {
                logger.debug("Table view requested for non-existent or empty table: " + tableName);
                return "Table '" + tableName + "' doesn't exist or is empty.";
            }

            Boolean endOfTable = false;
            Integer endIndex = offset + 10;
            if (offset + 10 > rowKeys.size()) {
                endIndex = rowKeys.size();
                endOfTable = true;
            }

            if (offset < 0 || offset >= rowKeys.size()) {
                logger.warn("Invalid offset for table view: table=" + tableName + ", offset=" + offset + ", totalRows="
                        + rowKeys.size());
                return "Invalid offset. Table has " + rowKeys.size() + " rows.";
            }

            List<String> validRowKeys = rowKeys.subList(offset, endIndex);
            List<Row> rows = new ArrayList<>();
            Set<String> allCols = new HashSet<>();
            for (String rowKey : validRowKeys) {
                Row row = getRow(tableName, rowKey);
                if (row != null) {
                    rows.add(row);
                    allCols.addAll(row.columns());
                } else {
                    logger.warn("Row not found when viewing table: table=" + tableName + ", rowKey=" + rowKey);
                }
            }

            logger.debug("Viewing table " + tableName + ": showing rows " + offset + "-" + (endIndex - 1) + " of "
                    + rowKeys.size());

            String html = """
                    <html>
                    <head>
                    <style>
                    table, th, td {
                      border: 1px solid black;
                      border-collapse: collapse;
                    }
                    th, td {
                      padding: 8px;
                    }
                    </style>
                    <title>CIS5550 HW5 Table</title>
                    </head>
                    <body>
                    <h2>""";
            html += tableName;
            html += """
                    </h2>
                    <table>
                    <tr>
                        <th>Row</th>
                    """;
            for (String col : allCols) {
                html += "<th>" + col + "</th>";
            }
            html += "</tr>";

            for (Row row : rows) {
                String rowKey = row.key();
                html += "<tr><td>" + rowKey + "</td>";
                for (String col : allCols) {
                    html += "<td>" + (row.get(col) != null ? row.get(col) : " ") + "</td>";
                }
                html += "</tr>";
            }

            html += "</table>";
            if (!endOfTable) {
                html += "<a href='/view/" + tableName + "?offset=" + (offset + 10) + "'>Next</a>";
            }
            html += "</body></html>";
            return html;
        } catch (Exception e) {
            logger.error("Error viewing table " + tableName + ": " + e.getMessage(), e);
            return "<html><body>Error viewing table: " + e.getMessage() + "</body></html>";
        }
    }

    public static List<String> getTables() {
        List<String> allTables = new ArrayList<>();
        // Persistent tables
        try {
            Path storageDir = Path.of(storageDirPath);

            if (Files.exists(storageDir)) {
                List<String> persistentTables = Files.list(storageDir)
                        .filter(Files::isDirectory)
                        .map(path -> path.getFileName().toString())
                        .toList();
                allTables.addAll(persistentTables);
            }
        } catch (IOException e) {
            logger.error("Error listing persistent tables: " + e.getMessage(), e);
            return List.of();
        }

        List<String> inMemoryTables = data.keySet().stream().toList();
        allTables.addAll(inMemoryTables);
        return allTables;
    }

    public static List<String> getRowKeys(String table) throws IOException {
        if (table.startsWith("pt-")) {
            return persistedGetRowKeys(table);
        } else {
            return inMemoryGetRowKeys(table);
        }
    }

    public static List<String> persistedGetRowKeys(String table) {
        Path storageDir = Path.of(storageDirPath);
        Path tablePath = storageDir.resolve(table);
        if (!Files.exists(tablePath)) {
            return null;
        }

        try {
            List<String> rowFiles = new ArrayList<>();
            Predicate<Path> validFile = p -> Files.isRegularFile(p) && !p.getFileName().toString().startsWith(".");

            // Add all regular files directly inside the table directory
            try (Stream<Path> stream = Files.list(tablePath)) {
                rowFiles.addAll(
                        stream.filter(validFile)
                                .map(path -> path.getFileName().toString())
                                .toList());
            }

            // Add all files inside subdirectories starting with "__"
            try (Stream<Path> stream = Files.list(tablePath)) {
                stream.filter(path -> Files.isDirectory(path)
                        && path.getFileName().toString().startsWith("__"))
                        .forEach(dir -> {
                            try (Stream<Path> inner = Files.list(dir)) {
                                inner.filter(validFile)
                                        .map(path -> path.getFileName().toString())
                                        .forEach(rowFiles::add);
                            } catch (IOException e) {
                                logger.warn("Error reading subfolder " + dir + ": " + e.getMessage());
                            }
                        });
            }

            return rowFiles;

        } catch (IOException e) {
            logger.error("Error getting row keys for persistent table " + table + ": " + e.getMessage(), e);
            return null;
        }
    }

    public static List<String> inMemoryGetRowKeys(String table) {
        if (!data.containsKey(table)) {
            return null;
        }
        Map<String, Row> tableData = data.get(table);
        if (tableData == null) {
            return null;
        }
        return tableData.keySet().stream().toList();
    }

    public static synchronized Row getRow(String table, String rowKey) {
        if (table.startsWith("pt-")) {
            return persistedGetRow(table, rowKey);
        } else {
            return inMemoryGetRow(table, rowKey);
        }
    }

    public static Row persistedGetRow(String table, String rowKey) {
        Path storageDir = Path.of(storageDirPath);
        Path tablePath = storageDir.resolve(table);
        String fileName = encode(rowKey);
        Path rowFile;

        if (fileName.length() >= 6) {
            String subfolder = "__" + fileName.substring(0, 2);
            rowFile = tablePath.resolve(subfolder + File.separator + fileName);
        } else {
            rowFile = tablePath.resolve(fileName);
        }

        if (!Files.exists(tablePath) || !Files.exists(rowFile)) {
            return null;
        }
        try (InputStream in = Files.newInputStream(rowFile)) {
            Row row = Row.readFrom(in);
            if (row == null) {
                logger.warn("Row file exists but could not be read: " + rowFile);
            }
            return row;
        } catch (IOException e) {
            logger.error("IO error reading row file " + rowFile + ": " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error reading row from file " + rowFile + ": " + e.getMessage(), e);
        }
        return null;
    }

    public static Row inMemoryGetRow(String table, String rowKey) {
        if (!data.containsKey(table) || !data.get(table).containsKey(rowKey)) {
            return null;
        } else {
            return data.get(table).get(rowKey).clone();
        }
    }

    public static void putRow(String table, Row row) throws IOException {
        if (table.startsWith("pt-")) {
            persistedPutRow(table, row);
        } else {
            inMemoryPutRow(table, row);
        }
    }

    public static void persistedPutRow(String table, Row row) throws IOException {
        if (row == null) {
            throw new IllegalArgumentException("Cannot write null row to table " + table);
        }

        Path storageDir = Path.of(storageDirPath);
        Path tablePath = storageDir.resolve(encode(table));
        if (!Files.exists(tablePath)) {
            Files.createDirectories(tablePath);
            logger.debug("Created table directory: " + tablePath);
        }

        String fileName = encode(row.key());
        Path rowFile;
        if (fileName.length() >= 6) {
            String subfolder = "__" + fileName.substring(0, 2);
            Path subFolderPath = storageDir.resolve(encode(table) + File.separator + subfolder);
            if (!Files.exists(subFolderPath)) {
                Files.createDirectories(subFolderPath);
                logger.debug("Created subfolder: " + subFolderPath);
            }
            rowFile = subFolderPath.resolve(fileName);
        } else {
            rowFile = tablePath.resolve(fileName);
        }

        // READ existing row if present (for merge)
        Row mergedRow = row;
        if (Files.exists(rowFile)) {
            try (FileInputStream fis = new FileInputStream(rowFile.toFile())) {
                Row existingRow = Row.readFrom(fis);
                if (existingRow != null) {
                    // MERGE: start with existing row, add/overwrite with new columns
                    for (String col : row.columns()) {
                        existingRow.put(col, row.getBytes(col));
                    }
                    mergedRow = existingRow;
                    //logger.debug("Merged row with existing: table=" + table + ", rowKey=" + row.key() + 
                      //          ", existingCols=" + existingRow.columns().size() + ", newCols=" + row.columns().size());
                }
            } catch (Exception e) {
                // If read fails, just use the new row (overwrite)
                logger.warn("Failed to read existing row for merge, overwriting: " + e.getMessage());
            }
        }

        // WRITE to temp file, then atomic rename (prevents partial reads)
        byte[] rowData = mergedRow.toByteArray();
        Path tempFile = rowFile.resolveSibling(rowFile.getFileName() + ".tmp." + Thread.currentThread().getId());
        try {
            try (FileOutputStream outputStream = new FileOutputStream(tempFile.toFile())) {
                outputStream.write(rowData);
            }
            // Atomic move - readers will see either old complete file or new complete file, never partial
            Files.move(tempFile, rowFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
           // logger.debug("Wrote row to persistent storage: table=" + table + ", rowKey=" + mergedRow.key() + ", size="
             //       + rowData.length + " bytes, file=" + rowFile);
        } catch (AtomicMoveNotSupportedException e) {
            // Fallback: non-atomic move (still better than direct write)
            Files.move(tempFile, rowFile, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Wrote row (non-atomic fallback): table=" + table + ", rowKey=" + mergedRow.key());
        } finally {
            // Clean up temp file if it still exists (e.g., if move failed)
            Files.deleteIfExists(tempFile);
        }
    }

    public static void inMemoryPutRow(String table, Row row) {
        // Use computeIfAbsent for thread-safe table creation
        ConcurrentHashMap<String, Row> tableData = data.computeIfAbsent(table, k -> new ConcurrentHashMap<>());
        
        // For in-memory, we can use compute() for atomic read-modify-write
        tableData.compute(row.key(), (key, existing) -> {
            if (existing != null) {
                for (String col : row.columns()) {
                    existing.put(col, row.getBytes(col));
                }
                return existing;
            } else {
                return row;
            }
        });
    }

    private static String generateRandomId(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
    
    // ============================================================
    // BOTTLENECK STATS LOGGING
    // Search for [KVS_BOTTLENECK] in kvs-worker.log to find these
    // ============================================================
    
    /**
     * Start periodic bottleneck stats logging (every 10 seconds).
     */
    private static void startStatsLogging(String storageDir, String workerId) {
        try {
            // Create logs directory in storage path
            Path logsDir = Paths.get(storageDir, "logs");
            Files.createDirectories(logsDir);
            
            // Open log file
            String logFileName = logsDir.resolve("kvs-worker.log").toString();
            statsLogFile = new PrintWriter(new FileWriter(logFileName, true), false);
            
            // Start periodic logging (every 10 seconds)
            statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "kvs-stats-logger");
                t.setDaemon(true);
                return t;
            });
            
            final String wid = workerId;
            statsScheduler.scheduleAtFixedRate(() -> logStats(wid), 10, 10, TimeUnit.SECONDS);
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                synchronized (statsLogLock) {
                    if (statsLogFile != null) {
                        statsLogFile.flush();
                        statsLogFile.close();
                    }
                }
            }));
            
            logStatsRaw("[KVS_BOTTLENECK] Stats logging started for worker " + workerId);
            logger.info("KVS bottleneck stats logging started: " + logFileName);
            
        } catch (IOException e) {
            logger.error("Failed to initialize stats logging: " + e.getMessage());
        }
    }
    
    /**
     * Log current bottleneck stats.
     */
    private static void logStats(String workerId) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("[KVS_BOTTLENECK] === KVS Worker Stats (").append(workerId).append(") ===\n");
            
            // BulkWritePool stats
            if (bulkWritePool != null) {
                sb.append("[KVS_BOTTLENECK] [BULK_WRITE_POOL] ");
                sb.append("buffered_rows=").append(bulkWritePool.getCurrentBufferedRows());
                sb.append(", buffered_tables=").append(bulkWritePool.getBufferedTableCount());
                sb.append(", pending_tasks=").append(bulkWritePool.getPendingWriteTasks());
                sb.append(", active_threads=").append(bulkWritePool.getActiveWriteThreads());
                sb.append(", total_buffered=").append(bulkWritePool.getTotalRowsBuffered());
                sb.append(", total_flushed=").append(bulkWritePool.getTotalRowsFlushed());
                
                // Warnings
                int buffered = bulkWritePool.getCurrentBufferedRows();
                int pending = bulkWritePool.getPendingWriteTasks();
                if (buffered > 500) {
                    sb.append(" [WARNING: HIGH_BUFFER]");
                }
                if (pending > 10) {
                    sb.append(" [WARNING: WRITE_BACKLOG]");
                }
                sb.append("\n");
            }
            
            // In-memory table stats
            sb.append("[KVS_BOTTLENECK] [MEMORY] ");
            sb.append("tables=").append(data.size());
            long totalRows = 0;
            for (ConcurrentHashMap<String, Row> table : data.values()) {
                totalRows += table.size();
            }
            sb.append(", total_rows=").append(totalRows);
            
            // Memory usage
            Runtime runtime = Runtime.getRuntime();
            long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            long maxMB = runtime.maxMemory() / (1024 * 1024);
            sb.append(", heap_used_mb=").append(usedMB);
            sb.append(", heap_max_mb=").append(maxMB);
            
            if (usedMB > maxMB * 0.9) {
                sb.append(" [WARNING: HIGH_MEMORY]");
            }
            sb.append("\n");
            
            logStatsRaw(sb.toString().trim());
            
            // Flush periodically
            synchronized (statsLogLock) {
                if (statsLogFile != null) {
                    statsLogFile.flush();
                }
            }
        } catch (Exception e) {
            logger.error("Error logging stats: " + e.getMessage());
        }
    }
    
    /**
     * Log raw message to stats file.
     */
    private static void logStatsRaw(String message) {
        synchronized (statsLogLock) {
            if (statsLogFile != null) {
                String timestamp = statsDateFormat.format(new Date());
                statsLogFile.println("[" + timestamp + "] " + message);
            }
        }
    }
}
