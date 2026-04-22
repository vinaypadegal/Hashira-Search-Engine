package cis5550.generic;

import static cis5550.webserver.Server.get;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {
    private static final Integer VALID_DURATION_MILLIS = 15000;
    private static Map<String, WorkerMetadata> workerMetadata = new ConcurrentHashMap<>();

    public static List<WorkerMetadata> getWorkers() {
        long currentTime = System.currentTimeMillis();
        workerMetadata.entrySet().removeIf(entry -> entry.getValue().getLastPing() + VALID_DURATION_MILLIS < currentTime);
        return new ArrayList<>(workerMetadata.values());
    }

    public static List<String> getWorkersString() {
        return getWorkers().stream().map(m -> m.ipAddress + ":" + m.portNumber).toList();
    }

    public static String workerTable() {
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
            <title>Worker Table</title>
            </head>
            <body>
            <table>
                <tr>
                    <th>ID</th>
                    <th>IP Address</th>
                    <th>Port</th>
                    <th>Link</th>
                </tr>
            """;
        List<WorkerMetadata> workers = getWorkers();
        for (WorkerMetadata worker: workers) {
            String link = "http://" + worker.getIpAddress() + ":" + worker.getPortNumber();
            html += "<tr><td>" + worker.getWorkerId() + "</td><td>" + worker.getIpAddress() + "</td><td>" + worker.getPortNumber() + "</td><td><a href=" + link + ">" + link + "</a></td></tr>";
        }
        html += "</table></body></html>";
        return html;
    }

    public static void registerRoutes() {
        cis5550.webserver.Server.after((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS");
            res.header("Access-Control-Allow-Headers", "*");
        });
        get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String ipAddress = req.ip();
            String portNumber = req.queryParams("port");
            if (id == null || portNumber == null) {
                res.status(400, "ID or port missing.");
                return "ID or port missing.";
            } else {
                workerMetadata.put(id, new WorkerMetadata(id, ipAddress, portNumber, System.currentTimeMillis()));
                return "OK";
            }
        });

        get("/workers", (req, res) -> {
            List<WorkerMetadata> workers = getWorkers();
            String ret = workers.size() + "\n";
            for (WorkerMetadata worker: workers) {
                ret += worker.getWorkerId() + "," + worker.getIpAddress() + ":" + worker.getPortNumber() + "\n";
            }
            return ret;
        });

        get("/", (req, res) -> workerTable());
    }

    static class WorkerMetadata {
        String workerId;
        String ipAddress;
        String portNumber;
        long lastPing;

        public WorkerMetadata(String workerId, String ipAddress, String portNumber, long lastPing) {
            this.workerId = workerId;
            this.ipAddress = ipAddress;
            this.portNumber = portNumber;
            this.lastPing = lastPing;
        }

        public String getWorkerId() {
            return workerId;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public String getPortNumber() {
            return portNumber;
        }

        public long getLastPing() {
            return lastPing;
        }
    }
}
