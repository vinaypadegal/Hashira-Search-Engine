package cis5550.generic;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class Worker {
    public static void startPingThread(String coordinator, String workerId, int port) {
        Thread pingThread = new Thread(() -> {
            HttpClient client = HttpClient.newHttpClient();
            String url = "http://" + coordinator + "/ping?id=" + workerId + "&port=" + port;
            while (true) {
                try {
                    sendPing(url, client);
                    Thread.sleep(5000); // 5 seconds
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        pingThread.setDaemon(true);
        pingThread.start();
    }

    private static void sendPing(String url, HttpClient client) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
