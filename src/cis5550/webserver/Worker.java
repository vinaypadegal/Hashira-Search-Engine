package cis5550.webserver;

import cis5550.tools.Logger;

import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class Worker extends Thread{
    private final String baseDir;
    private final BlockingQueue<Socket> sockQ;
    private final Logger logger = Logger.getLogger(Worker.class);
    Worker(String baseDir, BlockingQueue<Socket> sockQ) {
        this.baseDir = baseDir;
        this.sockQ = sockQ;
    }

    @Override
    public void run() {
        while (true) {
            try {
                    Socket sock = sockQ.take();
                    Server.handleClient(sock, baseDir);
            } catch (Exception e) {
                logger.error("Error in worker thread: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
