package cis5550.kvs;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.staticFiles;

public class Coordinator extends cis5550.generic.Coordinator {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Incorrect number of arguments provided. Provide only port number as an argument.");
            System.exit(1);
        }
        Integer portNumber = 8000;
        try {
            portNumber = Integer.parseInt(args[0]);
            if (portNumber <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException nfe) {
            System.err.println("Port number argument must be a valid number");
            System.exit(1);
        }

        port(portNumber);
        // Enable static file serving from project root directory
        staticFiles.location(".");
        registerRoutes();
        get("/", (req, res) -> "<html><head><title>KVS Coordinator</title></head><body>" + workerTable() + "</body></html>");
    }
}
