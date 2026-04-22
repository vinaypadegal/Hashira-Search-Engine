package cis5550.webserver;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class ResponseImpl implements Response{

    private int statusCode;
    private String contentType;
    private byte[] body;
    private String reasonPhrase;
    private boolean isCommitted;
    private List<String[]> headers;
    private OutputStream outputStream;
    private byte[] bodyAsBytes;
    private String sessionId;

    private boolean halted = false;
    private boolean streamingMode = false;  // For streaming responses without Content-Length
    private boolean shouldCloseConnection = false;  // Track if connection should close after response

    public ResponseImpl(OutputStream o) {
        this.outputStream = o;
        this.isCommitted = false;
        this.headers = new ArrayList<>();
        this.bodyAsBytes = null;
        this.body = null;
        this.contentType = "text/html";
        this.statusCode = 200;
        this.reasonPhrase = "OK";
//        setSessionId(sessionId);
    }
    
    /**
     * Enable streaming mode for this response.
     * In streaming mode, Content-Length is NOT sent, and the client reads until Connection: close.
     * Call this before the first write() if you plan to stream multiple chunks.
     */
    @Override
    public void setStreamingMode(boolean streaming) {
        if (!isCommitted) {
            this.streamingMode = streaming;
        }
    }


    @Override
    public void body(String body) {
        if(!isCommitted)
        this.body = body.getBytes();
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if(!isCommitted)
        this.bodyAsBytes = bodyArg;
    }

    @Override
    public void header(String name, String value) {
        if(!isCommitted)
        this.headers.add(new String[]{name, value});
    }

    @Override
    public void type(String contentType) {
        if(!isCommitted)
        this.contentType = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if(!isCommitted) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    public void setSessionId(String sessionId) {
        setSessionId(sessionId, false);
    }

    public void setSessionId(String sessionId, boolean isSecure) {
        this.sessionId = sessionId;
        if (sessionId != null) {
            StringBuilder cookie = new StringBuilder();
            cookie.append("SessionID=").append(sessionId);
            cookie.append("; HttpOnly");
            cookie.append("; SameSite=Strict");
            cookie.append("; Path=/");
            if (isSecure) {
                cookie.append("; Secure");
            }
            // Use your header helper so the cookie is included in the headers list
            header("Set-Cookie", cookie.toString());
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        if(!isCommitted) {
            isCommitted = true;
            if (contentType == null) {
                contentType = "text/html";
            }
            if (statusCode == 0) {
                statusCode = 200;
                reasonPhrase = "OK";
            }
            StringBuilder responseHeader = new StringBuilder();
            responseHeader.append("HTTP/1.1 ").append(statusCode).append(" ").append(reasonPhrase).append("\r\n");
            responseHeader.append("Content-Type: ").append(contentType).append("\r\n");
            // Only set Content-Length for non-streaming responses
            // In streaming mode, client reads until Connection: close
            if (!streamingMode) {
                responseHeader.append("Content-Length: ").append(b.length).append("\r\n");
                // Keep-alive by default when Content-Length is known
                responseHeader.append("Connection: keep-alive\r\n");
            } else {
                // Streaming mode: client reads until connection closes
            responseHeader.append("Connection: close\r\n");
                shouldCloseConnection = true;
            }
            for (String[] header : headers) {
                responseHeader.append(header[0]).append(": ").append(header[1]).append("\r\n");
            }
            responseHeader.append("\r\n");
            outputStream.write(responseHeader.toString().getBytes());
        }
        outputStream.write(b);
        outputStream.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (!isCommitted) {
            statusCode = responseCode;
            reasonPhrase = getReasonPhraseForRedirect(responseCode);
            header("Location", url);
            try {
                write(new byte[0]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String getReasonPhraseForRedirect(int code) {
        return switch (code) {
            case 301 -> "Moved Permanently";
            case 302 -> "Found";
            case 303 -> "See Other";
            case 307 -> "Temporary Redirect";
            case 308 -> "Permanent Redirect";
            default -> "Redirect";
        };
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        this.halted = true;
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
        // Set a default body for halt
        this.body = (statusCode + " " + reasonPhrase).getBytes();
    }
    public boolean isHalted() {
        return halted;
    }

    //Getters
    public int getStatusCode() {
        return statusCode;
    }
    public String getContentType() {
        return contentType;
    }
    public byte[] getBody() {
        if(bodyAsBytes != null) {
            return bodyAsBytes;
        }
        return body;
    }
    public String getReasonPhrase() {
        return reasonPhrase;
    }
    public List<String[]> getHeaders() {
        return headers;
    }
    public boolean isCommitted() {
        return isCommitted;
    }
    
    /**
     * Check if connection should be closed after this response.
     * Only true for streaming mode responses.
     */
    public boolean shouldCloseConnection() {
        return shouldCloseConnection;
    }
}
