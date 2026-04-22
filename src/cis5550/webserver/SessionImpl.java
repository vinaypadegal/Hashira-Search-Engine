package cis5550.webserver;

import java.util.Map;

public class SessionImpl implements Session{
    private final String sessionId;
    private final long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval; // in seconds
    private boolean isValid;
    private final Map<String, Object> attributes;

    public SessionImpl(String sessionId, Map<String, Object> attributes) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.maxActiveInterval = 300; // default: 5 minutes
        this.isValid = true;
        this.attributes = attributes;
    }

    public SessionImpl(String sessionId){
        this(sessionId, new java.util.concurrent.ConcurrentHashMap<>());
    }

    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    public boolean isValid() {
        return isValid;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    public void updateLastAccessedTime() {
        this.lastAccessedTime = System.currentTimeMillis();
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds;
    }

    public int getMaxActiveInterval() {
        return this.maxActiveInterval;
    }

    @Override
    public void invalidate() {
        this.isValid = false;
        this.attributes.clear();
    }

    @Override
    public Object attribute(String name) {
        if (!isValid) {
            throw new IllegalStateException("Session is invalidated");
        }
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        if (!isValid) {
            throw new IllegalStateException("Session is invalidated");
        }
        attributes.put(name, value);
    }

}
