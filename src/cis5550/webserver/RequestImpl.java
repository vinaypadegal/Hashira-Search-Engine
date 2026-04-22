package cis5550.webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;
  private SessionImpl session;
  private boolean newSessionCreated = false;


  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg, SessionImpl sessionArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
    session = sessionArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }

  @Override
  public Session session() {
    if(session != null) {
      int maxInterval = session.getMaxActiveInterval();
        if(maxInterval > 0 && (System.currentTimeMillis() - session.lastAccessedTime()) > maxInterval * 1000) {
            // session expired
            session.invalidate();
            session = null;
        }
    }

    if((session == null || !session.isValid()) && !newSessionCreated) {
      String newSessionId = server.generateSessionId(20);
      session = new SessionImpl(newSessionId);
      synchronized (Server.sessions) {
        server.sessions.put(newSessionId, session);
      }
      newSessionCreated = true;
    }
    session.updateLastAccessedTime();
    return session;
  }

  public boolean isNewSessionCreated() {
    return this.newSessionCreated;
  }


  public Map<String,String> params() {
    return params;
  }

}
