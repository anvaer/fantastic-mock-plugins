package com.fantasticmock.example.http.netty;

import com.fantasticmock.protocol.api.http.HttpRequestData;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import java.net.URI;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Host HTTP engine view of a captured request. Header lookups use Netty's case-insensitive
 * owned copy directly; nothing transport-owned escapes to workers.
 */
record FastHttpRequest(String getMethod, String getServerName, int getServerPort,
    String getRequestURI, String getQueryString, HttpHeaders headers) implements HttpRequestData {

  /** Throws {@link IllegalArgumentException} for an invalid Host header. */
  static FastHttpRequest of(CapturedRequest request) {
    String host = request.headers().get(HttpHeaderNames.HOST);
    URI authority = host == null ? null : URI.create("http://" + host);
    if (authority != null && (authority.getHost() == null || authority.getRawUserInfo() != null
        || !authority.getRawPath().isEmpty() || authority.getRawQuery() != null || authority.getRawFragment() != null
        || authority.getPort() > 65535)) throw new IllegalArgumentException("Invalid Host header");
    String target = request.uri();
    int question = target.indexOf('?');
    return new FastHttpRequest(request.method(),
        authority == null ? request.local().getHostString() : authority.getHost(),
        authority == null ? request.local().getPort() : (authority.getPort() < 0 ? 80 : authority.getPort()),
        question < 0 ? target : target.substring(0, question),
        question < 0 ? null : target.substring(question + 1), request.headers());
  }

  public String getScheme() { return "http"; }

  public String getRequestURL() {
    String host = getServerName.indexOf(':') >= 0 && !getServerName.startsWith("[")
        ? "[" + getServerName + "]" : getServerName;
    return "http://" + host + (getServerPort == 80 ? "" : ":" + getServerPort) + getRequestURI;
  }

  public String getContentType() { return headers.get(HttpHeaderNames.CONTENT_TYPE); }

  public String getHeader(String name) { return headers.get(name); }

  public Enumeration<String> getHeaderNames() { return Collections.enumeration(headers.names()); }

  public Enumeration<String> getHeaders(String name) { return Collections.enumeration(headers.getAll(name)); }
}
