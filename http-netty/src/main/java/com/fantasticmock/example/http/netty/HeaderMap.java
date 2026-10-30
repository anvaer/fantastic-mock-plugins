package com.fantasticmock.example.http.netty;

import io.netty.handler.codec.http.HttpHeaders;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only {@code request.header} view over Netty's case-insensitive headers. A lookup reads
 * one header; the full map is only built when a log needs every header.
 */
final class HeaderMap extends AbstractMap<String, String> {
  private final HttpHeaders headers;
  private Map<String, String> joined;

  HeaderMap(HttpHeaders headers) {
    this.headers = headers;
  }

  @Override
  public String get(Object key) {
    if (!(key instanceof String name)) return null;
    List<String> values = headers.getAll(name);
    if (values.isEmpty()) return null;
    return values.size() == 1 ? values.getFirst() : String.join(", ", values);
  }

  @Override
  public boolean containsKey(Object key) {
    return key instanceof String name && headers.contains(name);
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    if (joined == null) {
      Map<String, String> all = new LinkedHashMap<>();
      for (String name : headers.names()) all.put(name, get(name));
      joined = all;
    }
    return joined.entrySet();
  }
}
