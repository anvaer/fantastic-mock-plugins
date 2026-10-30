package com.fantasticmock.example.http.netty;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;

/**
 * Request data detached from Netty's reference-counted buffers, so it can outlive the event
 * loop callback. The body is copied once; headers are an owned copy.
 */
record CapturedRequest(
    String method,
    String uri,
    HttpVersion version,
    boolean keepAlive,
    HttpHeaders headers,
    byte[] body,
    InetSocketAddress local) {

  static CapturedRequest capture(FullHttpRequest request, InetSocketAddress local) {
    return new CapturedRequest(
        request.method().name(),
        request.uri(),
        request.protocolVersion(),
        HttpUtil.isKeepAlive(request),
        request.headers().copy(),
        ByteBufUtil.getBytes(request.content()),
        local);
  }

  /** The absolute request URL, for the {@code url} message field. */
  String absoluteTarget() {
    if (uri.startsWith("http://") || uri.startsWith("https://")) return uri;
    String host = headers.get(HttpHeaderNames.HOST);
    String authority = host == null || host.isBlank()
        ? local.getHostString() + ":" + local.getPort() : host;
    return "http://" + authority + (uri.startsWith("/") ? uri : "/" + uri);
  }
}
