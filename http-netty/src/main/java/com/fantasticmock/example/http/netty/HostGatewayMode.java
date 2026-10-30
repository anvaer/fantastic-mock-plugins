package com.fantasticmock.example.http.netty;

import com.fantasticmock.protocol.api.http.HttpExchangeGateway;
import com.fantasticmock.protocol.api.http.HttpExchangeResponse;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@code exchange-mode: http}: serves the host's existing {@code http} templates through its
 * optimized HTTP engine, including proxy modes, callbacks, logs and metrics.
 */
final class HostGatewayMode implements NettyHttpMode {
  private final ProtocolEndpoint endpoint;
  private final HttpExchangeGateway gateway;

  HostGatewayMode(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
    this.gateway = endpoint.requireHostService(HttpExchangeGateway.class);
  }

  @Override
  public String name() {
    return "http";
  }

  @Override
  public boolean decompressesRequests() {
    return false;
  }

  @Override
  public CompletableFuture<?> process(ChannelHandlerContext context, CapturedRequest request) {
    FastHttpRequest data;
    try {
      data = FastHttpRequest.of(request);
    } catch (IllegalArgumentException invalid) {
      return NettyHttpResponses.text(context, request, 400, true);
    }
    // The gateway blocks (proxying, callbacks, delays), so it runs on a virtual thread.
    return endpoint.submitAsync(() -> write(context, request, exchange(data, request)))
        .exceptionallyCompose(failure -> NettyHttpResponses.text(context, request,
            failure instanceof RejectedExecutionException ? 503 : 500, true));
  }

  private HttpExchangeResponse exchange(FastHttpRequest data, CapturedRequest request) {
    try {
      return gateway.exchange(data, request.body());
    } catch (IllegalArgumentException invalid) {
      return error(400);
    } catch (Exception failure) {
      endpoint.statistics().increment(ProtocolStatistics.FAILURES);
      return error(500);
    }
  }

  private static CompletableFuture<Long> write(
      ChannelHandlerContext context, CapturedRequest request, HttpExchangeResponse response) {
    return NettyHttpResponses.write(context, request, response.status(), headers -> {
      response.forEachHeader((name, values) -> values.forEach(value -> headers.add(name, value)));
      if (response.body() != null && !headers.contains(HttpHeaderNames.CONTENT_TYPE)) {
        headers.set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
      }
    }, response.body(), false);
  }

  private static HttpExchangeResponse error(int status) {
    return HttpExchangeResponse.of(status,
        java.util.Map.of("Content-Type", java.util.List.of("text/plain; charset=utf-8")),
        ("HTTP " + status).getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
