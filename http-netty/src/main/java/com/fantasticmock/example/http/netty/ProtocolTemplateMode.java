package com.fantasticmock.example.http.netty;

import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.http.HttpMessages;
import com.fantasticmock.protocol.api.http.HttpResponseDocument;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * {@code exchange-mode: protocol}: matches {@code http-netty} templates through the generic
 * protocol pipeline. The endpoint admits, matches, delays and acknowledges; this class only
 * converts the request and writes the rendered response.
 */
final class ProtocolTemplateMode implements NettyHttpMode {
  private final ProtocolEndpoint endpoint;

  ProtocolTemplateMode(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Override
  public String name() {
    return "protocol";
  }

  @Override
  public boolean decompressesRequests() {
    return true;
  }

  @Override
  public CompletableFuture<?> process(ChannelHandlerContext context, CapturedRequest request) {
    byte[] body = request.body();
    Supplier<String> text = () -> new String(body, StandardCharsets.UTF_8);
    var message = HttpMessages.request(endpoint, request.method(), request.absoluteTarget(),
        new HeaderMap(request.headers()), text, body.length);
    var exchange = endpoint.handleAsync(message,
        response -> write(context, request, HttpResponseDocument.of(response)));
    // A client that disconnects during a template delay must not hold admission until it ends.
    ChannelFutureListener disconnected = closed -> exchange.cancel(false);
    context.channel().closeFuture().addListener(disconnected);
    exchange.whenComplete((ignored, failure) -> context.channel().closeFuture().removeListener(disconnected));
    return exchange
        .thenCompose(result -> answerUnanswered(context, request, result))
        .exceptionallyCompose(failure -> NettyHttpResponses.text(context, request,
            failure instanceof RejectedExecutionException ? 503 : 500, true));
  }

  private static CompletableFuture<Long> answerUnanswered(
      ChannelHandlerContext context, CapturedRequest request, ProtocolExchangeResult result) {
    return result.responses().isEmpty()
        ? write(context, request, HttpResponseDocument.unanswered(result))
        : CompletableFuture.completedFuture(null);
  }

  private static CompletableFuture<Long> write(
      ChannelHandlerContext context, CapturedRequest request, HttpResponseDocument response) {
    return NettyHttpResponses.write(context, request, response.status(),
        headers -> response.headers().forEach(headers::add), response.body(), false);
  }
}
