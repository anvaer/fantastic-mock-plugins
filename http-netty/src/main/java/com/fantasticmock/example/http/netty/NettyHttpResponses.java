package com.fantasticmock.example.http.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DateFormatter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Writes one HTTP response with correct framing for both exchange modes. */
final class NettyHttpResponses {
  private static final byte[] EMPTY = new byte[0];
  private static volatile DateValue date = new DateValue(-1, "");

  private NettyHttpResponses() {
  }

  /**
   * Writes a response; the body is wrapped, not copied. Completes with the body bytes sent
   * after the write succeeded, and closes the connection afterwards when it is not reused.
   */
  static CompletableFuture<Long> write(ChannelHandlerContext context, CapturedRequest request,
      int status, Consumer<HttpHeaders> headers, byte[] body, boolean close) {
    byte[] payload = body == null ? EMPTY : body;
    boolean allowed = bodyAllowed(request.method(), status);
    var response = new DefaultFullHttpResponse(request.version(), HttpResponseStatus.valueOf(status),
        allowed && payload.length > 0 ? Unpooled.wrappedBuffer(payload) : Unpooled.EMPTY_BUFFER);
    HttpHeaders out = response.headers();
    headers.accept(out);
    if (!out.contains(HttpHeaderNames.DATE)) out.set(HttpHeaderNames.DATE, date());
    out.remove(HttpHeaderNames.TRANSFER_ENCODING);
    out.remove(HttpHeaderNames.CONTENT_LENGTH);
    boolean head = "HEAD".equals(request.method());
    if (allowed || head && status >= 200 && status != 204 && status != 205 && status != 304) {
      HttpUtil.setContentLength(response, payload.length);
    } else if (status == 205) {
      HttpUtil.setContentLength(response, 0);
    }
    boolean reuse = !close && request.keepAlive()
        && !"close".equalsIgnoreCase(out.get(HttpHeaderNames.CONNECTION));
    HttpUtil.setKeepAlive(response, reuse);
    long sent = allowed ? payload.length : 0;
    CompletableFuture<Long> done = new CompletableFuture<>();
    context.writeAndFlush(response).addListener(future -> {
      if (!reuse) context.close();
      if (future.isSuccess()) done.complete(sent);
      else done.completeExceptionally(future.cause());
    });
    return done;
  }

  /** A plain-text status response, such as a rejection or failure. */
  static CompletableFuture<Long> text(ChannelHandlerContext context, CapturedRequest request,
      int status, boolean close) {
    byte[] body = HttpResponseStatus.valueOf(status).toString().getBytes(StandardCharsets.UTF_8);
    return write(context, request, status,
        headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8"), body, close);
  }

  private static boolean bodyAllowed(String method, int status) {
    return !"HEAD".equals(method) && status >= 200 && status != 204 && status != 205 && status != 304;
  }

  private static String date() {
    long now = System.currentTimeMillis();
    DateValue current = date;
    if (current.second() != now / 1000) {
      date = current = new DateValue(now / 1000, DateFormatter.format(new Date(now)));
    }
    return current.text();
  }

  private record DateValue(long second, String text) {
  }
}
