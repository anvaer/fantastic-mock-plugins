package com.fantasticmock.example.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.TooLongHttpHeaderException;
import io.netty.handler.codec.http.TooLongHttpLineException;
import io.netty.handler.timeout.IdleStateEvent;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;

/**
 * One connection. HTTP/1.1 answers pipelined requests in order, so requests are processed one
 * at a time; while one is running, further reads are paused instead of queueing without bound.
 * All state is confined to the channel's event loop.
 */
final class NettyHttpConnection extends SimpleChannelInboundHandler<FullHttpRequest> {
  private final NettyHttpMode mode;
  private final ArrayDeque<Pending> pending = new ArrayDeque<>();
  private boolean busy;
  private boolean closing;

  NettyHttpConnection(NettyHttpMode mode) {
    this.mode = mode;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest incoming) {
    if (closing) return;
    if (!incoming.decoderResult().isSuccess()) {
      reject(context, incoming.protocolVersion(), rejectedStatus(incoming.decoderResult().cause()));
      return;
    }
    CapturedRequest request = CapturedRequest.capture(
        incoming, (InetSocketAddress) context.channel().localAddress());
    if (!request.keepAlive()) closing = true;
    enqueue(context, new Pending(request, 0));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable failure) {
    if (closing) context.close();
    else reject(context, HttpVersion.HTTP_1_1, 400);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
    if (event instanceof IdleStateEvent) {
      if (!busy && pending.isEmpty()) context.close();
    } else {
      super.userEventTriggered(context, event);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    pending.clear();
    super.channelInactive(context);
  }

  /** Answers after any earlier pipelined response, then closes the connection. */
  private void reject(ChannelHandlerContext context, HttpVersion version, int status) {
    closing = true;
    enqueue(context, new Pending(new CapturedRequest("GET", "/", version, false,
        io.netty.handler.codec.http.EmptyHttpHeaders.INSTANCE, new byte[0],
        (InetSocketAddress) context.channel().localAddress()), status));
  }

  private void enqueue(ChannelHandlerContext context, Pending request) {
    pending.add(request);
    if (busy) context.channel().config().setAutoRead(false);
    else next(context);
  }

  private void next(ChannelHandlerContext context) {
    Pending request = pending.poll();
    if (request == null) {
      busy = false;
      if (!closing) context.channel().config().setAutoRead(true);
      return;
    }
    busy = true;
    CompletableFuture<?> done;
    try {
      done = request.rejectStatus() != 0
          ? NettyHttpResponses.text(context, request.request(), request.rejectStatus(), true)
          : mode.process(context, request.request());
    } catch (RuntimeException failure) {
      done = NettyHttpResponses.text(context, request.request(), 500, true);
    }
    done.whenComplete((ignored, failure) -> context.executor().execute(() -> next(context)));
  }

  private static int rejectedStatus(Throwable cause) {
    if (cause instanceof TooLongHttpHeaderException) return 431;
    if (cause instanceof TooLongHttpLineException) return 414;
    return 400;
  }

  private record Pending(CapturedRequest request, int rejectStatus) {
  }
}
