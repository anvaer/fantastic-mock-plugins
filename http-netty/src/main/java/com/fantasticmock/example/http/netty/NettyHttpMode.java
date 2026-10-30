package com.fantasticmock.example.http.netty;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;

/** How one captured request is answered: through the host HTTP engine or protocol templates. */
interface NettyHttpMode {

  String name();

  /** Whether requests are decompressed before the exchange. */
  boolean decompressesRequests();

  /**
   * Answers one request off the event loop. Completes once its response was written or could
   * not be; never leaves the request unanswered while the connection is open.
   */
  CompletableFuture<?> process(ChannelHandlerContext context, CapturedRequest request);
}
