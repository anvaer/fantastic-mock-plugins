package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.fantasticmock.protocol.api.plugin.ProtocolSettings;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

/**
 * Bounded unary callbacks. Channels are reused per target, as gRPC intends, instead of paying
 * a TCP, HTTP/2 and TLS handshake per callback; the least recently used beyond
 * {@value #MAX_CHANNELS} targets is shut down gracefully.
 */
final class GrpcCallbackClient implements AutoCloseable {
  private static final int MAX_CHANNELS = 64;

  private final GrpcSchema schema;
  private final int maxMessageBytes;
  private final Semaphore permits;
  private final Set<CompletableFuture<Long>> active = ConcurrentHashMap.newKeySet();
  private final Map<Target, ManagedChannel> channels = new LinkedHashMap<Target, ManagedChannel>(16, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Target, ManagedChannel> eldest) {
      if (size() <= MAX_CHANNELS) return false;
      eldest.getValue().shutdown(); // Calls already started on it complete normally.
      return true;
    }
  };
  private volatile boolean closed;

  GrpcCallbackClient(GrpcSchema schema, ProtocolSettings settings) {
    this.schema = schema;
    this.maxMessageBytes = settings.integer("max-message-bytes");
    // Separate from inbound admission, so callbacks to this same server cannot starve it.
    this.permits = new Semaphore(settings.integer("max-in-flight"));
  }

  CompletionStage<Long> send(JsonNode document) {
    if (closed) return CompletableFuture.failedFuture(new IllegalStateException("gRPC callback client is closed"));
    if (!permits.tryAcquire()) {
      return CompletableFuture.failedFuture(new RejectedExecutionException("gRPC callback max-in-flight reached"));
    }
    var completion = new CompletableFuture<Long>();
    active.add(completion);
    completion.whenComplete((ignored, failure) -> {
      active.remove(completion);
      permits.release();
    });
    try {
      start(document, completion);
    } catch (Exception | Error failure) {
      completion.completeExceptionally(failure);
    }
    if (closed) completion.completeExceptionally(stopped());
    return completion;
  }

  private void start(JsonNode document, CompletableFuture<Long> completion) {
    Target target = Target.parse(requiredText(document, "url"));
    String name = requiredText(document, "method");
    var method = schema.method(name.startsWith("/") ? name.substring(1) : name);
    if (method == null) throw new IllegalArgumentException("Unknown gRPC callback method: " + name);
    if (method.streaming()) throw new IllegalArgumentException("gRPC callbacks call unary methods; " + name + " is server-streaming");
    JsonNode timeout = document.path("timeoutMs");
    if (!timeout.isMissingNode() && (!timeout.isIntegralNumber() || !timeout.canConvertToLong() || timeout.longValue() <= 0))
      throw new IllegalArgumentException("gRPC callback timeoutMs must be a positive integer");
    long timeoutMs = timeout.isMissingNode() ? 5000 : timeout.longValue();
    var request = DynamicMessage.newBuilder(method.input());
    JsonNode body = document.path("body");
    try {
      schema.parser.merge(body.isTextual() ? body.asText() : body.isMissingNode() ? "{}" : body.toString(), request);
    } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("Invalid gRPC callback body: " + invalid.getMessage(), invalid);
    }
    DynamicMessage payload = request.build();
    long size = payload.getSerializedSize();
    if (size > maxMessageBytes) throw new IllegalArgumentException("gRPC callback exceeds max-message-bytes");
    Metadata headers = GrpcMetadata.from(document.path("headers"));
    var call = channel(target).newCall(method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS));
    completion.whenComplete((ignored, failure) -> {
      if (failure != null) call.cancel("gRPC callback cancelled", failure);
    });
    call.start(new ClientCall.Listener<>() {
      private int responses;
      @Override public void onMessage(DynamicMessage response) { responses++; }
      @Override public void onClose(Status status, Metadata trailers) {
        if (!status.isOk()) completion.completeExceptionally(status.asRuntimeException(trailers));
        else if (responses != 1) completion.completeExceptionally(
            Status.INTERNAL.withDescription("Unary callback requires one response").asRuntimeException());
        else completion.complete(size);
      }
    }, headers);
    call.request(2);
    call.sendMessage(payload);
    call.halfClose();
  }

  private ManagedChannel channel(Target target) {
    synchronized (channels) {
      if (closed) throw stopped();
      ManagedChannel channel = channels.get(target);
      if (channel == null || channel.isShutdown()) {
        var builder = NettyChannelBuilder.forAddress(target.host(), target.port())
            .maxInboundMessageSize(maxMessageBytes);
        if (target.tls()) builder.useTransportSecurity();
        else builder.usePlaintext();
        channel = builder.build();
        channels.put(target, channel);
      }
      return channel;
    }
  }

  private static String requiredText(JsonNode document, String field) {
    JsonNode value = document == null ? null : document.get(field);
    if (value == null || !value.isTextual() || value.asText().isBlank())
      throw new IllegalArgumentException("gRPC callback requires '" + field + "'");
    return value.asText();
  }

  private static StatusRuntimeException stopped() {
    return Status.CANCELLED.withDescription("gRPC instance stopped").asRuntimeException();
  }

  @Override public void close() {
    closed = true;
    for (var completion : List.copyOf(active)) completion.completeExceptionally(stopped());
    synchronized (channels) {
      channels.values().forEach(ManagedChannel::shutdownNow);
      channels.clear();
    }
  }

  /** A validated {@code grpc://host:port} or {@code grpcs://host:port} target, of a callback or of calls passed on. */
  record Target(String host, int port, boolean tls) {
    static Target parse(String url) {
      URI target;
      try {
        target = URI.create(url == null ? "" : url.trim());
      } catch (IllegalArgumentException invalid) {
        target = URI.create("");
      }
      boolean tls = "grpcs".equalsIgnoreCase(target.getScheme());
      if ((!tls && !"grpc".equalsIgnoreCase(target.getScheme())) || target.getHost() == null
          || target.getPort() < 1 || target.getPort() > 65535 || target.getRawUserInfo() != null
          || target.getRawQuery() != null || target.getRawFragment() != null
          || (target.getPath() != null && !target.getPath().isEmpty() && !target.getPath().equals("/")))
        throw new IllegalArgumentException("A gRPC url must be grpc://host:port or grpcs://host:port");
      return new Target(target.getHost(), target.getPort(), tls);
    }

    @Override public String toString() {
      return (tls ? "grpcs://" : "grpc://") + host + ":" + port;
    }
  }
}
