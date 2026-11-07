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

  private static final java.util.function.BiConsumer<JsonNode, Long> UNREPORTED = (message, size) -> { };

  private final GrpcSchema schema;
  private final com.fasterxml.jackson.databind.ObjectMapper mapper;
  private final int maxMessageBytes;
  private final Semaphore permits;
  private final Set<CompletableFuture<Long>> active = ConcurrentHashMap.newKeySet();
  // Descriptor sets callbacks name instead of the instance's, by file.
  private final Map<java.nio.file.Path, Loaded> descriptorSets = new ConcurrentHashMap<>();
  private final Map<Target, ManagedChannel> channels = new LinkedHashMap<Target, ManagedChannel>(16, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Target, ManagedChannel> eldest) {
      if (size() <= MAX_CHANNELS) return false;
      eldest.getValue().shutdown(); // Calls already started on it complete normally.
      return true;
    }
  };
  private volatile boolean closed;

  GrpcCallbackClient(GrpcSchema schema, com.fasterxml.jackson.databind.ObjectMapper mapper, ProtocolSettings settings) {
    this.schema = schema;
    this.mapper = mapper;
    this.maxMessageBytes = settings.integer("max-message-bytes");
    // Separate from inbound admission, so callbacks to this same server cannot starve it.
    this.permits = new Semaphore(settings.integer("max-in-flight"));
  }

  CompletionStage<Long> send(JsonNode document) {
    return send(document, UNREPORTED, UNREPORTED);
  }

  /**
   * Calls the callback's method and reports its request to {@code sent} once it is written, as
   * the request fields of a received call (method, service, header, body), and the reply to
   * {@code received} when the call closes, as a template writes a response (status,
   * description, headers, trailers, body). Completes once the reply came.
   */
  CompletionStage<Long> send(JsonNode document, java.util.function.BiConsumer<JsonNode, Long> sent,
      java.util.function.BiConsumer<JsonNode, Long> received) {
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
      start(document, completion, sent == null ? UNREPORTED : sent, received == null ? UNREPORTED : received);
    } catch (Exception | Error failure) {
      completion.completeExceptionally(failure);
    }
    if (closed) completion.completeExceptionally(stopped());
    return completion;
  }

  private void start(JsonNode document, CompletableFuture<Long> completion,
      java.util.function.BiConsumer<JsonNode, Long> sent, java.util.function.BiConsumer<JsonNode, Long> received) {
    Target target = Target.parse(requiredText(document, "url"));
    String name = requiredText(document, "method");
    String descriptorFile = document.path("descriptorFile").asText("").trim();
    GrpcSchema schema = descriptorFile.isEmpty() ? this.schema : descriptors(descriptorFile);
    var method = schema.method(name.startsWith("/") ? name.substring(1) : name);
    if (method == null) throw new IllegalArgumentException("Unknown gRPC callback method: " + name + " ("
        + (descriptorFile.isEmpty() ? "not in the descriptor set of this instance; set descriptorFile to use another"
            : "not in " + descriptorFile) + ")");
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
      private Metadata replyHeaders;
      private DynamicMessage reply;
      @Override public void onHeaders(Metadata metadata) { replyHeaders = metadata; }
      @Override public void onMessage(DynamicMessage response) {
        responses++;
        if (reply == null) reply = response;
      }
      @Override public void onClose(Status status, Metadata trailers) {
        // Reported before the callback is over, so the host logs it with this callback.
        if (sent != UNREPORTED || received != UNREPORTED) {
          received.accept(replied(schema, status, replyHeaders, trailers, reply),
              reply == null ? 0L : reply.getSerializedSize());
        }
        if (!status.isOk()) completion.completeExceptionally(status.asRuntimeException(trailers));
        else if (responses != 1) completion.completeExceptionally(
            Status.INTERNAL.withDescription("Unary callback requires one response").asRuntimeException());
        else completion.complete(size);
      }
    }, headers);
    call.request(2);
    call.sendMessage(payload);
    call.halfClose();
    if (sent != UNREPORTED) sent.accept(asked(schema, method, headers, payload), size);
  }

  /**
   * The descriptor set a callback names, read once and again only when the file changes. A path
   * that is not absolute is read from the application's working directory, as the instance's
   * own descriptor-file is.
   */
  private GrpcSchema descriptors(String file) {
    java.nio.file.Path location = java.nio.file.Path.of(file).toAbsolutePath().normalize();
    try {
      var attributes = java.nio.file.Files.readAttributes(location, java.nio.file.attribute.BasicFileAttributes.class);
      Loaded known = descriptorSets.get(location);
      if (known != null && known.modified().equals(attributes.lastModifiedTime()) && known.size() == attributes.size()) {
        return known.schema();
      }
      GrpcSchema read = new GrpcSchema(location);
      descriptorSets.put(location, new Loaded(read, attributes.lastModifiedTime(), attributes.size()));
      return read;
    } catch (Exception unreadable) {
      throw new IllegalArgumentException("gRPC callback descriptorFile " + file + " cannot be read: "
          + (unreadable instanceof java.nio.file.NoSuchFileException ? "no such file" : unreadable.getMessage()),
          unreadable);
    }
  }

  private record Loaded(GrpcSchema schema, java.nio.file.attribute.FileTime modified, long size) {
  }

  // The request as a received call's fields are logged.
  private JsonNode asked(GrpcSchema schema, GrpcSchema.Method method, Metadata headers, DynamicMessage payload) {
    var document = mapper.createObjectNode()
        .put("method", method.wire().getFullMethodName())
        .put("service", method.service());
    Map<String, String> header = GrpcMetadata.fields(headers);
    if (!header.isEmpty()) header.forEach(document.putObject("header")::put);
    document.put("body", printed(schema, payload));
    return document;
  }

  // The reply as a template writes a response.
  private JsonNode replied(GrpcSchema schema, Status status, Metadata headers, Metadata trailers, DynamicMessage reply) {
    var document = mapper.createObjectNode().put("status", status.getCode().name());
    if (status.getDescription() != null && !status.getDescription().isEmpty()) {
      document.put("description", status.getDescription());
    }
    Map<String, String> sentHeaders = headers == null ? Map.of() : GrpcMetadata.fields(headers);
    if (!sentHeaders.isEmpty()) sentHeaders.forEach(document.putObject("headers")::put);
    Map<String, String> sentTrailers = trailers == null ? Map.of() : GrpcMetadata.fields(trailers);
    if (!sentTrailers.isEmpty()) sentTrailers.forEach(document.putObject("trailers")::put);
    if (reply != null) {
      try {
        document.set("body", mapper.readTree(printed(schema, reply)));
      } catch (Exception unreadable) {
        document.put("body", printed(schema, reply));
      }
    }
    return document;
  }

  private static String printed(GrpcSchema schema, DynamicMessage message) {
    try {
      return schema.printer.print(message);
    } catch (com.google.protobuf.InvalidProtocolBufferException unprintable) {
      return "Not shown: " + unprintable.getMessage();
    }
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
