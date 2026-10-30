package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import io.grpc.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * One unary or server-streaming call. The endpoint admits it, matches, delays and acknowledges;
 * this class only converts protobuf to message fields and writes the rendered status, metadata
 * and body. A unary call is answered by its first response, and acknowledgement follows gRPC's
 * server-side completion, not scheduling. A streaming call sends each response as one message
 * of the stream and ends with OK after the last; a response with another status ends it early.
 */
final class GrpcCall extends ServerCall.Listener<GrpcSchema.InboundMessage> {
  // Completing a write sends the next response, which must not run on the transport thread.
  private static final Executor VIRTUAL = Thread::startVirtualThread;

  /**
   * A reply of another server to this call, as it arrived: {@code document} is its response
   * document, by which {@link #write} knows it again. {@code headers} is null when the server
   * answered with trailers alone, {@code message} when it sent none.
   */
  record Relayed(JsonNode document, Status status, Metadata headers, DynamicMessage message, Metadata trailers,
      boolean gzip) {}

  /** The message of this call as the endpoint reads it, which also leads back to the call. */
  final class Message implements ProtocolMessageView {
    private final ProtocolMessageView fields;
    private final long payloadSize;

    private Message(ProtocolMessageView fields, long payloadSize) {
      this.fields = fields;
      this.payloadSize = payloadSize;
    }

    GrpcCall call() { return GrpcCall.this; }
    @Override public String protocol() { return fields.protocol(); }
    @Override public String endpoint() { return fields.endpoint(); }
    @Override public String messageType() { return fields.messageType(); }
    @Override public Optional<Object> value(String path) { return fields.value(path); }
    @Override public Optional<ByteBuffer> raw() { return fields.raw(); }
    @Override public Map<String, Object> logFields() { return fields.logFields(); }
    @Override public Long payloadSize() { return payloadSize; }
  }

  private final ServerCall<GrpcSchema.InboundMessage, DynamicMessage> call;
  private final Metadata headers;
  private final GrpcSchema.Method method;
  private final GrpcSchema schema;
  private final ProtocolEndpoint endpoint;
  private final Set<GrpcCall> active;
  private final CompletableFuture<Void> completed = new CompletableFuture<>();
  private DynamicMessage request;
  private long requestSize;
  private boolean closed;
  // Closed by a response of the template, so a later one is not lost without a word.
  private boolean answered;
  private CompletableFuture<ProtocolExchangeResult> exchange;
  private volatile Relayed relayed;
  // A stream: whether its headers went out, the trailers it ends with, and a write that waits for flow control.
  private boolean headersSent;
  private Metadata streamTrailers = new Metadata();
  private CompletableFuture<Long> ready;
  private long readySize;

  GrpcCall(ServerCall<GrpcSchema.InboundMessage, DynamicMessage> call, Metadata headers,
      GrpcSchema.Method method, GrpcSchema schema, ProtocolEndpoint endpoint, Set<GrpcCall> active) {
    this.call = call;
    this.headers = headers;
    this.method = method;
    this.schema = schema;
    this.endpoint = endpoint;
    this.active = active;
  }

  @Override public synchronized void onMessage(GrpcSchema.InboundMessage message) {
    if (request != null) end(Status.INVALID_ARGUMENT.withDescription("Unary call requires one request"));
    else { request = message.message(); requestSize = message.payloadSize(); }
  }

  @Override public void onHalfClose() {
    DynamicMessage body;
    synchronized (this) {
      if (closed) return;
      if (request == null) { end(Status.INVALID_ARGUMENT.withDescription("Missing request")); return; }
      body = request;
    }
    // Printing protobuf as JSON and copying metadata happen only if a template or log needs them.
    Supplier<Map<String, String>> header = () -> GrpcMetadata.fields(headers);
    Supplier<String> json = () -> print(body);
    var message = new Message(endpoint.message(Map.of(
        "method", method.wire().getFullMethodName(),
        "service", method.service(),
        "header", header,
        "body", json)), requestSize);
    // Listener callbacks run on the transport thread; the exchange runs on a virtual thread.
    CompletableFuture<ProtocolExchangeResult> started = endpoint.handleAsync(message, this::write);
    synchronized (this) {
      exchange = started;
      if (closed) started.cancel(false);
    }
    started.whenComplete((result, failure) -> {
      if (failure == null) {
        if (result.responses().isEmpty()) end(result.matched()
            ? Status.INTERNAL.withDescription("Template produced no response")
            : Status.NOT_FOUND.withDescription("No matching template"));
        else if (method.streaming()) finishStream(); // The last message was handed to the transport.
      } else if (!(failure instanceof CancellationException)) {
        end(failure instanceof RejectedExecutionException
            ? Status.RESOURCE_EXHAUSTED.withDescription("Protocol capacity reached")
            : Status.INTERNAL.withDescription("gRPC exchange failed: " + failure.getMessage()));
      }
    });
  }

  Metadata headers() { return headers; }

  GrpcSchema.Method method() { return method; }

  synchronized DynamicMessage request() { return request; }

  /** Keeps a reply of another server for the moment its document is written. */
  void relay(Relayed reply) { relayed = reply; }

  private synchronized CompletionStage<Long> write(JsonNode document) throws Exception {
    if (answered) return CompletableFuture.failedFuture(new IllegalStateException("The gRPC call to "
        + method.wire().getFullMethodName() + " has already ended; the template's later responses are not sent"
        + (method.streaming() ? "" : " (a unary method answers once)")));
    if (call.isCancelled()) return CompletableFuture.failedFuture(gone());
    if (closed) return CompletableFuture.failedFuture(Status.CANCELLED.asRuntimeException());
    Relayed reply = relayed;
    if (reply != null && reply.document() == document) {
      // The reply itself, not its document: every field of the protobuf, and the shape of the answer.
      if (reply.gzip() && reply.headers() != null) call.setCompression("gzip");
      if (reply.headers() != null) call.sendHeaders(reply.headers());
      if (reply.message() != null) call.sendMessage(reply.message());
      answered = true;
      end(reply.status(), reply.trailers());
      long relayedSize = reply.message() == null ? 0 : reply.message().getSerializedSize();
      return completed.thenApply(ignored -> relayedSize);
    }
    Status status = Status.fromCode(Status.Code.valueOf(document.path("status").asText("OK")));
    Metadata metadata = GrpcMetadata.from(document.path("headers"));
    Metadata trailers = GrpcMetadata.from(document.path("trailers"));
    DynamicMessage response = null;
    if (status.isOk()) {
      var builder = DynamicMessage.newBuilder(method.output());
      JsonNode body = document.path("body");
      schema.parser.merge(body.isTextual() ? body.asText() : body.isMissingNode() ? "{}" : body.toString(), builder);
      response = builder.build(); // Validate before writing headers.
    }
    if (method.streaming()) return stream(document, status, metadata, trailers, response);
    call.sendHeaders(metadata);
    if (response != null) call.sendMessage(response);
    answered = true;
    end(status.withDescription(document.path("description").asText("")), trailers);
    long payloadSize = response == null ? 0 : response.getSerializedSize();
    return completed.thenApply(ignored -> payloadSize);
  }

  /**
   * One response of a server stream: its message, or the end of the stream with its status when
   * that is not OK. The headers of the first response open the stream; the trailers of the last
   * close it. The write completes once the transport takes more, so a slow client holds back the
   * next response instead of letting messages pile up in memory.
   */
  private CompletionStage<Long> stream(JsonNode document, Status status, Metadata headers, Metadata trailers,
      DynamicMessage message) {
    if (!headersSent) {
      call.sendHeaders(headers);
      headersSent = true;
    }
    if (message == null) {
      answered = true;
      end(status.withDescription(document.path("description").asText("")), trailers);
      return completed.thenApply(ignored -> 0L);
    }
    call.sendMessage(message);
    streamTrailers = trailers;
    long payloadSize = message.getSerializedSize();
    if (call.isReady()) return CompletableFuture.completedFuture(payloadSize);
    ready = new CompletableFuture<>();
    readySize = payloadSize;
    return ready;
  }

  private synchronized void finishStream() {
    end(Status.OK, streamTrailers);
  }

  @Override public void onReady() {
    CompletableFuture<Long> waiting;
    long payloadSize;
    synchronized (this) {
      if (ready == null || !call.isReady()) return;
      waiting = ready;
      payloadSize = readySize;
      ready = null;
    }
    VIRTUAL.execute(() -> waiting.complete(payloadSize));
  }

  private String print(DynamicMessage message) {
    try {
      return schema.printer.print(message);
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalStateException("Cannot print gRPC request as JSON", failure);
    }
  }

  private void end(Status status) {
    end(status, new Metadata());
  }

  private synchronized void end(Status status, Metadata trailers) {
    if (!closed) { closed = true; call.close(status, trailers); }
  }

  @Override public void onComplete() {
    completed.complete(null);
    active.remove(this);
  }

  @Override public void onCancel() {
    CompletableFuture<ProtocolExchangeResult> pending;
    CompletableFuture<Long> waiting;
    synchronized (this) {
      closed = true; // The transport already closed it; preserve its error status.
      pending = exchange;
      waiting = ready;
      ready = null;
    }
    completed.completeExceptionally(Status.CANCELLED.asRuntimeException());
    // A deadline or disconnect drops delayed responses and releases admission at once.
    if (pending != null) pending.cancel(false);
    // A client that went away is not a failure of the mock: a cancellation is not counted as one.
    if (waiting != null) waiting.completeExceptionally(gone());
    active.remove(this);
  }

  void cancel() {
    end(Status.CANCELLED);
    onCancel();
  }

  private static CancellationException gone() {
    return new CancellationException("The client cancelled the gRPC call");
  }
}
