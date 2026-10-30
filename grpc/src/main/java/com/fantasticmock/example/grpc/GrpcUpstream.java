package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamClient;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Passes a received call on to another server. The request goes out as it came (see
 * {@link GrpcWire}), but for what the upstream's rules change in its headers and its message, and the reply is reported twice: to the host as a response document, for
 * the traffic log, and to the call it answers as it arrived, so that the caller gets the
 * status, metadata, trailers and protobuf the other server sent, not a rendering of them.
 */
final class GrpcUpstream implements AutoCloseable {
  // Replies are decoded off the connection's event loop, like the exchange they belong to.
  private static final Executor VIRTUAL = Thread::startVirtualThread;

  private final GrpcSchema schema;
  private final ObjectMapper mapper;
  private final GrpcWire wire;

  GrpcUpstream(GrpcSchema schema, ObjectMapper mapper, int maxMessageBytes) {
    this.schema = schema;
    this.mapper = mapper;
    this.wire = new GrpcWire(maxMessageBytes);
  }

  /** The request fields an upstream's rules may change: the metadata of the call and its message. */
  static final String HEADER = "header";
  static final String BODY = "body";

  CompletionStage<List<JsonNode>> forward(ProtocolMessageView request, ProtocolUpstreamClient.Upstream upstream) {
    try {
      GrpcCallbackClient.Target target = GrpcCallbackClient.Target.parse(upstream.target());
      // A call of this instance carries what was received; a replayed request only its fields.
      GrpcCall call = request instanceof GrpcCall.Message received ? received.call() : null;
      String name = call != null ? call.method().wire().getFullMethodName() : request.text("request.method").orElse("");
      GrpcSchema.Method method = schema.method(name);
      if (method == null) throw new IllegalArgumentException("Unknown gRPC method: " + name);
      if (method.streaming()) throw new IllegalArgumentException("A server-streaming call is not passed on: " + name);
      List<GrpcWire.Header> headers;
      byte[] message;
      if (call != null) {
        headers = GrpcMetadata.wire(call.headers());
        message = call.request().toByteArray();
      } else {
        headers = GrpcMetadata.wire(request.value("request.header").orElse(null));
        Object body = request.value("request.body").orElse("{}");
        var parsed = DynamicMessage.newBuilder(method.input());
        schema.parser.merge(body instanceof String text ? text : mapper.writeValueAsString(body), parsed);
        message = parsed.build().toByteArray();
      }
      if (!upstream.changes().isEmpty()) {
        headers = changed(headers, upstream.changes());
        message = changed(message, method, upstream.changes());
      }
      CompletableFuture<GrpcWire.Reply> sent = wire.call(target, name, headers, message);
      CompletableFuture<List<JsonNode>> replies =
          sent.thenApplyAsync(reply -> List.of(report(reply, method, call, target)), VIRTUAL);
      // The host cancels what it returned here; the call itself is abandoned with it.
      replies.whenComplete((reported, failure) -> {
        if (replies.isCancelled()) sent.cancel(false);
      });
      return replies;
    } catch (Exception | Error failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  /**
   * The headers with the changes of an upstream: {@code header.<name>} with its new value, or
   * null to leave the header out. A changed header keeps its place and has the one new value;
   * a new one follows the others. Every other header stays as it came.
   */
  static List<GrpcWire.Header> changed(List<GrpcWire.Header> received, Map<String, String> changes) {
    List<GrpcWire.Header> headers = new ArrayList<>(received);
    for (Map.Entry<String, String> change : changes.entrySet()) {
      if (!change.getKey().startsWith(HEADER + ".")) continue;
      String name = change.getKey().substring(HEADER.length() + 1).toLowerCase(Locale.ROOT);
      int place = -1;
      for (int index = headers.size() - 1; index >= 0; index--) {
        if (headers.get(index).name().equals(name)) {
          headers.remove(index);
          place = index;
        }
      }
      if (change.getValue() != null) headers.add(place < 0 ? headers.size() : place, new GrpcWire.Header(name, change.getValue()));
    }
    return headers;
  }

  /**
   * The message with the changes of an upstream: {@code body} with the whole message as JSON, or
   * {@code body.<path>} with the value of one field inside it; null leaves that out. Without such
   * a change the message is the bytes that were received. With one it is written anew from its
   * JSON, and keeps the fields the descriptor set does not know at its top level.
   */
  byte[] changed(byte[] received, GrpcSchema.Method method, Map<String, String> changes) {
    boolean any = false;
    for (String field : changes.keySet()) {
      if (field.equals(BODY) || field.startsWith(BODY + ".")) {
        any = true;
        break;
      }
    }
    if (!any) return received;
    try {
      DynamicMessage original = DynamicMessage.parseFrom(method.input(), received);
      JsonNode document = mapper.readTree(schema.printer.print(original));
      for (Map.Entry<String, String> change : changes.entrySet()) {
        String field = change.getKey();
        if (field.equals(BODY)) {
          document = change.getValue() == null ? mapper.createObjectNode() : mapper.readTree(change.getValue());
        } else if (field.startsWith(BODY + ".")) {
          if (!(document instanceof ObjectNode root)) break;
          set(root, field.substring(BODY.length() + 1), change.getValue());
        }
      }
      if (!document.isObject()) throw new IllegalArgumentException("a message is a JSON object");
      DynamicMessage.Builder built = DynamicMessage.newBuilder(method.input());
      schema.parser.merge(mapper.writeValueAsString(document), built);
      built.mergeUnknownFields(original.getUnknownFields());
      return built.build().toByteArray();
    } catch (Exception invalid) {
      throw new IllegalArgumentException("The rules of the upstream do not leave a "
          + method.input().getFullName() + ": " + invalid.getMessage(), invalid);
    }
  }

  // Sets or removes one value by its path: names separated by dots, an element of a list as name[0].
  private void set(ObjectNode root, String path, String value) throws Exception {
    String[] steps = path.split("\\.");
    JsonNode parent = root;
    for (int index = 0; index < steps.length; index++) {
      String step = steps[index];
      int element = -1;
      int bracket = step.indexOf('[');
      if (bracket > 0 && step.endsWith("]")) {
        element = Integer.parseInt(step.substring(bracket + 1, step.length() - 1));
        step = step.substring(0, bracket);
      }
      boolean last = index == steps.length - 1;
      if (!(parent instanceof ObjectNode object)) return;
      if (element >= 0) {
        // An element is changed where the list has it; a list is not made longer by a rule.
        if (!(object.get(step) instanceof ArrayNode list) || element >= list.size()) return;
        if (!last) {
          parent = list.get(element);
        } else if (value == null) {
          list.remove(element);
        } else {
          list.set(element, node(value));
        }
      } else if (!last) {
        JsonNode next = object.get(step);
        if (next == null || next.isNull()) {
          if (value == null) return;
          next = object.putObject(step);
        }
        parent = next;
      } else if (value == null) {
        object.remove(step);
      } else {
        object.set(step, node(value));
      }
    }
  }

  // A value as rules give it is text; protobuf JSON reads numbers, booleans and enums from text.
  private JsonNode node(String value) throws Exception {
    String trimmed = value.trim();
    return trimmed.startsWith("{") || trimmed.startsWith("[") ? mapper.readTree(trimmed) : mapper.getNodeFactory().textNode(value);
  }

  /** The reply as a response document, and handed to the call it answers as it arrived. */
  private JsonNode report(GrpcWire.Reply reply, GrpcSchema.Method method, GrpcCall call, GrpcCallbackClient.Target target) {
    Status status = reply.status();
    DynamicMessage message = null;
    if (status.isOk()) {
      try {
        message = DynamicMessage.parseFrom(method.output(), reply.message());
      } catch (InvalidProtocolBufferException invalid) {
        status = Status.INTERNAL.withDescription(
            "The reply of " + target + " is not a " + method.output().getFullName() + ": " + invalid.getMessage());
      }
    }
    ObjectNode document = mapper.createObjectNode().put("status", status.getCode().name());
    if (status.getDescription() != null && !status.getDescription().isEmpty()) {
      document.put("description", status.getDescription());
    }
    ObjectNode sent = reply.headers() == null ? null : GrpcMetadata.document(reply.headers(), mapper.createObjectNode());
    if (sent != null) document.set("headers", sent);
    sent = GrpcMetadata.document(reply.trailers(), mapper.createObjectNode());
    if (sent != null) document.set("trailers", sent);
    if (message != null) {
      try {
        document.set("body", mapper.readTree(schema.printer.print(message)));
      } catch (Exception unprintable) {
        // The caller still gets the message; only the log cannot show it as JSON.
        document.put("body", "Not shown: " + unprintable.getMessage());
      }
    }
    if (call != null) {
      Metadata headers = reply.headers() == null ? null : GrpcMetadata.metadata(reply.headers());
      call.relay(new GrpcCall.Relayed(document, status, headers, message, GrpcMetadata.metadata(reply.trailers()),
          "gzip".equals(reply.encoding())));
    }
    return document;
  }

  @Override
  public void close() {
    wire.close();
  }
}
