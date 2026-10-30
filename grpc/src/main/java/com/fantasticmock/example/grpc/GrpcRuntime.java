package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Dedicated plaintext gRPC listener. No servlet/container or core changes. */
final class GrpcRuntime implements ProtocolPluginInstance, ProtocolOutboundSender, ProtocolUpstreamClient {
  private final ProtocolEndpoint endpoint;
  private final Set<GrpcCall> calls = ConcurrentHashMap.newKeySet();
  private Server server;
  private List<String> skipped = List.of();
  private volatile GrpcCallbackClient callbacks;
  private volatile GrpcUpstream upstream;
  private boolean closed;

  GrpcRuntime(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Override
  public synchronized void start() throws Exception {
    if (closed || server != null) throw new IllegalStateException("gRPC instance already started or closed");
    ProtocolSettings settings = endpoint.settings();
    var schema = new GrpcSchema(Path.of(settings.text("descriptor-file")));
    var services = new LinkedHashMap<String, ServerServiceDefinition.Builder>();
    for (var method : schema.methods) {
      services.computeIfAbsent(method.service(), ServerServiceDefinition::builder)
          .addMethod(method.serverWire(), (call, headers) -> {
            var handler = new GrpcCall(call, headers, method, schema, endpoint, calls);
            calls.add(handler);
            call.request(2); // Unary and server-streaming calls carry exactly one request message.
            return handler;
          });
    }
    skipped = List.copyOf(schema.skipped);
    var builder = NettyServerBuilder.forAddress(new InetSocketAddress(settings.text("host"), settings.integer("port")))
        .maxInboundMessageSize(settings.integer("max-message-bytes"))
        .maxConcurrentCallsPerConnection(settings.integer("max-in-flight"))
        .directExecutor(); // Listener callbacks only hand template work to the endpoint.
    services.values().forEach(service -> builder.addService(service.build()));
    server = builder.build();
    server.start();
    callbacks = new GrpcCallbackClient(schema, settings);
    upstream = new GrpcUpstream(schema, endpoint.objectMapper(), settings.integer("max-message-bytes"));
  }

  int port() { return server.getPort(); } // Ephemeral-port tests.

  @Override
  public CompletionStage<Long> send(JsonNode callback) {
    var client = callbacks;
    return client == null ? CompletableFuture.failedFuture(
        new IllegalStateException("gRPC instance is not started")) : client.send(callback);
  }

  @Override
  public CompletionStage<List<JsonNode>> forward(ProtocolMessageView request, Upstream target) {
    var client = upstream;
    return client == null ? CompletableFuture.failedFuture(
        new IllegalStateException("gRPC instance is not started")) : client.forward(request, target);
  }

  /** The metadata of a call and its message can be changed on its way; the method that is called cannot. */
  @Override
  public Set<String> rewritableFields() {
    return Set.of(GrpcUpstream.HEADER, GrpcUpstream.BODY);
  }

  @Override
  public void checkTarget(String target) {
    GrpcCallbackClient.Target.parse(target);
  }

  @Override
  public String targetHint() {
    return "grpc://host:port";
  }

  @Override
  public Map<String, Object> healthDetails() {
    Server current = server;
    var sockets = current == null ? List.<java.net.SocketAddress>of() : current.getListenSockets();
    return Map.of(
        "localAddress", sockets.isEmpty() ? "" : String.valueOf(sockets.getFirst()),
        "activeCalls", calls.size(),
        "notServed", skipped);
  }

  @Override
  public synchronized void close() throws InterruptedException {
    closed = true;
    if (callbacks != null) callbacks.close();
    if (upstream != null) upstream.close();
    calls.forEach(GrpcCall::cancel);
    if (server != null) {
      server.shutdownNow();
      // Release the port before a reloaded instance binds it again.
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
