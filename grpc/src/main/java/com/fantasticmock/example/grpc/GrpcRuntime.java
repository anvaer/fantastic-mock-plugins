package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.netty.shaded.io.netty.util.concurrent.EventExecutor;
import io.grpc.netty.shaded.io.netty.util.concurrent.SingleThreadEventExecutor;
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
  // The server's own event loops, made as gRPC makes its shared defaults, so they can be read.
  private EventLoopGroup acceptors;
  private volatile EventLoopGroup workers;
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
    // As gRPC's own defaults: one thread accepts, twice the processors read and write (0), on epoll
    // where the platform has it.
    boolean epoll = Epoll.isAvailable();
    acceptors = loops(epoll, 1, "grpc-boss");
    workers = loops(epoll, 0, "grpc-worker");
    Class<? extends ServerChannel> channel = epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    var builder = NettyServerBuilder.forAddress(new InetSocketAddress(settings.text("host"), settings.integer("port")))
        .bossEventLoopGroup(acceptors)
        .workerEventLoopGroup(workers)
        .channelType(channel)
        .maxInboundMessageSize(settings.integer("max-message-bytes"))
        .maxConcurrentCallsPerConnection(settings.integer("max-in-flight"))
        .directExecutor(); // Listener callbacks only hand template work to the endpoint.
    services.values().forEach(service -> builder.addService(service.build()));
    server = builder.build();
    server.start();
    callbacks = new GrpcCallbackClient(schema, endpoint.objectMapper(), settings);
    upstream = new GrpcUpstream(schema, endpoint.objectMapper(), settings.integer("max-message-bytes"));
  }

  int port() { return server.getPort(); } // Ephemeral-port tests.

  private static EventLoopGroup loops(boolean epoll, int threads, String name) {
    var factory = new DefaultThreadFactory(name, true);
    return epoll ? new EpollEventLoopGroup(threads, factory) : new NioEventLoopGroup(threads, factory);
  }

  /**
   * The worker event loops, which read and write every call: busy are those with tasks queued
   * for them, such as the replies the virtual threads that answered hand back to be written, and
   * waiting is those tasks. Declared without {@code @Override}: hosts before 1.0.6 do not ask.
   */
  public Map<String, Integer> threads() {
    EventLoopGroup group = workers;
    if (group == null) return Map.of();
    int loops = 0;
    int busy = 0;
    int waiting = 0;
    for (EventExecutor loop : group) {
      loops++;
      int queued = loop instanceof SingleThreadEventExecutor single ? single.pendingTasks() : 0;
      if (queued > 0) busy++;
      waiting += queued;
    }
    return Map.of("busy", busy, "max", loops, "waiting", waiting);
  }

  @Override
  public CompletionStage<Long> send(JsonNode callback) {
    var client = callbacks;
    return client == null ? CompletableFuture.failedFuture(
        new IllegalStateException("gRPC instance is not started")) : client.send(callback);
  }

  /**
   * Sends a callback as {@link #send(JsonNode)} does, and reports its request to {@code sent}
   * and the reply to {@code received}, so that the traffic log shows both. Declared without
   * {@code @Override}: hosts before 1.0.6 call {@link #send(JsonNode)} only.
   */
  public CompletionStage<Long> send(JsonNode callback, java.util.function.BiConsumer<JsonNode, Long> sent,
      java.util.function.BiConsumer<JsonNode, Long> received) {
    var client = callbacks;
    return client == null ? CompletableFuture.failedFuture(
        new IllegalStateException("gRPC instance is not started")) : client.send(callback, sent, received);
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
    // gRPC leaves event loops it was given to their owner.
    EventLoopGroup workerGroup = workers;
    if (workerGroup != null) workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    if (acceptors != null) acceptors.shutdownGracefully(0, 5, TimeUnit.SECONDS);
  }
}
