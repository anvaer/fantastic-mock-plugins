package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.*;
import com.fantasticmock.protocol.api.exchange.*;
import com.fantasticmock.protocol.api.metadata.ProtocolCapability;
import com.fantasticmock.protocol.api.plugin.*;
import com.fantasticmock.protocol.api.testing.ProtocolTestKit;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GrpcPluginTest {
  @Test void providesGrpcClientGuidance() {
    var hint = new GrpcProtocolPlugin().clientHint().orElseThrow();
    assertEquals("grpc", hint.scheme());
    assertEquals("grpcs", hint.secureScheme());
    assertFalse(hint.applicationListener());
    assertTrue(hint.message().contains(".proto or descriptor set"));
  }

  final ObjectMapper mapper = new ObjectMapper();
  final AtomicInteger acknowledged = new AtomicInteger();
  final AtomicReference<Long> responseSize = new AtomicReference<>();
  final CountDownLatch sent = new CountDownLatch(1);
  final GrpcProtocolPlugin plugin = new GrpcProtocolPlugin();
  ProtocolTestKit kit;
  ProtocolEndpoint endpoint;
  GrpcRuntime runtime;
  ManagedChannel channel;
  GrpcSchema.Method method;
  DynamicMessage request;
  @TempDir Path temp;

  @AfterEach void close() throws Exception {
    if (channel != null) channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    if (runtime != null) runtime.close();
    if (kit != null) kit.close();
  }
  void start(ProtocolExchangeGateway gateway, Map<String, Object> overrides) throws Exception {
    Path descriptors = Path.of("examples/hello.protoset").toAbsolutePath();
    var options = new HashMap<String, Object>(Map.of("host", "127.0.0.1", "port", 0,
        "descriptor-file", descriptors.toString()));
    options.putAll(overrides);
    kit = ProtocolTestKit.of(plugin).exchanges(gateway);
    endpoint = kit.endpoint("grpc-demo", options);
    runtime = (GrpcRuntime) plugin.createInstance(endpoint);
    ProtocolValidation.instance(plugin.definition(), runtime);
    runtime.start();
    channel = NettyChannelBuilder.forAddress("127.0.0.1", runtime.port()).usePlaintext().build();
    method = new GrpcSchema(descriptors).methods.getFirst();
    var input = method.output().getFile().findMessageTypeByName("HelloRequest");
    request = DynamicMessage.newBuilder(input).setField(input.findFieldByName("name"), "Ada").build();
  }
  ProtocolExchangeResult result(String body, long delay) throws Exception {
    return ProtocolExchangeResult.matched("grpc-test", List.of(new ProtocolResponse(
        mapper.readTree(body), delay, bytes -> { responseSize.set(bytes); acknowledged.incrementAndGet(); sent.countDown(); })));
  }
  DynamicMessage invoke(Channel target) {
    return ClientCalls.blockingUnaryCall(target, method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS), request);
  }
  void assertStatus(Status.Code code, Runnable call) {
    assertEquals(code, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
  }

  @Test void reportsItsOwnWorkerEventLoops() throws Exception {
    var response = result("""
        {"body":{"message":"Hello Ada"}}
        """, 0);
    start(message -> response, Map.of());
    invoke(channel);
    Map<String, Integer> threads = runtime.threads();
    assertEquals(Set.of("busy", "max", "waiting"), threads.keySet());
    // As many as gRPC's shared default would have: twice the processors, unless set otherwise.
    assertTrue(threads.get("max") >= 1);
    assertTrue(threads.get("busy") <= threads.get("max"));
    runtime.close();
    runtime = null;
  }

  @Test void unaryProtobufMetadataAndDelayedAcknowledgement() throws Exception {
    var response = result("""
        {"status":"OK","headers":{"x-reply":"mock"},"body":{"message":"Hello Ada"}}
        """, 100);
    start(message -> {
      assertTrue(Thread.currentThread().isVirtual());
      assertEquals((long) request.getSerializedSize(), message.payloadSize());
      assertEquals("demo.Greeter/SayHello", message.text("request.method").orElseThrow());
      assertEquals("Ada", message.text("request.body.name").orElseThrow());
      assertEquals("test", message.text("request.header.x-client").orElseThrow());
      return response;
    }, Map.of());
    var headers = new Metadata();
    headers.put(Metadata.Key.of("x-client", Metadata.ASCII_STRING_MARSHALLER), "test");
    var received = new AtomicReference<Metadata>();
    Channel client = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers),
        MetadataUtils.newCaptureMetadataInterceptor(received, new AtomicReference<>()));
    long started = System.nanoTime();
    DynamicMessage reply = invoke(client);
    assertEquals("Hello Ada", reply.getField(method.output().findFieldByName("message")));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 90);
    assertEquals("mock", received.get().get(Metadata.Key.of("x-reply", Metadata.ASCII_STRING_MARSHALLER)));
    assertTrue(sent.await(3, TimeUnit.SECONDS));
    assertEquals(1, acknowledged.get());
    assertEquals((long) reply.getSerializedSize(), responseSize.get());
    assertEquals(1, kit.statistics("grpc-demo").value(ProtocolStatistics.MESSAGES));
  }
  @Test void requestBodyIsOnlyPrintedWhenATemplateReadsIt() throws Exception {
    var response = result("{\"body\":{\"message\":\"routed\"}}", 0);
    start(message -> {
      assertEquals("demo.Greeter/SayHello", message.text("request.method").orElseThrow());
      return response;
    }, Map.of());
    assertEquals("routed", invoke(channel).getField(method.output().findFieldByName("message")));
  }
  @Test void inboundSizePreservesDuplicateProtobufFieldsBeforeDecoding() throws Exception {
    var schema = new GrpcSchema(Path.of("examples/hello.protoset"));
    var marshaller = schema.methods.getFirst().serverWire().getRequestMarshaller();
    byte[] encoded = new byte[] { 10, 1, 65, 10, 1, 66 };
    var inbound = marshaller.parse(new java.io.ByteArrayInputStream(encoded));
    assertEquals(6, inbound.payloadSize());
    assertEquals(3, inbound.message().getSerializedSize());
  }

  @Test void unmatchedRequestReturnsNotFound() throws Exception {
    start(message -> ProtocolExchangeResult.noMatch(), Map.of());
    assertStatus(Status.Code.NOT_FOUND, () -> invoke(channel));
    assertEquals(0, acknowledged.get());
    assertEquals(1, kit.statistics("grpc-demo").value(ProtocolStatistics.UNMATCHED));
  }
  @Test void templateCanReturnGrpcErrorStatus() throws Exception {
    var response = result("{\"status\":\"PERMISSION_DENIED\",\"description\":\"Mock denial\"}", 0);
    start(message -> response, Map.of());
    var error = assertThrows(StatusRuntimeException.class, () -> invoke(channel));
    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    assertEquals("Mock denial", error.getStatus().getDescription());
    assertTrue(sent.await(3, TimeUnit.SECONDS));
  }
  @Test void responseMetadataArraysAreRepeatedValues() throws Exception {
    var response = result("{\"headers\":{\"X-Many\":[\"a\",\"b\"]},\"body\":{\"message\":\"ok\"}}", 0);
    start(message -> response, Map.of());
    var received = new AtomicReference<Metadata>();
    invoke(ClientInterceptors.intercept(channel,
        MetadataUtils.newCaptureMetadataInterceptor(received, new AtomicReference<>())));
    var values = new ArrayList<String>();
    received.get().getAll(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER)).forEach(values::add);
    assertEquals(List.of("a", "b"), values);
  }
  @Test void aTemplateCopiedFromARecordedReplySendsNoHeaderTheLibraryWritesItself() throws Exception {
    var response = result("""
        {"headers":{"content-type":"application/grpc","grpc-encoding":"gzip","grpc-accept-encoding":"zstd","x-reply":"copied"},
         "trailers":{"content-type":"application/grpc","x-trail":"end"},"body":{"message":"ok"}}
        """, 0);
    start(message -> response, Map.of());
    var headers = new AtomicReference<Metadata>();
    var trailers = new AtomicReference<Metadata>();
    invoke(ClientInterceptors.intercept(channel, MetadataUtils.newCaptureMetadataInterceptor(headers, trailers)));
    var sent = GrpcMetadata.wire(headers.get()).stream().map(header -> header.name() + ": " + header.value()).sorted().toList();
    assertEquals(List.of("content-type: application/grpc", "grpc-accept-encoding: gzip", "grpc-encoding: identity",
        "x-reply: copied"), sent);
    assertEquals(List.of(new GrpcWire.Header("x-trail", "end")), GrpcMetadata.wire(trailers.get()));
  }
  @Test void invalidResponseIsNotAcknowledged() throws Exception {
    var response = result("{\"body\":{\"nonexistentField\":true}}", 0);
    start(message -> response, Map.of());
    assertStatus(Status.Code.INTERNAL, () -> invoke(channel));
    assertEquals(0, acknowledged.get());
  }
  @Test void admissionIncludesPendingExchangeAndRejectsOverload() throws Exception {
    var entered = new CountDownLatch(1);
    var pending = new CompletableFuture<ProtocolExchangeResult>();
    start(message -> { entered.countDown(); return pending.join(); }, Map.of("max-in-flight", 1));
    var first = ClientCalls.futureUnaryCall(channel.newCall(method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS)), request);
    assertTrue(entered.await(3, TimeUnit.SECONDS));
    // Separate connection avoids HTTP/2's per-connection admission queuing.
    var second = NettyChannelBuilder.forAddress("127.0.0.1", runtime.port()).usePlaintext().build();
    try { assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> invoke(second)); }
    finally { second.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    pending.complete(result("{\"body\":{\"message\":\"ok\"}}", 0));
    assertNotNull(first.get(3, TimeUnit.SECONDS));
    assertEquals(1, kit.statistics("grpc-demo").value(ProtocolStatistics.REJECTED));
  }
  @Test void deadlineCancelsDelayedResponseWithoutLoggingIt() throws Exception {
    var response = result("{\"body\":{\"message\":\"late\"}}", 60000);
    start(message -> response, Map.of());
    assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> ClientCalls.blockingUnaryCall(
        channel, method.wire(), CallOptions.DEFAULT.withDeadlineAfter(300, TimeUnit.MILLISECONDS), request));
    for (int i = 0; i < 100 && endpoint.inFlight() != 0; i++) Thread.sleep(10);
    assertEquals(0, endpoint.inFlight());
    assertEquals(0, acknowledged.get());
    assertEquals(0, kit.statistics("grpc-demo").value(ProtocolStatistics.FAILURES));
  }
  @Test void enforcesInboundSizeLimit() throws Exception {
    start(message -> { fail("Oversized request reached matcher"); return null; }, Map.of("max-message-bytes", 1));
    assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> invoke(channel));
  }
  @Test void skipsClientStreamingMethodsAndRejectsMissingImportedDescriptors() throws Exception {
    var set = DescriptorProtos.FileDescriptorSet.parseFrom(Files.readAllBytes(Path.of("examples/hello.protoset")));
    Path file = temp.resolve("streaming.protoset");
    Files.write(file, descriptors(DescriptorProtos.MethodDescriptorProto.Builder::setClientStreaming));
    assertTrue(assertThrows(IllegalArgumentException.class, () -> new GrpcSchema(file)).getMessage()
        .contains("not served: [demo.Greeter/SayHello]"));
    var mixed = set.toBuilder();
    var service = mixed.getFileBuilder(0).getServiceBuilder(0);
    service.addMethod(service.getMethod(0).toBuilder().setName("Chat").setClientStreaming(true).setServerStreaming(true));
    service.addMethod(service.getMethod(0).toBuilder().setName("Watch").setServerStreaming(true));
    Files.write(file, mixed.build().toByteArray());
    var schema = new GrpcSchema(file);
    assertEquals(List.of("demo.Greeter/SayHello", "demo.Greeter/Watch"),
        schema.methods.stream().map(served -> served.wire().getFullMethodName()).toList());
    assertTrue(schema.method("demo.Greeter/Watch").streaming());
    assertFalse(schema.method("demo.Greeter/SayHello").streaming());
    assertEquals(List.of("demo.Greeter/Chat"), schema.skipped);
    var missing = set.toBuilder();
    missing.getFileBuilder(0).addDependency("missing.proto");
    Files.write(file, missing.build().toByteArray());
    assertTrue(assertThrows(IllegalArgumentException.class, () -> new GrpcSchema(file)).getMessage().contains("--include_imports"));
  }

  // The example descriptor set with SayHello changed, such as made server-streaming.
  static byte[] descriptors(java.util.function.BiFunction<DescriptorProtos.MethodDescriptorProto.Builder, Boolean, ?> change)
      throws Exception {
    var set = DescriptorProtos.FileDescriptorSet.parseFrom(Files.readAllBytes(Path.of("examples/hello.protoset"))).toBuilder();
    change.apply(set.getFileBuilder(0).getServiceBuilder(0).getMethodBuilder(0), true);
    return set.build().toByteArray();
  }

  // Starts the instance with SayHello as a server-streaming method.
  void startStreaming(ProtocolExchangeGateway gateway) throws Exception {
    Path file = temp.resolve("watch.protoset");
    Files.write(file, descriptors(DescriptorProtos.MethodDescriptorProto.Builder::setServerStreaming));
    start(gateway, Map.of("descriptor-file", file.toString()));
    method = new GrpcSchema(file).methods.getFirst();
    assertTrue(method.streaming());
  }

  ProtocolExchangeResult sequence(long delay, String... bodies) throws Exception {
    var responses = new ArrayList<ProtocolResponse>();
    for (String body : bodies) responses.add(new ProtocolResponse(mapper.readTree(body), delay, bytes -> acknowledged.incrementAndGet()));
    return ProtocolExchangeResult.matched("grpc-test", responses);
  }

  List<String> messages(Iterator<DynamicMessage> replies) {
    var received = new ArrayList<String>();
    replies.forEachRemaining(reply -> received.add((String) reply.getField(method.output().findFieldByName("message"))));
    return received;
  }

  @Test void aStreamSendsEachResponseAsAMessageAndEndsWithTheTrailersOfTheLast() throws Exception {
    var responses = sequence(20, "{\"headers\":{\"x-first\":\"1\"},\"body\":{\"message\":\"a\"}}",
        "{\"body\":{\"message\":\"b\"}}", "{\"trailers\":{\"x-last\":\"3\"},\"body\":{\"message\":\"c\"}}");
    startStreaming(message -> responses);
    var headers = new AtomicReference<Metadata>();
    var trailers = new AtomicReference<Metadata>();
    Channel client = ClientInterceptors.intercept(channel, MetadataUtils.newCaptureMetadataInterceptor(headers, trailers));
    var replies = ClientCalls.blockingServerStreamingCall(client, method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS), request);
    assertEquals(List.of("a", "b", "c"), messages(replies));
    assertEquals("1", headers.get().get(Metadata.Key.of("x-first", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals("3", trailers.get().get(Metadata.Key.of("x-last", Metadata.ASCII_STRING_MARSHALLER)));
    for (int i = 0; i < 100 && acknowledged.get() < 3; i++) Thread.sleep(10);
    assertEquals(3, acknowledged.get());
    assertEquals(0, kit.statistics("grpc-demo").value(ProtocolStatistics.FAILURES));
  }

  @Test void aResponseWithAnotherStatusEndsTheStream() throws Exception {
    var responses = sequence(0, "{\"body\":{\"message\":\"a\"}}",
        "{\"status\":\"UNAVAILABLE\",\"description\":\"Feed down\"}");
    startStreaming(message -> responses);
    var replies = ClientCalls.blockingServerStreamingCall(channel, method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS), request);
    assertEquals("a", replies.next().getField(method.output().findFieldByName("message")));
    var error = assertThrows(StatusRuntimeException.class, replies::hasNext);
    assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
    assertEquals("Feed down", error.getStatus().getDescription());
  }

  @Test void cancellingAStreamDropsItsPendingResponses() throws Exception {
    var responses = sequence(200, "{\"body\":{\"message\":\"a\"}}", "{\"body\":{\"message\":\"b\"}}",
        "{\"body\":{\"message\":\"c\"}}");
    startStreaming(message -> responses);
    var context = Context.current().withCancellation();
    var replies = context.call(() -> ClientCalls.blockingServerStreamingCall(channel, method.wire(),
        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS), request));
    assertEquals("a", context.call(() -> replies.next()).getField(method.output().findFieldByName("message")));
    context.cancel(null);
    for (int i = 0; i < 100 && endpoint.inFlight() != 0; i++) Thread.sleep(10);
    assertEquals(0, endpoint.inFlight());
    Thread.sleep(500);
    assertEquals(1, acknowledged.get());
    // The client went away: that is not a failure of the mock.
    assertEquals(0, kit.statistics("grpc-demo").value(ProtocolStatistics.FAILURES));
  }

  @Test void aUnaryMethodAnswersWithTheFirstResponseOnly() throws Exception {
    var responses = sequence(0, "{\"body\":{\"message\":\"first\"}}", "{\"body\":{\"message\":\"second\"}}");
    start(message -> responses, Map.of());
    assertEquals("first", invoke(channel).getField(method.output().findFieldByName("message")));
    for (int i = 0; i < 100 && kit.statistics("grpc-demo").value(ProtocolStatistics.FAILURES) == 0; i++) Thread.sleep(10);
    assertEquals(1, acknowledged.get());
    assertEquals(1, kit.statistics("grpc-demo").value(ProtocolStatistics.FAILURES));
  }

  @Test void streamingMethodsAreNeitherCalledBackNorPassedOn() throws Exception {
    startStreaming(message -> ProtocolExchangeResult.noMatch());
    var stream = callback().put("method", "demo.Greeter/SayHello");
    var failure = assertThrows(ExecutionException.class,
        () -> runtime.send(stream).toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("server-streaming"), failure.getCause().getMessage());
  }

  com.fasterxml.jackson.databind.node.ObjectNode callback() {
    return mapper.createObjectNode().put("url", "grpc://127.0.0.1:" + runtime.port())
        .put("method", "demo.Greeter/SayHello").set("body", mapper.createObjectNode().put("name", "Ada"));
  }

  @Test void callbackSendsProtobufAndMetadataAndReportsEncodedSize() throws Exception {
    var response = result("{\"body\":{\"message\":\"received\"}}", 0);
    var received = new CountDownLatch(2);
    start(message -> {
      assertEquals("Ada", message.text("request.body.name").orElseThrow());
      assertEquals("second", message.text("request.header.x-client").orElseThrow());
      assertEquals("AQID", message.text("request.header.x-data-bin").orElseThrow());
      received.countDown();
      return response;
    }, Map.of());
    var document = callback();
    document.putObject("headers").put("x-data-bin", "AQID").putArray("x-client").add("first").add("second");
    assertEquals((long) request.getSerializedSize(), runtime.send(document).toCompletableFuture().get(3, TimeUnit.SECONDS));
    // Textual protobuf JSON also works; the second call reuses the cached channel.
    document.put("method", "/demo.Greeter/SayHello").put("body", "{\"name\":\"Ada\"}");
    runtime.send(document).toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(received.await(3, TimeUnit.SECONDS));
  }

  @Test void callbackPropagatesRemoteError() throws Exception {
    var response = result("{\"status\":\"PERMISSION_DENIED\",\"description\":\"denied\"}", 0);
    start(message -> response, Map.of());
    var failure = assertThrows(ExecutionException.class,
        () -> runtime.send(callback()).toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(Status.Code.PERMISSION_DENIED, Status.fromThrowable(failure.getCause()).getCode());
    assertEquals("denied", Status.fromThrowable(failure.getCause()).getDescription());
  }

  @Test void callbackDeadlineAndShutdownReleasePendingCalls() throws Exception {
    var entered = new LinkedBlockingQueue<Boolean>();
    var response = result("{\"body\":{\"message\":\"late\"}}", 60000);
    start(message -> { entered.add(true); return response; }, Map.of());
    var failure = assertThrows(ExecutionException.class, () -> runtime.send(callback().put("timeoutMs", 300))
        .toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(failure.getCause()).getCode());
    assertNotNull(entered.poll(3, TimeUnit.SECONDS));
    var pending = runtime.send(callback().put("timeoutMs", 60000)).toCompletableFuture();
    assertNotNull(entered.poll(3, TimeUnit.SECONDS));
    var afterStop = callback();
    runtime.close();
    var stopped = assertThrows(ExecutionException.class, () -> pending.get(3, TimeUnit.SECONDS));
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(stopped.getCause()).getCode());
    assertThrows(ExecutionException.class, () -> runtime.send(afterStop).toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(0, acknowledged.get());
  }

  @Test void callbackAdmissionIsBoundedAndIndependentOfInboundAdmission() throws Exception {
    var entered = new CountDownLatch(1);
    var pending = new CompletableFuture<ProtocolExchangeResult>();
    start(message -> { entered.countDown(); return pending.join(); }, Map.of("max-in-flight", 1));
    var first = runtime.send(callback()).toCompletableFuture();
    assertTrue(entered.await(3, TimeUnit.SECONDS));
    var failure = assertThrows(ExecutionException.class,
        () -> runtime.send(callback()).toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertInstanceOf(RejectedExecutionException.class, failure.getCause());
    pending.complete(result("{\"body\":{\"message\":\"ok\"}}", 0));
    assertEquals((long) request.getSerializedSize(), first.get(3, TimeUnit.SECONDS));
  }

  @Test void invalidCallbackDocumentsFailBeforeReachingTheNetwork() throws Exception {
    start(message -> { fail("Invalid callback reached server"); return null; }, Map.of("max-message-bytes", 16));
    var invalid = List.of(
        callback().put("url", "http://localhost:9091"),
        callback().put("url", "grpc://localhost"),
        callback().put("url", "grpc://localhost:9091/path"),
        callback().put("method", "unknown/Method"),
        callback().put("timeoutMs", 0),
        callback().put("timeoutMs", "100"),
        callback().set("body", mapper.readTree("{\"unknownField\":true}")),
        callback().set("body", mapper.createObjectNode().put("name", "x".repeat(17))),
        callback().set("headers", mapper.readTree("{\"x-data-bin\":\"!invalid!\"}")));
    for (var document : invalid) assertThrows(ExecutionException.class,
        () -> runtime.send(document).toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertThrows(ExecutionException.class,
        () -> runtime.send(null).toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  /** This server as its own upstream: the first exchange passes the request on, the second answers it. */
  ProtocolExchangeGateway forwarding(ProtocolExchangeResult upstream, AtomicReference<Throwable> failure,
      AtomicReference<JsonNode> reply) {
    var exchanges = new AtomicInteger();
    return message -> {
      try {
        if (exchanges.incrementAndGet() > 1) {
          assertEquals("Ada", message.text("request.body.name").orElseThrow());
          assertEquals("test", message.text("request.header.x-client").orElseThrow());
          assertEquals("AQID", message.text("request.header.x-data-bin").orElseThrow());
          return upstream;
        }
        var replies = runtime.forward(message, to("grpc://127.0.0.1:" + runtime.port()))
            .toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertEquals(1, replies.size());
        reply.set(replies.getFirst());
        return ProtocolExchangeResult.answered("forwarded", List.of(new ProtocolResponse(replies.getFirst(), 0)));
      } catch (Throwable error) {
        failure.compareAndSet(null, error);
        throw new IllegalStateException(error);
      }
    };
  }
  /** Every request goes to the server on {@code port}; what the call it arrived with was, and the reply, are kept. */
  ProtocolExchangeGateway passingOn(int port, AtomicReference<Throwable> failure, AtomicReference<JsonNode> reply,
      AtomicReference<Metadata> arrived) {
    return message -> {
      try {
        arrived.set(((GrpcCall.Message) message).call().headers());
        var replies = runtime.forward(message, to("grpc://127.0.0.1:" + port)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        reply.set(replies.getFirst());
        return ProtocolExchangeResult.answered("forwarded", List.of(new ProtocolResponse(replies.getFirst(), 0)));
      } catch (Throwable error) {
        failure.compareAndSet(null, error);
        throw new IllegalStateException(error);
      }
    };
  }
  Metadata clientMetadata() {
    var headers = new Metadata();
    headers.put(Metadata.Key.of("x-client", Metadata.ASCII_STRING_MARSHALLER), "test");
    headers.put(Metadata.Key.of("x-data-bin", Metadata.BINARY_BYTE_MARSHALLER), new byte[] { 1, 2, 3 });
    return headers;
  }
  ProtocolMessageView greeting() {
    return endpoint.message(Map.of("method", "demo.Greeter/SayHello", "service", "demo.Greeter",
        "header", Map.of(), "body", "{\"name\":\"Ada\"}"));
  }
  List<String> sorted(Metadata metadata) {
    return GrpcMetadata.wire(metadata).stream().map(header -> header.name() + ": " + header.value()).sorted().toList();
  }
  DynamicMessage hello(String message, int unknownField) {
    var reply = DynamicMessage.newBuilder(method.output()).setField(method.output().findFieldByName("message"), message);
    if (unknownField > 0) reply.setUnknownFields(UnknownFieldSet.newBuilder().addField(unknownField,
        UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("kept")).build()).build());
    return reply.build();
  }

  /** A real server of the same service, not this plugin: it answers as told and keeps what it received. */
  interface Answer {
    void to(ServerCall<DynamicMessage, DynamicMessage> call, DynamicMessage request) throws Exception;
  }
  final List<Server> fixtures = new ArrayList<>();
  final AtomicReference<Metadata> fixtureHeaders = new AtomicReference<>();
  final AtomicReference<DynamicMessage> fixtureRequest = new AtomicReference<>();
  @AfterEach void closeFixtures() throws Exception {
    for (Server fixture : fixtures) fixture.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
  }
  int fixture(Answer answer) throws Exception { return fixture(answer, null, null); }
  int fixture(Answer answer, CompressorRegistry compressors,
      io.grpc.netty.shaded.io.netty.handler.ssl.SslContext tls) throws Exception {
    var greeter = new GrpcSchema(Path.of("examples/hello.protoset").toAbsolutePath()).methods.getFirst();
    ServerCallHandler<DynamicMessage, DynamicMessage> handler = (call, headers) -> {
      fixtureHeaders.set(headers);
      call.request(2);
      return new ServerCall.Listener<>() {
        private DynamicMessage received;
        @Override public void onMessage(DynamicMessage message) { received = message; fixtureRequest.set(message); }
        @Override public void onHalfClose() {
          try {
            answer.to(call, received);
          } catch (Exception error) {
            call.close(Status.INTERNAL.withDescription(String.valueOf(error)), new Metadata());
          }
        }
      };
    };
    var builder = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forPort(0)
        .addService(ServerServiceDefinition.builder(greeter.service()).addMethod(greeter.wire(), handler).build());
    if (compressors != null) builder.compressorRegistry(compressors);
    if (tls != null) builder.sslContext(tls);
    Server server = builder.build().start();
    fixtures.add(server);
    return server.getPort();
  }
  Answer plain(String message) {
    return (call, received) -> {
      call.sendHeaders(new Metadata());
      call.sendMessage(hello(message, 0));
      call.close(Status.OK, new Metadata());
    };
  }

  private static ProtocolUpstreamClient.Upstream to(String target) {
    return new ProtocolUpstreamClient.Upstream(target);
  }

  @Test void forwardsARequestAndRelaysTheReplyWithItsMetadataAndTrailers() throws Exception {
    var failure = new AtomicReference<Throwable>();
    var reply = new AtomicReference<JsonNode>();
    start(forwarding(result("""
        {"headers":{"x-reply":"upstream","x-many":["a","b"]},"trailers":{"x-trail":"end"},"body":{"message":"Hello Ada"}}
        """, 0), failure, reply), Map.of());
    var headers = new AtomicReference<Metadata>();
    var trailers = new AtomicReference<Metadata>();
    DynamicMessage received = invoke(ClientInterceptors.intercept(channel,
        MetadataUtils.newAttachHeadersInterceptor(clientMetadata()),
        MetadataUtils.newCaptureMetadataInterceptor(headers, trailers)));
    assertNull(failure.get(), () -> String.valueOf(failure.get()));
    assertEquals("Hello Ada", received.getField(method.output().findFieldByName("message")));
    assertEquals("upstream", headers.get().get(Metadata.Key.of("x-reply", Metadata.ASCII_STRING_MARSHALLER)));
    var many = new ArrayList<String>();
    headers.get().getAll(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER)).forEach(many::add);
    assertEquals(List.of("a", "b"), many);
    assertEquals("end", trailers.get().get(Metadata.Key.of("x-trail", Metadata.ASCII_STRING_MARSHALLER)));
    // The document says everything the other server sent, in the fields of a template's response.
    assertEquals(mapper.readTree("""
        {"status":"OK","headers":{"content-type":"application/grpc","grpc-encoding":"identity","grpc-accept-encoding":"gzip",
         "x-reply":"upstream","x-many":["a","b"]},"trailers":{"x-trail":"end"},"body":{"message":"Hello Ada"}}
        """), reply.get());
    assertEquals(2, kit.statistics("grpc-demo").value(ProtocolStatistics.MESSAGES));
    assertEquals(0, kit.statistics("grpc-demo").value(ProtocolStatistics.UNMATCHED));
  }

  @Test void aForwardedErrorIsRelayedWithItsStatusAndTrailers() throws Exception {
    var failure = new AtomicReference<Throwable>();
    var reply = new AtomicReference<JsonNode>();
    start(forwarding(result("""
        {"status":"NOT_FOUND","description":"no such user","trailers":{"x-detail-bin":"BAUG"}}
        """, 0), failure, reply), Map.of());
    var error = assertThrows(StatusRuntimeException.class, () -> invoke(ClientInterceptors.intercept(channel,
        MetadataUtils.newAttachHeadersInterceptor(clientMetadata()))));
    assertNull(failure.get(), () -> String.valueOf(failure.get()));
    assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    assertEquals("no such user", error.getStatus().getDescription());
    assertArrayEquals(new byte[] { 4, 5, 6 },
        error.getTrailers().get(Metadata.Key.of("x-detail-bin", Metadata.BINARY_BYTE_MARSHALLER)));
    assertEquals(mapper.readTree("""
        {"status":"NOT_FOUND","description":"no such user",
         "headers":{"content-type":"application/grpc","grpc-encoding":"identity","grpc-accept-encoding":"gzip"},
         "trailers":{"x-detail-bin":"BAUG"}}
        """), reply.get());
  }

  @Test void aRequestReachesTheOtherServerAsItCame() throws Exception {
    var failure = new AtomicReference<Throwable>();
    var reply = new AtomicReference<JsonNode>();
    var arrived = new AtomicReference<Metadata>();
    int port = fixture(plain("ok"));
    start(passingOn(port, failure, reply, arrived), Map.of());
    // A caller with a user agent and accepted encodings of its own, repeated and binary metadata,
    // a packed request and a field the descriptor set does not know.
    var caller = NettyChannelBuilder.forAddress("127.0.0.1", runtime.port()).usePlaintext().userAgent("fixture-client/7.1")
        .decompressorRegistry(DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, true).with(new Codec.Gzip(), true))
        .build();
    try {
      var headers = clientMetadata();
      headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer token");
      headers.put(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER), "first");
      headers.put(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER), "second");
      DynamicMessage sent = request.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
          UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("kept")).build()).build()).build();
      ClientCalls.blockingUnaryCall(ClientInterceptors.intercept(caller, MetadataUtils.newAttachHeadersInterceptor(headers)),
          method.wire(), CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS).withCompression("gzip"), sent);
      assertNull(failure.get(), () -> String.valueOf(failure.get()));
      // Every header, value for value: also the ones a gRPC channel would have written itself.
      assertEquals(sorted(arrived.get()), sorted(fixtureHeaders.get()));
      var seen = sorted(fixtureHeaders.get());
      assertTrue(seen.stream().anyMatch(header -> header.startsWith("user-agent: fixture-client/7.1 grpc-java-netty/")), seen::toString);
      assertTrue(seen.containsAll(List.of("grpc-accept-encoding: identity,gzip", "grpc-encoding: gzip", "content-type: application/grpc",
          "authorization: Bearer token", "x-many: first", "x-many: second", "x-client: test", "x-data-bin: AQID")), seen::toString);
      assertTrue(seen.stream().anyMatch(header -> header.startsWith("grpc-timeout: ")), seen::toString);
      // And the message byte for byte, the unknown field too.
      assertEquals(sent.toByteString(), fixtureRequest.get().toByteString());
      assertTrue(fixtureRequest.get().getUnknownFields().hasField(99));
    } finally {
      caller.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test void whatACallerOffersIsPassedOnAsFarAsItCanBeReadHere() throws Exception {
    int port = fixture(plain("ok"));
    start(message -> { fail("Nothing reaches this server"); return null; }, Map.of());
    // The headers of a call recorded from Postman, replayed from their fields.
    Map<String, Object> postman = new LinkedHashMap<>();
    postman.put("content-type", "application/grpc");
    postman.put("grpc-accept-encoding", "identity,deflate,gzip");
    postman.put("accept-encoding", "identity");
    postman.put("user-agent", "grpc-node-js/1.14.0-postman.4");
    var recorded = endpoint.message(Map.of("method", "demo.Greeter/SayHello", "service", "demo.Greeter",
        "header", postman, "body", "{\"name\":\"Ada\"}"));
    var replies = runtime.forward(recorded, to("grpc://127.0.0.1:" + port)).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("OK", replies.getFirst().path("status").asText());
    assertEquals(postman, GrpcMetadata.fields(fixtureHeaders.get()));
    assertEquals(request.toByteString(), fixtureRequest.get().toByteString());

    // An encoding that could not be read here is not offered in the caller's name, and
    // compression of the whole stream is declined.
    postman.put("grpc-accept-encoding", "zstd, identity,gzip,br");
    postman.put("accept-encoding", "gzip");
    postman.put("grpc-encoding", "zstd");
    runtime.forward(endpoint.message(Map.of("method", "demo.Greeter/SayHello", "header", postman, "body", "{}")),
        to("grpc://127.0.0.1:" + port)).toCompletableFuture().get(5, TimeUnit.SECONDS);
    var seen = GrpcMetadata.fields(fixtureHeaders.get());
    assertEquals("identity,gzip", seen.get("grpc-accept-encoding"));
    assertEquals("identity", seen.get("accept-encoding"));
    assertNull(seen.get("grpc-encoding"));
    assertEquals("grpc-node-js/1.14.0-postman.4", seen.get("user-agent"));
  }

  @Test void theRulesOfAnUpstreamChangeHeadersAndLeaveTheRestAsItCame() throws Exception {
    int port = fixture(plain("ok"));
    start(message -> ProtocolExchangeResult.noMatch(), Map.of());
    assertEquals(Set.of("header", "body"), runtime.rewritableFields());
    var headers = new LinkedHashMap<String, Object>();
    headers.put("x-first", "1");
    headers.put("x-tenant", "blue");
    headers.put("authorization", "Bearer token");
    headers.put("x-last", "9");
    Map<String, String> changes = new LinkedHashMap<>();
    changes.put("header.X-Tenant", "green");
    changes.put("header.authorization", null);
    changes.put("header.x-absent", null);
    changes.put("header.x-key", "secret");
    var replies = runtime.forward(endpoint.message(Map.of("method", "demo.Greeter/SayHello", "header", headers, "body", "{}")),
        new ProtocolUpstreamClient.Upstream("grpc://127.0.0.1:" + port, changes)).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("OK", replies.getFirst().path("status").asText());
    var seen = sorted(fixtureHeaders.get());
    assertTrue(seen.containsAll(List.of("x-first: 1", "x-tenant: green", "x-last: 9", "x-key: secret")), seen::toString);
    assertTrue(seen.stream().noneMatch(header -> header.startsWith("authorization") || header.startsWith("x-absent")), seen::toString);

    // A changed header keeps its place among the others; a new one follows them.
    var received = List.of(new GrpcWire.Header("x-first", "1"), new GrpcWire.Header("x-many", "a"),
        new GrpcWire.Header("x-tenant", "blue"), new GrpcWire.Header("x-many", "b"), new GrpcWire.Header("x-last", "9"));
    Map<String, String> more = new LinkedHashMap<>();
    more.put("header.x-many", "one");
    more.put("header.x-last", null);
    more.put("header.x-new", "n");
    more.put("body.name", "not a header");
    assertEquals(List.of(new GrpcWire.Header("x-first", "1"), new GrpcWire.Header("x-many", "one"),
        new GrpcWire.Header("x-tenant", "blue"), new GrpcWire.Header("x-new", "n")), GrpcUpstream.changed(received, more));
    assertEquals(5, received.size());
  }

  @Test void theRulesOfAnUpstreamChangeTheMessageAndKeepWhatTheyDoNotName() throws Exception {
    var seen = new AtomicReference<String>();
    var schema = new GrpcSchema(Path.of("examples/hello.protoset").toAbsolutePath());
    var greeter = schema.methods.getFirst();
    var passing = new GrpcUpstream(schema, mapper, 4 * 1024 * 1024);
    int port = fixture(plain("ok"));
    start(message -> {
      // What a rule on the whole body works on: the message as JSON, as templates read it.
      seen.set(message.text("request.body").orElse(null));
      return ProtocolExchangeResult.noMatch();
    }, Map.of());
    var caller = NettyChannelBuilder.forAddress("127.0.0.1", runtime.port()).usePlaintext().build();
    try {
      assertThrows(StatusRuntimeException.class, () -> ClientCalls.blockingUnaryCall(caller, method.wire(),
          CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), request));
    } finally {
      caller.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
    assertEquals(mapper.readTree(schema.printer.print(request)), mapper.readTree(seen.get()));

    // One field inside the message, by its path; the rest of the message stays, and so does a
    // field the descriptor set does not know.
    DynamicMessage sent = request.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
        UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("kept")).build()).build()).build();
    byte[] received = sent.toByteArray();
    Map<String, String> one = new LinkedHashMap<>();
    one.put("body.name", "Grace");
    DynamicMessage changed = DynamicMessage.parseFrom(greeter.input(), passing.changed(received, greeter, one));
    assertEquals("Grace", changed.getField(greeter.input().findFieldByName("name")));
    assertTrue(changed.getUnknownFields().hasField(99));
    // Changes of other fields leave the bytes that were received.
    assertSame(received, passing.changed(received, greeter, Map.of("header.x-tenant", "green")));

    // The whole message, as a rule's regex left its JSON; then a field of that, in the order of the rules.
    Map<String, String> whole = new LinkedHashMap<>();
    whole.put("body", seen.get().replace("Ada", "Whole"));
    assertEquals("Whole", DynamicMessage.parseFrom(greeter.input(), passing.changed(received, greeter, whole))
        .getField(greeter.input().findFieldByName("name")));
    whole.put("body.name", null);
    assertEquals("", DynamicMessage.parseFrom(greeter.input(), passing.changed(received, greeter, whole))
        .getField(greeter.input().findFieldByName("name")));
    // What the rules leave has to be a message of the method.
    assertTrue(assertThrows(IllegalArgumentException.class, () -> passing.changed(received, greeter, Map.of("body", "[1]")))
        .getMessage().contains("do not leave a " + greeter.input().getFullName()));
    assertThrows(IllegalArgumentException.class, () -> passing.changed(received, greeter, Map.of("body.noSuchField", "x")));

    // And on the way to the other server.
    var replies = runtime.forward(endpoint.message(Map.of("method", "demo.Greeter/SayHello", "header", Map.of(), "body", "{\"name\":\"Ada\"}")),
        new ProtocolUpstreamClient.Upstream("grpc://127.0.0.1:" + port, one)).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("OK", replies.getFirst().path("status").asText());
    assertEquals("Grace", fixtureRequest.get().getField(fixtureRequest.get().getDescriptorForType().findFieldByName("name")));
    passing.close();
  }

  @Test void aReplyReturnsToTheCallerAsItWasSent() throws Exception {
    var failure = new AtomicReference<Throwable>();
    var reply = new AtomicReference<JsonNode>();
    int port = fixture((call, received) -> {
      var headers = new Metadata();
      headers.put(Metadata.Key.of("x-reply", Metadata.ASCII_STRING_MARSHALLER), "real");
      headers.put(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER), "a");
      headers.put(Metadata.Key.of("x-many", Metadata.ASCII_STRING_MARSHALLER), "b");
      headers.put(Metadata.Key.of("x-data-bin", Metadata.BINARY_BYTE_MARSHALLER), new byte[] { 1, 2, 3 });
      var trailers = new Metadata();
      trailers.put(Metadata.Key.of("x-trail", Metadata.ASCII_STRING_MARSHALLER), "end");
      trailers.put(Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER), new byte[] { 9, 8 });
      call.setCompression("gzip");
      call.sendHeaders(headers);
      call.sendMessage(hello("real answer", 77));
      call.close(Status.OK, trailers);
    });
    start(passingOn(port, failure, reply, new AtomicReference<>()), Map.of());
    var headers = new AtomicReference<Metadata>();
    var trailers = new AtomicReference<Metadata>();
    DynamicMessage received = invoke(ClientInterceptors.intercept(channel,
        MetadataUtils.newCaptureMetadataInterceptor(headers, trailers)));
    assertNull(failure.get(), () -> String.valueOf(failure.get()));
    // The protobuf itself, with the field the descriptor set does not know.
    assertEquals(hello("real answer", 77), received);
    assertTrue(received.getUnknownFields().hasField(77));
    var sentHeaders = sorted(headers.get());
    assertTrue(sentHeaders.containsAll(List.of("x-reply: real", "x-many: a", "x-many: b", "x-data-bin: AQID",
        "grpc-encoding: gzip")), sentHeaders::toString);
    assertEquals(1, sentHeaders.stream().filter(header -> header.startsWith("content-type: ")).count(), sentHeaders::toString);
    assertEquals(List.of("grpc-status-details-bin: CQg", "x-trail: end"), sorted(trailers.get()));
    assertEquals(mapper.readTree("""
        {"status":"OK","headers":{"content-type":"application/grpc","grpc-encoding":"gzip","grpc-accept-encoding":"gzip",
         "x-reply":"real","x-many":["a","b"],"x-data-bin":"AQID"},
         "trailers":{"x-trail":"end","grpc-status-details-bin":"CQg"},"body":{"message":"real answer"}}
        """), reply.get());
  }

  @Test void anAnswerOfTrailersAloneKeepsItsShapeAndItsDescription() throws Exception {
    var failure = new AtomicReference<Throwable>();
    var reply = new AtomicReference<JsonNode>();
    int port = fixture((call, received) -> {
      var trailers = new Metadata();
      trailers.put(Metadata.Key.of("x-reason", Metadata.ASCII_STRING_MARSHALLER), "quota");
      call.close(Status.RESOURCE_EXHAUSTED.withDescription("100% used, résumé later"), trailers);
    });
    start(passingOn(port, failure, reply, new AtomicReference<>()), Map.of());
    var headers = new AtomicReference<Metadata>();
    var trailers = new AtomicReference<Metadata>();
    var error = assertThrows(StatusRuntimeException.class, () -> invoke(ClientInterceptors.intercept(channel,
        MetadataUtils.newCaptureMetadataInterceptor(headers, trailers))));
    assertNull(failure.get(), () -> String.valueOf(failure.get()));
    assertEquals(Status.Code.RESOURCE_EXHAUSTED, error.getStatus().getCode());
    assertEquals("100% used, résumé later", error.getStatus().getDescription());
    assertEquals("quota", error.getTrailers().get(Metadata.Key.of("x-reason", Metadata.ASCII_STRING_MARSHALLER)));
    assertNull(headers.get(), "No headers were sent before the status, as the other server sent none");
    assertFalse(reply.get().has("headers"));
    assertEquals("RESOURCE_EXHAUSTED", reply.get().path("status").asText());
    assertEquals("100% used, résumé later", reply.get().path("description").asText());
    assertEquals("quota", reply.get().path("trailers").path("x-reason").asText());
    assertFalse(reply.get().has("body"));
  }

  @Test void largeAndPackedRepliesAreRead() throws Exception {
    // A reply packed in an encoding only this side of the two knows how to read.
    var compressors = CompressorRegistry.newEmptyInstance();
    compressors.register(new Compressor() {
      @Override public String getMessageEncoding() { return "deflate"; }
      @Override public java.io.OutputStream compress(java.io.OutputStream out) { return new java.util.zip.DeflaterOutputStream(out); }
    });
    String large = "x".repeat(1_500_000);
    int port = fixture((call, received) -> {
      call.setCompression("deflate");
      call.sendHeaders(new Metadata());
      call.sendMessage(hello(received.getField(received.getDescriptorForType().findFieldByName("name")) + "!", 0));
      call.close(Status.OK, new Metadata());
    }, compressors, null);
    start(message -> { fail("Nothing reaches this server"); return null; }, Map.of());
    Map<String, Object> headers = Map.of("grpc-accept-encoding", "identity,deflate,gzip");
    var replies = runtime.forward(endpoint.message(Map.of("method", "demo.Greeter/SayHello", "header", headers,
        "body", mapper.createObjectNode().put("name", large).toString())), to("grpc://127.0.0.1:" + port))
        .toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals("OK", replies.getFirst().path("status").asText(), replies.getFirst().path("description").asText());
    assertEquals("deflate", replies.getFirst().path("headers").path("grpc-encoding").asText());
    assertEquals(large + "!", replies.getFirst().path("body").path("message").asText());
    assertEquals(large, fixtureRequest.get().getField(fixtureRequest.get().getDescriptorForType().findFieldByName("name")));
    // Not packed, the same reply is many times the window of a new HTTP/2 stream.
    replies = runtime.forward(endpoint.message(Map.of("method", "demo.Greeter/SayHello", "header", Map.of(),
        "body", mapper.createObjectNode().put("name", large).toString())), to("grpc://127.0.0.1:" + port))
        .toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals("identity", replies.getFirst().path("headers").path("grpc-encoding").asText());
    assertEquals(large + "!", replies.getFirst().path("body").path("message").asText());
  }

  @Test void aReplyBeyondTheMessageLimitIsNotRead() throws Exception {
    int port = fixture(plain("y".repeat(5000)));
    start(message -> { fail("Nothing reaches this server"); return null; }, Map.of("max-message-bytes", 1024));
    var replies = runtime.forward(greeting(), to("grpc://127.0.0.1:" + port)).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("RESOURCE_EXHAUSTED", replies.getFirst().path("status").asText());
    assertFalse(replies.getFirst().has("body"));
  }

  @Test void callsArePassedOnOverTls() throws Exception {
    // A certificate for localhost, made for this run and trusted through the JVM's trust store setting.
    Path store = temp.resolve("fixture.p12");
    Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
        "-genkeypair", "-alias", "fixture", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
        "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12",
        "-keystore", store.toString(), "-storepass", "changeit").redirectErrorStream(true).start();
    String output = new String(keytool.getInputStream().readAllBytes());
    assertEquals(0, keytool.waitFor(), output);
    var keys = java.security.KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(store)) { keys.load(input, "changeit".toCharArray()); }
    var keyManagers = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, "changeit".toCharArray());
    int port = fixture(plain("over tls"), null, io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.configure(
        io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder.forServer(keyManagers)).build());
    Map<String, String> trust = Map.of("javax.net.ssl.trustStore", store.toString(),
        "javax.net.ssl.trustStorePassword", "changeit", "javax.net.ssl.trustStoreType", "PKCS12");
    Map<String, String> before = new HashMap<>();
    trust.forEach((name, value) -> { before.put(name, System.getProperty(name)); System.setProperty(name, value); });
    try {
      start(message -> { fail("Nothing reaches this server"); return null; }, Map.of());
      var replies = runtime.forward(greeting(), to("grpcs://localhost:" + port)).toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals("OK", replies.getFirst().path("status").asText(), replies.getFirst().path("description").asText());
      assertEquals("over tls", replies.getFirst().path("body").path("message").asText());
      // Plain HTTP/2 to the same port is not understood by a server that expects TLS.
      var plain = runtime.forward(greeting(), to("grpc://localhost:" + port)).toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals("UNAVAILABLE", plain.getFirst().path("status").asText());
    } finally {
      before.forEach((name, value) -> { if (value == null) System.clearProperty(name); else System.setProperty(name, value); });
    }
  }

  @Test void anUnreachableServerIsAReplyAndABadTargetIsRefusedWhenItIsSet() throws Exception {
    start(message -> { fail("Nothing reaches this server"); return null; }, Map.of());
    int closedPort;
    try (var probe = new java.net.ServerSocket(0)) { closedPort = probe.getLocalPort(); }
    var replies = runtime.forward(greeting(), to("grpc://127.0.0.1:" + closedPort))
        .toCompletableFuture().get(15, TimeUnit.SECONDS);
    assertEquals("UNAVAILABLE", replies.getFirst().path("status").asText());
    assertTrue(replies.getFirst().path("description").asText().contains("grpc://127.0.0.1:" + closedPort));
    assertFalse(replies.getFirst().has("body"));
    // So is a server that is there but does not speak gRPC.
    try (var other = new java.net.ServerSocket(0)) {
      Thread.startVirtualThread(() -> {
        try (var connection = other.accept()) {
          connection.getOutputStream().write("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n".getBytes());
        } catch (java.io.IOException ignored) {
        }
      });
      var answered = runtime.forward(greeting(), to("grpc://127.0.0.1:" + other.getLocalPort()))
          .toCompletableFuture().get(15, TimeUnit.SECONDS);
      assertEquals("UNAVAILABLE", answered.getFirst().path("status").asText());
    }
    // A request the descriptor set cannot encode gets no reply at all.
    var unknown = endpoint.message(Map.of("method", "demo.Greeter/Missing", "header", Map.of(), "body", "{}"));
    assertThrows(ExecutionException.class, () -> runtime.forward(unknown, to("grpc://127.0.0.1:" + closedPort))
        .toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals("grpc://host:port", runtime.targetHint());
    runtime.checkTarget("grpc://upstream.test:9090");
    runtime.checkTarget(" grpcs://upstream.test:443 ");
    for (String invalid : Arrays.asList("upstream.test:9090", "http://upstream.test:9090", "grpc://upstream.test",
        "grpc://upstream.test:9090/demo.Greeter", "not a url", "", null)) {
      assertTrue(assertThrows(IllegalArgumentException.class, () -> runtime.checkTarget(invalid))
          .getMessage().contains("grpc://host:port"), String.valueOf(invalid));
    }
  }

  @Test void aForwardedCallEndsWithTheCallersDeadlineOrWhenTheHostStopsWaiting() throws Exception {
    var entered = new LinkedBlockingQueue<Boolean>();
    var forwarded = new CompletableFuture<List<JsonNode>>();
    var late = result("{\"body\":{\"message\":\"late\"}}", 60000);
    var exchanges = new AtomicInteger();
    start(message -> {
      if (exchanges.incrementAndGet() == 1 && message.text("request.header.x-client").isPresent()) {
        runtime.forward(message, to("grpc://127.0.0.1:" + runtime.port())).whenComplete((replies, error) -> {
          if (error == null) forwarded.complete(replies); else forwarded.completeExceptionally(error);
        });
        return ProtocolExchangeResult.answered("forwarded", List.of(new ProtocolResponse(forwarded.join().getFirst(), 0)));
      }
      entered.add(true);
      return late;
    }, Map.of());
    // The caller allows 400 ms; the call passed on for it is given no longer.
    assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> ClientCalls.blockingUnaryCall(
        ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(clientMetadata())),
        method.wire(), CallOptions.DEFAULT.withDeadlineAfter(400, TimeUnit.MILLISECONDS), request));
    assertEquals("DEADLINE_EXCEEDED", forwarded.get(5, TimeUnit.SECONDS).getFirst().path("status").asText());
    assertNotNull(entered.poll(3, TimeUnit.SECONDS));

    // Without a caller's deadline the call lasts until the host cancels the stage.
    var waiting = runtime.forward(greeting(), to("grpc://127.0.0.1:" + runtime.port())).toCompletableFuture();
    assertNotNull(entered.poll(3, TimeUnit.SECONDS));
    assertFalse(waiting.isDone());
    waiting.cancel(false);
    for (int i = 0; i < 300 && endpoint.inFlight() != 0; i++) Thread.sleep(10);
    assertEquals(0, endpoint.inFlight());
    // Stopping the instance fails what is still on its way.
    var pending = runtime.forward(greeting(), to("grpc://127.0.0.1:" + runtime.port())).toCompletableFuture();
    assertNotNull(entered.poll(3, TimeUnit.SECONDS));
    runtime.close();
    assertThrows(ExecutionException.class, () -> pending.get(3, TimeUnit.SECONDS));
    assertThrows(ExecutionException.class, () -> runtime.forward(greeting(), to("grpc://127.0.0.1:1"))
        .toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(0, acknowledged.get());
  }

  @Test void metadataDerivesCapabilities() throws Exception {
    var descriptor = ProtocolValidation.plugin(plugin);
    assertEquals(ProtocolPlugin.API_VERSION, descriptor.apiVersion());
    assertTrue(descriptor.capabilities().containsAll(Set.of(ProtocolCapability.CALLBACKS,
        ProtocolCapability.REQUEST_RESPONSE, ProtocolCapability.NATIVE_RUNTIME)));
    var definition = plugin.definition();
    // Server streams send a response sequence; a unary call takes the first response.
    assertFalse(definition.template().singleResponse());
    assertEquals(Set.of("url", "method", "headers", "body", "timeoutMs"), definition.template().callback().keySet());
    assertTrue(definition.message().request().get("method").routing());
    assertTrue(definition.message().request().get("service").routing());
    assertEquals(List.of("request.method", "request.service"), definition.routing().fields());
  }
}
