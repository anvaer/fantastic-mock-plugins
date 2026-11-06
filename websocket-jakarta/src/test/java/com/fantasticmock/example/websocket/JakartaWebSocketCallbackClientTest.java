package com.fantasticmock.example.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.SendHandler;
import jakarta.websocket.SendResult;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JakartaWebSocketCallbackClientTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final WebSocketContainer container = mock(WebSocketContainer.class);
  private final Session session = mock(Session.class);
  private final RemoteEndpoint.Async remote = mock(RemoteEndpoint.Async.class);
  // What the callback wrote on its session, and the messages it reported, in order: "sent hello 5".
  private final List<String> written = new CopyOnWriteArrayList<>();
  private final List<String> traffic = new CopyOnWriteArrayList<>();
  private final List<JsonNode> sentMessages = new CopyOnWriteArrayList<>();
  private final List<JsonNode> receivedMessages = new CopyOnWriteArrayList<>();
  private final List<CloseReason> closes = new CopyOnWriteArrayList<>();
  private volatile Endpoint endpoint;
  private volatile MessageHandler.Whole<String> handler;
  private JakartaWebSocketCallbackClient client;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void start() throws Exception {
    when(session.isOpen()).thenReturn(true);
    when(session.getId()).thenReturn("session-1");
    when(session.getAsyncRemote()).thenReturn(remote);
    doAnswer(call -> {
      written.add(call.getArgument(0));
      ((SendHandler) call.getArgument(1)).onResult(new SendResult());
      return null;
    }).when(remote).sendText(any(String.class), any(SendHandler.class));
    doAnswer(call -> {
      handler = call.getArgument(1);
      return null;
    }).when(session).addMessageHandler(any(Class.class), any(MessageHandler.Whole.class));
    doAnswer(call -> {
      closes.add(call.getArgument(0));
      return null;
    }).when(session).close(any(CloseReason.class));
    // A real container opens the session before connectToServer returns it.
    when(container.connectToServer(any(Endpoint.class), any(ClientEndpointConfig.class), any(URI.class)))
        .thenAnswer(call -> {
          endpoint = call.getArgument(0);
          endpoint.onOpen(session, mock(EndpointConfig.class));
          return session;
        });
    client = new JakartaWebSocketCallbackClient(ProtocolStatistics.noop(), new JakartaWebSocketExchangeHandler(null),
        scheduler);
    client.start(container);
  }

  @AfterEach
  void stop() {
    client.close();
    scheduler.shutdownNow();
  }

  private ObjectNode callback() {
    return mapper.createObjectNode().put("url", "ws://localhost/ws-test").put("body", "hello");
  }

  private CompletableFuture<Long> send(JsonNode callback) {
    return client.send(callback,
        (message, size) -> {
          sentMessages.add(message);
          traffic.add("sent " + message.path("body").asText() + " " + size);
        },
        (message, size) -> {
          receivedMessages.add(message);
          traffic.add("received " + message.path("body").asText() + " " + size);
        })
        .toCompletableFuture();
  }

  private List<String> received() {
    return traffic.stream().filter(item -> item.startsWith("received ")).toList();
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) throw new AssertionError("condition not met in time");
      Thread.sleep(2);
    }
  }

  @Test
  void aCallbackThatWaitsForNothingClosesRightAfterSending() throws Exception {
    assertThat(send(callback()).get(5, TimeUnit.SECONDS)).isEqualTo(5L);

    assertThat(written).containsExactly("hello");
    assertThat(closes).singleElement().satisfies(reason -> assertThat(reason.getReasonPhrase())
        .isEqualTo("Callback delivered"));
    handler.onMessage("too late");
    assertThat(traffic).containsExactly("sent hello 5");
  }

  @Test
  void reportsWhatWentOverTheWireWithItsSessionAndTheHandshakeHeadersWithTheFirstMessage() throws Exception {
    ObjectNode callback = callback().put("awaitReplies", 1).put("replyPattern", "done").put("timeoutMs", 5000)
        .put("ping", "ping").put("pingIntervalMs", 15);
    callback.putObject("headers").put("Authorization", "Bearer token");
    var completion = send(callback);
    await(() -> sentMessages.size() >= 2);
    handler.onMessage("done");
    completion.get(5, TimeUnit.SECONDS);

    assertThat(sentMessages.getFirst()).isEqualTo(mapper.readTree(
        "{\"session\":\"session-1\",\"headers\":{\"Authorization\":\"Bearer token\"},\"body\":\"hello\"}"));
    assertThat(sentMessages.get(1)).isEqualTo(mapper.readTree("{\"session\":\"session-1\",\"body\":\"ping\"}"));
    assertThat(receivedMessages).containsExactly(mapper.readTree("{\"session\":\"session-1\",\"body\":\"done\"}"));
  }

  @Test
  void waitsForTheRepliesItCountsAndReportsEachOne() throws Exception {
    var completion = send(callback().put("awaitReplies", 2));
    await(() -> !written.isEmpty());

    handler.onMessage("one");
    Thread.sleep(20);
    assertThat(completion).isNotDone();
    assertThat(closes).isEmpty();
    handler.onMessage("two");

    assertThat(completion.get(5, TimeUnit.SECONDS)).isEqualTo(5L);
    assertThat(traffic).containsExactly("sent hello 5", "received one 3", "received two 3");
    assertThat(closes).hasSize(1);
  }

  @Test
  void aReplyPatternAloneWaitsForOneReplyItMatchesAndReportsTheOthersToo() throws Exception {
    // Rendered from a template, a number may arrive as text.
    var completion = send(callback().put("replyPattern", "\"state\":\"done\"").put("timeoutMs", "5000"));
    await(() -> !written.isEmpty());

    handler.onMessage("{\"state\":\"working\"}");
    Thread.sleep(20);
    assertThat(completion).isNotDone();
    handler.onMessage("{\"state\":\"done\"}");

    completion.get(5, TimeUnit.SECONDS);
    assertThat(received()).extracting(reply -> reply.substring(0, reply.lastIndexOf(' ')))
        .containsExactly("received {\"state\":\"working\"}", "received {\"state\":\"done\"}");
  }

  @Test
  void fewerRepliesThanAwaitedFailTheCallbackAtItsTimeout() throws Exception {
    var completion = send(callback().put("awaitReplies", 2).put("timeoutMs", 50));
    await(() -> !written.isEmpty());
    handler.onMessage("only one");

    assertThatThrownBy(() -> completion.get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .cause().isInstanceOf(TimeoutException.class)
        .hasMessage("Received 1 of 2 replies within 50 ms");
    assertThat(traffic).containsExactly("sent hello 5", "received only one 8");
    assertThat(closes).singleElement().satisfies(reason -> assertThat(reason.getReasonPhrase())
        .isEqualTo("Callback failed"));
  }

  @Test
  void sendsThePingMessageAtItsIntervalOnlyWhileItWaits() throws Exception {
    var completion = send(callback().put("awaitReplies", 1).put("ping", "{\"op\":\"ping\"}").put("pingIntervalMs", 15));
    await(() -> written.size() >= 3);
    assertThat(completion).isNotDone();
    handler.onMessage("pong");

    completion.get(5, TimeUnit.SECONDS);
    // A ping already handed to its thread may still land; none starts after that.
    Thread.sleep(30);
    int sent = written.size();
    Thread.sleep(80);
    assertThat(written).hasSize(sent);
    assertThat(written.getFirst()).isEqualTo("hello");
    assertThat(written.subList(1, sent)).containsOnly("{\"op\":\"ping\"}");
    // Each ping is reported as a message of its own; one written after the end is not.
    List<String> reported = traffic.stream().filter(item -> item.startsWith("sent ")).toList();
    assertThat(reported.getFirst()).isEqualTo("sent hello 5");
    assertThat(reported.subList(1, reported.size())).hasSizeGreaterThanOrEqualTo(2).hasSizeLessThanOrEqualTo(sent - 1)
        .containsOnly("sent {\"op\":\"ping\"} 13");
    assertThat(received()).containsExactly("received pong 4");
  }

  @Test
  void aServerThatClosesWhileTheCallbackWaitsFailsIt() throws Exception {
    var completion = send(callback().put("replyPattern", "^done$"));
    await(() -> !written.isEmpty());
    handler.onMessage("working");
    endpoint.onClose(session, new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "bye"));

    assertThatThrownBy(() -> completion.get(5, TimeUnit.SECONDS))
        .cause().isInstanceOf(IOException.class)
        .hasMessage("The server closed the connection (1000 bye); received 0 of 1 reply matching ^done$ (1 in all)");
  }

  @Test
  void invalidWaitSettingsFailBeforeConnecting() {
    assertThatThrownBy(() -> send(callback().put("replyPattern", "(")).join())
        .cause().isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("WebSocket callback 'replyPattern' is not a valid regular expression");
    assertThatThrownBy(() -> send(callback().put("awaitReplies", -1)).join())
        .cause().hasMessage("WebSocket callback 'awaitReplies' must be 0 or more");
    assertThatThrownBy(() -> send(callback().put("timeoutMs", "soon")).join())
        .cause().hasMessage("WebSocket callback 'timeoutMs' must be a whole number, not 'soon'");
    verifyNoInteractions(container);
  }

  @Test
  void anEarlierHostsSendStillWaitsWithoutReportingReplies() throws Exception {
    var completion = client.send(callback().put("awaitReplies", 1)).toCompletableFuture();
    await(() -> !written.isEmpty());
    assertThat(completion).isNotDone();
    handler.onMessage("reply");
    assertThat(completion.get(5, TimeUnit.SECONDS)).isEqualTo(5L);
  }
}
