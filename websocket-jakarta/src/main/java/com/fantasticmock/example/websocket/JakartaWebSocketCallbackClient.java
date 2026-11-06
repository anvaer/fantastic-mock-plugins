package com.fantasticmock.example.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Opens one short-lived client session for each outbound WebSocket callback. The session
 * closes after the message was sent, or, when the callback waits for replies, once enough of
 * them arrived or its timeout passed; meanwhile it can send a ping message at an interval.
 */
final class JakartaWebSocketCallbackClient implements AutoCloseable {
  static final long DEFAULT_TIMEOUT_MS = 10_000;
  static final long DEFAULT_PING_INTERVAL_MS = 30_000;
  private static final BiConsumer<JsonNode, Long> UNREPORTED = (message, size) -> { };

  private final ProtocolStatistics statistics;
  private final JakartaWebSocketExchangeHandler exchanges;
  private final ScheduledExecutorService scheduler;
  private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
  private final ExecutorService connections = Executors.newThreadPerTaskExecutor(
      Thread.ofVirtual().name("fstub-ws-callback-", 0).factory());
  private volatile WebSocketContainer container;
  // The server's container makes and starts its client on the first connection, unguarded:
  // one asked for meanwhile fails as "not started". So connections are opened one at a time
  // until one has gone through it, and then side by side.
  private final java.util.concurrent.locks.ReentrantLock firstConnection = new java.util.concurrent.locks.ReentrantLock();
  private volatile boolean clientStarted;

  JakartaWebSocketCallbackClient(
      ProtocolStatistics statistics,
      JakartaWebSocketExchangeHandler exchanges,
      ScheduledExecutorService scheduler) {
    this.statistics = statistics;
    this.exchanges = exchanges;
    this.scheduler = scheduler;
  }

  void start(WebSocketContainer container) {
    this.container = container;
  }

  private Session connect(WebSocketContainer current, Endpoint endpoint, ClientEndpointConfig config, URI uri)
      throws Exception {
    if (clientStarted) return current.connectToServer(endpoint, config, uri);
    firstConnection.lock();
    try {
      // Whether it connects or not, the container's client is made and started by now.
      return current.connectToServer(endpoint, config, uri);
    } finally {
      clientStarted = true;
      firstConnection.unlock();
    }
  }

  /** Completes with the payload bytes sent, after the callback session wrote them. */
  CompletionStage<Long> send(JsonNode message) {
    return send(message, UNREPORTED, UNREPORTED);
  }

  /**
   * Sends the callback and reports each message its session carries, as
   * {@code {"session": ..., "body": ...}}: the callback's message (with the handshake headers it
   * opened the session with, when it set any) and each ping to {@code sent} once written, and
   * each message that arrived to {@code received}.
   * Completes when the callback is over: right after the write when it waits for no replies,
   * otherwise once enough of them arrived; fails when they did not arrive in time.
   */
  CompletionStage<Long> send(JsonNode message, BiConsumer<JsonNode, Long> sent, BiConsumer<JsonNode, Long> received) {
    String target = text(message, "url", null);
    if (target == null || target.isBlank()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "WebSocket callback requires 'url'"));
    }

    URI uri;
    Wait wait;
    try {
      uri = URI.create(target);
      String scheme = uri.getScheme();
      if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
        throw new IllegalArgumentException("WebSocket callback URL must use ws:// or wss://");
      }
      wait = Wait.of(message);
    } catch (RuntimeException exception) {
      return CompletableFuture.failedFuture(exception);
    }

    WebSocketContainer current = container;
    if (current == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("WebSocket protocol instance is not started"));
    }

    String payload = payload(message);
    long size = payload.getBytes(StandardCharsets.UTF_8).length;
    JsonNode headers = message == null ? null : message.get("headers");
    JsonNode handshake = headers != null && headers.isObject() && !headers.isEmpty() ? headers.deepCopy() : null;
    Call call = new Call(wait, size, handshake, sent == null ? UNREPORTED : sent,
        received == null ? UNREPORTED : received);
    ClientEndpointConfig config = clientConfig(headers);
    try {
      connections.execute(() -> call.connectAndSend(current, uri, config, payload));
    } catch (RuntimeException exception) {
      statistics.increment("failures");
      call.completion.completeExceptionally(exception);
    }
    return call.completion;
  }

  /** What a callback waits for after sending, read from its rendered document. */
  record Wait(int replies, Pattern pattern, long timeoutMs, String ping, long pingIntervalMs) {
    boolean waits() {
      return replies > 0;
    }

    static Wait of(JsonNode message) {
      String patternText = optionalText(message, "replyPattern");
      Pattern pattern;
      try {
        pattern = patternText == null ? null : Pattern.compile(patternText);
      } catch (PatternSyntaxException invalid) {
        throw new IllegalArgumentException("WebSocket callback 'replyPattern' is not a valid regular expression: "
            + invalid.getDescription());
      }
      long awaited = number(message, "awaitReplies", 0);
      if (awaited < 0 || awaited > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("WebSocket callback 'awaitReplies' must be 0 or more");
      }
      // A pattern alone waits for one reply it matches.
      int replies = awaited == 0 && pattern != null ? 1 : (int) awaited;
      long timeout = number(message, "timeoutMs", 0);
      long interval = number(message, "pingIntervalMs", 0);
      return new Wait(replies, pattern,
          timeout > 0 ? timeout : DEFAULT_TIMEOUT_MS,
          optionalText(message, "ping"),
          interval > 0 ? interval : DEFAULT_PING_INTERVAL_MS);
    }

    private static String optionalText(JsonNode message, String field) {
      JsonNode value = message == null || !message.isObject() ? null : message.get(field);
      if (value == null || value.isNull()) return null;
      String text = value.isTextual() ? value.textValue() : value.toString();
      return text.isEmpty() ? null : text;
    }

    // Numbers may be rendered from templates as text, such as "{{count}}" giving "3".
    private static long number(JsonNode message, String field, long fallback) {
      JsonNode value = message == null || !message.isObject() ? null : message.get(field);
      if (value == null || value.isNull()) return fallback;
      if (value.canConvertToLong()) return value.longValue();
      String text = value.asText("").trim();
      if (text.isEmpty()) return fallback;
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException invalid) {
        throw new IllegalArgumentException("WebSocket callback '" + field + "' must be a whole number, not '"
            + text + "'");
      }
    }
  }

  /** One callback session: sends, then counts replies until it is done. */
  private final class Call {
    final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final Wait wait;
    private final long size;
    private final JsonNode handshake;
    private final BiConsumer<JsonNode, Long> sent;
    private final BiConsumer<JsonNode, Long> received;
    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile Session session;
    private volatile ScheduledFuture<?> timeout;
    private volatile ScheduledFuture<?> pings;
    // Written by the session's message handler only; Jakarta delivers one message at a time.
    private volatile int replies;
    private volatile int matched;

    Call(Wait wait, long size, JsonNode handshake, BiConsumer<JsonNode, Long> sent,
        BiConsumer<JsonNode, Long> received) {
      this.wait = wait;
      this.size = size;
      this.handshake = handshake;
      this.sent = sent;
      this.received = received;
    }

    void connectAndSend(WebSocketContainer current, URI uri, ClientEndpointConfig config, String payload) {
      try {
        Session connected = connect(current, new CallbackEndpoint(this), config, uri);
        session = connected;
        sessions.add(connected);
        if (finished.get()) {
          // Failed while connecting, before the session was known.
          closeQuietly(connected, "Callback failed");
          sessions.remove(connected);
          return;
        }
        exchanges.send(connected, payload).whenComplete((ignored, failure) -> {
          if (failure != null) {
            finish(failure);
            return;
          }
          // Reported before the callback is over, so the host logs it as this callback's message.
          sent.accept(reported(connected, handshake, payload), size);
          if (!wait.waits()) finish(null);
          else waitForReplies();
        });
      } catch (Exception exception) {
        finish(exception);
      }
    }

    private void waitForReplies() {
      try {
        timeout = scheduler.schedule(() -> later(this::timedOut), wait.timeoutMs(), TimeUnit.MILLISECONDS);
        if (wait.ping() != null) {
          pings = scheduler.scheduleWithFixedDelay(() -> later(this::ping),
              wait.pingIntervalMs(), wait.pingIntervalMs(), TimeUnit.MILLISECONDS);
        }
      } catch (RuntimeException rejected) {
        finish(rejected);
        return;
      }
      // Done already if the replies came while the message was being written.
      if (matched >= wait.replies()) finish(null);
      else if (finished.get()) cancelTimers();
    }

    // Timers only time; the shared scheduler must not wait for a write or a close.
    private void later(Runnable task) {
      try {
        connections.execute(task);
      } catch (RuntimeException stopping) {
        finish(stopping);
      }
    }

    // Every message that arrives while the callback lasts is reported; only replies it waits for count.
    void received(Session on, String text) {
      if (finished.get()) return;
      received.accept(reported(on, null, text), (long) text.getBytes(StandardCharsets.UTF_8).length);
      if (!wait.waits()) return;
      replies++;
      if (wait.pattern() == null || wait.pattern().matcher(text).find()) matched++;
      // The timers start once the message was sent; before that the write completion checks.
      if (matched >= wait.replies() && timeout != null) finish(null);
    }

    private void ping() {
      Session current = session;
      if (finished.get() || current == null) return;
      String ping = wait.ping();
      exchanges.send(current, ping).whenComplete((ignored, failure) -> {
        if (failure != null) finish(failure);
        else if (!finished.get()) sent.accept(reported(current, null, ping),
            (long) ping.getBytes(StandardCharsets.UTF_8).length);
      });
    }

    private void timedOut() {
      finish(new TimeoutException(progress() + " within " + wait.timeoutMs() + " ms"));
    }

    void closedByServer(CloseReason reason) {
      // A callback that waits for nothing is over when its write is.
      if (!wait.waits() || finished.get()) return;
      String why = reason == null ? "" : " (" + reason.getCloseCode().getCode()
          + (reason.getReasonPhrase() == null || reason.getReasonPhrase().isBlank()
              ? "" : " " + reason.getReasonPhrase()) + ")";
      finish(new IOException("The server closed the connection" + why + "; " + lowerFirst(progress())));
    }

    // "Received 1 of 2 replies matching ^done$", for timeouts and early closes.
    private String progress() {
      String counted = wait.pattern() == null ? "" : " matching " + wait.pattern().pattern();
      return "Received " + matched + " of " + wait.replies() + (wait.replies() == 1 ? " reply" : " replies") + counted
          + (wait.pattern() != null && replies > matched ? " (" + replies + " in all)" : "");
    }

    void finish(Throwable failure) {
      if (!finished.compareAndSet(false, true)) return;
      cancelTimers();
      Session current = session;
      if (current != null) {
        sessions.remove(current);
        closeQuietly(current, failure == null ? "Callback delivered" : "Callback failed");
      }
      if (failure == null) {
        statistics.increment("callbacks");
        completion.complete(size);
      } else {
        statistics.increment("failures");
        completion.completeExceptionally(failure);
      }
    }

    private void cancelTimers() {
      ScheduledFuture<?> waiting = timeout;
      if (waiting != null) waiting.cancel(false);
      ScheduledFuture<?> pinging = pings;
      if (pinging != null) pinging.cancel(false);
    }
  }

  // What one message carried and the session it went on; the handshake headers go with the
  // message that opened the session.
  private static ObjectNode reported(Session session, JsonNode handshake, String body) {
    ObjectNode message = JsonNodeFactory.instance.objectNode();
    String id = session == null ? null : session.getId();
    if (id != null) message.put("session", id);
    if (handshake != null) message.set("headers", handshake);
    message.put("body", body);
    return message;
  }

  private static String lowerFirst(String text) {
    return text.isEmpty() ? text : Character.toLowerCase(text.charAt(0)) + text.substring(1);
  }

  private ClientEndpointConfig clientConfig(JsonNode headers) {
    if (headers == null || !headers.isObject() || headers.isEmpty()) {
      return ClientEndpointConfig.Builder.create().build();
    }
    return ClientEndpointConfig.Builder.create()
        .configurator(new ClientEndpointConfig.Configurator() {
          @Override
          public void beforeRequest(Map<String, List<String>> requestHeaders) {
            headers.properties().forEach(entry ->
                requestHeaders.put(entry.getKey(), headerValues(entry.getValue())));
          }
        })
        .build();
  }

  private List<String> headerValues(JsonNode value) {
    List<String> values = new ArrayList<>();
    if (value != null && value.isArray()) {
      value.forEach(item -> values.add(scalarText(item)));
    } else if (value != null && !value.isNull()) {
      values.add(scalarText(value));
    }
    return values;
  }

  private String payload(JsonNode message) {
    JsonNode body = message != null && message.isObject() ? message.get("body") : message;
    return body == null || body.isNull() ? "" : scalarText(body);
  }

  private String text(JsonNode document, String field, String fallback) {
    if (document == null || !document.isObject()) return fallback;
    JsonNode value = document.get(field);
    return value == null || value.isNull() ? fallback : scalarText(value);
  }

  private String scalarText(JsonNode value) {
    return value != null && value.isTextual() ? value.textValue() : String.valueOf(value);
  }

  @Override
  public void close() {
    for (Session session : List.copyOf(sessions)) {
      closeQuietly(session, "FantasticMock stopping");
    }
    sessions.clear();
    connections.shutdownNow();
  }

  private void closeQuietly(Session session, String message) {
    if (session == null) return;
    try {
      session.close(new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, message));
    } catch (IOException ignored) {
      // The callback session is already unusable.
    }
  }

  private static final class CallbackEndpoint extends Endpoint {
    private final Call call;

    private CallbackEndpoint(Call call) {
      this.call = call;
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
      // Registered before the message is sent, so no reply can come first.
      session.addMessageHandler(String.class, (MessageHandler.Whole<String>) text -> call.received(session, text));
    }

    @Override
    public void onClose(Session session, CloseReason reason) {
      call.closedByServer(reason);
    }

    @Override
    public void onError(Session session, Throwable error) {
      call.finish(error);
    }
  }
}
