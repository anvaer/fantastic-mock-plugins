package com.fantasticmock.example.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Opens one short-lived client session for each outbound WebSocket callback. */
final class JakartaWebSocketCallbackClient implements AutoCloseable {
  private final ProtocolStatistics statistics;
  private final JakartaWebSocketExchangeHandler exchanges;
  private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
  private final ExecutorService connections = Executors.newThreadPerTaskExecutor(
      Thread.ofVirtual().name("fstub-ws-callback-", 0).factory());
  private volatile WebSocketContainer container;

  JakartaWebSocketCallbackClient(
      ProtocolStatistics statistics,
      JakartaWebSocketExchangeHandler exchanges) {
    this.statistics = statistics;
    this.exchanges = exchanges;
  }

  void start(WebSocketContainer container) {
    this.container = container;
  }

  /** Completes with the payload bytes sent, after the callback session wrote them. */
  CompletionStage<Long> send(JsonNode message) {
    String target = text(message, "url", null);
    if (target == null || target.isBlank()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "WebSocket callback requires 'url'"));
    }

    URI uri;
    try {
      uri = URI.create(target);
      String scheme = uri.getScheme();
      if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
        throw new IllegalArgumentException("WebSocket callback URL must use ws:// or wss://");
      }
    } catch (RuntimeException exception) {
      return CompletableFuture.failedFuture(exception);
    }

    WebSocketContainer current = container;
    if (current == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("WebSocket protocol instance is not started"));
    }

    CompletableFuture<Void> completion = new CompletableFuture<>();
    String payload = payload(message);
    ClientEndpointConfig config = clientConfig(message == null ? null : message.get("headers"));
    try {
      connections.execute(() -> connectAndSend(current, uri, config, payload, completion));
    } catch (RuntimeException exception) {
      statistics.increment("failures");
      completion.completeExceptionally(exception);
    }
    long size = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    return completion.thenApply(ignored -> size);
  }

  private void connectAndSend(
      WebSocketContainer current,
      URI uri,
      ClientEndpointConfig config,
      String payload,
      CompletableFuture<Void> completion) {
    Session session = null;
    try {
      session = current.connectToServer(new CallbackEndpoint(completion), config, uri);
      sessions.add(session);
      Session connected = session;
      exchanges.send(connected, payload).whenComplete((ignored, failure) -> {
        sessions.remove(connected);
        closeQuietly(connected, "Callback delivered");
        if (failure == null) {
          statistics.increment("callbacks");
          completion.complete(null);
        } else {
          statistics.increment("failures");
          completion.completeExceptionally(failure);
        }
      });
    } catch (Exception exception) {
      if (session != null) sessions.remove(session);
      closeQuietly(session, "Callback failed");
      statistics.increment("failures");
      completion.completeExceptionally(exception);
    }
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
    private final CompletableFuture<Void> completion;

    private CallbackEndpoint(CompletableFuture<Void> completion) {
      this.completion = completion;
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
      // The sender writes after connectToServer returns the open session.
    }

    @Override
    public void onError(Session session, Throwable error) {
      completion.completeExceptionally(error);
    }
  }
}
