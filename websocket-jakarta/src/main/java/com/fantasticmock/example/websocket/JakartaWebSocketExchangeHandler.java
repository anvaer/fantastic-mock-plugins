package com.fantasticmock.example.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolResponseDelivery;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import jakarta.websocket.Session;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Converts Jakarta messages to protocol exchanges and writes mock responses. The endpoint
 * counts messages and failures and delivers each response on the session.
 */
final class JakartaWebSocketExchangeHandler {
  private final ProtocolEndpoint endpoint;

  JakartaWebSocketExchangeHandler(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  /** One response sender per WebSocket session; closing it cancels pending responses. */
  ProtocolResponseDelivery delivery(Session session) {
    return endpoint.responseDelivery(document -> {
      String text = responseBody(document);
      long size = text.getBytes(StandardCharsets.UTF_8).length;
      return send(session, text).thenApply(ignored -> size);
    });
  }

  CompletableFuture<?> receive(Session session, String text, ProtocolResponseDelivery delivery) {
    var message = endpoint.message(requestFields(session, text))
        .withPayloadSize(text.getBytes(StandardCharsets.UTF_8).length);
    return endpoint.handle(message, delivery);
  }

  CompletableFuture<Void> send(Session session, String text) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    if (!session.isOpen()) {
      return CompletableFuture.failedFuture(new ClosedChannelException());
    }
    try {
      session.getAsyncRemote().sendText(text, result -> {
        if (result.isOK()) future.complete(null);
        else future.completeExceptionally(result.getException());
      });
    } catch (RuntimeException failure) {
      future.completeExceptionally(failure);
    }
    return future;
  }

  // The message's body, and the session it came on with the path and query that session was
  // opened with: protocol.yml declares those three as metadata, which logs show apart.
  private Map<String, Object> requestFields(Session session, String text) {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("session", session.getId());
    fields.put("path", session.getRequestURI().getPath());
    if (session.getQueryString() != null) {
      fields.put("query", session.getQueryString());
    }
    fields.put("body", text);
    return fields;
  }

  private String responseBody(JsonNode document) {
    JsonNode body = document != null && document.isObject()
        ? document.get("body")
        : document;
    return body == null || body.isNull()
        ? ""
        : body.isTextual() ? body.textValue() : body.toString();
  }
}
