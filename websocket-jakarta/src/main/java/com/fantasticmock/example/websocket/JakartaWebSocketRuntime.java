package com.fantasticmock.example.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolResponseDelivery;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolOutboundSender;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;
import jakarta.servlet.ServletContext;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerContainer;
import jakarta.websocket.server.ServerEndpointConfig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/** Owns one configured endpoint and its connected server and callback sessions. */
final class JakartaWebSocketRuntime implements ProtocolPluginInstance, ProtocolOutboundSender {
  private final ProtocolEndpoint endpoint;
  private final JakartaWebSocketExchangeHandler exchanges;
  private final JakartaWebSocketCallbackClient callbacks;
  private final Map<Session, ProtocolResponseDelivery> sessions = new ConcurrentHashMap<>();
  private final String path;
  private JakartaWebSocketEndpointRegistry.Registration endpointRegistration;

  JakartaWebSocketRuntime(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
    this.exchanges = new JakartaWebSocketExchangeHandler(endpoint);
    this.callbacks = new JakartaWebSocketCallbackClient(endpoint.statistics(), exchanges);
    String configured = endpoint.settings().text("endpoint-path",
        endpoint.definition().runtime().endpoint().path());
    this.path = configured.startsWith("/") ? configured : "/" + configured;
  }

  @Override
  public void start() throws Exception {
    ServerContainer container = endpoint.requireHostService(ServerContainer.class);
    ServletContext servletContext = endpoint.requireHostService(ServletContext.class);
    JakartaWebSocketEndpointRegistry.Registration registration =
        JakartaWebSocketEndpointRegistry.bind(
            servletContext,
            path,
            endpoint.id(),
            this::open,
            this::closed,
            ignored -> failed());
    try {
      if (registration.newMapping()) {
        ServerEndpointConfig config = ServerEndpointConfig.Builder
            .create(JakartaWebSocketEndpoint.class, path)
            .build();
        config.getUserProperties().put(
            JakartaWebSocketEndpointRegistry.ENDPOINT_BINDING_KEY,
            registration.binding());
        container.addEndpoint(config);
      }
      callbacks.start(container);
      endpointRegistration = registration;
    } catch (Exception exception) {
      registration.rollback();
      throw exception;
    }
  }

  void open(Session session) {
    ProtocolResponseDelivery delivery = exchanges.delivery(session);
    sessions.put(session, delivery);
    endpoint.statistics().increment("connections");
    session.addMessageHandler(String.class, text -> exchanges.receive(session, text, delivery));
  }

  void closed(Session session) {
    ProtocolResponseDelivery delivery = sessions.remove(session);
    if (delivery != null) delivery.close();
  }

  void failed() {
    endpoint.statistics().increment("failures");
  }

  @Override
  public CompletionStage<Long> send(JsonNode callback) {
    return callbacks.send(callback);
  }

  @Override
  public Map<String, Object> healthDetails() {
    return Map.of("path", path, "sessions", sessions.size());
  }

  @Override
  public void close() {
    JakartaWebSocketEndpointRegistry.Registration registration = endpointRegistration;
    endpointRegistration = null;
    if (registration != null) registration.release();
    callbacks.close();
    for (Session session : List.copyOf(sessions.keySet())) {
      closed(session);
      closeQuietly(session);
    }
    sessions.clear();
  }

  private void closeQuietly(Session session) {
    try {
      CloseReason reason = new CloseReason(
          CloseReason.CloseCodes.GOING_AWAY, "FantasticMock stopping");
      session.close(reason);
    } catch (IOException ignored) {
      // Nothing can be recovered while the endpoint is stopping.
    }
  }
}
