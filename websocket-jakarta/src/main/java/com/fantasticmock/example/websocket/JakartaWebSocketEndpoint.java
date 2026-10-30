package com.fantasticmock.example.websocket;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Thin, stable Jakarta adapter whose runtime delegates can be replaced on reload. */
public final class JakartaWebSocketEndpoint extends Endpoint {
  private Object[] sessionDelegates;

  @Override
  public void onOpen(Session session, EndpointConfig config) {
    Object configured = config.getUserProperties()
        .get(JakartaWebSocketEndpointRegistry.ENDPOINT_BINDING_KEY);
    if (!(configured instanceof AtomicReference<?> reference)) return;
    Object value = reference.get();
    if (!(value instanceof Object[] delegates)) return;
    sessionDelegates = delegates;
    callback(delegates, 0).accept(session);
  }

  @Override
  public void onClose(Session session, CloseReason reason) {
    if (sessionDelegates != null) callback(sessionDelegates, 1).accept(session);
  }

  @Override
  public void onError(Session session, Throwable error) {
    if (sessionDelegates != null) failureCallback(sessionDelegates).accept(error);
  }

  @SuppressWarnings("unchecked")
  private Consumer<Session> callback(Object[] delegates, int index) {
    return (Consumer<Session>) delegates[index];
  }

  @SuppressWarnings("unchecked")
  private Consumer<Throwable> failureCallback(Object[] delegates) {
    return (Consumer<Throwable>) delegates[2];
  }
}
