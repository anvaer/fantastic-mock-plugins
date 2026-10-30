package com.fantasticmock.example.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

import com.fasterxml.jackson.databind.node.TextNode;
import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.exchange.ProtocolResponse;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import com.fantasticmock.protocol.api.testing.ProtocolTestKit;
import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.SendHandler;
import jakarta.websocket.SendResult;
import jakarta.websocket.Session;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JakartaWebSocketDeliveryTest {
  private final Session session = mock(Session.class);
  private final AtomicInteger acknowledgements = new AtomicInteger();
  private final AtomicLong delay = new AtomicLong();
  private ProtocolTestKit kit;
  private JakartaWebSocketExchangeHandler handler;

  @BeforeEach
  void start() {
    when(session.getRequestURI()).thenReturn(URI.create("ws://localhost/events"));
    kit = ProtocolTestKit.of(new JakartaWebSocketProtocolPlugin())
        .exchanges(request -> ProtocolExchangeResult.matched("template", List.of(
            new ProtocolResponse(TextNode.valueOf("hello"), delay.get(), ignored -> acknowledgements.incrementAndGet()))));
    handler = new JakartaWebSocketExchangeHandler(kit.endpoint("test", Map.of()));
  }

  @AfterEach
  void stop() throws Exception {
    kit.close();
  }

  private long failures() {
    return kit.statistics("test").value(ProtocolStatistics.FAILURES);
  }

  @Test
  void closedSessionFailsWithoutAcknowledgingTheResponse() {
    var completion = handler.receive(session, "hi", handler.delivery(session));
    assertThatThrownBy(completion::join).hasCauseInstanceOf(ClosedChannelException.class);
    assertThat(failures()).isEqualTo(1);
    assertThat(acknowledgements).hasValue(0);
    verify(session, never()).getAsyncRemote();
  }

  @Test
  void acknowledgementWaitsForJakartaWriteConfirmation() {
    when(session.isOpen()).thenReturn(true);
    RemoteEndpoint.Async remote = mock(RemoteEndpoint.Async.class);
    when(session.getAsyncRemote()).thenReturn(remote);
    AtomicReference<SendHandler> callback = new AtomicReference<>();
    doAnswer(call -> { callback.set(call.getArgument(1)); return null; })
        .when(remote).sendText(eq("hello"), any(SendHandler.class));
    var completion = handler.receive(session, "hi", handler.delivery(session));
    assertThat(completion).isNotDone();
    assertThat(acknowledgements).hasValue(0);
    callback.get().onResult(new SendResult());
    assertThat(completion).isCompleted();
    assertThat(acknowledgements).hasValue(1);
    assertThat(kit.statistics("test").value(ProtocolStatistics.MESSAGES)).isEqualTo(1);
  }

  @Test
  void synchronousJakartaSendFailureIsCountedOnce() {
    when(session.isOpen()).thenReturn(true);
    RemoteEndpoint.Async remote = mock(RemoteEndpoint.Async.class);
    when(session.getAsyncRemote()).thenReturn(remote);
    doThrow(new IllegalStateException("session closed during write"))
        .when(remote).sendText(anyString(), any(SendHandler.class));
    assertThatThrownBy(() -> handler.receive(session, "hi", handler.delivery(session)).join())
        .hasCauseInstanceOf(IllegalStateException.class);
    assertThat(failures()).isEqualTo(1);
    assertThat(acknowledgements).hasValue(0);
  }

  @Test
  void closingDeliveryCancelsDelayedWebSocketWrites() {
    delay.set(60_000);
    var delivery = handler.delivery(session);
    var completion = handler.receive(session, "hi", delivery);
    delivery.close();
    assertThat(completion).isCompletedExceptionally();
    verify(session, never()).getAsyncRemote();
    assertThat(acknowledgements).hasValue(0);
    assertThat(failures()).isZero();
  }
}
