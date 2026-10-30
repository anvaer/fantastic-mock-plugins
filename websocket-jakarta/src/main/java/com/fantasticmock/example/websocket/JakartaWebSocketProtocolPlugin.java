package com.fantasticmock.example.websocket;

import java.util.Optional;
import com.fantasticmock.protocol.api.plugin.ProtocolClientHint;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolPlugin;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;

/** WebSocket protocol SPI entry point. Metadata and projection live in protocol.yml. */
public final class JakartaWebSocketProtocolPlugin implements ProtocolPlugin {
  @Override
  public Optional<ProtocolClientHint> clientHint() {
    return Optional.of(ProtocolClientHint.applicationListener("ws", "wss",
        "Connect with a WebSocket client and send a message matching your template."));
  }

  @Override
  public ProtocolPluginInstance createInstance(ProtocolEndpoint endpoint) {
    return new JakartaWebSocketRuntime(endpoint);
  }
}
