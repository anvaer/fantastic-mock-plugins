package com.fantasticmock.example.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fantasticmock.protocol.api.metadata.ProtocolCapability;
import com.fantasticmock.protocol.api.metadata.ProtocolMessageDefinition;
import com.fantasticmock.protocol.api.plugin.ProtocolPlugin;
import com.fantasticmock.protocol.api.plugin.ProtocolValidation;
import com.fantasticmock.protocol.api.testing.ProtocolTestKit;
import jakarta.servlet.ServletContext;
import jakarta.websocket.server.ServerContainer;
import jakarta.websocket.server.ServerEndpointConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JakartaWebSocketProtocolPluginExampleTest {

  @Test
  void protocolYmlNamesThePluginAndDerivesCallbackSupport() {
    ProtocolPlugin plugin = new JakartaWebSocketProtocolPlugin();
    var definition = plugin.definition();
    assertThat(definition.provider()).isEqualTo(JakartaWebSocketProtocolPlugin.class.getName());
    assertThat(definition.descriptor().displayName()).isEqualTo("WebSocket (Jakarta)");
    assertThat(definition.runtime().endpoint().path()).isEqualTo("/events");
    assertThat(definition.message().request().get("query").decoders()).containsExactly("query");
    assertThat(definition.message().request().get("body").decoders()).containsExactly("json", "xml");
    assertThat(definition.template().callback().keySet()).contains("url", "headers", "body");
    assertThat(definition.message().request().get("body").use()).contains(
        ProtocolMessageDefinition.Use.MATCHER, ProtocolMessageDefinition.Use.VARIABLE);
    assertThat(ProtocolValidation.plugin(plugin).capabilities()).contains(
        ProtocolCapability.NATIVE_RUNTIME, ProtocolCapability.REQUEST_RESPONSE, ProtocolCapability.CALLBACKS);
  }

  @Test
  void mountsJakartaEndpointThroughGenericHostService() throws Exception {
    ServerContainer serverContainer = mock(ServerContainer.class);
    ServletContext servletContext = mock(ServletContext.class);
    try (var kit = ProtocolTestKit.of(new JakartaWebSocketProtocolPlugin())
        .hostService(ServerContainer.class, serverContainer)
        .hostService(ServletContext.class, servletContext)) {
      var instance = kit.start("ws-jakarta-test", Map.of());

      ArgumentCaptor<ServerEndpointConfig> config = ArgumentCaptor.forClass(ServerEndpointConfig.class);
      verify(serverContainer).addEndpoint(config.capture());
      assertThat(config.getValue().getPath()).isEqualTo("/events");
      assertThat(config.getValue().getEndpointClass()).isEqualTo(JakartaWebSocketEndpoint.class);
      assertThat(instance.healthDetails()).containsEntry("path", "/events").containsEntry("sessions", 0);
    }
  }
}
