package com.fantasticmock.example.http.netty;

import java.util.Optional;
import com.fantasticmock.protocol.api.plugin.ProtocolClientHint;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolPlugin;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;

/**
 * HTTP/1.1 on a dedicated Netty listener. Routing, settings and the template schema live in
 * protocol.yml; this class only creates the listener.
 */
public final class NettyHttpProtocolPlugin implements ProtocolPlugin {
  @Override
  public Optional<ProtocolClientHint> clientHint() {
    return Optional.of(ProtocolClientHint.listener("http", "https",
        "Use an HTTP client with the path and method from your template."));
  }

  @Override
  public ProtocolPluginInstance createInstance(ProtocolEndpoint endpoint) {
    return new NettyHttpServer(endpoint);
  }
}
