package com.fantasticmock.example.grpc;

import java.util.Optional;
import com.fantasticmock.protocol.api.plugin.*;

/**
 * Unary gRPC: core owns matching, variables and delays; grpc-java owns HTTP/2 and protobuf.
 * Routing and the one-response rule are declared in protocol.yml.
 */
public final class GrpcProtocolPlugin implements ProtocolPlugin {
  @Override
  public Optional<ProtocolClientHint> clientHint() {
    return Optional.of(ProtocolClientHint.listener("grpc", "grpcs",
        "Use a gRPC client with your .proto or descriptor set and the template's service/method."));
  }

  @Override
  public ProtocolPluginInstance createInstance(ProtocolEndpoint endpoint) {
    return new GrpcRuntime(endpoint);
  }
}
