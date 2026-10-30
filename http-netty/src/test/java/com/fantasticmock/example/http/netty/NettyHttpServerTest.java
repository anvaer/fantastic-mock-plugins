package com.fantasticmock.example.http.netty;

import static org.assertj.core.api.Assertions.assertThat;

import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import com.fantasticmock.protocol.api.testing.ProtocolTestKit;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NettyHttpServerTest {

  @Test
  void protocolModeProjectsChunkedRequestsAndWritesTheRenderedResponse() throws Exception {
    AtomicReference<ProtocolMessageView> captured = new AtomicReference<>();
    try (var kit = ProtocolTestKit.of(new NettyHttpProtocolPlugin())) {
      var response = kit.objectMapper().createObjectNode();
      response.put("status", 201);
      response.putObject("headers").put("Content-Type", "text/plain; charset=utf-8");
      response.put("body", "accepted");
      kit.exchanges(request -> {
        captured.set(request);
        return ProtocolTestKit.respond(response);
      });
      var instance = kit.start("netty-test", Map.of("exchange-mode", "protocol", "port", 0));
      String address = String.valueOf(instance.healthDetails().get("localAddress"));
      int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create("http://127.0.0.1:" + port + "/chunked?part=one"))
          .header("Content-Type", "text/plain")
          .header("X-Request-Id", "netty-test")
          .POST(HttpRequest.BodyPublishers.ofInputStream(() ->
              new ByteArrayInputStream("request-body".getBytes(StandardCharsets.UTF_8))))
          .build();

      HttpResponse<String> reply = HttpClient.newHttpClient().send(
          request, HttpResponse.BodyHandlers.ofString());

      assertThat(reply.statusCode()).isEqualTo(201);
      assertThat(reply.body()).isEqualTo("accepted");
      assertThat(captured.get().protocol()).isEqualTo("http-netty");
      assertThat(captured.get().endpoint()).isEqualTo("netty-test");
      assertThat(captured.get().text("request.method")).contains("POST");
      assertThat(captured.get().text("request.path")).contains("/chunked");
      assertThat(captured.get().text("request.query.part")).contains("one");
      assertThat(captured.get().text("request.header.x-request-id")).contains("netty-test");
      assertThat(captured.get().text("request.body")).contains("request-body");
      assertThat(kit.statistics("netty-test").value(ProtocolStatistics.MESSAGES)).isEqualTo(1);
    }
  }
}
