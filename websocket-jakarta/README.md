# WebSocket plugin

Mock WebSocket services on Fantastic Mock's own HTTP port. A client connects to
`ws://localhost:8080/events`, sends text messages, and gets back the messages your templates
describe: one answer, an ordered sequence with delays, or a message repeated until something
happens. Callbacks open their own connection to another WebSocket server.

    http://localhost:8080/__admin/     the Fantastic Mock workspace
    ws://localhost:8080/events         the WebSocket endpoint

The plugin brings no libraries: Fantastic Mock's own web server provides WebSocket support.

## Install

Follow the [installation steps](../README.md#install) in the repository README. The plugin
folder is `protocol-plugins/websocket-jakarta/` and holds the plugin JAR.

Then add the instances to `application.yml`. This is
[`examples/application-fragment.yml`](examples/application-fragment.yml): the first instance
serves `/events`, the second `/callbacks`, where the example template sends its callback.

```yaml
fantasticmock:
  protocols:
    instances:
      ws-jakarta-demo:
        protocol: websocket-jakarta
        enabled: true
      ws-jakarta-callback-receiver:
        protocol: websocket-jakarta
        enabled: true
        settings:
          endpoint-path: /callbacks
```

Copy [`examples/WS_JAKARTA__OrderCreated.json`](examples/WS_JAKARTA__OrderCreated.json) into
your templates folder (`mock-templates/` by default) and restart Fantastic Mock.

The plugin does not open a port of its own. It uses Fantastic Mock's `server.port`, so to
serve WebSockets on port 9090, set `server.port: 9090`; the workspace moves to that port too.
`settings.endpoint-path` changes the path (default `/events`); `host` and `port` settings
have no effect here.

## Try it

Open [`examples/client.html`](examples/client.html) in a browser and click **Connect and
send**. It connects to `ws://localhost:8080/events?tenant=demo`, sends
`{"type":"order.created","orderId":"ORD-42"}` and shows the two answers:

```text
{"type":"order.accepted","orderId":"ORD-42","tenant":"demo"}
{"type":"order.ready","orderId":"ORD-42"}
```

Then the template's callback connects to `/callbacks` and sends an `order.audit` message,
which shows up in **Traffic logs** next to the two answers.

## Templates

Set `settings.transport` to `websocket-jakarta` and `settings.endpoint` to your instance ID.
Match messages with `request.matchers`:

| Request source | Value |
| --- | --- |
| `request.path` | The path the client connected to, such as `/events` |
| `request.url` | The full connection URL |
| `request.query.<name>` | A query parameter of the connection URL |
| `request.body` | The text message |
| `request.body.<field>` | A field of a JSON message, such as `request.body.orderId` |
| `request.messageType` | Always `text` |

```json
"request": {
  "matchers": [
    { "location": "request.path", "regex": "^/events$" },
    { "location": "request.body.type", "regex": "^order\\.created$" }
  ]
}
```

The same sources feed variables: `{ "name": "orderId", "source": "request.body.orderId" }`.

### Responses

A response has a `body`, the text to send. Use a `response` array to send several messages on
the client's connection, in order; each item's `delayMs` (or `minDelayMs`/`maxDelayMs`) is
waited before that item:

```json
"response": [
  { "body": "{\"type\":\"order.accepted\",\"orderId\":\"{{orderId}}\"}" },
  { "body": "{\"type\":\"order.ready\",\"orderId\":\"{{orderId}}\"}", "delayMs": 25 }
]
```

A response item can also be sent several times with `repeat` (the total number of sends), or
again and again `until` a condition is met. The condition is checked after each copy; it is
met unless it renders empty, `false` or `0`. This template sends `in_transit` every second
until another call, for example an HTTP template that runs `{{put("delivered", orderId, "yes")}}`,
reports the order as delivered:

```json
"response": [
  { "body": "{\"type\":\"accepted\",\"orderId\":\"{{orderId}}\"}" },
  { "body": "{\"type\":\"in_transit\",\"orderId\":\"{{orderId}}\"}", "delayMs": 1000,
    "until": "{{poll(\"delivered\", orderId)}}" },
  { "body": "{\"type\":\"delivered\",\"orderId\":\"{{orderId}}\"}" }
]
```

While such loops run, the workspace header shows them, with a Stop button for each.

Closing the connection cancels the responses still waiting. Messages of one request are sent
in order; answers to different messages on the same connection may interleave.

### Callbacks

A callback opens a new client connection to another WebSocket server, sends one text message
and closes the connection. It does not write to the client's own connection.

```json
"callbacks": [
  {
    "transport": "websocket-jakarta",
    "url": "{{callbackBase}}/callbacks",
    "headers": { "X-FantasticMock-Callback": "order-audit" },
    "body": "{\"type\":\"order.audit\",\"orderId\":\"{{orderId}}\"}",
    "delayMs": 50,
    "async": true
  }
]
```

`url` is the `ws://` or `wss://` address to connect to; `headers` are added to the opening
handshake. Fantastic Mock picks an enabled instance of the plugin to send it, so `endpoint`
is not needed. Callbacks of other transports work too: a WebSocket template can call an HTTP
webhook with an `http` callback.

## Limits

Text messages only. Binary messages, origin rules, subprotocols and connection limits are
not supported. Changing `endpoint-path` needs a restart of Fantastic Mock: WebSocket servers
cannot remove an endpoint while they run.

## Implementation

    src/main/java/com/fantasticmock/example/websocket/
      JakartaWebSocketProtocolPlugin.java       entry point
      JakartaWebSocketRuntime.java              lifecycle and sessions
      JakartaWebSocketExchangeHandler.java      message and async-write adapter
      JakartaWebSocketCallbackClient.java       outbound callback sessions
      JakartaWebSocketEndpoint.java             Jakarta WebSocket adapter
      JakartaWebSocketEndpointRegistry.java     one endpoint per path, kept across reloads

`protocol.yml` holds the descriptor, the endpoint, the message fields and the template fields.
Matching comes from the protocol API; the Java code only does WebSocket session I/O.

Each session uses `endpoint.responseDelivery(writer)`, and each message is handled with
`endpoint.handle(message, session)`. Fantastic Mock sequences the responses, schedules the
delays, records confirmed writes in the traffic log and counts each failed exchange once. The
plugin only converts the response body and wraps `AsyncRemote.sendText` in a completion
stage. A write already handed to the WebSocket server cannot be recalled; if it succeeds
later, it is still logged.

The endpoint adapter is registered once, and its runtime is replaced when protocol plugins
are reloaded: open sessions finish on the previous runtime, new connections use the new one.

Tests in `src/test/java` use `ProtocolTestKit`, with Jetty's WebSocket implementation (the one
Fantastic Mock runs on) on the test class path.
