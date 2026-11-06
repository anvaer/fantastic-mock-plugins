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

`settings.max-in-flight` (default 1024) is how many messages an endpoint handles at once,
answers still waiting for their delay included. A message beyond it gets no answer and no
log entry; the Dashboard shows it as refused on the endpoint's card. A callback that waits
for replies from one of Fantastic Mock's own endpoints holds a place there until the last
reply is sent, so raise it when many such callbacks run at once.

An endpoint accepts exactly its path, so a client on `/events/42` does not connect to `/events`.
A placeholder lets the path vary: `endpoint-path: /orders/{id}` accepts `/orders/42` and
`/orders/99`, and a template can match on or read `request.path.segment1`. The Example
request asks for a path only on such an endpoint; on a fixed path, tests use that path.

## Try it

Open [`examples/client.html`](examples/client.html) in a browser and click **Connect and
send**. It connects to `ws://localhost:8080/events?tenant=demo`, sends
`{"type":"order.created","orderId":"ORD-42"}` and shows the two answers:

```text
{"type":"order.accepted","orderId":"ORD-42","tenant":"demo"}
{"type":"order.ready","orderId":"ORD-42"}
```

The address and message are editable: change the address if your endpoint has another
`endpoint-path` or port, such as `ws://localhost:8080/websocket`.

Then the template's callback connects to `/callbacks` and sends an `order.audit` message,
which shows up in **Traffic logs** next to the two answers. The callback address in the
example is `ws://localhost:8080/callbacks`; change it if your `server.port` is not 8080.

## Templates

Set `settings.transport` to `websocket-jakarta` and `settings.endpoint` to your instance ID.
Match messages with `request.matchers`:

| Request source | Value |
| --- | --- |
| `request.path` | The path the client connected to. With a fixed `endpoint-path` it is always that path, so the Example request does not ask for it; with a placeholder such as `/orders/{id}` you enter one there |
| `request.path.segment<N>` | One segment of that path, counting from `0`: with `endpoint-path: /orders/{id}`, a client on `/orders/42` gives `segment1` = `42`. Needs Fantastic Mock 1.0.6 or later |
| `request.query.<name>` | A query parameter of the connection URL |
| `request.body` | The text message |
| `request.body.<field>` | A field of a JSON message, such as `request.body.orderId` |
| `request.session` | The ID of the WebSocket session the message came on, for a response to echo; templates do not match on it |

In **Traffic logs** a message's **Message** tab holds only its body. The session ID, path and
query say which connection it came on, so they are under **Details** (Fantastic Mock 1.0.6 or
later; earlier versions show them with the message).

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

Closing the connection cancels the responses still waiting. A message none of whose answers
was sent by then still shows up in **Traffic logs**, marked **Cancelled** (Fantastic Mock 1.0.6
or later). Messages of one request are sent in order; answers to different messages on the
same connection may interleave.

### Callbacks

A callback opens a new client connection to another WebSocket server, sends one text message
and closes the connection. It does not write to the client's own connection. It can also keep
the connection open and wait for the server's replies first.

```json
"callbacks": [
  {
    "transport": "websocket-jakarta",
    "url": "ws://localhost:8080/callbacks",
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

#### Waiting for replies

| Field | Default | Meaning |
| --- | --- | --- |
| `awaitReplies` | `0` | Keep the connection open until this many replies arrived. `0` closes it right after sending |
| `replyPattern` | none | A regular expression a reply must contain to count toward `awaitReplies`. Set alone, the callback waits for one such reply |
| `timeoutMs` | `10000` | Longest wait for the replies, from when the message was sent |
| `ping` | none | A text message sent on the connection while the callback waits, such as `{"op":"ping"}` |
| `pingIntervalMs` | `30000` | Time between two pings; the first goes one interval after the message |

`timeoutMs` does not cover opening the connection: the application's WebSocket client gives up
on a handshake after about 6 seconds (Jetty's 5-second connect timeout plus one second), time
waiting for a free connection included. The template editor shows each default in its empty
field, and the ping fields together under **Ping**.

This callback subscribes to an order's updates, keeps the connection alive every 5 seconds and
closes it once the order is filled, or fails after a minute:

```json
"callbacks": [
  {
    "transport": "websocket-jakarta",
    "url": "ws://localhost:9000/orders",
    "body": "{\"op\":\"subscribe\",\"orderId\":\"{{orderId}}\"}",
    "replyPattern": "\"status\":\"filled\"",
    "timeoutMs": 60000,
    "ping": "{\"op\":\"ping\"}",
    "pingIntervalMs": 5000,
    "async": true
  }
]
```

Each message of the callback's connection is a log entry of its own in **Traffic logs**, in
the chain of the request that started it. The entries cover the message the callback sent,
each ping, and every message that arrived, including those that do not match `replyPattern`.
The Status column says **sent** or **received**. Each entry shows the one message it holds:
its **Message** tab has only what went over the connection, the body, and the handshake
headers on the first message. Settings such as `timeoutMs` or `pingIntervalMs` are not logged.
**Details** has the connection's address (**Callback URL**), the client session's
**Session ID**, and the **Direction**.

When the replies did not come in time, or the server closed the connection first, the
callback is **Failed**: one more entry gives the reason. A callback with `"async": false`
holds the template's own response until it is over, replies included. Values are rendered
like the body, so `"awaitReplies": "{{count}}"` works.

When the callback calls a Fantastic Mock WebSocket endpoint, that endpoint's templates match
its pings like any other message. To leave them unanswered, give them a template of their
own with an empty response list (Fantastic Mock 1.0.6 or later; earlier versions send one
empty message):

```json
{
  "settings": { "transport": "websocket-jakarta", "endpoints": ["ws-test"] },
  "request": { "matchers": [ { "location": "request.body", "regex": "^ping$" } ] },
  "response": []
}
```

Messages are logged one by one by Fantastic Mock 1.0.6 or later. An earlier version waits and
pings all the same, and logs the callback once, as the document it sent.

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
