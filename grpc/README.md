# gRPC plugin: unary and server streaming

Mock gRPC services from their `.proto` definitions: unary and server-streaming methods,
served on a port of their own by grpc-java's Netty server. You describe your services with
a protobuf descriptor set; no generated Java code is needed. Requests and responses are
written as JSON in templates.

## Install

Follow the [installation steps](../README.md#install) in the repository README. The plugin
folder is `protocol-plugins/grpc/`: the plugin JAR, its libraries (gRPC, protobuf, Guava)
and `examples/hello.protoset` sit in it.

Then add an instance to `application.yml`. This is
[`examples/application-fragment.yml`](examples/application-fragment.yml):

```yaml
fantasticmock:
  protocols:
    instances:
      grpc-demo:
        protocol: grpc
        enabled: true
        settings:
          host: 127.0.0.1
          port: 9091
          descriptor-file: protocol-plugins/grpc/examples/hello.protoset
```

Paths such as `descriptor-file` are relative to the folder Fantastic Mock runs in, not to the
plugin folder. Copy [`examples/GRPC__Hello.json`](examples/GRPC__Hello.json) into your
templates folder (`mock-templates/` by default) and restart Fantastic Mock.

## Try it

Using a client with `examples/hello.proto`, call `demo.Greeter/SayHello` with
`{"name":"Ada"}`. The example returns `{"message":"Hello Ada"}` after 25 ms,
with response metadata `x-mock: fantasticmock`.

With grpcurl installed, from the plugin folder (PowerShell):

```powershell
'{"name":"Ada"}' | grpcurl -plaintext -import-path examples -proto hello.proto -d '@' localhost:9091 demo.Greeter/SayHello
```

Server reflection is not enabled: supply the proto or descriptor to your client.
No generated Java stubs are required by the plugin.

## Your own service

Generate a descriptor set with all imports:

```text
protoc --proto_path=protos --include_imports --descriptor_set_out=service.protoset service.proto
```

Point `descriptor-file` at it, or send it with the Upload button where the admin manages endpoints
(the setting is declared `format: file`). All unary and server-streaming methods are registered at
instance startup; client-streaming and bidirectional methods are left out (a call to one gets
`UNIMPLEMENTED`) and listed as `notServed` in the instance health. Restart/reload the instance
after changing descriptors.

Set template `settings.transport` to `grpc` and `settings.endpoint` to your instance ID; a
template without an endpoint serves only an instance named `default`. Match the full method
name without a leading slash:

```json
{ "location": "request.method", "regex": "\\A\\Qdemo.Greeter/SayHello\\E\\z" }
```

The strict literal also enables the core's prepared routing index. Direct routing
fields are supported too: `request.method` and `request.service` can be set
directly in the template request to require exact values, without a matcher; the
method selects the route-index bucket.

| Request source | Value |
| --- | --- |
| `request.service` | Fully qualified service, e.g. `demo.Greeter` |
| `request.method` | Fully qualified service/method |
| `request.header.<key>` | Metadata; last value for repeated keys |
| `request.body.<field>` | Decoded protobuf JSON, including nested fields |

The request is converted to protobuf JSON and its metadata copied only when a
template, variable or traffic log reads them; routing on the method alone never
prints the body.

Protobuf JSON conventions apply: lower-camel-case names, string-encoded 64-bit
integers, base64 bytes and omitted unset/default fields. Binary metadata keys
ending in `-bin` use base64 strings. Metadata keys must be valid gRPC keys.

Responses contain `status` (gRPC status name, default `OK`), optional `description`,
`headers`, `trailers`, and `body`. Body accepts an object or a string containing protobuf JSON.
Unknown protobuf response fields are rejected. Non-OK statuses send no message:

```json
{ "status": "PERMISSION_DENIED", "description": "Account is blocked" }
```

`headers` are sent before the message and `trailers` with the status, which is where
clients read error details such as `grpc-status-details-bin`:

```json
{ "status": "INVALID_ARGUMENT", "trailers": { "grpc-status-details-bin": "CAMSC2JhZCByZXF1ZXN0" } }
```

Core variables, matchers, delays and logging are reused. A unary method answers with the
template's first response; should a template give more, the others are not sent and count as
failures. An unmatched call returns `NOT_FOUND`. Callbacks to other transports can be attached
to a gRPC template, and templates from any transport can send outbound gRPC callbacks.

## Server streaming

For a server-streaming method every response of the template is one message of the stream,
sent in order, each after its own `delayMs`; `repeat` sends a response several times and
`until` keeps sending it until its condition is met or the client cancels. The stream ends
with `OK` after the last response; the `headers` of the first response open it and the
`trailers` of the last close it. A response with another `status` ends the stream there with
that status and no message. A quote stream, 100 messages as fast as the client reads them:

```json
"response": [ { "body": { "symbol": "{{symbol}}", "bid": 125.5, "ask": 125.52 }, "repeat": 100 } ]
```

and a tick every millisecond until the client cancels:

```json
"response": [ { "body": { "symbol": "{{symbol}}", "bid": 125.5, "ask": 125.52 }, "delayMs": 1, "until": "false" } ]
```

Each message waits until the client's flow-control window has room, so a slow client holds
the stream back instead of filling the mock's memory. A stream holds one `max-in-flight` slot
for its whole life. Server-streaming methods cannot be outbound callbacks or passed on to an
upstream.

## Outbound callbacks

Add this object to a template's `callbacks` array:

```json
{
  "transport": "grpc",
  "endpoint": "grpc-demo",
  "url": "grpc://127.0.0.1:9092",
  "method": "demo.Greeter/SayHello",
  "descriptorFile": "protocol-plugins/grpc/examples/hello.protoset",
  "headers": { "x-callback": "fantasticmock" },
  "body": "{\"name\":\"{{name}}\"}",
  "timeoutMs": 5000,
  "delayMs": 100,
  "async": true
}
```

`endpoint` selects the local plugin instance; `url` selects the remote gRPC
server. Use `grpc://host:port` for plaintext or `grpcs://host:port` for TLS with
the JVM's default trust configuration. The URL requires an explicit port and no
method path; `method` is the full service/method name (an initial `/` is accepted).
The method and its message types come from the instance's descriptor set
(`descriptor-file`), or from `descriptorFile` when the callback names one: any
`protoc --include_imports` descriptor set, so a callback can call a service the
instance does not serve. It is read once, and again when the file changes; a
relative path is read from the working directory. `body` is the request as
protobuf JSON text: a template stores a callback's `body` as a string, so an
object there fails the template. Unknown protobuf fields are rejected. `headers` accepts scalar values or arrays for repeated metadata;
binary keys ending in `-bin` require base64 values.

Core renders variables, applies callback matchers and delays, and dispatches each
callback. The plugin sends one unary RPC and completes the callback only after
one response and an `OK` status. Remote errors and deadline failures propagate
to core. With FantasticMock 1.0.6 or later, Traffic logs show the request and the
reply as entries of their own (the reply with its status, metadata, trailers and
body, as a template writes a response); an earlier host logs the callback once, as
rendered, and not the reply. The callback completes with the encoded
request payload size after success. `timeoutMs` must be a positive integer and
defaults to 5000, including connection setup. Channels are reused per target, as
gRPC intends, instead of paying a TCP, HTTP/2 and TLS handshake per callback; the
least recently used beyond 64 targets is shut down gracefully, and all channels
close when the instance stops.

## Limits and execution

| Instance setting | Default |
| --- | --- |
| `host` | `127.0.0.1` |
| `port` | `9091` |
| `descriptor-file` | `protocol-plugins/grpc/examples/hello.protoset` |
| `max-in-flight` | `1024` |
| `max-message-bytes` | `4194304` (inbound requests, callback requests and replies) |

Descriptors are resolved once. Each call runs through `ProtocolEndpoint.handleAsync`
on a virtual thread, not a Netty event loop, and delays use the shared delivery
scheduler. Admission stays occupied until the response is written; overload
returns `RESOURCE_EXHAUSTED`. HTTP/2 also limits streams per connection.
Cancellation and deadlines cancel the exchange, dropping a pending delayed
response and releasing its admission at once. Health reports the bound address,
active calls and the standard `messages`, `rejected`, `unmatched` and `failures`
counters.

Callbacks have separate admission with the same `max-in-flight` limit, so an
inbound exchange can await a synchronous callback without competing for its own
admission slot. Pending callbacks fail with `CANCELLED` when the instance stops,
and closing waits for the listener to release its port.

Logging acknowledgement follows gRPC server `onComplete`, not scheduling. This is
server-side completion, **not proof of client receipt**, as documented by
[grpc-java](https://grpc.github.io/grpc-java/javadoc/io/grpc/ServerCall.Listener.html).

The inbound listener supports unary and server-streaming RPCs over plaintext HTTP/2 only. No
client or bidirectional streaming, server TLS, built-in authentication, reflection or
wire-level proxy/recording.
Bind beyond loopback only in an isolated test environment. Outbound callbacks
also support TLS and authentication metadata, but not custom trust stores or mTLS
settings.

## Implementation

- `GrpcProtocolPlugin`: entry point; creates the runtime for an endpoint.
- `GrpcSchema`: descriptor imports, method lookup and protobuf JSON codecs.
- `GrpcRuntime`: listener lifecycle, health details and the callback sender.
- `GrpcCall`: lazy request fields, response writing (one reply, or a stream with flow
  control) and cancellation.
- `GrpcCallbackClient`: outbound unary calls, cached channels and deadlines.
- `GrpcMetadata`: one metadata conversion for responses, callbacks and requests.
- `GrpcUpstream`, `GrpcWire`: the optional `ProtocolUpstreamClient` of an instance. A received
  call is sent on to another server at the HTTP/2 level, with the headers and the protobuf
  it came with, and its reply is handed back as it arrived. The rules of an upstream may
  change `header` and `body` (`rewritableFields`): a changed header keeps its place and a
  new one follows the others; the message is changed as JSON, whole or one field by its
  path, and written anew only then. The method that is called is not a rule's to change.

`protocol.yml` declares UI fields, sources, routing and settings. Tests in
`src/test/java` use `ProtocolTestKit` and exercise
real network calls, metadata, delays, errors, cancellation, overload, size limits
and descriptor validation, plus outbound callbacks, admission, failure and shutdown.

Dependencies follow the [grpc-java setup](https://github.com/grpc/grpc-java#download).
Shaded Netty keeps its transport implementation separate from any other Netty copy; the host itself bundles none.
