# HTTP Netty plugin

`http-netty` adds an HTTP/1.1 port served by [Netty](https://netty.io/), for load tests
that need more throughput or lower latency than the built-in listener. It serves your
existing HTTP templates (`settings.transport: http`) through the same engine as the main
port, so matching, variables, custom handlers, callbacks, proxy modes, traffic logs and
HTTP metrics behave exactly as they do there. Point your load test at the new port; nothing
else changes.

## Install and configure

Follow the [installation steps](../README.md#install) in the repository README. The plugin
folder is `protocol-plugins/http-netty/`: the plugin JAR and the Netty libraries sit in it.
Then add an instance to `application.yml` and restart Fantastic Mock:

```yaml
fantasticmock:
  protocols:
    instances:
      http-netty:
        protocol: http-netty
        enabled: true
        settings:
          exchange-mode: http
          host: 127.0.0.1
          port: 9083
```

All settings are declared in `protocol.yml` and apply to both exchange modes:

| Setting | Default | Meaning |
| --- | --- | --- |
| `exchange-mode` | `http` | `http` serves `http` templates through the host engine; `protocol` serves `http-netty` templates |
| `host` | `127.0.0.1` | Bind address |
| `port` | `9083` | Listening port; `0` picks a free port |
| `worker-threads` | `1` | Netty I/O threads; `0` uses Netty's default of twice the processors |
| `max-in-flight` | `1024` | Admitted requests across connections; excess requests get 503 |
| `max-content-length` | `16777216` | Aggregated request body limit; larger bodies get 413 |
| `max-initial-line-length` | `8192` | Request line limit; longer lines get 414 |
| `max-header-size` | `16384` | Header block limit; larger blocks get 431 |
| `max-chunk-size` | `8192` | Netty decoder chunk size |
| `idle-timeout-ms` | `30000` | Closes idle keep-alive connections; `0` disables |
| `compression-enabled` | `false` | Compresses responses when the client accepts it |

A misspelled setting is reported as a startup warning.

## Execution and ownership

Netty event loops only capture requests: headers and body bytes are copied once
and detached from Netty buffers. Each connection answers pipelined requests in
order, one at a time; while one is running the connection stops reading instead
of queueing without bound. The exchange runs on a virtual thread through the
protocol endpoint, which admits it against `max-in-flight`, keeps the instance
statistics and releases admission when the response write completes. Excess
work returns 503 and closes that connection.

Malformed requests are answered 400, 414 or 431 and never reach templates. The
instance health reports open connections, the bound address, the exchange mode
and in-flight requests; statistics count `messages`, `rejected` and `failures`,
and in `protocol` mode also `unmatched`.

In `http` mode the gateway returns status, headers and borrowed response bytes;
there is no JSON response-document conversion or defensive body copy. Chunked
request bodies are aggregated, and compressed request bytes are passed unchanged
to the host engine, which applies its normal body-decoding and proxy rules.
Management prefixes on this listener return 404. There is no TLS, HTTP/2 or
WebSocket support in this adapter.

## Protocol mode

`exchange-mode: protocol` uses the generic protocol pipeline and `http-netty`
templates, in the same format as built-in `http` templates: `request.method`
(case-insensitive, `ANY` matches every method), `request.pathRegex` (full match)
and `request.matchers`. Templates send one response, whose `delayMs` is applied
without holding a thread. A client that disconnects during the delay cancels the
exchange and releases its admission at once. Request bodies are decompressed
before matching.

Upgrading from 1.1.0: both modes now share the defaults above. Protocol mode
previously bound `0.0.0.0`, used Netty's default thread count and admitted
10000 requests; set `host`, `worker-threads: 0` and `max-in-flight` explicitly
to keep that behavior.

## Validation

`src/test/java` holds a plugin-level test using `ProtocolTestKit`. Fantastic Mock's own test
suite also compiles and loads this plugin, with socket tests for raw and encoded targets,
repeated headers, UTF-8 and chunked binary bodies, HEAD/204/205/304 framing, pipelined
ordering, concurrent slow work, body limits, malformed requests, overload recovery, reserved
paths, proxy redirects, logging and metrics, port cleanup and protocol mode.

Benchmark results that include this listener are on
[fantasticmock.com](https://fantasticmock.com/#performance).
