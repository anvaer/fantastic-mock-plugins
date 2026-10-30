package com.fantasticmock.example.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.bootstrap.Bootstrap;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFuture;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2DataFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2HeadersFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ResetFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2StreamChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.netty.shaded.io.netty.util.concurrent.ScheduledFuture;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import javax.net.ssl.SSLParameters;

/**
 * Unary gRPC written at the HTTP/2 level, for calls that are passed on to another server. A
 * gRPC channel writes several headers of every call itself (its own user agent, the encodings
 * it accepts), so a request sent through one does not arrive as it came. Here every header
 * goes out as it was received and the message as the bytes of the received protobuf.
 *
 * <p>Only what describes this connection, not the call, is this side's own: the authority is
 * the target's, and of the encodings the caller accepts those are offered that can be read
 * here ({@code identity}, {@code gzip}, {@code deflate}), since the reply has to be read to be
 * logged and passed back. One connection is kept for each target and shared by its calls.</p>
 */
final class GrpcWire implements AutoCloseable {
  /** One header as it is on the wire: a lowercase name, and a binary value as base64. */
  record Header(String name, String value) {}

  /**
   * What the other server answered. {@code headers} is null when it answered with trailers
   * alone; {@code message} is the protobuf as sent, uncompressed, or null when there is none;
   * {@code encoding} is how the message was compressed on its way here.
   */
  record Reply(Status status, List<Header> headers, List<Header> trailers, byte[] message, String encoding) {}

  private static final int MAX_CONNECTIONS = 8;
  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final Set<String> READABLE = Set.of("identity", "gzip", "deflate");
  /** Streams a server opens itself are no part of a unary call. */
  private static final ChannelInboundHandlerAdapter IGNORED = new Ignored();

  @ChannelHandler.Sharable
  private static final class Ignored extends ChannelInboundHandlerAdapter {
  }

  private final int maxMessageBytes;
  private final Set<CompletableFuture<Reply>> active = ConcurrentHashMap.newKeySet();
  private final Map<GrpcCallbackClient.Target, Connection> connections =
      new LinkedHashMap<GrpcCallbackClient.Target, Connection>(8, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<GrpcCallbackClient.Target, Connection> eldest) {
          if (size() <= MAX_CONNECTIONS) return false;
          eldest.getValue().close(); // A target that is no longer used; a route has one target.
          return true;
        }
      };
  private volatile EventLoopGroup loops;
  private SslContext tls;
  private volatile boolean closed;

  GrpcWire(int maxMessageBytes) {
    this.maxMessageBytes = maxMessageBytes;
  }

  /**
   * Sends one call and completes with the reply. A server that cannot be reached, or that
   * ends the call without a gRPC status, is reported as a reply with the status gRPC gives
   * such a call; the future fails only when this instance stops, and is cancelled by the
   * caller to abandon the call.
   */
  CompletableFuture<Reply> call(GrpcCallbackClient.Target target, String method, List<Header> headers, byte[] message) {
    var completion = new CompletableFuture<Reply>();
    if (closed) {
      completion.completeExceptionally(stopped());
      return completion;
    }
    active.add(completion);
    completion.whenComplete((reply, failure) -> active.remove(completion));
    try {
      new Exchange(target, method, headers, message, completion).send(true);
    } catch (Exception | Error failure) {
      completion.completeExceptionally(failure);
    }
    if (closed) completion.completeExceptionally(stopped());
    return completion;
  }

  @Override
  public void close() {
    closed = true;
    for (var completion : List.copyOf(active)) completion.completeExceptionally(stopped());
    EventLoopGroup group;
    synchronized (connections) {
      connections.values().forEach(Connection::close);
      connections.clear();
      group = loops;
    }
    if (group != null) group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
  }

  private static StatusRuntimeException stopped() {
    return Status.CANCELLED.withDescription("gRPC instance stopped").asRuntimeException();
  }

  /** The connection to a target, opened when the first call needs it and replaced once it is going away. */
  private Connection connection(GrpcCallbackClient.Target target) throws Exception {
    synchronized (connections) {
      if (closed) throw stopped();
      Connection connection = connections.get(target);
      if (connection == null || connection.closing) {
        if (loops == null) {
          loops = new NioEventLoopGroup(Math.min(4, Runtime.getRuntime().availableProcessors()),
              new DefaultThreadFactory("grpc-relay", true));
        }
        if (target.tls() && tls == null) tls = GrpcSslContexts.forClient().build();
        connection = new Connection(target);
        connections.put(target, connection);
        connection.open(loops, tls);
      }
      return connection;
    }
  }

  /** One HTTP/2 connection. Calls already on it finish after a GOAWAY; new ones get another. */
  private static final class Connection extends ChannelInboundHandlerAdapter {
    private final GrpcCallbackClient.Target target;
    private final CompletableFuture<Channel> ready = new CompletableFuture<>();
    private volatile Channel channel;
    private volatile boolean closing;

    private Connection(GrpcCallbackClient.Target target) {
      this.target = target;
    }

    private void open(EventLoopGroup loops, SslContext tls) {
      Connection connection = this;
      new Bootstrap().group(loops).channel(NioSocketChannel.class)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) {
              if (target.tls()) {
                SslHandler ssl = tls.newHandler(channel.alloc(), target.host(), target.port());
                // The certificate has to be the target's, as a gRPC channel requires.
                SSLParameters parameters = ssl.engine().getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.engine().setSSLParameters(parameters);
                channel.pipeline().addLast(ssl);
              }
              channel.pipeline().addLast(
                  Http2FrameCodecBuilder.forClient().encoderEnforceMaxConcurrentStreams(true).build(),
                  new Http2MultiplexHandler(IGNORED),
                  connection);
            }
          })
          .connect(target.host(), target.port())
          .addListener((ChannelFuture connected) -> {
            if (!connected.isSuccess()) {
              fail(connected.cause());
              return;
            }
            Channel connectedChannel = connected.channel();
            channel = connectedChannel;
            if (closing) connectedChannel.close();
            SslHandler ssl = connectedChannel.pipeline().get(SslHandler.class);
            if (ssl == null) {
              ready.complete(connectedChannel);
              return;
            }
            ssl.handshakeFuture().addListener(handshake -> {
              if (!handshake.isSuccess()) fail(handshake.cause());
              else if (!"h2".equals(ssl.applicationProtocol())) fail(new IOException("The server does not offer HTTP/2"));
              else ready.complete(connectedChannel);
            });
          });
    }

    private void fail(Throwable cause) {
      closing = true;
      ready.completeExceptionally(cause);
      close();
    }

    private void close() {
      closing = true;
      Channel connected = channel;
      if (connected != null) connected.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object frame) {
      if (frame instanceof Http2GoAwayFrame) closing = true;
      ReferenceCountUtil.release(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
      closing = true;
      ready.completeExceptionally(new IOException("The connection was closed"));
      context.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      closing = true;
      ready.completeExceptionally(cause);
      context.close();
    }
  }

  /** One call: its request as it goes out, and what has arrived of the reply. */
  private final class Exchange {
    private final GrpcCallbackClient.Target target;
    private final Http2Headers request;
    private final byte[] payload;
    private final CompletableFuture<Reply> completion;
    private final Long timeoutNanos;
    private volatile ScheduledFuture<?> timer;
    private volatile long deadline;

    private Exchange(GrpcCallbackClient.Target target, String method, List<Header> headers, byte[] message,
        CompletableFuture<Reply> completion) throws IOException {
      this.target = target;
      this.completion = completion;
      String encoding = null;
      String timeout = null;
      boolean contentType = false;
      Http2Headers out = new DefaultHttp2Headers().method("POST").scheme(target.tls() ? "https" : "http")
          .path("/" + method).authority(target.host() + ":" + target.port());
      for (Header header : headers) {
        String name = header.name();
        String value = header.value();
        switch (name) {
          // Written below, or no part of an HTTP/2 request.
          case "te", "host", "connection", "content-length", "transfer-encoding", "keep-alive", "upgrade" -> {
            continue;
          }
          case "grpc-accept-encoding" -> {
            // Of what the caller can read, what can also be read here.
            value = readable(value);
            if (value.isEmpty()) continue;
          }
          // Compression of the whole stream is not read here.
          case "accept-encoding" -> value = "identity";
          case "grpc-encoding" -> {
            // The message was unpacked on arrival: it is packed again the same way, or sent as it is.
            if (value.equals("gzip") || value.equals("deflate")) encoding = value;
            else if (!value.equals("identity")) continue;
          }
          case "grpc-timeout" -> timeout = value;
          case "content-type" -> contentType = true;
          default -> { }
        }
        if (!name.isEmpty() && name.charAt(0) != ':') out.add(name, value);
      }
      if (!contentType) out.add("content-type", "application/grpc");
      out.add("te", "trailers");
      this.request = out;
      this.timeoutNanos = GrpcMetadata.timeoutNanos(timeout);
      byte[] body = encoding == null ? message : pack(encoding, message);
      byte[] framed = new byte[5 + body.length];
      framed[0] = (byte) (encoding == null ? 0 : 1);
      framed[1] = (byte) (body.length >>> 24);
      framed[2] = (byte) (body.length >>> 16);
      framed[3] = (byte) (body.length >>> 8);
      framed[4] = (byte) body.length;
      System.arraycopy(body, 0, framed, 5, body.length);
      this.payload = framed;
    }

    /** A connection that went away between two calls is replaced once, before anything is sent. */
    private void send(boolean retry) throws Exception {
      Connection connection = connection(target);
      if (timer == null && timeoutNanos != null) {
        // The caller's own deadline: the call ends here when the caller stops waiting for it.
        deadline = System.nanoTime() + timeoutNanos;
        timer = loops.next().schedule(() -> finish(Status.DEADLINE_EXCEEDED.withDescription(
            "The deadline of the call passed before " + target + " answered")), timeoutNanos, TimeUnit.NANOSECONDS);
        completion.whenComplete((reply, failure) -> timer.cancel(false));
      }
      connection.ready.whenComplete((channel, failure) -> {
        if (completion.isDone()) return;
        if (failure != null) {
          unreachable(failure);
        } else if (!channel.isActive() || connection.closing) {
          again(retry, new IOException("The connection was closed"));
        } else {
          new Http2StreamChannelBootstrap(channel).handler(new Reader()).open().addListener(opened -> {
            if (!opened.isSuccess()) {
              again(retry, opened.cause());
              return;
            }
            Http2StreamChannel opening = (Http2StreamChannel) opened.getNow();
            // Abandoned or answered meanwhile: closing the stream tells the server so.
            completion.whenComplete((reply, abandoned) -> opening.close());
            if (completion.isDone()) return;
            ByteBuf data = opening.alloc().buffer(payload.length).writeBytes(payload);
            opening.write(new DefaultHttp2HeadersFrame(request, false));
            opening.writeAndFlush(new DefaultHttp2DataFrame(data, true)).addListener(written -> {
              if (!written.isSuccess()) unreachable(written.cause());
            });
          });
        }
      });
    }

    private void again(boolean retry, Throwable cause) {
      if (!retry) {
        unreachable(cause);
        return;
      }
      try {
        send(false);
      } catch (Exception | Error failure) {
        completion.completeExceptionally(failure);
      }
    }

    private void unreachable(Throwable cause) {
      String reason = cause == null || cause.getMessage() == null ? String.valueOf(cause) : cause.getMessage();
      finish(Status.UNAVAILABLE.withDescription(target + " cannot be reached: " + reason).withCause(cause));
    }

    private void finish(Status status) {
      completion.complete(new Reply(status, null, List.of(), null, null));
    }

    /** Reads the reply of one stream; its callbacks run on the connection's event loop. */
    private final class Reader extends ChannelInboundHandlerAdapter {
      private final ByteArrayOutputStream body = new ByteArrayOutputStream();
      private Http2Headers headers;
      private Http2Headers trailers;

      @Override
      public void channelRead(ChannelHandlerContext context, Object frame) {
        try {
          if (frame instanceof Http2HeadersFrame received) {
            CharSequence http = received.headers().status();
            boolean informational = http != null && http.length() == 3 && http.charAt(0) == '1';
            if (received.isEndStream()) trailers = received.headers();
            else if (headers == null && !informational) headers = received.headers();
            if (received.isEndStream()) answered();
          } else if (frame instanceof Http2DataFrame received) {
            ByteBuf content = received.content();
            if ((long) body.size() + content.readableBytes() > 5L + maxMessageBytes) {
              finish(Status.RESOURCE_EXHAUSTED.withDescription("The reply of " + target + " exceeds max-message-bytes"));
              context.close();
              return;
            }
            content.readBytes(body, content.readableBytes());
            if (received.isEndStream()) answered();
          } else if (frame instanceof Http2ResetFrame received) {
            reset(received);
          }
        } catch (Exception | Error failure) {
          finish(Status.INTERNAL.withDescription("The reply of " + target + " could not be read: " + failure.getMessage())
              .withCause(failure));
        } finally {
          ReferenceCountUtil.release(frame);
        }
      }

      @Override
      public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event instanceof Http2ResetFrame received) reset(received);
        context.fireUserEventTriggered(event);
      }

      @Override
      public void channelInactive(ChannelHandlerContext context) {
        if (!completion.isDone()) unreachable(new IOException("The connection was closed before the call was answered"));
        context.fireChannelInactive();
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        unreachable(cause);
        context.close();
      }

      private void reset(Http2ResetFrame frame) {
        // 8 is CANCEL and 7 REFUSED_STREAM, as gRPC maps the errors of a stream. A server that
        // was given the caller's deadline cancels the call when it passes.
        boolean late = timeoutNanos != null && System.nanoTime() - deadline > -TimeUnit.MILLISECONDS.toNanos(100);
        Status status = frame.errorCode() == 8 ? (late ? Status.DEADLINE_EXCEEDED : Status.CANCELLED)
            : frame.errorCode() == 7 ? Status.UNAVAILABLE : Status.INTERNAL;
        finish(status.withDescription(target + " reset the call (HTTP/2 error " + frame.errorCode() + ")"));
      }

      private void answered() throws IOException {
        Http2Headers first = headers != null ? headers : trailers;
        String http = first == null || first.status() == null ? "" : first.status().toString();
        CharSequence code = trailers == null ? null : trailers.get("grpc-status");
        Status status;
        if (code != null) {
          status = Status.fromCodeValue(Integer.parseInt(code.toString().trim()));
          CharSequence text = trailers.get("grpc-message");
          if (text != null) status = status.withDescription(unescape(text.toString()));
        } else if (!http.equals("200")) {
          status = httpStatus(http).withDescription("HTTP status code " + http + " from " + target);
        } else {
          status = Status.UNKNOWN.withDescription(target + " ended the call without a gRPC status");
        }
        CharSequence packed = headers == null ? null : headers.get("grpc-encoding");
        String encoding = packed == null ? null : packed.toString();
        byte[] message = null;
        if (status.isOk()) {
          byte[] received = body.toByteArray();
          if (received.length < 5) {
            status = Status.INTERNAL.withDescription(target + " answered without a message");
          } else {
            int length = ((received[1] & 0xff) << 24) | ((received[2] & 0xff) << 16) | ((received[3] & 0xff) << 8) | (received[4] & 0xff);
            if (length != received.length - 5) {
              status = Status.INTERNAL.withDescription(target + " did not answer with exactly one message");
            } else if (received[0] == 0) {
              message = java.util.Arrays.copyOfRange(received, 5, received.length);
            } else if (encoding != null && READABLE.contains(encoding) && !encoding.equals("identity")) {
              message = unpack(encoding, received, 5, length);
              if (message == null) status = Status.RESOURCE_EXHAUSTED.withDescription("The reply of " + target + " exceeds max-message-bytes");
            } else {
              status = Status.INTERNAL.withDescription(target + " packed its reply as " + encoding + ", which cannot be read here");
            }
          }
        }
        completion.complete(new Reply(status, headers == null ? null : list(headers, false),
            trailers == null ? List.of() : list(trailers, true), message, encoding));
      }
    }
  }

  /** The headers of a block as sent, without the HTTP/2 ones and, of trailers, without the status they carry. */
  private static List<Header> list(Http2Headers block, boolean trailers) {
    List<Header> headers = new ArrayList<>(block.size());
    for (Map.Entry<CharSequence, CharSequence> entry : block) {
      String name = entry.getKey().toString();
      if (name.isEmpty() || name.charAt(0) == ':') continue;
      if (trailers && (name.equals("grpc-status") || name.equals("grpc-message"))) continue;
      headers.add(new Header(name, entry.getValue().toString()));
    }
    return headers;
  }

  private static String readable(String accepted) {
    StringBuilder kept = new StringBuilder();
    for (String encoding : accepted.split(",")) {
      String name = encoding.trim();
      if (READABLE.contains(name)) kept.append(kept.isEmpty() ? "" : ",").append(name);
    }
    return kept.toString();
  }

  private static byte[] pack(String encoding, byte[] message) throws IOException {
    ByteArrayOutputStream packed = new ByteArrayOutputStream(Math.max(32, message.length / 2));
    try (OutputStream out = encoding.equals("gzip") ? new GZIPOutputStream(packed) : new DeflaterOutputStream(packed)) {
      out.write(message);
    }
    return packed.toByteArray();
  }

  /** Null when the unpacked message would exceed the limit. */
  private byte[] unpack(String encoding, byte[] received, int offset, int length) throws IOException {
    InputStream packed = new ByteArrayInputStream(received, offset, length);
    try (InputStream in = encoding.equals("gzip") ? new GZIPInputStream(packed) : new InflaterInputStream(packed)) {
      byte[] message = in.readNBytes(maxMessageBytes + 1);
      return message.length > maxMessageBytes ? null : message;
    }
  }

  /** The status gRPC gives a call whose HTTP response is not a gRPC one. */
  private static Status httpStatus(String code) {
    return switch (code) {
      case "400" -> Status.INTERNAL;
      case "401" -> Status.UNAUTHENTICATED;
      case "403" -> Status.PERMISSION_DENIED;
      case "404" -> Status.UNIMPLEMENTED;
      case "429", "502", "503", "504" -> Status.UNAVAILABLE;
      default -> Status.UNKNOWN;
    };
  }

  /** {@code grpc-message} is percent-encoded UTF-8. */
  private static String unescape(String text) {
    if (text.indexOf('%') < 0) return text;
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(text.length());
    for (int index = 0; index < text.length(); index++) {
      char next = text.charAt(index);
      int high = next == '%' && index + 2 < text.length() ? Character.digit(text.charAt(index + 1), 16) : -1;
      int low = high < 0 ? -1 : Character.digit(text.charAt(index + 2), 16);
      if (low >= 0) {
        bytes.write((high << 4) | low);
        index += 2;
      } else {
        // Anything else is sent as it is; a server that did not encode is still read.
        byte[] plain = String.valueOf(next).getBytes(StandardCharsets.UTF_8);
        bytes.write(plain, 0, plain.length);
      }
    }
    return bytes.toString(StandardCharsets.UTF_8);
  }
}
