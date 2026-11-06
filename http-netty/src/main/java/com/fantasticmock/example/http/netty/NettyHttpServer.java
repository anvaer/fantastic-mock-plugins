package com.fantasticmock.example.http.netty;

import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;
import com.fantasticmock.protocol.api.plugin.ProtocolSettings;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpDecoderConfig;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * One configured listener. Both exchange modes share this pipeline and settings; only the
 * last handler's {@link NettyHttpMode} differs. Admission, statistics and response delivery
 * come from the endpoint.
 */
final class NettyHttpServer implements ProtocolPluginInstance {
  private final ProtocolEndpoint endpoint;
  private final NettyHttpMode mode;
  private final String host;
  private final int port;
  private final int workerThreads;
  private final int maxContentLength;
  private final HttpDecoderConfig decoder;
  private final int idleTimeoutMs;
  private final boolean compression;
  private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private volatile EventLoopGroup acceptors;
  private volatile EventLoopGroup workers;
  private volatile Channel server;

  NettyHttpServer(ProtocolEndpoint endpoint) {
    this.endpoint = endpoint;
    ProtocolSettings settings = endpoint.settings();
    this.mode = switch (settings.text("exchange-mode")) {
      case "http" -> new HostGatewayMode(endpoint);
      case "protocol" -> new ProtocolTemplateMode(endpoint);
      default -> throw new IllegalArgumentException("exchange-mode must be http or protocol");
    };
    this.host = settings.text("host");
    this.port = settings.integer("port");
    this.workerThreads = settings.integer("worker-threads");
    this.maxContentLength = settings.integer("max-content-length");
    this.decoder = new HttpDecoderConfig()
        .setMaxInitialLineLength(settings.integer("max-initial-line-length"))
        .setMaxHeaderSize(settings.integer("max-header-size"))
        .setMaxChunkSize(settings.integer("max-chunk-size"));
    this.idleTimeoutMs = settings.integer("idle-timeout-ms");
    this.compression = settings.bool("compression-enabled");
  }

  @Override
  public void start() throws Exception {
    acceptors = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    // 0 selects Netty's default of twice the available processors.
    workers = workerThreads == 0
        ? new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory())
        : new MultiThreadIoEventLoopGroup(workerThreads, NioIoHandler.newFactory());
    try {
      server = new ServerBootstrap()
          .group(acceptors, workers)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_REUSEADDR, true)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
              connections.add(channel);
              var pipeline = channel.pipeline();
              pipeline.addLast("http-codec", new HttpServerCodec(decoder));
              // The host HTTP engine applies its own body-decoding rules to raw bytes.
              if (mode.decompressesRequests()) {
                pipeline.addLast("request-decompressor", new HttpContentDecompressor(maxContentLength));
              }
              pipeline.addLast("request-aggregator", new HttpObjectAggregator(maxContentLength));
              if (idleTimeoutMs > 0) {
                pipeline.addLast("idle", new IdleStateHandler(0, 0, idleTimeoutMs, TimeUnit.MILLISECONDS));
              }
              if (compression) pipeline.addLast("response-compressor", new HttpContentCompressor());
              pipeline.addLast("fantasticmock", new NettyHttpConnection(mode));
            }
          })
          .bind(new InetSocketAddress(host, port))
          .sync()
          .channel();
    } catch (Exception failure) {
      close();
      throw failure;
    }
  }

  @Override
  public Map<String, Object> healthDetails() {
    Channel listener = server;
    Object address = listener == null ? null : listener.localAddress();
    return Map.of(
        "connections", connections.size(),
        "localAddress", address == null ? "" : address.toString(),
        "exchangeMode", mode.name());
  }

  /**
   * The worker event loops, which read and write every connection: busy are those with tasks
   * queued for them, such as the responses the virtual threads that answered hand back to be
   * written, and waiting is those tasks. Declared without {@code @Override}: hosts before 1.0.6
   * do not ask.
   */
  public Map<String, Integer> threads() {
    EventLoopGroup group = workers;
    if (group == null) return Map.of();
    int loops = 0;
    int busy = 0;
    int waiting = 0;
    for (EventExecutor loop : group) {
      loops++;
      int queued = loop instanceof SingleThreadEventExecutor single ? single.pendingTasks() : 0;
      if (queued > 0) busy++;
      waiting += queued;
    }
    return Map.of("busy", busy, "max", loops, "waiting", waiting);
  }

  @Override
  public void close() {
    Channel listener = server;
    if (listener != null) listener.close().awaitUninterruptibly();
    connections.close().awaitUninterruptibly();
    EventLoopGroup workerGroup = workers;
    if (workerGroup != null) workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
    EventLoopGroup acceptorGroup = acceptors;
    if (acceptorGroup != null) acceptorGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
  }

  @Override
  public String toString() {
    return "http-netty/" + endpoint.id();
  }
}
