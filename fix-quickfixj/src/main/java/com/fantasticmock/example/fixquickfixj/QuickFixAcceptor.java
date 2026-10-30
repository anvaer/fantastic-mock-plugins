package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolOutboundSender;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;
import com.fantasticmock.protocol.api.plugin.ProtocolSettings;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamClient;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.mina.core.service.IoAcceptor;
import quickfix.Acceptor;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.DefaultSessionFactory;
import quickfix.Initiator;
import quickfix.Log;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketAcceptor;
import quickfix.mina.NetworkingOptions;
import quickfix.mina.SessionConnector;
import quickfix.mina.acceptor.AcceptorSessionProvider;

/**
 * One configured endpoint as a QuickFIX/J acceptor. A session is created for each client
 * CompID that logs on; QuickFIX/J keeps its sequence numbers, and unless
 * {@code persist-messages} is off its sent messages, in memory until the instance stops, so
 * they outlive a connection. An instance speaks the one version named by {@code begin-string}.
 */
final class QuickFixAcceptor
    implements ProtocolPluginInstance, ProtocolOutboundSender, ProtocolUpstreamClient, AcceptorSessionProvider {

  private final ProtocolEndpoint endpoint;
  private final String beginString;
  private final String compId;
  private final String dictionary;
  private final boolean rejectUnmatched;
  private final boolean disconnectIdle;
  private final boolean validate;
  private final boolean persistMessages;
  private final QuickFixApplication application;
  private final QuickFixUpstream upstream;
  private final Map<SessionID, Session> sessions = new ConcurrentHashMap<>();
  private volatile SessionFactory factory;
  private volatile ThreadedSocketAcceptor acceptor;
  private volatile String lastFailure = "";

  QuickFixAcceptor(ProtocolEndpoint endpoint) {
    ProtocolSettings settings = endpoint.settings();
    this.endpoint = endpoint;
    this.beginString = settings.text("begin-string");
    this.compId = settings.text("sender-comp-id", "");
    this.dictionary = settings.text("data-dictionary", "");
    this.rejectUnmatched = settings.bool("reject-unmatched");
    this.disconnectIdle = settings.bool("disconnect-idle");
    this.validate = settings.bool("validate");
    this.persistMessages = settings.bool("persist-messages");
    this.upstream = new QuickFixUpstream(endpoint, this);
    this.application = new QuickFixApplication(endpoint, this, upstream);
  }

  boolean rejectUnmatched() { return rejectUnmatched; }

  /** False once the endpoint is stopping. */
  boolean running() { return acceptor != null; }

  int port() { // Ephemeral-port tests.
    return ((InetSocketAddress) localAddress()).getPort();
  }

  @Override
  public void start() throws Exception {
    ProtocolSettings options = endpoint.settings();
    String host = options.text("host");
    int port = options.integer("port");
    checkDictionary();

    SessionSettings settings = sessionSettings(true);
    // The template only names the listener: sessions are created in getSession.
    SessionID template = new SessionID(beginString, "*", "*");
    settings.setBool(template, Acceptor.SETTING_ACCEPTOR_TEMPLATE, true);
    settings.setString(template, Acceptor.SETTING_SOCKET_ACCEPT_ADDRESS, host);
    settings.setLong(template, Acceptor.SETTING_SOCKET_ACCEPT_PORT, port);
    // Received messages keep their text, so one that is sent again or passed on does not change.
    factory = new DefaultSessionFactory(
        application, new MemoryStoreFactory(), this::log, new WireMessage.Factory(dictionary()));
    // One thread per session keeps a session's messages in order without holding up the others.
    ThreadedSocketAcceptor engine = new ThreadedSocketAcceptor(factory, settings);
    engine.setSessionProvider(new InetSocketAddress(host, port), this);
    acceptor = engine;
    engine.start();
  }

  /**
   * The session of a connecting client, created on its first logon. Called by QuickFIX/J for
   * every connection that is not yet bound to a session.
   */
  @Override
  public synchronized Session getSession(SessionID requested, SessionConnector connector) {
    // The endpoint id as qualifier keeps these sessions apart from another instance's with the
    // same CompIDs: QuickFIX/J otherwise shares one session registry among all its acceptors.
    SessionID id = new SessionID(requested.getBeginString(), requested.getSenderCompID(),
        requested.getTargetCompID(), endpoint.id());
    Session session = sessions.get(id);
    if (session == null) {
      try {
        // A client of another version gets a session only to be told so in a Logout.
        session = factory.create(id, sessionSettings(beginString.equals(requested.getBeginString())));
      } catch (ConfigError | RuntimeException failure) {
        lastFailure = requested + ": " + failure.getMessage();
        return null;
      }
      sessions.put(id, session);
      connector.addDynamicSession(session);
    }
    return session;
  }

  /** Why a Logon to this session is refused, or null when it is accepted. */
  String refusal(SessionID sessionId) {
    if (!beginString.equals(sessionId.getBeginString())) {
      return "This endpoint speaks " + beginString + ", not " + sessionId.getBeginString();
    }
    // For an acceptor the session's SenderCompID is the TargetCompID the client addressed.
    if (!compId.isEmpty() && !compId.equals(sessionId.getSenderCompID())) {
      return "TargetCompID " + sessionId.getSenderCompID() + " is not served here, expected " + compId;
    }
    return null;
  }

  /**
   * Sends one rendered response or callback on a session. Returns the message as it was
   * written, which also gives its bytes; QuickFIX/J queues the write, so this neither blocks
   * nor confirms receipt.
   */
  SentText send(Session session, JsonNode document) throws IOException {
    Message message = QuickFixMessages.message(document, beginString, session.getDataDictionary());
    // Unsent messages would be stored and resent at the next logon; a mock response is not owed then.
    if (!session.isLoggedOn() || !session.send(message)) {
      throw new IOException("FIX session " + describe(session.getSessionID()) + " is not logged on");
    }
    return new SentText(message.toString());
  }

  /**
   * Sends a callback to the logged-on session of {@code targetCompId}, or to every logged-on
   * session when it is empty. Fails when nobody received it.
   */
  @Override
  public CompletionStage<Long> send(JsonNode callback) {
    CompletableFuture<Long> done = new CompletableFuture<>();
    try {
      String target = callback.path("targetCompId").asText("").trim();
      long bytes = 0;
      int recipients = 0;
      IOException failure = null;
      for (Session session : sessions.values()) {
        if (!session.isLoggedOn()
            || (!target.isEmpty() && !target.equals(session.getSessionID().getTargetCompID()))) {
          continue;
        }
        try {
          bytes += send(session, callback).length();
          recipients++;
        } catch (IOException failed) {
          failure = failed;
        }
      }
      if (recipients > 0) {
        done.complete(bytes);
      } else {
        done.completeExceptionally(failure != null ? failure : new IllegalStateException(target.isEmpty()
            ? "No FIX session is logged on"
            : "No FIX session is logged on for CompID " + target));
      }
    } catch (RuntimeException failure) {
      done.completeExceptionally(failure);
    }
    return done;
  }

  /**
   * Passes a message of one of this endpoint's sessions on to the real server, over the
   * session that client has with it. The server answers when it will: what it sends comes
   * back through {@link QuickFixUpstream}, not from here.
   */
  @Override
  public CompletionStage<List<JsonNode>> forward(ProtocolMessageView request, Upstream target) {
    try {
      if (!(request instanceof QuickFixMessageView received)) {
        throw new IllegalArgumentException("Only a message of a logged-on FIX session can be passed on");
      }
      return upstream.forward(received, target);
    } catch (ConfigError | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  @Override
  public boolean repliesLater() {
    return true;
  }

  /**
   * The tags of a message can be changed on its way, one by one or in its whole text, but for
   * the ones a session writes itself and for what kind of message it is.
   */
  @Override
  public Set<String> rewritableFields() {
    return Set.of(QuickFixUpstream.TAG, QuickFixUpstream.BODY);
  }

  @Override
  public void checkTarget(String target) {
    QuickFixUpstream.Target.parse(target);
  }

  @Override
  public String targetHint() {
    return "fix://host:port";
  }

  /** Remembers why a client's session with the real server is not what it should be. */
  void failed(SessionID client, String reason) {
    lastFailure = describe(client) + ": " + reason;
  }

  @Override
  public Map<String, Object> healthDetails() {
    SocketAddress address = localAddress();
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("localAddress", address == null ? "" : String.valueOf(address));
    details.put("beginString", beginString);
    details.put("sessions", sessions.values().stream()
        .filter(Session::isLoggedOn)
        .map(session -> describe(session.getSessionID()))
        .sorted().limit(100).toList());
    List<String> passedOn = upstream.sessions();
    if (!passedOn.isEmpty()) details.put("upstreamSessions", passedOn.stream().limit(100).toList());
    details.put("lastFailure", lastFailure);
    return details;
  }

  @Override
  public void close() {
    ThreadedSocketAcceptor engine = acceptor;
    if (engine == null) return;
    acceptor = null;
    for (Session session : sessions.values()) {
      if (!session.isLoggedOn()) continue;
      try {
        // logout() only asks for it; next() writes the Logout before the connection is cut.
        session.logout("The FIX endpoint is stopping");
        session.next();
      } catch (IOException | RuntimeException ignored) {
        // The connection is cut below either way.
      }
    }
    // Forced: a reloaded instance must be able to bind the port at once.
    engine.stop(true);
    application.close();
    upstream.close();
    sessions.clear();
  }

  private SocketAddress localAddress() {
    ThreadedSocketAcceptor engine = acceptor;
    if (engine == null) return null;
    for (IoAcceptor listener : engine.getEndpoints()) {
      if (listener.getLocalAddress() != null) return listener.getLocalAddress();
    }
    return null;
  }

  // QuickFIX/J settings of a session; without a dictionary for a client of another version.
  private SessionSettings sessionSettings(boolean served) {
    SessionSettings settings = new SessionSettings();
    settings.setString(SessionFactory.SETTING_CONNECTION_TYPE, SessionFactory.ACCEPTOR_CONNECTION_TYPE);
    settings.setBool(Session.SETTING_NON_STOP_SESSION, true);
    // A mock should not drop a client whose clock differs.
    settings.setBool(Session.SETTING_CHECK_LATENCY, false);
    settings.setBool(Session.SETTING_DISABLE_HEART_BEAT_CHECK, !disconnectIdle);
    // Each stored message stays until the instance stops: off, a busy session's memory stays flat.
    settings.setBool(Session.SETTING_PERSIST_MESSAGES, persistMessages);
    // As java.net.ServerSocket does: on Windows this option would let a second listener take the port.
    settings.setBool(NetworkingOptions.SETTING_SOCKET_REUSE_ADDRESS,
        !System.getProperty("os.name", "").toLowerCase().contains("win"));
    settings.setBool(Session.SETTING_USE_DATA_DICTIONARY, served);
    if (served) {
      // Without the setting QuickFIX/J loads the standard dictionary of the version from lib/.
      if (!dictionary.isEmpty()) settings.setString(Session.SETTING_DATA_DICTIONARY, dictionary);
      settings.setBool(Session.SETTING_VALIDATE_INCOMING_MESSAGE, validate);
      settings.setBool(Session.SETTING_VALIDATE_FIELDS_OUT_OF_ORDER, validate);
      settings.setBool(Session.SETTING_VALIDATE_FIELDS_HAVE_VALUES, validate);
      settings.setBool(Session.SETTING_VALIDATE_UNORDERED_GROUP_FIELDS, validate);
    }
    return settings;
  }

  /**
   * QuickFIX/J settings for sessions with a real server: this side logs on, and whatever the
   * server sends is taken as it is, since it is passed on and not judged here.
   */
  SessionSettings upstreamSettings() {
    SessionSettings settings = new SessionSettings();
    settings.setString(SessionFactory.SETTING_CONNECTION_TYPE, SessionFactory.INITIATOR_CONNECTION_TYPE);
    settings.setBool(Session.SETTING_NON_STOP_SESSION, true);
    settings.setBool(Session.SETTING_CHECK_LATENCY, false);
    // A lost connection is tried again at once; QuickFixUpstream decides whether it is tried further.
    settings.setLong(Initiator.SETTING_RECONNECT_INTERVAL, 1);
    settings.setLong(Session.SETTING_LOGON_TIMEOUT, 10);
    settings.setLong(Session.SETTING_LOGOUT_TIMEOUT, 2);
    settings.setBool(Session.SETTING_USE_DATA_DICTIONARY, true);
    if (!dictionary.isEmpty()) settings.setString(Session.SETTING_DATA_DICTIONARY, dictionary);
    settings.setBool(Session.SETTING_VALIDATE_INCOMING_MESSAGE, false);
    settings.setBool(Session.SETTING_VALIDATE_FIELDS_OUT_OF_ORDER, false);
    settings.setBool(Session.SETTING_VALIDATE_FIELDS_HAVE_VALUES, false);
    settings.setBool(Session.SETTING_VALIDATE_UNORDERED_GROUP_FIELDS, false);
    return settings;
  }

  /** The dictionary of the sessions this endpoint serves: the configured one, or the standard one of its version. */
  DataDictionary dictionary() throws ConfigError {
    return new DataDictionary(dictionary.isEmpty() ? beginString.replace(".", "") + ".xml" : dictionary);
  }

  // Fails at startup instead of at the first logon.
  private void checkDictionary() throws ConfigError {
    if (!dictionary.isEmpty()) {
      if (!Files.isRegularFile(Path.of(dictionary))) {
        throw new IllegalArgumentException("Protocol setting 'data-dictionary' is not a file: " + dictionary);
      }
      new DataDictionary(dictionary);
      return;
    }
    String standard = beginString.replace(".", "") + ".xml";
    if (getClass().getClassLoader().getResource(standard) == null) {
      throw new IllegalStateException(standard + " was not found: install the plugin with its lib/*.jar");
    }
  }

  // Keeps QuickFIX/J's session log off the console; the last error is shown in the health details.
  private Log log(SessionID sessionId) {
    return new Log() {
      @Override
      public void clear() {
      }

      @Override
      public void onIncoming(String message) {
      }

      @Override
      public void onOutgoing(String message) {
      }

      @Override
      public void onEvent(String text) {
      }

      @Override
      public void onErrorEvent(String text) {
        lastFailure = describe(sessionId) + ": " + text;
      }
    };
  }

  // The client first: for an acceptor it is the session's TargetCompID.
  private static String describe(SessionID sessionId) {
    return sessionId.getTargetCompID() + " -> " + sessionId.getSenderCompID();
  }
}
