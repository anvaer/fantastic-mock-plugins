package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamClient;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamHost;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.DefaultSessionFactory;
import quickfix.FieldNotFound;
import quickfix.Initiator;
import quickfix.Log;
import quickfix.MemoryStore;
import quickfix.Message;
import quickfix.MessageStore;
import quickfix.Session;
import quickfix.SessionFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketInitiator;

/**
 * The real server behind an endpoint that passes messages on. Every client session gets a
 * session of its own with that server, under the client's CompIDs and logged on with what the
 * client's Logon carried, so the server sees the client it knows. QuickFIX/J runs both sessions:
 * each has its own sequence numbers, heartbeats and resends.
 *
 * <p>Application messages cross as they arrived (see {@link WireMessage}): the client's to the
 * server when the host says so, the server's to the client whenever they come. The host records
 * the server's messages; they are handed over with the request they answer where the message
 * says which that is, by a request ID it repeats or the sequence number it rejects.</p>
 */
final class QuickFixUpstream implements Application, AutoCloseable {

  /** A validated {@code fix://host:port}. */
  record Target(String host, int port) {
    static Target parse(String url) {
      URI target;
      try {
        target = URI.create(url == null ? "" : url.trim());
      } catch (IllegalArgumentException invalid) {
        target = URI.create("");
      }
      if (!"fix".equalsIgnoreCase(target.getScheme()) || target.getHost() == null
          || target.getPort() < 1 || target.getPort() > 65535 || target.getRawUserInfo() != null
          || target.getRawQuery() != null || target.getRawFragment() != null
          || (target.getPath() != null && !target.getPath().isEmpty() && !target.getPath().equals("/"))) {
        throw new IllegalArgumentException("A FIX target must be fix://host:port");
      }
      return new Target(target.getHost(), target.getPort());
    }

    @Override
    public String toString() {
      return "fix://" + host + ":" + port;
    }
  }

  private static final int MSG_SEQ_NUM = 34;
  private static final int MSG_TYPE = 35;
  private static final int REF_SEQ_NUM = 45;
  private static final int TEXT = 58;
  private static final int ENCRYPT_METHOD = 98;
  private static final int HEART_BT_INT = 108;
  private static final int RESET_SEQ_NUM_FLAG = 141;
  private static final int NEXT_EXPECTED_MSG_SEQ_NUM = 789;
  // Request IDs that an answer repeats: ClOrdID, OrigClOrdID, OrderID, QuoteReqID, QuoteID,
  // MDReqID, SecurityReqID, TradeRequestID, PosReqID, UserRequestID, RFQReqID, CollInquiryID, ListID.
  static final Set<Integer> REQUEST_IDS = Set.of(11, 41, 37, 131, 117, 262, 320, 568, 710, 923, 644, 909, 66);
  private static final int REMEMBERED = 4096;
  /** The request fields an upstream's rules may change: the tags of a message, and its whole text. */
  static final String TAG = "tag";
  static final String BODY = "body";
  // What a session writes itself, or is: BeginString, BodyLength, CheckSum, MsgSeqNum, MsgType,
  // PossDupFlag, SenderCompID, SendingTime, TargetCompID, OrigSendingTime, LastMsgSeqNumProcessed.
  private static final Set<Integer> SESSION_OWN = Set.of(8, 9, 10, 34, 35, 43, 49, 52, 56, 122, 369);

  private final ProtocolEndpoint endpoint;
  private final QuickFixAcceptor acceptor;
  // The Logon of each client session as it arrived: what the server is logged on to with.
  private final Map<SessionID, String> logons = new ConcurrentHashMap<>();
  // By client session: the link its messages take now. By the session with the server: all there are.
  private final Map<SessionID, Link> current = new ConcurrentHashMap<>();
  private final Map<SessionID, Link> links = new ConcurrentHashMap<>();
  private SessionSettings settings;
  private ThreadedSocketInitiator initiator;
  private volatile ProtocolUpstreamHost host;
  // The target that was asked for last, as it was written and as it is read: the same for every
  // message while the routing stays as it is.
  private volatile Map.Entry<String, Target> parsed;
  private volatile boolean closed;

  QuickFixUpstream(ProtocolEndpoint endpoint, QuickFixAcceptor acceptor) {
    this.endpoint = endpoint;
    this.acceptor = acceptor;
  }

  /** A client's Logon, kept for the server. */
  void logon(SessionID client, String wire) {
    logons.put(client, wire);
  }

  /** The client is logged on: the server is too, when the endpoint's messages go there. */
  void joined(SessionID client) {
    try {
      Optional<ProtocolUpstreamHost.Route> route = route();
      if (route.isPresent()) link(client, target(route.get().target()));
    } catch (ConfigError | RuntimeException failure) {
      acceptor.failed(client, "the real server cannot be connected: " + failure.getMessage());
    }
  }

  /** The client is gone: so is its session with the server, which keeps its sequence numbers. */
  void left(SessionID client) {
    logons.remove(client);
    Link link = current.remove(client);
    if (link != null) link.leave("The client logged out");
  }

  /** Sends a client's message to the server, once that is logged on. Completes when it is on its way. */
  CompletionStage<List<JsonNode>> forward(QuickFixMessageView request, ProtocolUpstreamClient.Upstream upstream)
      throws ConfigError {
    Link link = link(request.session(), target(upstream.target()));
    Map<Integer, String> replaced = upstream.changes().isEmpty() ? Map.of() : tags(upstream.changes());
    // The whole text as a rule left it, with | for SOH as request.body has it; null without such a rule.
    String body = upstream.changes().isEmpty() ? null : upstream.changes().get(BODY);
    return link.ready.thenApply(loggedOn -> {
      link.send(request, replaced, body);
      return List.of();
    });
  }

  private Target target(String url) {
    Map.Entry<String, Target> last = parsed;
    if (last != null && last.getKey().equals(url)) return last.getValue();
    Target target = Target.parse(url);
    parsed = Map.entry(url, target);
    return target;
  }

  /**
   * The changes of an upstream by tag number: {@code tag.<number>} with its new value, or null
   * to leave the tag out. What a session writes itself is not for a rule to change.
   */
  static Map<Integer, String> tags(Map<String, String> changes) {
    Map<Integer, String> replaced = new HashMap<>();
    for (Map.Entry<String, String> change : changes.entrySet()) {
      if (!change.getKey().startsWith(TAG + ".")) continue;
      int tag;
      try {
        tag = Integer.parseInt(change.getKey().substring(TAG.length() + 1).trim());
      } catch (NumberFormatException notATag) {
        continue;
      }
      if (tag > 0 && !SESSION_OWN.contains(tag)) replaced.put(tag, change.getValue());
    }
    return replaced;
  }

  /** Who is logged on to which server, for the health details. */
  List<String> sessions() {
    List<String> described = new ArrayList<>();
    for (Link link : current.values()) {
      Session session = link.session;
      if (session != null && session.isLoggedOn()) {
        described.add(link.client.getTargetCompID() + " -> " + link.client.getSenderCompID() + " at " + link.target);
      }
    }
    described.sort(null);
    return described;
  }

  @Override
  public synchronized void close() {
    closed = true;
    for (Link link : links.values()) link.leave("The FIX endpoint is stopping");
    current.clear();
    links.clear();
    if (initiator != null) initiator.stop(true);
    initiator = null;
  }

  private Optional<ProtocolUpstreamHost.Route> route() {
    ProtocolUpstreamHost service = host;
    if (service == null) {
      // There only on a host that passes messages on, and perhaps not before the endpoint started.
      service = endpoint.hostService(ProtocolUpstreamHost.class).orElse(null);
      if (service == null) return Optional.empty();
      host = service;
    }
    return service.route(endpoint);
  }

  /** The link a client's messages take to a target, connected and logging on. */
  private synchronized Link link(SessionID client, Target target) throws ConfigError {
    if (closed) throw new IllegalStateException("The FIX endpoint is stopping");
    Link link = current.get(client);
    if (link != null && !link.target.equals(target)) {
      link.leave("The endpoint is routed to another server");
      link = null;
    }
    if (link == null) {
      // The client's CompIDs the other way round: to the server this side is the client.
      SessionID id = new SessionID(client.getBeginString(), client.getTargetCompID(), client.getSenderCompID(),
          endpoint.id() + ">" + target.host() + ":" + target.port());
      link = links.get(id);
      if (link == null) {
        link = new Link(client, id, target);
        links.put(id, link);
        try {
          create(link);
        } catch (ConfigError | RuntimeException failure) {
          links.remove(id);
          throw failure;
        }
      }
      current.put(client, link);
    }
    link.join();
    return link;
  }

  private void create(Link link) throws ConfigError {
    String logon = logons.getOrDefault(link.client, "");
    List<QuickFixMessages.Field> fields = QuickFixMessages.fields(logon, null);
    String heartbeat = QuickFixMessages.first(fields, HEART_BT_INT);
    boolean reset = "Y".equals(QuickFixMessages.first(fields, RESET_SEQ_NUM_FLAG));
    if (settings == null) settings = acceptor.upstreamSettings();
    SessionID id = link.id;
    settings.setString(id, Initiator.SETTING_SOCKET_CONNECT_HOST, link.target.host());
    settings.setLong(id, Initiator.SETTING_SOCKET_CONNECT_PORT, link.target.port());
    settings.setLong(id, Session.SETTING_HEARTBTINT, positive(heartbeat, 30));
    // A client that starts its numbers anew at logon has the server do the same.
    settings.setBool(id, Session.SETTING_RESET_ON_LOGON, reset);
    // Otherwise this side continues where the client is: a server that knows the client expects that.
    link.firstSequence = reset ? 1 : positive(QuickFixMessages.first(fields, MSG_SEQ_NUM), 1);
    if (initiator == null) {
      SessionFactory factory = new DefaultSessionFactory(this, this::store, this::log,
          new WireMessage.Factory(acceptor.dictionary()));
      // One thread per session, as for the clients: a session's messages stay in order.
      ThreadedSocketInitiator engine = new ThreadedSocketInitiator(factory, settings);
      engine.start();
      initiator = engine;
    } else {
      initiator.createDynamicSession(id);
    }
    link.session = Session.lookupSession(id);
    if (link.session == null) throw new IllegalStateException("No session was created for " + link.target);
  }

  private MessageStore store(SessionID id) {
    try {
      MemoryStore store = new MemoryStore(id);
      Link link = links.get(id);
      if (link != null && link.firstSequence > 1) store.setNextSenderMsgSeqNum(link.firstSequence);
      return store;
    } catch (IOException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  // Keeps QuickFIX/J's session log off the console. An error before the server is logged on is why it is not.
  private Log log(SessionID id) {
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
        Link link = links.get(id);
        if (link != null) link.failed(text);
      }
    };
  }

  private static int positive(String number, int otherwise) {
    try {
      int value = number == null ? 0 : Integer.parseInt(number.trim());
      return value > 0 ? value : otherwise;
    } catch (NumberFormatException invalid) {
      return otherwise;
    }
  }

  @Override
  public void onCreate(SessionID id) {
  }

  @Override
  public void onLogon(SessionID id) {
    Link link = links.get(id);
    if (link == null) return;
    link.fresh = false;
    link.ended = "";
    // Whoever waits for the server waits on this future; a failed one is replaced.
    if (!link.ready.complete(null)) link.ready = CompletableFuture.completedFuture(null);
    endpoint.statistics().increment("upstreamLogons");
  }

  @Override
  public void onLogout(SessionID id) {
    Link link = links.get(id);
    if (link != null) link.ended();
  }

  /** The Logon to the server carries what the client's did: its credentials and whatever else it sent. */
  @Override
  public void toAdmin(Message message, SessionID id) {
    Link link = links.get(id);
    if (link == null || !"A".equals(type(message))) return;
    DataDictionary dictionary = link.session == null ? null : link.session.getDataDictionary();
    List<QuickFixMessages.Field> fields = QuickFixMessages.fields(logons.getOrDefault(link.client, ""), dictionary);
    Set<Integer> seen = new HashSet<>();
    Set<Integer> repeated = new HashSet<>();
    for (QuickFixMessages.Field field : fields) {
      if (!seen.add(field.tag())) repeated.add(field.tag());
    }
    for (QuickFixMessages.Field field : fields) {
      int tag = field.tag();
      // The header and trailer are this session's, and so is how it starts and beats.
      if (tag == ENCRYPT_METHOD || tag == HEART_BT_INT || tag == RESET_SEQ_NUM_FLAG || tag == NEXT_EXPECTED_MSG_SEQ_NUM
          || repeated.contains(tag) || WireMessage.sessionField(dictionary, tag)) {
        continue;
      }
      message.setString(tag, field.value());
    }
  }

  @Override
  public void fromAdmin(Message message, SessionID id) {
    Link link = links.get(id);
    if (link == null) return;
    String type = type(message);
    try {
      if ("A".equals(type) && link.fresh && link.session != null) {
        // A session that is new here takes the server's numbers as they are: what the server
        // sent the client before this endpoint stood between them is not asked for again.
        link.session.setNextTargetMsgSeqNum(message.getHeader().getInt(MSG_SEQ_NUM));
      } else if ("5".equals(type)) {
        link.ended = message.isSetField(TEXT) ? message.getString(TEXT) : "";
      } else if ("3".equals(type)) {
        // The server refused a message that was passed on: the client is told, as by the server.
        link.relay(message.toRawString(), type);
      }
    } catch (FieldNotFound | IOException ignored) {
      // QuickFIX/J has checked the message; without the field there is nothing to do.
    }
  }

  @Override
  public void toApp(Message message, SessionID id) {
  }

  @Override
  public void fromApp(Message message, SessionID id) {
    Link link = links.get(id);
    if (link != null) link.relay(message.toRawString(), type(message));
  }

  private static String type(Message message) {
    try {
      return message.getHeader().getString(MSG_TYPE);
    } catch (FieldNotFound missing) {
      return "";
    }
  }

  /** One client session's session with a server. */
  private final class Link {
    private final SessionID client;
    private final SessionID id;
    private final Target target;
    private volatile Session session;
    // Completed while the server is logged on; failed with the reason while it cannot be.
    private volatile CompletableFuture<Void> ready = new CompletableFuture<>();
    private volatile boolean fresh = true;
    private volatile boolean leaving;
    private volatile String ended = "";
    private int firstSequence = 1;
    // The requests passed on, by what an answer says of them. Guarded by this.
    private final Map<String, Passed> byId = remembered();
    private final Map<String, Passed> bySequence = remembered();
    private Passed last;

    private Link(SessionID client, SessionID id, Target target) {
      this.client = client;
      this.id = id;
      this.target = target;
    }

    /** Logs on again where this side had logged off; a session in use is left as it is. */
    private void join() {
      Session server = session;
      if (server == null || !leaving) return;
      leaving = false;
      ended = "";
      ready = new CompletableFuture<>();
      server.logon();
    }

    /** Logs off and stays off until the client is back. */
    private void leave(String reason) {
      leaving = true;
      ready.completeExceptionally(new IOException(reason));
      Session server = session;
      if (server == null) return;
      boolean loggedOn = server.isLoggedOn();
      // logout() only asks for it, and keeps the session from connecting again.
      server.logout(reason);
      try {
        if (loggedOn) server.next(); // Writes the Logout now, not at the next timer tick.
        else server.disconnect(reason, false);
      } catch (IOException | RuntimeException ignored) {
        // The session is off either way.
      }
    }

    private void failed(String reason) {
      Session server = session;
      if (leaving || (server != null && server.isLoggedOn())) return;
      acceptor.failed(client, target + ": " + reason);
      // The server is tried again when there is something for it, not in a loop of its own.
      leave(target + " cannot be reached: " + reason);
    }

    /** The session with the server is over: by the server, or because this side left. */
    private void ended() {
      String reason = ended.isEmpty() ? "the connection was closed" : ended;
      boolean wasLoggedOn = ready.isDone() && !ready.isCompletedExceptionally();
      if (wasLoggedOn) ready = new CompletableFuture<>();
      if (leaving) {
        ready.completeExceptionally(new IOException("Not connected to " + target));
        return;
      }
      acceptor.failed(client, target + " ended the session: " + reason);
      // A server that was logged on is connected again at once. One that would not have this
      // side is tried again when there is something for it.
      if (!wasLoggedOn) leave(target + " ended the session: " + reason);
      // Where every message goes to the server, a client without the server has nobody to talk to.
      Optional<ProtocolUpstreamHost.Route> route = route();
      Session clientSession = Session.lookupSession(client);
      if (route.isPresent() && route.get().everyRequest() && clientSession != null && clientSession.isLoggedOn()
          && current.get(client) == this) {
        clientSession.logout(reason);
      }
    }

    private void send(QuickFixMessageView request, Map<Integer, String> replaced, String body) {
      Session server = session;
      boolean rules = body != null || !replaced.isEmpty();
      String wire = request.wire();
      if (body != null) {
        // Tag rules then work on the text the body rules left.
        wire = body.replace('|', QuickFixMessages.SOH);
        List<QuickFixMessages.Field> left = QuickFixMessages.fields(wire, server.getDataDictionary());
        if (left.size() < 3 || left.getFirst().tag() != 8 || QuickFixMessages.first(left, MSG_TYPE) == null) {
          throw new IllegalArgumentException("The rules of the upstream do not leave a FIX message");
        }
      }
      // Without rules a message is written by the code that wrote it before there were any.
      WireMessage message = rules
          ? WireMessage.changed(wire, server.getDataDictionary(), replaced)
          : WireMessage.relayed(wire, server.getDataDictionary(), replaced);
      // What kind of message it is, is not a rule's to change; whose it is, the session writes.
      if (body != null) message.getHeader().setString(MSG_TYPE, request.messageType());
      synchronized (this) {
        if (!server.isLoggedOn() || !server.send(message)) {
          throw new IllegalStateException(target + " is not logged on");
        }
        Passed passed = new Passed(request, System.nanoTime());
        for (QuickFixMessages.Field field : request.wireFields()) {
          if (REQUEST_IDS.contains(field.tag())) byId.put(field.tag() + "=" + field.value(), passed);
        }
        // The server answers with the IDs it was sent, which a rule may have changed.
        if (rules && message.written() != null) {
          for (QuickFixMessages.Field field : QuickFixMessages.fields(message.written(), server.getDataDictionary())) {
            if (REQUEST_IDS.contains(field.tag())) byId.put(field.tag() + "=" + field.value(), passed);
          }
        }
        // A reject names the message by the number it had on this side.
        try {
          bySequence.put(message.getHeader().getString(MSG_SEQ_NUM), passed);
        } catch (FieldNotFound impossible) {
          // QuickFIX/J numbers every message it sends.
        }
        last = passed;
      }
      endpoint.statistics().increment("forwarded");
    }

    /** Passes a message of the server on to the client, as it arrived. */
    private void relay(String wire, String type) {
      Session clientSession = Session.lookupSession(client);
      if (leaving || clientSession == null || !clientSession.isLoggedOn() || current.get(client) != this) return;
      Optional<ProtocolUpstreamHost.Route> route = route();
      if (route.isEmpty() || !routedHere(route.get())) {
        // The endpoint answers from its templates again: the server has nothing more to say here.
        leave("The endpoint no longer passes messages on");
        return;
      }
      List<QuickFixMessages.Field> fields = QuickFixMessages.fields(wire, clientSession.getDataDictionary());
      Map<Integer, String> replaced = Map.of();
      Passed answered;
      synchronized (this) {
        answered = answered(fields, type, route.get().replyTimeoutMs());
        boolean reject = type.equals("3") || type.equals("j");
        if (reject && answered != null && answered.request.sequence() != null) {
          // To the client the refused message has the number the client gave it.
          replaced = Map.of(REF_SEQ_NUM, answered.request.sequence());
        }
      }
      WireMessage message = WireMessage.relayed(wire, clientSession.getDataDictionary(), replaced);
      if (!clientSession.send(message) || message.written() == null) return;
      // Recorded as the client received it: with its own session's sequence numbers.
      String sent = message.written();
      ProtocolMessageView request = answered != null ? answered.request : new Unasked(type);
      ProtocolExchangeResult recorded = host.received(endpoint, request,
          List.of(QuickFixMessages.document(endpoint.objectMapper(), sent, type)));
      recorded.responses().getFirst().markSent(sent.length());
      endpoint.statistics().increment("relayed");
    }

    private boolean routedHere(ProtocolUpstreamHost.Route route) {
      try {
        return target(route.target()).equals(target);
      } catch (IllegalArgumentException elsewhere) {
        return false;
      }
    }

    // The request a message of the server answers, or null when it answers none.
    private Passed answered(List<QuickFixMessages.Field> fields, String type, long replyTimeoutMs) {
      Passed found = null;
      if (type.equals("3") || type.equals("j")) {
        String refused = QuickFixMessages.first(fields, REF_SEQ_NUM);
        found = refused == null ? null : bySequence.get(refused);
      }
      for (int index = 0; found == null && index < fields.size(); index++) {
        QuickFixMessages.Field field = fields.get(index);
        if (REQUEST_IDS.contains(field.tag())) found = byId.get(field.tag() + "=" + field.value());
      }
      // A message that names no request is taken as the first answer to the last one, while it is awaited.
      if (found == null && last != null && !last.answered
          && System.nanoTime() - last.at < TimeUnit.MILLISECONDS.toNanos(replyTimeoutMs)) {
        found = last;
      }
      if (found != null) found.answered = true;
      return found;
    }
  }

  /** A request that was passed on. */
  private static final class Passed {
    private final QuickFixMessageView request;
    private final long at;
    private boolean answered;

    private Passed(QuickFixMessageView request, long at) {
      this.request = request;
      this.at = at;
    }
  }

  private static <V> Map<String, V> remembered() {
    return new LinkedHashMap<>(64, 0.75f, false) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
        return size() > REMEMBERED;
      }
    };
  }

  /** What a message of the server that answers no request is recorded as: its type alone. */
  private final class Unasked implements ProtocolMessageView {
    private final String type;

    private Unasked(String type) {
      this.type = type;
    }

    @Override
    public String protocol() {
      return endpoint.protocol();
    }

    @Override
    public String endpoint() {
      return endpoint.id();
    }

    @Override
    public String messageType() {
      return type;
    }

    @Override
    public Optional<Object> value(String path) {
      return Optional.empty();
    }
  }
}
