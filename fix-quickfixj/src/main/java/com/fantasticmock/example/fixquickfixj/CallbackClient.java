package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.DefaultSessionFactory;
import quickfix.FieldNotFound;
import quickfix.Initiator;
import quickfix.Log;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;

/**
 * Callbacks with a {@code url}: FIX client sessions with that server, one for each pair of
 * CompIDs. A callback sends over the session that is logged on with its CompIDs, and logs on in
 * a new one when there is none; a session that no callback has used for {@code keepSessionMs}
 * logs out. Each Logon resets sequence numbers (141=Y).
 *
 * <p>Each application message the server sends, and each Reject, goes to the callback whose
 * message it answers: the one whose request ID it repeats (ClOrdID, QuoteReqID and the like),
 * the one a Reject's RefSeqNum names, or else the latest one still waiting. A message that comes
 * while no callback waits is nobody's and is not reported. Session messages (Logon, Heartbeat,
 * Logout) are not reported either.</p>
 */
final class CallbackClient implements AutoCloseable, Application {
  static final long DEFAULT_KEEP_SESSION_MS = 30_000;
  private static final int MSG_SEQ_NUM = 34;
  private static final int MSG_TYPE = 35;
  private static final int REF_SEQ_NUM = 45;
  private static final int TEXT = 58;
  private static final int HEART_BT_INT = 108;
  private static final int DEFAULT_HEART_BT_INT = 30;
  // What the session writes into a Logon itself: BeginString, BodyLength, CheckSum, MsgSeqNum,
  // MsgType, SenderCompID, SendingTime, TargetCompID, HeartBtInt and ResetSeqNumFlag.
  private static final Set<Integer> SESSION_OWN = Set.of(8, 9, 10, 34, 35, 49, 52, 56, 108, 141);

  private final ScheduledExecutorService scheduler;
  private final String beginString;
  private final String defaultSender;
  private final Supplier<SessionSettings> baseSettings;
  private final Supplier<DataDictionary> dictionary;
  // The sessions that are open or opening, by server and CompIDs; and the same by SessionID.
  private final Map<Key, Link> links = new ConcurrentHashMap<>();
  private final Map<SessionID, Link> bySession = new ConcurrentHashMap<>();
  private final AtomicLong numbers = new AtomicLong();
  // Stopping a QuickFIX/J engine waits for its Logout and must not run on the engine's own thread.
  private final ExecutorService stops = Executors.newCachedThreadPool(
      Thread.ofPlatform().daemon().name("fstub-fix-callback-", 0).factory());
  // One factory for every session: it reads the data dictionary once.
  private volatile SessionFactory factory;

  private record Key(String host, int port, String sender, String target) {
  }

  CallbackClient(ScheduledExecutorService scheduler, String beginString, String defaultSender,
      Supplier<SessionSettings> baseSettings, Supplier<DataDictionary> dictionary) {
    this.scheduler = scheduler;
    this.beginString = beginString;
    this.defaultSender = defaultSender;
    this.baseSettings = baseSettings;
    this.dictionary = dictionary;
  }

  /** Sends the callback over the session with its server and CompIDs, logging on first if needed. */
  CompletionStage<Long> send(JsonNode callback, BiConsumer<JsonNode, Long> sent, BiConsumer<JsonNode, Long> received) {
    Call call;
    Key key;
    QuickFixUpstream.Target target;
    Map<Integer, String> logonTags;
    long keep;
    try {
      target = QuickFixUpstream.Target.parse(callback.path("url").asText(""));
      String sender = text(callback, "senderCompId", defaultSender);
      String server = text(callback, "targetCompId", "");
      if (server.isEmpty()) {
        throw new IllegalArgumentException("A FIX callback with 'url' needs 'targetCompId', the server's CompID");
      }
      if (sender.isEmpty()) {
        throw new IllegalArgumentException(
            "A FIX callback with 'url' needs 'senderCompId', or a sender-comp-id on the endpoint");
      }
      logonTags = logonTags(callback.path("logonTags"));
      keep = keepSession(callback);
      key = new Key(target.host(), target.port(), sender, server);
      call = new Call(callback, CallbackReplies.Wait.of(callback), sent, received);
    } catch (RuntimeException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
    while (true) {
      Link link = links.computeIfAbsent(key, ignored -> new Link(key, target, logonTags));
      if (link.add(call, keep)) break;
      // It is closing: a new one takes its place.
      links.remove(key, link);
    }
    return call.completion;
  }

  private SessionFactory factory() {
    SessionFactory current = factory;
    if (current == null) {
      synchronized (this) {
        current = factory;
        if (current == null) {
          current = new DefaultSessionFactory(this, new MemoryStoreFactory(), this::log,
              new WireMessage.Factory(dictionary.get()));
          factory = current;
        }
      }
    }
    return current;
  }

  private static long keepSession(JsonNode callback) {
    JsonNode value = callback.path("keepSessionMs");
    if (value.isMissingNode() || value.isNull() || value.asText("").isBlank()) return DEFAULT_KEEP_SESSION_MS;
    long keep = value.canConvertToLong() ? value.longValue() : parse(value.asText().trim());
    if (keep < 0) throw new IllegalArgumentException("FIX callback 'keepSessionMs' must be 0 or more");
    return keep;
  }

  private static long parse(String text) {
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("FIX callback 'keepSessionMs' must be a whole number, not '" + text + "'");
    }
  }

  private static Map<Integer, String> logonTags(JsonNode tags) {
    Map<Integer, String> values = new LinkedHashMap<>();
    if (tags.isMissingNode() || tags.isNull()) return values;
    if (!tags.isObject()) throw new IllegalArgumentException("FIX callback 'logonTags' must be an object of tag number to value");
    tags.properties().forEach(entry -> {
      int tag;
      try {
        tag = Integer.parseInt(entry.getKey().trim());
      } catch (NumberFormatException invalid) {
        tag = -1;
      }
      if (tag <= 0) throw new IllegalArgumentException("FIX callback 'logonTags' has a key that is no tag number: " + entry.getKey());
      String value = entry.getValue().isTextual() ? entry.getValue().textValue() : entry.getValue().asText();
      if (!value.isEmpty()) values.put(tag, value);
    });
    return values;
  }

  private static String text(JsonNode document, String field, String fallback) {
    String value = document.path(field).asText("").trim();
    return value.isEmpty() ? (fallback == null ? "" : fallback.trim()) : value;
  }

  private static int positive(String number, int otherwise) {
    try {
      int value = number == null ? 0 : Integer.parseInt(number.trim());
      return value > 0 ? value : otherwise;
    } catch (NumberFormatException invalid) {
      return otherwise;
    }
  }

  // Timers only time: what follows runs elsewhere.
  private void later(Runnable task) {
    try {
      stops.execute(task);
    } catch (RuntimeException stopping) {
      task.run();
    }
  }

  /** Ends every session and the callbacks still on them: the endpoint is stopping. */
  @Override
  public void close() {
    for (Link link : List.copyOf(links.values())) link.end(new IOException("The FIX endpoint is stopping"), true);
    stops.shutdown();
    try {
      stops.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  // ---- The engines' callbacks, passed to the session's link ------------------------------------

  @Override
  public void onCreate(SessionID sessionId) {
  }

  @Override
  public void onLogon(SessionID sessionId) {
    Link link = bySession.get(sessionId);
    if (link != null) link.loggedOn();
  }

  @Override
  public void onLogout(SessionID sessionId) {
    Link link = bySession.get(sessionId);
    if (link != null) link.loggedOut();
  }

  @Override
  public void toAdmin(Message message, SessionID sessionId) {
    Link link = bySession.get(sessionId);
    if (link != null && "A".equals(type(message))) link.addLogonTags(message);
  }

  @Override
  public void fromAdmin(Message message, SessionID sessionId) {
    Link link = bySession.get(sessionId);
    if (link == null) return;
    String type = type(message);
    if ("5".equals(type)) {
      String text = field(message, TEXT);
      if (text != null) link.trouble = text;
    } else if ("3".equals(type)) {
      // A Reject of a callback's message answers it as much as an application message does.
      link.arrived(message);
    }
  }

  @Override
  public void toApp(Message message, SessionID sessionId) {
  }

  @Override
  public void fromApp(Message message, SessionID sessionId) {
    Link link = bySession.get(sessionId);
    if (link != null) link.arrived(message);
  }

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
        Link link = bySession.get(sessionId);
        if (link != null) link.trouble = text;
      }
    };
  }

  // ---- A session with a server ------------------------------------------------------------------

  /** One session with a server, the engine that holds it and the callbacks it carries. */
  private final class Link {
    private final Key key;
    private final QuickFixUpstream.Target target;
    private final Map<Integer, String> logonTags;
    private final SessionID id;
    private final ReentrantLock lock = new ReentrantLock();
    // Guarded by lock: callbacks waiting for the Logon, and those sent and waiting for replies
    // (in the order they were sent); by the request IDs and MsgSeqNum of their messages.
    private final List<Call> queued = new ArrayList<>();
    private final List<Call> active = new ArrayList<>();
    private final Map<String, Call> byId = new HashMap<>();
    private final Map<Integer, Call> bySequence = new HashMap<>();
    private boolean started;
    private boolean loggedOn;
    private boolean closed;
    private ScheduledFuture<?> idle;
    private volatile long keepMs = DEFAULT_KEEP_SESSION_MS;
    private volatile SocketInitiator engine;
    // Why the session is not what it should be: QuickFIX/J's last error, or the server's Logout text.
    private volatile String trouble = "";

    Link(Key key, QuickFixUpstream.Target target, Map<Integer, String> logonTags) {
      this.key = key;
      this.target = target;
      this.logonTags = logonTags;
      // A qualifier of its own: QuickFIX/J keeps one registry of sessions for the whole process.
      this.id = new SessionID(beginString, key.sender(), key.target(), "callback-" + numbers.incrementAndGet());
    }

    /** Takes a callback, or says no when this session is closing. */
    boolean add(Call call, long keep) {
      boolean send;
      boolean start;
      lock.lock();
      try {
        if (closed) return false;
        keepMs = keep;
        if (idle != null) {
          idle.cancel(false);
          idle = null;
        }
        call.link = this;
        send = loggedOn;
        if (!send) queued.add(call);
        start = !started;
        started = true;
      } finally {
        lock.unlock();
      }
      if (send) {
        write(call);
      } else {
        call.awaitLogon();
        if (start) start();
      }
      return true;
    }

    private void start() {
      try {
        SessionSettings settings = baseSettings.get();
        settings.setString(id, Initiator.SETTING_SOCKET_CONNECT_HOST, target.host());
        settings.setLong(id, Initiator.SETTING_SOCKET_CONNECT_PORT, target.port());
        settings.setLong(id, Session.SETTING_HEARTBTINT, positive(logonTags.get(HEART_BT_INT), DEFAULT_HEART_BT_INT));
        settings.setBool(id, Session.SETTING_RESET_ON_LOGON, true);
        // Connects once: a session that cannot log on is closed and the next callback starts anew.
        settings.setLong(id, Initiator.SETTING_RECONNECT_INTERVAL, 3600);
        bySession.put(id, this);
        SocketInitiator initiator = new SocketInitiator(factory(), settings, 1024);
        engine = initiator;
        initiator.start();
      } catch (ConfigError | RuntimeException failure) {
        end(failure, false);
      }
    }

    void loggedOn() {
      List<Call> waiting;
      lock.lock();
      try {
        if (closed) return;
        loggedOn = true;
        waiting = List.copyOf(queued);
        queued.clear();
      } finally {
        lock.unlock();
      }
      for (Call call : waiting) write(call);
    }

    void addLogonTags(Message logon) {
      if (logonTags.isEmpty()) return;
      Session session = Session.lookupSession(id);
      DataDictionary rules = session == null ? null : session.getDataDictionary();
      logonTags.forEach((tag, value) -> {
        if (SESSION_OWN.contains(tag)) return;
        if (rules != null && rules.isHeaderField(tag)) logon.getHeader().setString(tag, value);
        else logon.setString(tag, value);
      });
    }

    /** Sends a callback's message, known by its request IDs and then its MsgSeqNum. */
    private void write(Call call) {
      call.loggedOn();
      try {
        Session session = Session.lookupSession(id);
        if (session == null) throw new IOException("The FIX session with " + target + " is gone");
        Message message = QuickFixMessages.message(call.callback, beginString, session.getDataDictionary());
        Set<String> ids = requestIds(message);
        lock.lock();
        try {
          if (closed) throw new IOException("The FIX session with " + target + " ended before the callback was sent");
          active.add(call);
          call.ids = ids;
          for (String value : ids) byId.put(value, call);
        } finally {
          lock.unlock();
        }
        if (!session.send(message)) throw new IOException("The FIX session with " + target + " ended before the callback was sent");
        Integer sequence = number(field(message.getHeader(), MSG_SEQ_NUM));
        if (sequence != null) {
          lock.lock();
          try {
            call.sequence = sequence;
            bySequence.put(sequence, call);
          } finally {
            lock.unlock();
          }
        }
        call.wrote(message.toString().replace(QuickFixMessages.SOH, '|'));
      } catch (IOException | RuntimeException failure) {
        call.finish(failure);
      }
    }

    /** Hands a message from the server to the callback it answers. */
    void arrived(Message message) {
      Call owner = null;
      lock.lock();
      try {
        for (int tag : QuickFixUpstream.REQUEST_IDS) {
          String value = field(message, tag);
          if (value != null && byId.containsKey(value)) {
            owner = byId.get(value);
            break;
          }
        }
        if (owner == null) {
          Integer reference = number(field(message, REF_SEQ_NUM));
          if (reference != null) owner = bySequence.get(reference);
        }
        if (owner == null && !active.isEmpty()) owner = active.getLast();
      } finally {
        lock.unlock();
      }
      if (owner == null) return;
      String wire = message.toRawString();
      owner.arrived((wire == null ? message.toString() : wire).replace(QuickFixMessages.SOH, '|'));
    }

    /** A callback is over: when none is left, the session waits keepSessionMs for the next one. */
    void done(Call call) {
      boolean closeNow = false;
      lock.lock();
      try {
        queued.remove(call);
        active.remove(call);
        for (String value : call.ids) byId.remove(value, call);
        if (call.sequence > 0) bySequence.remove(call.sequence, call);
        if (closed || !queued.isEmpty() || !active.isEmpty()) return;
        // One that never logged on is not kept: it would not try again for an hour.
        if (!loggedOn || keepMs <= 0) {
          closeNow = true;
        } else {
          idle = scheduler.schedule(() -> later(this::idleOut), keepMs, TimeUnit.MILLISECONDS);
        }
      } finally {
        lock.unlock();
      }
      if (closeNow) end(null, false);
    }

    private void idleOut() {
      lock.lock();
      try {
        if (closed || !queued.isEmpty() || !active.isEmpty()) return;
      } finally {
        lock.unlock();
      }
      end(null, false);
    }

    /** The server ended the session, or refused the Logon. */
    void loggedOut() {
      boolean wasLoggedOn;
      lock.lock();
      try {
        if (closed) return;
        wasLoggedOn = loggedOn;
      } finally {
        lock.unlock();
      }
      String why = trouble.isEmpty() ? "" : ": " + trouble;
      end(new IOException(wasLoggedOn ? target + " ended the session" + why : target + " refused the Logon" + why), false);
    }

    /** Closes the session; the callbacks still on it fail with the reason. */
    void end(Throwable reason, boolean now) {
      List<Call> ending;
      lock.lock();
      try {
        if (closed) return;
        closed = true;
        if (idle != null) idle.cancel(false);
        ending = new ArrayList<>(queued);
        ending.addAll(active);
      } finally {
        lock.unlock();
      }
      links.remove(key, this);
      for (Call call : ending) {
        call.finish(reason instanceof IOException io && call.wrote()
            ? new IOException(io.getMessage() + "; " + lowerFirst(call.progress()), io)
            : reason == null ? new IOException("The FIX session with " + target + " was closed") : reason);
      }
      SocketInitiator current = engine;
      Runnable stop = () -> {
        try {
          if (current != null) current.stop(now);
        } finally {
          bySession.remove(id, this);
        }
      };
      if (now) {
        stop.run();
      } else {
        later(stop);
      }
    }
  }

  // ---- A callback --------------------------------------------------------------------------------

  /** One callback: its message, and the replies it waits for. */
  private final class Call {
    final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final JsonNode callback;
    private final CallbackReplies.Wait wait;
    private final BiConsumer<JsonNode, Long> sent;
    private final BiConsumer<JsonNode, Long> received;
    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile Link link;
    // Guarded by the link's lock.
    private Set<String> ids = Set.of();
    private int sequence;
    private volatile ScheduledFuture<?> timeout;
    // The bytes of the callback's message; -1 until it was sent.
    private volatile long size = -1;
    // Messages of a session come one at a time on its engine's thread.
    private volatile int replies;
    private volatile int matched;

    Call(JsonNode callback, CallbackReplies.Wait wait, BiConsumer<JsonNode, Long> sent,
        BiConsumer<JsonNode, Long> received) {
      this.callback = callback;
      this.wait = wait;
      this.sent = sent == null ? (message, bytes) -> { } : sent;
      this.received = received == null ? (message, bytes) -> { } : received;
    }

    boolean wrote() {
      return size >= 0;
    }

    /** Waiting for a Logon: that must come within the timeout too. */
    void awaitLogon() {
      timeout = scheduler.schedule(() -> later(this::notLoggedOn), wait.timeoutMs(), TimeUnit.MILLISECONDS);
    }

    void loggedOn() {
      cancelTimeout();
    }

    void wrote(String text) {
      size = text.length();
      // Reported before the callback is over, so the host logs it as this callback's message.
      sent.accept(CallbackReplies.reported(text), size);
      if (!wait.waits()) {
        finish(null);
        return;
      }
      timeout = scheduler.schedule(() -> later(this::timedOut), wait.timeoutMs(), TimeUnit.MILLISECONDS);
      // Replies that came while the message was being written.
      if (matched >= wait.replies()) finish(null);
    }

    void arrived(String text) {
      if (finished.get()) return;
      received.accept(CallbackReplies.reported(text), (long) text.length());
      if (!wait.waits()) return;
      replies++;
      if (wait.pattern() == null || wait.pattern().matcher(text).find()) matched++;
      if (matched >= wait.replies() && size >= 0) finish(null);
    }

    private void notLoggedOn() {
      if (size >= 0 || finished.get()) return;
      Link current = link;
      String trouble = current == null || current.trouble.isEmpty() ? "" : ": " + current.trouble;
      finish(new TimeoutException("Not logged on to " + (current == null ? "the server" : current.target)
          + " within " + wait.timeoutMs() + " ms" + trouble));
    }

    private void timedOut() {
      finish(new TimeoutException(progress() + " within " + wait.timeoutMs() + " ms"));
    }

    // "Received 1 of 2 replies matching \|35=S\| (3 in all)", for timeouts and ended sessions.
    String progress() {
      String counted = wait.pattern() == null ? "" : " matching " + wait.pattern().pattern();
      return "Received " + matched + " of " + wait.replies() + (wait.replies() == 1 ? " reply" : " replies") + counted
          + (wait.pattern() != null && replies > matched ? " (" + replies + " in all)" : "");
    }

    void finish(Throwable failure) {
      if (!finished.compareAndSet(false, true)) return;
      cancelTimeout();
      if (failure == null) completion.complete(Math.max(0, size));
      else completion.completeExceptionally(failure);
      Link current = link;
      if (current != null) current.done(this);
    }

    private void cancelTimeout() {
      ScheduledFuture<?> waiting = timeout;
      if (waiting != null) waiting.cancel(false);
    }
  }

  // ---- Fields ----------------------------------------------------------------------------------

  // The request IDs a message carries, which an answer repeats.
  private static Set<String> requestIds(Message message) {
    Set<String> ids = new LinkedHashSet<>();
    for (int tag : QuickFixUpstream.REQUEST_IDS) {
      String value = field(message, tag);
      if (value != null && !value.isEmpty()) ids.add(value);
    }
    return ids;
  }

  private static String field(quickfix.FieldMap fields, int tag) {
    try {
      return fields.isSetField(tag) ? fields.getString(tag) : null;
    } catch (FieldNotFound missing) {
      return null;
    }
  }

  private static Integer number(String text) {
    try {
      return text == null ? null : Integer.valueOf(text.trim());
    } catch (NumberFormatException invalid) {
      return null;
    }
  }

  private static String type(Message message) {
    String type = field(message.getHeader(), MSG_TYPE);
    return type == null ? "" : type;
  }

  private static String lowerFirst(String text) {
    return text.isEmpty() ? text : Character.toLowerCase(text.charAt(0)) + text.substring(1);
  }
}
