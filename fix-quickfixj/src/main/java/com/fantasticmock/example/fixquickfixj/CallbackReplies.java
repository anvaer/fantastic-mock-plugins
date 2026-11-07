package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import quickfix.Message;
import quickfix.SessionID;

/**
 * Callbacks that wait for replies: the application messages their sessions send back while
 * they wait. A reply belongs to the callback, not to the templates: it is reported as received
 * and never matched. A message that a callback's reply pattern does not match is not its reply
 * and goes to the templates as usual. Each session a callback went to sends its own replies.
 */
final class CallbackReplies {
  static final long DEFAULT_TIMEOUT_MS = 10_000;

  private final ScheduledExecutorService scheduler;
  private final Map<SessionID, List<Call>> waiting = new ConcurrentHashMap<>();
  // Read for every received message: while no callback waits, that is all it costs.
  private final AtomicInteger calls = new AtomicInteger();

  CallbackReplies(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  /** What a callback waits for after sending, read from its rendered document. */
  record Wait(int replies, Pattern pattern, long timeoutMs) {
    boolean waits() {
      return replies > 0;
    }

    static Wait of(JsonNode callback) {
      String patternText = optionalText(callback, "replyPattern");
      Pattern pattern;
      try {
        pattern = patternText == null ? null : Pattern.compile(patternText);
      } catch (PatternSyntaxException invalid) {
        throw new IllegalArgumentException("FIX callback 'replyPattern' is not a valid regular expression: "
            + invalid.getDescription());
      }
      long awaited = number(callback, "awaitReplies");
      if (awaited < 0 || awaited > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("FIX callback 'awaitReplies' must be 0 or more");
      }
      // A pattern alone waits for one reply it matches.
      int replies = awaited == 0 && pattern != null ? 1 : (int) awaited;
      long timeout = number(callback, "timeoutMs");
      return new Wait(replies, pattern, timeout > 0 ? timeout : DEFAULT_TIMEOUT_MS);
    }

    private static String optionalText(JsonNode callback, String field) {
      JsonNode value = callback == null || !callback.isObject() ? null : callback.get(field);
      if (value == null || value.isNull()) return null;
      String text = value.isTextual() ? value.textValue() : value.toString();
      return text.isEmpty() ? null : text;
    }

    // Numbers may be rendered from templates as text, such as "{{count}}" giving "3".
    private static long number(JsonNode callback, String field) {
      JsonNode value = callback == null || !callback.isObject() ? null : callback.get(field);
      if (value == null || value.isNull()) return 0;
      if (value.canConvertToLong()) return value.longValue();
      String text = value.asText("").trim();
      if (text.isEmpty()) return 0;
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException invalid) {
        throw new IllegalArgumentException("FIX callback '" + field + "' must be a whole number, not '"
            + text + "'");
      }
    }
  }

  /**
   * Waits for replies from these sessions, each with its name. Called before the callback is
   * sent, so no reply can come first.
   */
  Call start(Wait wait, Map<SessionID, String> sessions, BiConsumer<JsonNode, Long> received) {
    Call call = new Call(wait, sessions, received);
    calls.incrementAndGet();
    for (SessionID session : sessions.keySet()) {
      waiting.compute(session, (key, list) -> {
        List<Call> current = list == null ? new CopyOnWriteArrayList<>() : list;
        current.add(call);
        return current;
      });
    }
    return call;
  }

  /** Whether a waiting callback took this message of the session as its reply; the oldest first. */
  boolean take(SessionID session, Message message) {
    if (calls.get() == 0) return false;
    List<Call> list = waiting.get(session);
    if (list == null) return false;
    String wire = message.toRawString();
    String text = (wire == null ? message.toString() : wire).replace(QuickFixMessages.SOH, '|');
    for (Call call : list) {
      if (call.take(session, text)) return true;
    }
    return false;
  }

  /** Fails the callbacks still waiting for replies from a session that logged out. */
  void loggedOut(SessionID session) {
    if (calls.get() == 0) return;
    List<Call> list = waiting.get(session);
    if (list != null) list.forEach(call -> call.loggedOut(session));
  }

  /** Fails every callback that still waits. */
  void close(String reason) {
    List<Call> all = new ArrayList<>();
    waiting.values().forEach(all::addAll);
    all.forEach(call -> call.finish(new IOException(reason)));
  }

  private void forget(SessionID session, Call call) {
    waiting.computeIfPresent(session, (key, list) -> {
      list.remove(call);
      return list.isEmpty() ? null : list;
    });
  }

  /**
   * One message as the traffic log shows it, with the request fields of the protocol: its type,
   * CompIDs and tags, and its whole text with | for SOH.
   */
  static ObjectNode reported(String text) {
    ObjectNode message = JsonNodeFactory.instance.objectNode();
    try {
      FixText fix = FixText.parse(text);
      putText(message, "msgType", fix.first(35));
      putText(message, "senderCompId", fix.first(49));
      putText(message, "targetCompId", fix.first(56));
      ObjectNode tags = message.putObject("tag");
      fix.tags().forEach(tags::put);
      message.put("body", fix.logged());
    } catch (IllegalArgumentException unreadable) {
      message.removeAll();
      message.put("body", text);
    }
    return message;
  }

  private static void putText(ObjectNode message, String name, String value) {
    if (value != null) message.put(name, value);
  }

  /** One callback that waits: done once each of its sessions sent the replies it waits for. */
  final class Call {
    final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final Wait wait;
    private final Map<SessionID, Replies> sessions = new ConcurrentHashMap<>();
    private final BiConsumer<JsonNode, Long> received;
    private final AtomicBoolean finished = new AtomicBoolean();
    // The bytes the callback wrote; -1 until it was sent.
    private volatile long sent = -1;
    private volatile ScheduledFuture<?> timeout;

    private Call(Wait wait, Map<SessionID, String> names, BiConsumer<JsonNode, Long> received) {
      this.wait = wait;
      this.received = received;
      names.forEach((session, name) -> sessions.put(session, new Replies(name)));
    }

    // A session's messages come one at a time, on its own thread.
    private boolean take(SessionID session, String text) {
      Replies replies = sessions.get(session);
      if (replies == null || finished.get() || replies.count.get() >= wait.replies()) return false;
      if (wait.pattern() != null && !wait.pattern().matcher(text).find()) return false;
      received.accept(reported(text), (long) text.length());
      replies.count.incrementAndGet();
      // Before it was sent, the end of sending checks.
      if (sent >= 0 && complete()) finish(null);
      return true;
    }

    /** The callback could not be written to this session: it waits for the others only. */
    void drop(SessionID session) {
      sessions.remove(session);
      forget(session, this);
    }

    /** The callback was written: waits until the replies arrived or the timeout passed. */
    void sent(long bytes) {
      sent = bytes;
      try {
        // The scheduler only times: the failure is handed to a thread of its own.
        timeout = scheduler.schedule(() -> Thread.startVirtualThread(this::timedOut),
            wait.timeoutMs(), TimeUnit.MILLISECONDS);
      } catch (RuntimeException rejected) {
        finish(rejected);
        return;
      }
      // Done already if the replies came while the message was being written.
      if (complete()) finish(null);
      else if (finished.get()) cancelTimeout();
    }

    private void loggedOut(SessionID session) {
      Replies replies = sessions.get(session);
      if (replies == null || replies.count.get() >= wait.replies()) return;
      finish(new IOException("The FIX session " + replies.name + " logged out; " + lowerFirst(progress())));
    }

    private void timedOut() {
      finish(new TimeoutException(progress() + " within " + wait.timeoutMs() + " ms"));
    }

    private boolean complete() {
      for (Replies replies : sessions.values()) {
        if (replies.count.get() < wait.replies()) return false;
      }
      return true;
    }

    // "Received 0 of 1 reply matching \|35=S\| from CLIENT1 -> FMOCK", for the sessions short of them.
    private String progress() {
      StringBuilder text = new StringBuilder("Received ");
      String noun = wait.replies() == 1 ? " reply" : " replies";
      String counted = wait.pattern() == null ? "" : " matching " + wait.pattern().pattern();
      sessions.values().stream()
          .filter(replies -> replies.count.get() < wait.replies())
          .sorted((left, right) -> left.name.compareTo(right.name))
          .forEach(replies -> {
            boolean first = text.length() == "Received ".length();
            if (!first) text.append(", ");
            text.append(replies.count.get()).append(" of ").append(wait.replies());
            if (first) text.append(noun).append(counted);
            text.append(" from ").append(replies.name);
          });
      return text.toString();
    }

    void finish(Throwable failure) {
      if (!finished.compareAndSet(false, true)) return;
      cancelTimeout();
      sessions.keySet().forEach(session -> forget(session, this));
      calls.decrementAndGet();
      if (failure == null) completion.complete(Math.max(0, sent));
      else completion.completeExceptionally(failure);
    }

    private void cancelTimeout() {
      ScheduledFuture<?> waitingFor = timeout;
      if (waitingFor != null) waitingFor.cancel(false);
    }
  }

  private static final class Replies {
    final String name;
    final AtomicInteger count = new AtomicInteger();

    Replies(String name) {
      this.name = name;
    }
  }

  private static String lowerFirst(String text) {
    return text.isEmpty() ? text : Character.toLowerCase(text.charAt(0)) + text.substring(1);
  }
}
