package com.fantasticmock.example.fixquickfixj;

import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.exchange.ProtocolResponseDelivery;
import com.fantasticmock.protocol.api.exchange.ProtocolResponseWriter;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;

/**
 * The QuickFIX/J callbacks of one endpoint. QuickFIX/J handles the administrative messages
 * itself; application messages go to the endpoint, and rendered responses back to the session.
 *
 * <p>QuickFIX/J calls {@link #fromApp} on the session's own thread, so a session's messages
 * are matched, and their immediate responses sent, in the order they arrived.</p>
 */
final class QuickFixApplication implements Application {

  private static final int MSG_SEQ_NUM = 34;
  private static final int MSG_TYPE = 35;
  private static final int REF_SEQ_NUM = 45;
  private static final int TEXT = 58;
  private static final int REF_MSG_TYPE = 372;
  private static final int BUSINESS_REJECT_REASON = 380;
  // BusinessRejectReason (380)
  private static final int OTHER = 0;
  private static final int APPLICATION_NOT_AVAILABLE = 4;

  private final ProtocolEndpoint endpoint;
  private final QuickFixAcceptor acceptor;
  private final QuickFixUpstream upstream;
  private final CallbackReplies replies;
  // One per logged-on session: closing it on logout cancels the session's delayed responses.
  private final Map<SessionID, ProtocolResponseDelivery> deliveries = new ConcurrentHashMap<>();

  QuickFixApplication(ProtocolEndpoint endpoint, QuickFixAcceptor acceptor, QuickFixUpstream upstream,
      CallbackReplies replies) {
    this.endpoint = endpoint;
    this.acceptor = acceptor;
    this.upstream = upstream;
    this.replies = replies;
  }

  @Override
  public void onCreate(SessionID sessionId) {
  }

  @Override
  public void onLogon(SessionID sessionId) {
    Session session = Session.lookupSession(sessionId);
    // Session.send only queues the bytes, so the writer may run on the shared timer thread. It
    // hands over the text it wrote, which the host keeps for the log.
    ProtocolResponseDelivery previous = deliveries.put(sessionId, endpoint.responseDelivery(ProtocolResponseWriter.recording(
        response -> CompletableFuture.completedFuture(acceptor.send(session, response)))));
    if (previous != null) previous.close();
    endpoint.statistics().increment("logons");
    // Where the endpoint's messages go to a real server, that server is logged on to now.
    upstream.joined(sessionId);
  }

  @Override
  public void onLogout(SessionID sessionId) {
    ProtocolResponseDelivery delivery = deliveries.remove(sessionId);
    if (delivery != null) delivery.close();
    replies.loggedOut(sessionId);
    upstream.left(sessionId);
    // A session this side logged out, because the real server ended its own, takes the client back.
    Session session = Session.lookupSession(sessionId);
    if (session != null && !session.isEnabled() && acceptor.running()) session.logon();
  }

  @Override
  public void toAdmin(Message message, SessionID sessionId) {
  }

  @Override
  public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound, RejectLogon {
    if (!"A".equals(message.getHeader().getString(MSG_TYPE))) return;
    String refusal = acceptor.refusal(sessionId);
    if (refusal != null) {
      endpoint.statistics().increment("refusedLogons");
      // QuickFIX/J answers with a Logout carrying this text and disconnects.
      throw new RejectLogon(refusal);
    }
    upstream.logon(sessionId, message.toRawString());
  }

  @Override
  public void toApp(Message message, SessionID sessionId) {
  }

  @Override
  public void fromApp(Message message, SessionID sessionId) throws UnsupportedMessageType {
    Session session = Session.lookupSession(sessionId);
    ProtocolResponseDelivery delivery = deliveries.get(sessionId);
    if (session == null || delivery == null) return;
    // A reply a callback waits for is the callback's, not a request for the templates.
    if (replies.take(sessionId, message)) return;

    CompletableFuture<ProtocolExchangeResult> exchange = endpoint.handle(
        new QuickFixMessageView(endpoint, message, sessionId, session.getDataDictionary()), delivery);
    exchange.whenComplete((result, failure) -> {
      // Answer only what was not written: an overload or a failure. A miss is answered below.
      if (failure == null || failure instanceof CancellationException) return;
      if (failure instanceof RejectedExecutionException) {
        businessReject(session, message, APPLICATION_NOT_AVAILABLE, "The mock endpoint is over capacity");
      } else {
        businessReject(session, message, OTHER, "The message was not answered: " + failure.getMessage());
      }
    });
    // A miss completes at once. QuickFIX/J turns this exception into a BusinessMessageReject
    // with 380=3, Unsupported Message Type.
    if (acceptor.rejectUnmatched() && exchange.isDone() && !exchange.isCompletedExceptionally()
        && !exchange.join().matched()) {
      endpoint.statistics().increment("businessRejects");
      throw new UnsupportedMessageType();
    }
  }

  /** Cancels the delayed responses of every session. */
  void close() {
    deliveries.values().forEach(ProtocolResponseDelivery::close);
    deliveries.clear();
  }

  private void businessReject(Session session, Message request, int reason, String text) {
    if (!session.isLoggedOn()) return;
    try {
      Message reject = new Message();
      reject.getHeader().setString(MSG_TYPE, "j");
      reject.setInt(REF_SEQ_NUM, request.getHeader().getInt(MSG_SEQ_NUM));
      reject.setString(REF_MSG_TYPE, request.getHeader().getString(MSG_TYPE));
      reject.setInt(BUSINESS_REJECT_REASON, reason);
      reject.setString(TEXT, plain(text));
      if (session.send(reject)) endpoint.statistics().increment("businessRejects");
    } catch (FieldNotFound impossible) {
      // QuickFIX/J only passes on messages that carry MsgType and MsgSeqNum.
    }
  }

  // Text (58) for a reject: one line of printable characters.
  private static String plain(String text) {
    String value = text == null ? "" : text.replaceAll("\\p{Cntrl}", " ").strip();
    if (value.isEmpty()) return "Unknown error";
    return value.length() > 200 ? value.substring(0, 200) : value;
  }
}
