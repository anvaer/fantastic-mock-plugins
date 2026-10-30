package com.fantasticmock.example.fixquickfixj;

import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import quickfix.DataDictionary;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.SessionID;

/**
 * The request fields declared in {@code protocol.yml}, read from a received message on demand.
 * Traffic logs and metrics are classified by MsgType.
 */
final class QuickFixMessageView implements ProtocolMessageView {

  private static final int MSG_SEQ_NUM = 34;
  private static final int MSG_TYPE = 35;
  private static final int SENDER_COMP_ID = 49;
  private static final int TARGET_COMP_ID = 56;

  private final Message message;
  private final SessionID session;
  private final String raw;
  private final DataDictionary dictionary;
  private final ProtocolMessageView fields;
  // Built on first use: routing on MsgType alone never needs it.
  private volatile Map<String, String> tags;

  QuickFixMessageView(ProtocolEndpoint endpoint, Message message, SessionID session, DataDictionary dictionary) {
    this.message = message;
    this.session = session;
    this.raw = message.toRawString();
    this.dictionary = dictionary;
    this.fields = endpoint.message(this, QuickFixMessageView::field);
  }

  /** The session the message came on. */
  SessionID session() {
    return session;
  }

  /** The message as it arrived, with SOH. */
  String wire() {
    return raw;
  }

  /** Its fields in wire order, every occurrence of a repeated tag. */
  List<QuickFixMessages.Field> wireFields() {
    return QuickFixMessages.fields(raw, dictionary);
  }

  /** The sequence number it arrived with. */
  String sequence() {
    return header(MSG_SEQ_NUM);
  }

  private Object field(String name) {
    return switch (name) {
      case "msgType" -> header(MSG_TYPE);
      case "senderCompId" -> header(SENDER_COMP_ID);
      case "targetCompId" -> header(TARGET_COMP_ID);
      case "tag" -> tags();
      case "body" -> raw.replace(QuickFixMessages.SOH, '|');
      default -> null;
    };
  }

  private String header(int tag) {
    try {
      return message.getHeader().isSetField(tag) ? message.getHeader().getString(tag) : null;
    } catch (FieldNotFound missing) {
      return null;
    }
  }

  // Tag number to value in wire order; a repeated tag keeps its first value.
  private Map<String, String> tags() {
    Map<String, String> result = tags;
    if (result == null) {
      Map<String, String> values = new LinkedHashMap<>();
      for (QuickFixMessages.Field field : QuickFixMessages.fields(raw, dictionary)) {
        values.putIfAbsent(Integer.toString(field.tag()), field.value());
      }
      tags = result = Collections.unmodifiableMap(values);
    }
    return result;
  }

  @Override
  public String protocol() {
    return fields.protocol();
  }

  @Override
  public String endpoint() {
    return fields.endpoint();
  }

  @Override
  public String messageType() {
    String type = header(MSG_TYPE);
    return type == null ? "unknown" : type;
  }

  @Override
  public Optional<Object> value(String path) {
    return fields.value(path);
  }

  @Override
  public Optional<ByteBuffer> raw() {
    return fields.raw();
  }

  @Override
  public Long payloadSize() {
    return (long) raw.length();
  }

  @Override
  public Map<String, Object> logFields() {
    return fields.logFields();
  }
}
