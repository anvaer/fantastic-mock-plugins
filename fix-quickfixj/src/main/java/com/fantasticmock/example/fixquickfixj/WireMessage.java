package com.fantasticmock.example.fixquickfixj;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.quickfixj.CharsetSupport;
import quickfix.DataDictionary;
import quickfix.DefaultMessageFactory;
import quickfix.Field;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageUtils;

/**
 * A message that is written as it was received. QuickFIX/J writes a message from the fields it
 * parsed: body tags in the order of their numbers and, of a tag that repeats outside the groups
 * its dictionary defines, a single value. A message that is passed on to another session, or
 * sent again when a counterparty asks for a resend, must not change that way. This one writes
 * the received text, and takes from its header what belongs to the session it is sent on: the
 * CompIDs, the sequence number and what a resend adds.
 *
 * <p>A message that was built here, not received, is written by QuickFIX/J as before.</p>
 */
final class WireMessage extends Message {

  private static final int BEGIN_STRING = 8;
  private static final int BODY_LENGTH = 9;
  private static final int CHECK_SUM = 10;
  private static final int MSG_TYPE = 35;
  private static final int POSS_DUP_FLAG = 43;
  private static final int SENDING_TIME = 52;
  private static final int ORIG_SENDING_TIME = 122;
  private static final int LAST_MSG_SEQ_NUM_PROCESSED = 369;
  // The header of FIX 4.2 and 4.4, for a session that has no dictionary.
  private static final Set<Integer> STANDARD_HEADER = Set.of(8, 9, 35, 49, 56, 115, 128, 90, 91, 34, 50, 142, 57,
      143, 116, 144, 129, 145, 43, 97, 52, 122, 212, 213, 347, 369, 370, 627, 628, 629, 630);

  /**
   * Creates every message of a session as a {@link WireMessage}, so that the ones QuickFIX/J
   * parses, as they arrive or from its store for a resend, are written as their text was.
   */
  static final class Factory implements MessageFactory {
    private final MessageFactory groups = new DefaultMessageFactory();
    private final DataDictionary dictionary;

    /** @param dictionary the sessions' dictionary, which says where the header ends; may be null */
    Factory(DataDictionary dictionary) {
      this.dictionary = dictionary;
    }

    @Override
    public Message create(String beginString, String msgType) {
      WireMessage message = new WireMessage();
      message.dictionary = dictionary;
      message.getHeader().setString(MSG_TYPE, msgType);
      return message;
    }

    @Override
    public Group create(String beginString, String msgType, int correspondingFieldID) {
      return groups.create(beginString, msgType, correspondingFieldID);
    }
  }

  // The text this message was received as; null until it is asked for, and for a message built here.
  private String wire;
  private boolean asking;
  private boolean builtHere;
  private DataDictionary dictionary;
  private List<QuickFixMessages.Field> fields;
  // The SendingTime a passed-on message came with; null for a resend, which gets a new one.
  private String sendingTime;
  // Values to write in place of the received ones, by tag.
  private Map<Integer, String> replaced = Map.of();
  // Whether the values are the changes of an upstream's rules: then null leaves a field out and
  // a tag that was not received is added. Otherwise a message is written as it always was.
  private boolean rules;
  private String written;

  /**
   * A received message to send on another session. Its header keeps every field that is not
   * that session's own, and its SendingTime; {@code replaced} gives the body fields that must
   * say something else there, such as the sequence number a reject refers to.
   */
  static WireMessage relayed(String wire, DataDictionary dictionary, Map<Integer, String> replaced) {
    return relayed(wire, dictionary, replaced, false);
  }

  /**
   * The same with the changes the rules of an upstream make: a tag with its new value, with
   * null to leave it out, and a tag the message did not have to add it, to the header when it
   * is a header field and after the body otherwise.
   */
  static WireMessage changed(String wire, DataDictionary dictionary, Map<Integer, String> changes) {
    return relayed(wire, dictionary, changes, true);
  }

  private static WireMessage relayed(String wire, DataDictionary dictionary, Map<Integer, String> replaced, boolean rules) {
    WireMessage message = new WireMessage();
    message.received(wire, dictionary);
    message.replaced = replaced;
    message.rules = rules;
    for (QuickFixMessages.Field field : message.fields) {
      int tag = field.tag();
      if (!message.header(tag)) break;
      switch (tag) {
        // Written anew, or true only of the session the message came on.
        case BEGIN_STRING, BODY_LENGTH, CHECK_SUM, POSS_DUP_FLAG, ORIG_SENDING_TIME, LAST_MSG_SEQ_NUM_PROCESSED -> { }
        case SENDING_TIME -> {
          message.sendingTime = field.value();
          message.getHeader().setString(tag, field.value());
        }
        default -> {
          if (!rules) {
            message.getHeader().setString(tag, field.value());
          } else {
            String value = replaced.containsKey(tag) ? replaced.get(tag) : field.value();
            if (value != null) message.getHeader().setString(tag, value);
          }
        }
      }
    }
    if (rules) {
      // A header field the message came without is written after the ones it had.
      for (Map.Entry<Integer, String> added : replaced.entrySet()) {
        if (added.getValue() != null && message.header(added.getKey()) && !message.getHeader().isSetField(added.getKey())) {
          message.getHeader().setString(added.getKey(), added.getValue());
        }
      }
    }
    return message;
  }

  /** The text this message was last written as, with the header of the session it was sent on; null before that. */
  String written() {
    return written;
  }

  private void received(String messageData, DataDictionary sessionDictionary) {
    wire = messageData;
    dictionary = sessionDictionary;
    fields = QuickFixMessages.fields(messageData, sessionDictionary);
  }

  private boolean header(int tag) {
    return dictionary != null ? dictionary.isHeaderField(tag) : STANDARD_HEADER.contains(tag);
  }

  /** Whether a tag belongs to the standard header, or to the header the dictionary defines. */
  static boolean headerField(DataDictionary dictionary, int tag) {
    return dictionary != null ? dictionary.isHeaderField(tag) : STANDARD_HEADER.contains(tag);
  }

  /** Whether a tag belongs to the header or the trailer, which every session writes itself. */
  static boolean sessionField(DataDictionary dictionary, int tag) {
    return dictionary != null ? dictionary.isHeaderField(tag) || dictionary.isTrailerField(tag)
        : STANDARD_HEADER.contains(tag) || tag == CHECK_SUM || tag == 89 || tag == 93;
  }

  /**
   * The received text with the header as it is set now. Header fields stay where they were;
   * one that was added since, as a resend adds PossDupFlag, follows them.
   */
  @Override
  public String toString() {
    if (wire == null) {
      // QuickFIX/J keeps the text it parsed a message from, and gives it out as the raw string.
      // For a message that was built, the raw string is this method again.
      if (asking) {
        builtHere = true;
        return super.toString();
      }
      asking = true;
      builtHere = false;
      String raw;
      try {
        raw = toRawString();
      } finally {
        asking = false;
      }
      if (builtHere || raw == null) {
        String built = raw != null ? raw : super.toString();
        written = built;
        return built;
      }
      received(raw, dictionary);
    }
    Header header = getHeader();
    StringBuilder content = new StringBuilder(wire.length() + 32);
    append(content, MSG_TYPE, text(header, MSG_TYPE, ""));
    Set<Integer> placed = new HashSet<>();
    boolean inHeader = true;
    for (QuickFixMessages.Field field : fields) {
      int tag = field.tag();
      if (tag == CHECK_SUM) continue;
      if (inHeader && header(tag)) {
        if (tag == BEGIN_STRING || tag == BODY_LENGTH || tag == MSG_TYPE || !header.isSetField(tag)) continue;
        append(content, tag, tag == SENDING_TIME && sendingTime != null ? sendingTime : text(header, tag, field.value()));
        placed.add(tag);
        continue;
      }
      if (inHeader) {
        inHeader = false;
        added(content, header, placed);
      }
      if (!rules) {
        String other = replaced.get(tag);
        append(content, tag, other != null ? other : field.value());
      } else if (!replaced.containsKey(tag)) {
        append(content, tag, field.value());
      } else if (replaced.get(tag) != null) {
        append(content, tag, replaced.get(tag));
      }
    }
    if (inHeader) added(content, header, placed);
    if (rules) {
      for (Map.Entry<Integer, String> added : replaced.entrySet()) {
        if (added.getValue() != null && !header(added.getKey()) && !had(added.getKey())) {
          append(content, added.getKey(), added.getValue());
        }
      }
    }

    String begin = text(header, BEGIN_STRING, fields.isEmpty() ? "" : fields.getFirst().value());
    var charset = CharsetSupport.getCharsetInstance();
    String text = BEGIN_STRING + "=" + begin + QuickFixMessages.SOH
        + BODY_LENGTH + "=" + MessageUtils.length(charset, content.toString()) + QuickFixMessages.SOH + content;
    text += CHECK_SUM + "=" + String.format("%03d", MessageUtils.checksum(charset, text, false)) + QuickFixMessages.SOH;
    written = text;
    return text;
  }

  private boolean had(int tag) {
    for (QuickFixMessages.Field field : fields) {
      if (field.tag() == tag) return true;
    }
    return false;
  }

  // Header fields that are set now and were not in the received text.
  private static void added(StringBuilder content, Header header, Set<Integer> placed) {
    for (Iterator<Field<?>> set = header.iterator(); set.hasNext(); ) {
      int tag = set.next().getTag();
      if (tag == BEGIN_STRING || tag == BODY_LENGTH || tag == MSG_TYPE || placed.contains(tag)) continue;
      append(content, tag, text(header, tag, ""));
    }
  }

  private static void append(StringBuilder content, int tag, String value) {
    content.append(tag).append('=').append(value).append(QuickFixMessages.SOH);
  }

  private static String text(Header header, int tag, String otherwise) {
    try {
      return header.isSetField(tag) ? header.getString(tag) : otherwise;
    } catch (FieldNotFound unset) {
      return otherwise;
    }
  }
}
