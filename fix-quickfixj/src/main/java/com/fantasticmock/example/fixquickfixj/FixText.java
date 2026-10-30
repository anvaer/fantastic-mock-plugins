package com.fantasticmock.example.fixquickfixj;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.quickfixj.CharsetSupport;
import quickfix.DataDictionary;
import quickfix.MessageUtils;

/**
 * A FIX message as text, as an Example request or a traffic log writes it: tag=value pairs
 * separated by |, SOH or line breaks, every field in order, a repeated tag as often as it
 * repeats. Changes keep the way it was written: its separator, its order, the occurrences of a
 * tag after the first.
 */
final class FixText {

  private static final char SOH = QuickFixMessages.SOH;
  private static final int BEGIN_STRING = 8;
  private static final int BODY_LENGTH = 9;
  private static final int CHECK_SUM = 10;
  private static final int MSG_TYPE = 35;
  private static final int SIGNATURE = 89;
  private static final int SIGNATURE_LENGTH = 93;

  private final List<QuickFixMessages.Field> fields;
  private final String separator;
  // Ends with a separator, as a logged message ends with |.
  private final boolean closed;

  private FixText(List<QuickFixMessages.Field> fields, String separator, boolean closed) {
    this.fields = fields;
    this.separator = separator;
    this.closed = closed;
  }

  /** Reads the text; a part that is not tag=value is refused, naming it. */
  static FixText parse(String text) {
    String value = text == null ? "" : text;
    boolean soh = value.indexOf(SOH) >= 0;
    boolean lines = !soh && value.indexOf('|') < 0 && value.indexOf('\n') >= 0;
    String separator = soh ? String.valueOf(SOH) : lines ? "\n" : "|";
    List<QuickFixMessages.Field> fields = new ArrayList<>();
    for (String pair : value.split(soh ? String.valueOf(SOH) : "[|\\r\\n]")) {
      if (pair.isBlank()) continue;
      int equals = pair.indexOf('=');
      int tag = equals < 0 ? -1 : tag(pair.substring(0, equals).strip());
      if (tag <= 0) throw new IllegalArgumentException("Not a FIX field, tag=value: " + pair.strip());
      String fieldValue = pair.substring(equals + 1);
      fields.add(new QuickFixMessages.Field(tag, lines ? fieldValue.stripTrailing() : fieldValue));
    }
    String end = value.stripTrailing();
    boolean closed = !lines && (end.isEmpty() || end.endsWith(separator));
    return new FixText(fields, separator, closed);
  }

  List<QuickFixMessages.Field> fields() {
    return fields;
  }

  /** The first value of each tag, in the order the tags first come. */
  Map<String, String> tags() {
    Map<String, String> tags = new LinkedHashMap<>();
    for (QuickFixMessages.Field field : fields) tags.putIfAbsent(Integer.toString(field.tag()), field.value());
    return tags;
  }

  /** The first value of a tag, or null. */
  String first(int tag) {
    return QuickFixMessages.first(fields, tag);
  }

  /** As a log writes a received message: | between fields and after the last. */
  String logged() {
    return joined("|", true);
  }

  /** Written the way it was read. */
  String text() {
    return joined(separator, closed);
  }

  /**
   * With changes by tag: a new value replaces the tag's first occurrence, null removes that
   * occurrence, and a tag the message lacks is added where it belongs, a header field after
   * the header, a body field before the trailer. BodyLength and CheckSum, where the message
   * has them and no change names them, are counted again.
   */
  FixText with(Map<Integer, String> changes, DataDictionary dictionary) {
    List<QuickFixMessages.Field> changed = new ArrayList<>(fields);
    changes.forEach((tag, value) -> {
      int index = indexOf(changed, tag);
      if (index >= 0) {
        if (value == null) changed.remove(index);
        else changed.set(index, new QuickFixMessages.Field(tag, value));
      } else if (value != null) {
        changed.add(position(changed, tag, dictionary), new QuickFixMessages.Field(tag, value));
      }
    });
    if (!changes.containsKey(BODY_LENGTH)) recount(changed, BODY_LENGTH);
    if (!changes.containsKey(CHECK_SUM)) recount(changed, CHECK_SUM);
    return new FixText(changed, separator, closed);
  }

  private String joined(String between, boolean after) {
    StringBuilder text = new StringBuilder();
    for (int index = 0; index < fields.size(); index++) {
      if (index > 0) text.append(between);
      text.append(fields.get(index).tag()).append('=').append(fields.get(index).value());
    }
    if (after && !fields.isEmpty()) text.append(between);
    return text.toString();
  }

  // A header field goes after the header (BeginString, BodyLength and MsgType first, in that
  // order), a trailer field to the end, any other before the trailer.
  private static int position(List<QuickFixMessages.Field> fields, int tag, DataDictionary dictionary) {
    if (tag == BEGIN_STRING) return 0;
    if (tag == BODY_LENGTH) return after(fields, BEGIN_STRING, 0);
    if (tag == MSG_TYPE) return after(fields, BODY_LENGTH, after(fields, BEGIN_STRING, 0));
    if (tag == CHECK_SUM) return fields.size();
    int trailer = fields.size();
    while (trailer > 0 && trailerField(fields.get(trailer - 1).tag())) trailer--;
    if (tag == SIGNATURE || tag == SIGNATURE_LENGTH) {
      int checkSum = indexOf(fields, CHECK_SUM);
      return checkSum >= 0 ? checkSum : fields.size();
    }
    if (!WireMessage.headerField(dictionary, tag)) return trailer;
    int header = 0;
    while (header < trailer && WireMessage.headerField(dictionary, fields.get(header).tag())) header++;
    return header;
  }

  private static int after(List<QuickFixMessages.Field> fields, int tag, int otherwise) {
    int index = indexOf(fields, tag);
    return index >= 0 ? index + 1 : otherwise;
  }

  private static boolean trailerField(int tag) {
    return tag == CHECK_SUM || tag == SIGNATURE || tag == SIGNATURE_LENGTH;
  }

  private static int indexOf(List<QuickFixMessages.Field> fields, int tag) {
    for (int index = 0; index < fields.size(); index++) {
      if (fields.get(index).tag() == tag) return index;
    }
    return -1;
  }

  // BodyLength counts what follows it up to CheckSum; CheckSum adds up everything before it.
  private static void recount(List<QuickFixMessages.Field> fields, int tag) {
    int index = indexOf(fields, tag);
    if (index < 0) return;
    StringBuilder counted = new StringBuilder();
    int from = tag == BODY_LENGTH ? index + 1 : 0;
    int to = tag == BODY_LENGTH ? Math.max(from, firstAfter(fields, CHECK_SUM, from)) : index;
    for (int position = from; position < to; position++) {
      QuickFixMessages.Field field = fields.get(position);
      counted.append(field.tag()).append('=').append(field.value()).append(SOH);
    }
    var charset = CharsetSupport.getCharsetInstance();
    String value = tag == BODY_LENGTH
        ? Integer.toString(MessageUtils.length(charset, counted.toString()))
        : String.format("%03d", MessageUtils.checksum(charset, counted.toString(), false));
    fields.set(index, new QuickFixMessages.Field(tag, value));
  }

  private static int firstAfter(List<QuickFixMessages.Field> fields, int tag, int from) {
    for (int index = from; index < fields.size(); index++) {
      if (fields.get(index).tag() == tag) return index;
    }
    return fields.size();
  }

  private static int tag(String name) {
    if (name.isEmpty() || name.length() > 9) return -1;
    int result = 0;
    for (int index = 0; index < name.length(); index++) {
      char digit = name.charAt(index);
      if (digit < '0' || digit > '9') return -1;
      result = result * 10 + (digit - '0');
    }
    return result;
  }
}
