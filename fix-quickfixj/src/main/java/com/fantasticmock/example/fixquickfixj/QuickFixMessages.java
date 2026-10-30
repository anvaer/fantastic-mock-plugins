package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import quickfix.DataDictionary;
import quickfix.InvalidMessage;
import quickfix.Message;
import quickfix.ValidationSettings;

/** Conversions between FantasticMock documents and QuickFIX/J messages. */
final class QuickFixMessages {

  static final char SOH = '\u0001';

  private static final int BEGIN_STRING = 8;
  private static final int BODY_LENGTH = 9;
  private static final int CHECK_SUM = 10;
  private static final int MSG_SEQ_NUM = 34;
  private static final int MSG_TYPE = 35;
  private static final int SIGNATURE = 89;
  private static final int SIGNATURE_LENGTH = 93;

  record Field(int tag, String value) {
  }

  private QuickFixMessages() {
  }

  /**
   * The message of a rendered response or callback document: {@code msgType}, the
   * {@code tags} map and the raw {@code body} pairs. Parsing the fields with the session's
   * dictionary builds the repeating groups it defines. QuickFIX/J adds the session header,
   * BodyLength and CheckSum when it sends.
   */
  static Message message(JsonNode document, String beginString, DataDictionary dictionary) {
    List<Field> fields = new ArrayList<>();
    JsonNode tags = document.path("tags");
    if (tags.isObject()) {
      tags.properties().forEach(entry -> add(fields, entry.getKey(), entry.getValue()));
    } else if (!tags.isMissingNode() && !tags.isNull()) {
      throw new IllegalArgumentException("FIX tags must be an object of tag number to value");
    }
    addPairs(fields, document.path("body").asText(""));

    String msgType = document.path("msgType").asText("").trim();
    StringBuilder header = new StringBuilder();
    StringBuilder body = new StringBuilder();
    StringBuilder trailer = new StringBuilder();
    for (Field field : fields) {
      switch (field.tag()) {
        case BEGIN_STRING, BODY_LENGTH, CHECK_SUM, MSG_SEQ_NUM -> { } // Always the session's own.
        case MSG_TYPE -> {
          if (msgType.isEmpty()) msgType = field.value();
        }
        default -> (dictionary.isHeaderField(field.tag()) ? header
            : dictionary.isTrailerField(field.tag()) ? trailer : body)
            .append(field.tag()).append('=').append(field.value()).append(SOH);
      }
    }
    if (msgType.isEmpty()) throw new IllegalArgumentException("A FIX message needs msgType (35)");

    String content = header.toString() + body + trailer;
    String raw = "8=" + beginString + SOH + "9=0" + SOH + "35=" + msgType + SOH + content + "10=000" + SOH;
    Message message;
    try {
      // Parsed without validation: a mock sends whatever a test needs.
      message = new Message(raw, dictionary, new ValidationSettings(), false);
    } catch (InvalidMessage invalid) {
      throw new IllegalArgumentException("Not a FIX message: " + invalid.getMessage(), invalid);
    }
    // QuickFIX/J keeps one value of a tag that repeats outside the groups its dictionary
    // defines. Better to fail than to send a group that lost its entries.
    if (count(message.toString()) - 4 < count(content)) {
      throw new IllegalArgumentException("A tag repeats in the " + msgType + " message, but the data "
          + "dictionary defines no repeating group for it there: " + repeated(content));
    }
    return message;
  }

  /**
   * A received message as a response document, for the traffic log: its type, and every field
   * in wire order with | in place of SOH. A template with this response sends the same message.
   */
  static JsonNode document(ObjectMapper mapper, String wire, String msgType) {
    return mapper.createObjectNode().put("msgType", msgType).put("body", wire.replace(SOH, '|'));
  }

  /** The first value of a tag among fields in wire order, or null. */
  static String first(List<Field> fields, int tag) {
    for (Field field : fields) {
      if (field.tag() == tag) return field.value();
    }
    return null;
  }

  private static int count(String fields) {
    return (int) fields.chars().filter(character -> character == SOH).count();
  }

  // The tags written more than once, for the error message.
  private static String repeated(String content) {
    List<String> seen = new ArrayList<>();
    List<String> repeated = new ArrayList<>();
    for (String field : content.split(String.valueOf(SOH))) {
      String tag = field.substring(0, field.indexOf('='));
      if (seen.contains(tag) && !repeated.contains(tag)) repeated.add(tag);
      seen.add(tag);
    }
    return String.join(", ", repeated);
  }

  /**
   * The fields of a received message in wire order. QuickFIX/J's own field map keeps one value
   * per tag outside the groups its dictionary knows, so the raw message is read instead.
   */
  static List<Field> fields(String raw, DataDictionary dictionary) {
    List<Field> fields = new ArrayList<>(24);
    int lengthTag = 0;
    int length = -1;
    int position = 0;
    while (position < raw.length()) {
      int separator = raw.indexOf('=', position);
      int tag = separator < 0 ? -1 : nonNegativeInt(raw.substring(position, separator));
      if (tag <= 0) break;
      int start = separator + 1;
      int end = -1;
      // A data field holds as many bytes as the length field before it says, SOH included.
      if (length >= 0 && dictionary != null && dictionary.isDataField(tag)
          && lengthTag == (tag == SIGNATURE ? SIGNATURE_LENGTH : tag - 1)
          && start + length < raw.length() && raw.charAt(start + length) == SOH) {
        end = start + length;
      }
      if (end < 0) end = raw.indexOf(SOH, start);
      if (end < 0) end = raw.length();
      String value = raw.substring(start, end);
      fields.add(new Field(tag, value));
      lengthTag = tag;
      length = nonNegativeInt(value);
      position = end + 1;
    }
    return fields;
  }

  // Line breaks separate pairs as well, so a long message can be written one field per line.
  private static void addPairs(List<Field> fields, String body) {
    String separators = body.indexOf(SOH) >= 0 ? String.valueOf(SOH) : "[|\\r\\n]";
    for (String pair : body.split(separators)) {
      if (pair.isBlank()) continue;
      int separator = pair.indexOf('=');
      if (separator < 0) throw new IllegalArgumentException("FIX body field is not tag=value: " + pair.strip());
      add(fields, pair.substring(0, separator), pair.substring(separator + 1).stripTrailing());
    }
  }

  private static void add(List<Field> fields, String name, JsonNode value) {
    if (value.isContainerNode()) {
      throw new IllegalArgumentException("FIX tag " + name + " needs a scalar value; use body for repeating groups");
    }
    add(fields, name, value.isNull() ? "" : value.asText());
  }

  private static void add(List<Field> fields, String name, String value) {
    int tag = nonNegativeInt(name.strip());
    if (tag <= 0) throw new IllegalArgumentException("FIX tag must be a positive number: " + name.strip());
    if (value.indexOf(SOH) >= 0) {
      throw new IllegalArgumentException("FIX tag " + tag + " contains the field separator SOH");
    }
    // An empty value is not valid FIX: an optional field whose variable resolved to nothing is left out.
    if (!value.isEmpty()) fields.add(new Field(tag, value));
  }

  private static int nonNegativeInt(String value) {
    if (value.isEmpty() || value.length() > 9) return -1;
    int result = 0;
    for (int index = 0; index < value.length(); index++) {
      char digit = value.charAt(index);
      if (digit < '0' || digit > '9') return -1;
      result = result * 10 + (digit - '0');
    }
    return result;
  }
}
