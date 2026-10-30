package com.fantasticmock.example.fixquickfixj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fantasticmock.protocol.api.execution.ProtocolBackpressure;
import com.fantasticmock.protocol.api.execution.ProtocolExecutionProfile;
import com.fantasticmock.protocol.api.execution.ProtocolOrdering;
import com.fantasticmock.protocol.api.plugin.ProtocolClientHint;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolPlugin;
import com.fantasticmock.protocol.api.plugin.ProtocolPluginInstance;
import com.fantasticmock.protocol.api.plugin.ProtocolRawMessages;
import com.fantasticmock.protocol.api.plugin.ProtocolSettings;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import quickfix.ConfigError;
import quickfix.DataDictionary;

/**
 * FIX 4.2 and 4.4 acceptor on QuickFIX/J: the engine owns the session layer, FantasticMock
 * owns matching, variables, rendering, delays, callbacks and logs. Fields and settings are
 * declared in {@code protocol.yml}; each instance speaks the version of its
 * {@code begin-string}. Only the public {@code protocol.api} package of the host is used.
 *
 * <p>A request's body is the whole message, and its other fields are read from it: the host
 * asks this plugin to read an Example request from its body, to write a changed tag into it,
 * and to show a response as the message it makes ({@link ProtocolRawMessages}).</p>
 */
public final class QuickFixProtocolPlugin implements ProtocolPlugin, ProtocolRawMessages {

  private static final String DEFAULT_BEGIN_STRING = "FIX.4.2";
  // The request fields that name a header tag; the others are tag, a map of every tag, and body.
  private static final Map<String, Integer> HEADER_TAGS = Map.of("msgType", 35, "senderCompId", 49, "targetCompId", 56);

  // The settings of this generation's endpoints, for messages read without a session.
  private final Map<String, ProtocolSettings> endpoints = new ConcurrentHashMap<>();
  private final Map<String, DataDictionary> dictionaries = new ConcurrentHashMap<>();

  @Override
  public Optional<ProtocolClientHint> clientHint() {
    return Optional.of(ProtocolClientHint.listener("tcp", "tcp",
        "Connect a FIX initiator of this endpoint's begin-string version and log on, "
            + "then send application messages matching your templates."));
  }

  @Override
  public ProtocolPluginInstance createInstance(ProtocolEndpoint endpoint) {
    endpoints.put(endpoint.id(), endpoint.settings());
    return new QuickFixAcceptor(endpoint);
  }

  /** A session's messages are matched in the order they arrive. */
  @Override
  public ProtocolExecutionProfile executionProfile() {
    return new ProtocolExecutionProfile(ProtocolOrdering.CONNECTION, null, 1024, ProtocolBackpressure.REJECT);
  }

  /** The fields a received message with this text would have; body as a received one is logged. */
  @Override
  public Map<String, Object> readRequest(String endpoint, String raw) {
    FixText text = FixText.parse(raw);
    Map<String, Object> fields = new LinkedHashMap<>();
    HEADER_TAGS.forEach((name, tag) -> fields.put(name, text.first(tag)));
    fields.put("tag", text.tags());
    fields.put("body", text.logged());
    return fields;
  }

  /** The text with changed tags: {@code tag.<number>}, or msgType, senderCompId and targetCompId. */
  @Override
  public String writeRequest(String endpoint, String raw, Map<String, String> changes) {
    Map<Integer, String> tags = new LinkedHashMap<>();
    changes.forEach((name, value) -> {
      Integer tag = HEADER_TAGS.get(name);
      if (tag == null && name.startsWith("tag.")) tag = number(name.substring(4).strip());
      if (tag == null || tag <= 0) return;
      if (value != null && (value.indexOf('|') >= 0 || value.indexOf(QuickFixMessages.SOH) >= 0
          || value.indexOf('\n') >= 0)) {
        throw new IllegalArgumentException("FIX tag " + tag + " cannot hold | or a line break: they separate fields");
      }
      tags.put(tag, value);
    });
    return FixText.parse(raw).with(tags, dictionary(endpoint)).text();
  }

  /**
   * The message a response or callback makes, with | for SOH: BeginString, BodyLength, MsgType,
   * the fields and CheckSum. The session adds the rest of the header when it sends: SenderCompID,
   * TargetCompID, MsgSeqNum and SendingTime.
   */
  @Override
  public String previewResponse(String endpoint, JsonNode response) {
    ProtocolSettings settings = endpoints.get(endpoint);
    String beginString = settings == null ? DEFAULT_BEGIN_STRING : settings.text("begin-string", DEFAULT_BEGIN_STRING);
    return QuickFixMessages.message(response, beginString, dictionary(endpoint)).toString()
        .replace(QuickFixMessages.SOH, '|');
  }

  // The dictionary the endpoint's sessions use, or the standard one of its version; read once.
  private DataDictionary dictionary(String endpoint) {
    ProtocolSettings settings = endpoints.get(endpoint);
    String custom = settings == null ? "" : settings.text("data-dictionary", "");
    String beginString = settings == null ? DEFAULT_BEGIN_STRING : settings.text("begin-string", DEFAULT_BEGIN_STRING);
    String location = custom.isEmpty() ? beginString.replace(".", "") + ".xml" : custom;
    return dictionaries.computeIfAbsent(location, file -> {
      try {
        return new DataDictionary(file);
      } catch (ConfigError unreadable) {
        throw new IllegalArgumentException("The FIX data dictionary " + file + " cannot be read: "
            + unreadable.getMessage(), unreadable);
      }
    });
  }

  private static Integer number(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException notATag) {
      return null;
    }
  }
}
