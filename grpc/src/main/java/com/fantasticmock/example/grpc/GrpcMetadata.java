package com.fantasticmock.example.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** One conversion between template metadata, gRPC metadata and headers on the wire. */
final class GrpcMetadata {
  /**
   * What the gRPC library writes itself on everything it sends, whatever it is given: how the
   * message is packed and what can be read. A reply's own values are reported, not sent, and
   * a template that names them, copied from a recorded reply perhaps, does not send them twice.
   */
  private static final Set<String> TRANSPORT_OWN = Set.of("content-type", "grpc-encoding", "grpc-accept-encoding");

  private GrpcMetadata() {
  }

  /**
   * Template {@code headers} to gRPC metadata. Keys are lowercased, an array adds one value per
   * item, and values of {@code -bin} keys are base64. What the library writes itself is left out.
   */
  static Metadata from(JsonNode headers) {
    Metadata metadata = new Metadata();
    if (headers == null || headers.isMissingNode() || headers.isNull()) return metadata;
    if (!headers.isObject()) throw new IllegalArgumentException("gRPC metadata must be an object");
    headers.properties().forEach(field -> {
      String key = field.getKey().toLowerCase(Locale.ROOT);
      if (TRANSPORT_OWN.contains(key)) return;
      JsonNode value = field.getValue();
      if (value.isArray()) value.forEach(item -> put(metadata, key, item));
      else put(metadata, key, value);
    });
    return metadata;
  }

  /** Inbound metadata as {@code request.header} values; binary values are base64. */
  static Map<String, String> fields(Metadata headers) {
    Map<String, String> fields = new LinkedHashMap<>();
    for (String key : headers.keys()) {
      if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        byte[] value = headers.get(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
        if (value != null) fields.put(key, Base64.getEncoder().encodeToString(value));
      } else {
        fields.put(key, headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)));
      }
    }
    return fields;
  }

  /** Received metadata as the headers it came as: every value of every key, binary ones as base64. */
  static List<GrpcWire.Header> wire(Metadata received) {
    List<GrpcWire.Header> headers = new ArrayList<>();
    for (String key : received.keys()) {
      if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        Iterable<byte[]> values = received.getAll(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
        if (values != null) {
          for (byte[] value : values) headers.add(new GrpcWire.Header(key, Base64.getEncoder().withoutPadding().encodeToString(value)));
        }
      } else {
        Iterable<String> values = received.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
        if (values != null) {
          for (String value : values) headers.add(new GrpcWire.Header(key, value));
        }
      }
    }
    return headers;
  }

  /** {@code request.header} values, as {@link #fields} gave them or as a traffic log kept them, as headers. */
  static List<GrpcWire.Header> wire(Object header) {
    List<GrpcWire.Header> headers = new ArrayList<>();
    if (header instanceof Map<?, ?> values) {
      values.forEach((name, value) -> {
        if (value != null) headers.add(new GrpcWire.Header(String.valueOf(name).toLowerCase(Locale.ROOT), String.valueOf(value)));
      });
    }
    return headers;
  }

  /** Headers of a reply as the metadata this server sends on: all of them but the ones it writes itself. */
  static Metadata metadata(List<GrpcWire.Header> headers) {
    Metadata metadata = new Metadata();
    for (GrpcWire.Header header : headers) {
      if (TRANSPORT_OWN.contains(header.name())) continue;
      if (header.name().endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        // Several binary values may share one header, separated by commas.
        for (String value : header.value().split(",")) put(metadata, header.name(), value.trim());
      } else {
        put(metadata, header.name(), header.value());
      }
    }
    return metadata;
  }

  /**
   * Headers of a reply as a template writes metadata: a key that came more than once is an
   * array. Every header that came is there. Null when there is none.
   */
  static ObjectNode document(List<GrpcWire.Header> headers, ObjectNode target) {
    for (GrpcWire.Header header : headers) {
      JsonNode present = target.get(header.name());
      if (present == null) target.put(header.name(), header.value());
      else if (present instanceof ArrayNode values) values.add(header.value());
      else target.putArray(header.name()).add(present).add(header.value());
    }
    return target.isEmpty() ? null : target;
  }

  /** The time a caller allows, as its {@code grpc-timeout} says it; null when it set no deadline. */
  static Long timeoutNanos(String timeout) {
    String text = timeout == null ? "" : timeout.trim();
    if (text.length() < 2) return null;
    TimeUnit unit = switch (text.charAt(text.length() - 1)) {
      case 'H' -> TimeUnit.HOURS;
      case 'M' -> TimeUnit.MINUTES;
      case 'S' -> TimeUnit.SECONDS;
      case 'm' -> TimeUnit.MILLISECONDS;
      case 'u' -> TimeUnit.MICROSECONDS;
      case 'n' -> TimeUnit.NANOSECONDS;
      default -> null;
    };
    try {
      return unit == null ? null : unit.toNanos(Long.parseLong(text, 0, text.length() - 1, 10));
    } catch (NumberFormatException invalid) {
      return null;
    }
  }

  private static void put(Metadata metadata, String key, JsonNode value) {
    if (!value.isValueNode() || value.isNull()) {
      throw new IllegalArgumentException("Invalid gRPC metadata value for " + key);
    }
    put(metadata, key, value.asText());
  }

  private static void put(Metadata metadata, String key, String value) {
    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      metadata.put(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER), Base64.getDecoder().decode(value));
    } else {
      metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
    }
  }
}
