package com.fantasticmock.example.fixquickfixj;

import com.fantasticmock.protocol.api.exchange.ProtocolResponseWriter;

/**
 * A message as it was written, for the traffic log: read with | in place of SOH, as a
 * request's body is logged. The text is converted only when the host reads it, which it does
 * only for a response it logs.
 */
final class SentText implements CharSequence, ProtocolResponseWriter.Written {

  private final String wire;

  SentText(String wire) {
    this.wire = wire;
  }

  @Override
  public Long payloadBytes() {
    return (long) wire.length();
  }

  @Override
  public CharSequence raw() {
    return this;
  }

  @Override
  public int length() {
    return wire.length();
  }

  @Override
  public char charAt(int index) {
    char character = wire.charAt(index);
    return character == QuickFixMessages.SOH ? '|' : character;
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return toString().substring(start, end);
  }

  @Override
  public String toString() {
    return wire.replace(QuickFixMessages.SOH, '|');
  }
}
