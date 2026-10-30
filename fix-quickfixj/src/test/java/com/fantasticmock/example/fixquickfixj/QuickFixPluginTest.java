package com.fantasticmock.example.fixquickfixj;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fantasticmock.protocol.api.exchange.ProtocolExchangeGateway;
import com.fantasticmock.protocol.api.exchange.ProtocolExchangeResult;
import com.fantasticmock.protocol.api.exchange.ProtocolMessageView;
import com.fantasticmock.protocol.api.exchange.ProtocolResponse;
import com.fantasticmock.protocol.api.metadata.ProtocolCapability;
import com.fantasticmock.protocol.api.plugin.ProtocolEndpoint;
import com.fantasticmock.protocol.api.plugin.ProtocolOutboundSender;
import com.fantasticmock.protocol.api.plugin.ProtocolStatistics;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamClient;
import com.fantasticmock.protocol.api.plugin.ProtocolUpstreamHost;
import com.fantasticmock.protocol.api.plugin.ProtocolValidation;
import com.fantasticmock.protocol.api.testing.ProtocolTestKit;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Drives the QuickFIX/J acceptor over real sockets with a minimal hand-written initiator. */
class QuickFixPluginTest {
  private static final String FIX42 = "FIX.4.2";
  private static final String FIX44 = "FIX.4.4";

  private final ObjectMapper mapper = new ObjectMapper();
  private final QuickFixProtocolPlugin plugin = new QuickFixProtocolPlugin();
  private final List<ProtocolMessageView> requests = new CopyOnWriteArrayList<>();
  private final List<Long> acknowledged = new CopyOnWriteArrayList<>();
  private final List<Client> clients = new ArrayList<>();
  private ProtocolTestKit kit;
  private QuickFixAcceptor acceptor;
  @TempDir Path temp;

  @AfterEach void close() throws Exception {
    for (Client client : clients) client.socket.close();
    if (kit != null) kit.close();
  }

  @Test void declaresAnOrderedProtocolWithCallbacks() {
    assertEquals(QuickFixProtocolPlugin.class.getName(), plugin.definition().provider());
    assertEquals("fix-quickfixj", plugin.descriptor().id());
    assertTrue(ProtocolValidation.plugin(plugin).capabilities().containsAll(List.of(
        ProtocolCapability.NATIVE_RUNTIME, ProtocolCapability.REQUEST_RESPONSE, ProtocolCapability.CALLBACKS,
        ProtocolCapability.ORDERED_DELIVERY, ProtocolCapability.CONNECTION_ORIENTED)));
  }

  @Test void readsEveryRequestFieldFromTheRawMessage() {
    Map<String, Object> fields = plugin.readRequest("fix-test",
        "8=FIX.4.2|9=70|35=D|49=CLIENT1|56=FMOCK|11=ORD-1|453=2|448=A|448=B|10=123|");
    assertEquals("D", fields.get("msgType"));
    assertEquals("CLIENT1", fields.get("senderCompId"));
    assertEquals("FMOCK", fields.get("targetCompId"));
    // Tags gives the first value of a tag that repeats; the raw message keeps both.
    assertEquals(Map.of("8", "FIX.4.2", "9", "70", "35", "D", "49", "CLIENT1", "56", "FMOCK", "11", "ORD-1",
        "453", "2", "448", "A", "10", "123"), fields.get("tag"));
    assertEquals("8=FIX.4.2|9=70|35=D|49=CLIENT1|56=FMOCK|11=ORD-1|453=2|448=A|448=B|10=123|", fields.get("body"));

    // One field per line reads the same, and is logged as a received message would be.
    Map<String, Object> lines = plugin.readRequest("fix-test", "35=D\n  11=ORD-1  \n\n55=IBM\n");
    assertEquals("35=D|11=ORD-1|55=IBM|", lines.get("body"));
    assertNull(lines.get("senderCompId"));
    assertEquals(Map.of("35", "D", "11", "ORD-1", "55", "IBM"), lines.get("tag"));

    var refused = assertThrows(IllegalArgumentException.class, () -> plugin.readRequest("fix-test", "35=D|oops|"));
    assertTrue(refused.getMessage().contains("oops"), refused.getMessage());
  }

  @Test void writesChangedTagsIntoTheRawMessageWhereTheyBelong() {
    String raw = "8=FIX.4.2|9=10|35=D|49=CLIENT1|56=FMOCK|11=ORD-1|448=A|448=B|10=000|";
    Map<String, String> changes = new LinkedHashMap<>();
    changes.put("tag.11", "ORD-2");
    changes.put("tag.38", "100");
    changes.put("tag.448", null);
    changes.put("msgType", "F");
    changes.put("tag.115", "DESK");
    String written = plugin.writeRequest("fix-test", raw, changes);
    // A header field joins the header, a body field goes before CheckSum, a removal takes the
    // first occurrence; BodyLength and CheckSum are counted again.
    String body = "35=F|49=CLIENT1|56=FMOCK|115=DESK|11=ORD-2|448=B|38=100|";
    String expected = "8=FIX.4.2|9=" + body.length() + "|" + body;
    assertEquals(expected + "10=" + checksum(expected) + "|", written);

    // A message written one field per line stays so, without BodyLength or CheckSum to count.
    assertEquals("35=D\n11=ORD-9", plugin.writeRequest("fix-test", "35=D\n11=ORD-1\n", Map.of("tag.11", "ORD-9")));
    assertEquals("35=D|", plugin.writeRequest("fix-test", "", Map.of("msgType", "D")));
    // A tag the changes name keeps the value they give it.
    assertEquals("8=FIX.4.2|9=99|35=D|", plugin.writeRequest("fix-test", "8=FIX.4.2|9=5|35=D|", Map.of("tag.9", "99")));
    assertThrows(IllegalArgumentException.class,
        () -> plugin.writeRequest("fix-test", "35=D|", Map.of("tag.58", "a|b")));
  }

  @Test void previewsAResponseAsTheMessageItMakes() throws Exception {
    JsonNode response = mapper.readTree("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"11\":\"{{request.tag.11}}\","
        + "\"58\":\"\"},\"body\":\"382=2|375=BRK1|437=60\\n375=BRK2|437=40\",\"delayMs\":50}");
    String preview = plugin.previewResponse("unknown-endpoint", response);
    assertTrue(preview.startsWith("8=FIX.4.2|9="), preview);
    assertTrue(preview.contains("|35=8|"), preview);
    assertTrue(preview.contains("11={{request.tag.11}}|"), preview);
    assertTrue(preview.contains("|382=2|375=BRK1|437=60|375=BRK2|437=40|"), preview);
    assertFalse(preview.contains("58="), preview);
    assertTrue(preview.matches(".*\\|10=\\d{3}\\|$"), preview);

    // An endpoint's own version.
    start(request -> ProtocolExchangeResult.noMatch(), Map.of("begin-string", FIX44));
    assertTrue(plugin.previewResponse("fix-test", response).startsWith("8=FIX.4.4|"));

    var refused = assertThrows(IllegalArgumentException.class,
        () -> plugin.previewResponse("fix-test", mapper.readTree("{\"tags\":{\"11\":\"A\"}}")));
    assertTrue(refused.getMessage().contains("msgType"), refused.getMessage());
  }

  @Test void handsOverTheTextItSentForTheLog() throws Exception {
    List<String> sent = new CopyOnWriteArrayList<>();
    start(request -> result(ProtocolResponse.acknowledged(
        mapper.valueToTree(Map.of("msgType", "8", "tags", Map.of("37", "MOCK-1", "39", "0"))), 0,
        (bytes, raw) -> sent.add(bytes + " " + raw))), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-42");
    Received accepted = client.receive();
    awaitEquals(1, sent::size);
    // The message as the client received it, with the session's header, | for SOH.
    assertEquals(accepted.text.length() + " " + accepted.text, sent.getFirst());
  }

  private static String checksum(String text) {
    int sum = 0;
    for (char character : text.replace('|', '\u0001').toCharArray()) sum += character;
    return String.format("%03d", sum % 256);
  }

  @Test void answersAnOrderWithExecutionReportsInSequence() throws Exception {
    start(request -> result(
        response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"11\":\"ORD-42\",\"39\":\"0\"}}", 0),
        response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"39\":\"2\",\"44\":\"\"}}", 150)),
        Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    Received logon = client.logon(30);
    assertEquals("A", logon.msgType());
    assertEquals(FIX42, logon.get(8));
    assertEquals("FMOCK", logon.get(49));
    assertEquals("CLIENT1", logon.get(56));
    assertEquals("1", logon.get(34));
    assertEquals("0", logon.get(98));
    assertEquals("30", logon.get(108));

    long sent = System.nanoTime();
    String order = client.send("D", "11", "ORD-42", "55", "IBM", "54", "1", "38", "100");
    Received accepted = client.receive();
    Received filled = client.receive();
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sent) >= 140);
    assertEquals("8", accepted.msgType());
    assertEquals("2", accepted.get(34));
    assertEquals("ORD-42", accepted.get(11));
    assertEquals("0", accepted.get(39));
    assertEquals("3", filled.get(34));
    assertEquals("2", filled.get(39));
    assertNull(filled.get(44));

    ProtocolMessageView request = requests.getFirst();
    assertEquals("fix-quickfixj", request.protocol());
    assertEquals("fix-test", request.endpoint());
    assertEquals("D", request.messageType());
    assertEquals("D", request.text("request.msgType").orElseThrow());
    assertEquals("CLIENT1", request.text("request.senderCompId").orElseThrow());
    assertEquals("FMOCK", request.text("request.targetCompId").orElseThrow());
    assertEquals("ORD-42", request.text("request.tag.11").orElseThrow());
    // The body is the message exactly as it arrived.
    assertEquals(order, request.text("request.body").orElseThrow());
    assertEquals((long) order.length(), request.payloadSize());
    awaitEquals(2, acknowledged::size);
    assertEquals(List.of((long) accepted.text.length(), (long) filled.text.length()), acknowledged);
    assertEquals(1, kit.statistics("fix-test").value(ProtocolStatistics.MESSAGES));
    assertEquals(1, kit.statistics("fix-test").value("logons"));
    assertEquals(List.of("CLIENT1 -> FMOCK"), acceptor.healthDetails().get("sessions"));

    client.send("5");
    assertEquals("5", client.receive().msgType());
    assertNull(client.receiveOrNull());
    awaitEquals(0, () -> ((List<?>) acceptor.healthDetails().get("sessions")).size());
  }

  @Test void buildsRepeatingGroupsAndTheHeaderFromTheDictionary() throws Exception {
    start(request -> result(response(
        "{\"tags\":{\"37\":\"MOCK-1\",\"115\":\"DESK\",\"49\":\"OTHER\",\"34\":\"999\",\"35\":\"8\"},"
            + "\"body\":\"382=2|375=BRK1|437=60\\n375=BRK2|437=40\"}", 0)), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    Received report = client.receive();
    assertEquals("8", report.msgType());
    assertTrue(report.text.contains("|382=2|375=BRK1|437=60|375=BRK2|437=40|"), report.text);
    // OnBehalfOfCompID is a header field, so it is written before the body.
    assertTrue(report.tags().indexOf(115) < report.tags().indexOf(37), report.text);
    // The session header is QuickFIX/J's own.
    assertEquals("FMOCK", report.get(49));
    assertEquals("2", report.get(34));
  }

  @Test void anUnmatchedOrFailedMessageIsAnsweredWithABusinessReject() throws Exception {
    start(request -> "D".equals(request.messageType())
        ? ProtocolExchangeResult.noMatch()
        : result(response("{\"msgType\":\"8\",\"tags\":{\"OrderID\":\"MOCK-1\"}}", 0)), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);

    client.send("D", "11", "ORD-1");
    Received unmatched = client.receive();
    assertEquals("j", unmatched.msgType());
    assertEquals("2", unmatched.get(45));
    assertEquals("D", unmatched.get(372));
    assertEquals("3", unmatched.get(380));

    client.send("F", "41", "ORD-1");
    Received failed = client.receive();
    assertEquals("j", failed.msgType());
    assertEquals("3", failed.get(45));
    assertEquals("F", failed.get(372));
    assertEquals("0", failed.get(380));
    assertTrue(failed.get(58).contains("FIX tag must be a positive number: OrderID"), failed.get(58));

    assertEquals(1, kit.statistics("fix-test").value(ProtocolStatistics.UNMATCHED));
    assertEquals(1, kit.statistics("fix-test").value(ProtocolStatistics.FAILURES));
    awaitEquals(2, () -> (int) kit.statistics("fix-test").value("businessRejects"));
  }

  @Test void unmatchedMessagesCanBeIgnored() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of("reject-unmatched", false));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    client.send("1", "112", "PING");
    Received heartbeat = client.receive();
    assertEquals("0", heartbeat.msgType());
    assertEquals("PING", heartbeat.get(112));
    assertEquals("2", heartbeat.get(34));
  }

  @Test void aRequestBeyondMaxInFlightIsRejectedWhileADelayedResponseIsPending() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"LATE\"}}", 60_000)),
        Map.of("max-in-flight", 1));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    client.send("D", "11", "ORD-2");
    Received busy = client.receive();
    assertEquals("j", busy.msgType());
    assertEquals("3", busy.get(45));
    assertEquals("4", busy.get(380));
    assertEquals(1, requests.size());
  }

  @Test void resendsStoredMessagesWhenTheClientAsks() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"39\":\"0\"}}", 0)), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    Received original = client.receive();
    assertEquals("2", original.get(34));
    assertNull(original.get(43));

    // From the Logon onwards: the administrative message is skipped with a gap fill, the
    // stored report is sent again as a possible duplicate.
    client.send("2", "7", "1", "16", "0");
    Received gapFill = client.receive();
    assertEquals("4", gapFill.msgType());
    assertEquals("1", gapFill.get(34));
    assertEquals("Y", gapFill.get(123));
    assertEquals("2", gapFill.get(36));
    Received resent = client.receive();
    assertEquals("8", resent.msgType());
    assertEquals("2", resent.get(34));
    assertEquals("Y", resent.get(43));
    assertEquals("MOCK-1", resent.get(37));
    assertNotNull(resent.get(122));
    // The template was not asked again.
    assertEquals(1, requests.size());
  }

  @Test void withoutPersistedMessagesAResendIsAnsweredWithAGapFill() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"39\":\"0\"}}", 0)),
        Map.of("persist-messages", false));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    assertEquals("2", client.receive().get(34));

    client.send("2", "7", "1", "16", "0");
    Received gapFill = client.receive();
    assertEquals("4", gapFill.msgType());
    assertEquals("Y", gapFill.get(123));
    // Nothing was kept, so the report is skipped too and the next number is the one after it.
    assertEquals("3", gapFill.get(36));
    client.send("D", "11", "ORD-2");
    Received next = client.receive();
    assertEquals("8", next.msgType());
    assertEquals("3", next.get(34));
    assertNull(next.get(43));
  }

  @Test void checksSequenceNumbersAsACounterpartyWould() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\"}}", 0)), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);

    // 2..4 are missing: QuickFIX/J asks for them and holds the order back.
    client.sendAs(5, "D", "11", "ORD-5");
    Received resend = client.receive();
    assertEquals("2", resend.msgType());
    assertEquals("2", resend.get(7));
    assertTrue(requests.isEmpty());

    // A gap fill for 2..4 releases the queued order.
    client.sendAs(2, "4", "43", "Y", "122", client.now(), "123", "Y", "36", "5");
    assertEquals("8", client.receive().msgType());
    assertEquals("ORD-5", requests.getFirst().text("request.tag.11").orElseThrow());

    client.sendAs(3, "D", "11", "ORD-3");
    Received logout = client.receive();
    assertEquals("5", logout.msgType());
    assertTrue(logout.get(58).contains("MsgSeqNum too low, expecting 6 but received 3"), logout.get(58));
    assertNull(client.receiveOrNull());
    assertTrue(String.valueOf(acceptor.healthDetails().get("lastFailure")).contains("MsgSeqNum too low"));

    // A first Logon that is not numbered 1 is accepted, and what came before is asked for.
    Client later = connect("CLIENT2", "FMOCK");
    assertEquals("A", later.logonAs(41, 30).msgType());
    Received earlier = later.receive();
    assertEquals("2", earlier.msgType());
    assertEquals("1", earlier.get(7));
  }

  @Test void aClockDifferenceDoesNotEndTheSession() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.sendAs(1, "A", "52", "20200101-00:00:00.000", "98", "0", "108", "30");
    assertEquals("A", client.receive().msgType());
    client.sendAs(2, "1", "52", "20200101-00:00:05.000", "112", "PAST");
    assertEquals("PAST", client.receive().get(112));
  }

  @Test void sequenceNumbersContinueAcrossConnectionsUntilTheClientResetsThem() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of());
    Client first = connect("CLIENT1", "FMOCK");
    assertEquals("1", first.logon(30).get(34));
    first.send("5");
    assertEquals("2", first.receive().get(34));
    assertNull(first.receiveOrNull());
    awaitEquals(0, () -> ((List<?>) acceptor.healthDetails().get("sessions")).size());

    Client second = connect("CLIENT1", "FMOCK");
    assertEquals("3", second.logonAs(3, 30).get(34));
    second.socket.close();
    awaitEquals(0, () -> ((List<?>) acceptor.healthDetails().get("sessions")).size());

    Client third = connect("CLIENT1", "FMOCK");
    third.send("A", "98", "0", "108", "30", "141", "Y");
    Received fresh = third.receive();
    assertEquals("1", fresh.get(34));
    assertEquals("Y", fresh.get(141));

    // Another client has its own numbers.
    assertEquals("1", connect("CLIENT2", "FMOCK").logon(30).get(34));
  }

  @Test void refusesLogonsItCannotServeAndSaysWhy() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of("begin-string", FIX44, "sender-comp-id", "FMOCK"));
    assertEquals(FIX44, acceptor.healthDetails().get("beginString"));

    Client wrongTarget = connect("CLIENT1", "OTHER");
    Received refused = wrongTarget.logon(30);
    assertEquals("5", refused.msgType());
    assertEquals("TargetCompID OTHER is not served here, expected FMOCK", refused.get(58));
    assertNull(wrongTarget.receiveOrNull());

    // A client of the other version is told so in its own version.
    Client wrongVersion = connect(acceptor, FIX42, "CLIENT1", "FMOCK");
    Received other = wrongVersion.logon(30);
    assertEquals("5", other.msgType());
    assertEquals(FIX42, other.get(8));
    assertEquals("This endpoint speaks FIX.4.4, not FIX.4.2", other.get(58));
    assertNull(wrongVersion.receiveOrNull());

    Client client = connect("CLIENT1", "FMOCK");
    assertEquals("A", client.logon(30).msgType());
    Client duplicate = connect("CLIENT1", "FMOCK");
    duplicate.send("A", "98", "0", "108", "30");
    assertNull(duplicate.receiveOrNull());

    // The refusals did not disturb the live session.
    client.send("1", "112", "STILL-HERE");
    Received heartbeat = client.receive();
    assertEquals("STILL-HERE", heartbeat.get(112));
    assertEquals("2", heartbeat.get(34));
    assertEquals(2, kit.statistics("fix-test").value("refusedLogons"));
    assertEquals(1, kit.statistics("fix-test").value("logons"));
  }

  @Test void validatesAgainstTheDictionaryOnlyWhenAsked() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\"}}", 0)), Map.of("validate", true));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    // HandlInst (21), Symbol (55), Side (54), TransactTime (60) and OrdType (40) are required.
    client.send("D", "11", "ORD-1");
    Received rejected = client.receive();
    assertEquals("3", rejected.msgType());
    assertEquals("2", rejected.get(45));
    assertEquals("1", rejected.get(373));
    assertTrue(requests.isEmpty());

    client.send("D", "11", "ORD-2", "21", "1", "55", "IBM", "54", "1", "60", client.now(), "38", "100", "40", "1");
    assertEquals("8", client.receive().msgType());
    assertEquals("ORD-2", requests.getFirst().text("request.tag.11").orElseThrow());
  }

  @Test void aCustomDictionaryAddsItsOwnGroups() throws Exception {
    String document = "{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\"},"
        + "\"body\":\"5100=2|5101=XNYS|5102=60|5101=XNAS|5102=40\"}";
    String standard;
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("FIX42.xml")) {
      standard = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
    String custom = standard
        .replace("<message name=\"ExecutionReport\" msgtype=\"8\" msgcat=\"app\">",
            "<message name=\"ExecutionReport\" msgtype=\"8\" msgcat=\"app\">"
                + "<group name=\"NoVenues\" required=\"N\"><field name=\"VenueID\" required=\"N\"/>"
                + "<field name=\"VenueQty\" required=\"N\"/></group>")
        .replace("<fields>", "<fields><field number=\"5100\" name=\"NoVenues\" type=\"INT\"/>"
            + "<field number=\"5101\" name=\"VenueID\" type=\"STRING\"/>"
            + "<field number=\"5102\" name=\"VenueQty\" type=\"QTY\"/>");
    assertFalse(custom.equals(standard));
    Path dictionary = Files.writeString(temp.resolve("FIX42-venues.xml"), custom);

    start(request -> result(response(document, 0)), Map.of("data-dictionary", dictionary.toString()));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    assertTrue(client.receive().text.contains("|5100=2|5101=XNYS|5102=60|5101=XNAS|5102=40|"));

    // The standard dictionary does not know the group. QuickFIX/J would send one entry of the
    // two, so the response is refused with the reason instead.
    QuickFixAcceptor plain = (QuickFixAcceptor) kit.start("standard", Map.of("host", "127.0.0.1", "port", 0));
    Client other = connect(plain, FIX42, "CLIENT1", "FMOCK");
    other.logon(30);
    other.send("D", "11", "ORD-1");
    Received refused = other.receive();
    assertEquals("j", refused.msgType());
    assertEquals("0", refused.get(380));
    assertTrue(refused.get(58).contains("defines no repeating group for it there: 5101, 5102"), refused.get(58));
  }

  @Test void instancesKeepTheirSessionsApartEvenWithTheSameCompIds() throws Exception {
    kit = ProtocolTestKit.of(plugin).exchanges(request -> {
      requests.add(request);
      return result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\"}}", 0));
    });
    QuickFixAcceptor fix42 = (QuickFixAcceptor) kit.start("fix42", Map.of("host", "127.0.0.1", "port", 0));
    QuickFixAcceptor fix44 = (QuickFixAcceptor) kit.start("fix44",
        Map.of("host", "127.0.0.1", "port", 0, "begin-string", FIX44));
    QuickFixAcceptor second44 = (QuickFixAcceptor) kit.start("fix44-b",
        Map.of("host", "127.0.0.1", "port", 0, "begin-string", FIX44));

    Client old = connect(fix42, FIX42, "CLIENT1", "FMOCK");
    Client current = connect(fix44, FIX44, "CLIENT1", "FMOCK");
    assertEquals(FIX42, old.logon(30).get(8));
    assertEquals(FIX44, current.logon(30).get(8));
    old.send("D", "11", "ORD-42");
    assertEquals(FIX42, old.receive().get(8));
    current.send("D", "11", "ORD-44");
    Received toCurrent = current.receive();
    assertEquals(FIX44, toCurrent.get(8));
    assertEquals("2", toCurrent.get(34));

    // The same version and CompIDs on another instance are a separate session.
    Client twin = connect(second44, FIX44, "CLIENT1", "FMOCK");
    assertEquals("1", twin.logon(30).get(34));
    twin.send("D", "11", "ORD-44-B");
    assertEquals("2", twin.receive().get(34));
    // Templates choose the instance, and with it the version, by settings.endpoint.
    assertEquals(List.of("fix42", "fix44", "fix44-b"), requests.stream().map(ProtocolMessageView::endpoint).toList());

    // Once the 4.2 session exists, its client is still refused on the 4.4 port.
    old.send("5");
    assertEquals("5", old.receive().msgType());
    Client astray = connect(fix44, FIX42, "CLIENT1", "FMOCK");
    assertEquals("This endpoint speaks FIX.4.4, not FIX.4.2", astray.logon(30).get(58));
  }

  @Test void callbacksReachTheNamedSessionOrEverySession() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of());
    ProtocolOutboundSender sender = acceptor;
    ObjectNode fill = (ObjectNode) mapper.readTree("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"39\":\"2\"}}");

    ExecutionException nobody = assertThrows(ExecutionException.class,
        () -> sender.send(fill).toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals("No FIX session is logged on", nobody.getCause().getMessage());

    Client first = connect("CLIENT1", "FMOCK");
    Client second = connect("CLIENT2", "FMOCK");
    first.logon(30);
    second.logon(30);

    long bytes = sender.send(fill.deepCopy().put("targetCompId", "CLIENT2")).toCompletableFuture().get(5, TimeUnit.SECONDS);
    Received targeted = second.receive();
    assertEquals("8", targeted.msgType());
    assertEquals("CLIENT2", targeted.get(56));
    assertEquals("2", targeted.get(34));
    assertEquals((long) targeted.text.length(), bytes);

    long broadcast = sender.send(fill).toCompletableFuture().get(5, TimeUnit.SECONDS);
    Received toFirst = first.receive();
    Received toSecond = second.receive();
    assertEquals("2", toFirst.get(34));
    assertEquals("3", toSecond.get(34));
    assertEquals((long) toFirst.text.length() + toSecond.text.length(), broadcast);

    ExecutionException unknown = assertThrows(ExecutionException.class,
        () -> sender.send(fill.deepCopy().put("targetCompId", "CLIENT9")).toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals("No FIX session is logged on for CompID CLIENT9", unknown.getCause().getMessage());
  }

  @Test void probesASilentClientAndDisconnectsItWhenTheProbeIsUnanswered() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(1);
    long started = System.nanoTime();
    List<String> received = new ArrayList<>();
    Received next;
    while ((next = client.receiveOrNull()) != null) received.add(next.msgType());
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    assertTrue(received.contains("1") && received.stream().allMatch(type -> type.equals("0") || type.equals("1")),
        received.toString());
    assertTrue(elapsed >= 2000 && elapsed < 9000, "disconnected after " + elapsed + " ms");
  }

  @Test void aSilentClientIsKeptWhenDisconnectIdleIsOff() throws Exception {
    start(request -> ProtocolExchangeResult.noMatch(), Map.of("disconnect-idle", false));
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(1);
    // Well past the 2.4 intervals after which a silent client is otherwise dropped.
    Thread.sleep(3500);
    client.send("1", "112", "ALIVE");
    Received next;
    do {
      // Heartbeats and TestRequests are still sent; only the disconnect is off.
      next = client.receive();
      assertTrue(next.msgType().equals("0") || next.msgType().equals("1"), next.text());
    } while (!"ALIVE".equals(next.get(112)));
    assertEquals("0", next.msgType());
  }

  @Test void stoppingLogsClientsOutCancelsDelayedResponsesAndReleasesThePort() throws Exception {
    start(request -> result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"LATE\"}}", 60_000)), Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    awaitEquals(1, requests::size);
    int port = acceptor.port();

    acceptor.close();
    Received logout = client.receive();
    assertEquals("5", logout.msgType());
    assertEquals("The FIX endpoint is stopping", logout.get(58));
    assertNull(client.receiveOrNull());
    assertTrue(acknowledged.isEmpty());
    new ServerSocket(port).close();
  }

  @Test void rejectsSettingsItCannotStartWith() {
    kit = ProtocolTestKit.of(plugin);
    assertThrows(IllegalArgumentException.class, () -> kit.create("version", Map.of("begin-string", "FIX.5.0")));
    IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> kit.start("dictionary",
        Map.of("host", "127.0.0.1", "port", 0, "data-dictionary", temp.resolve("missing.xml").toString())));
    assertTrue(missing.getMessage().contains("data-dictionary"), missing.getMessage());
  }

  // ---- Passing messages on to a real server -------------------------------------------------

  /** What the host does for an endpoint whose messages are passed on: says where to, and records what comes back. */
  private final class Host implements ProtocolUpstreamHost {
    volatile Route route;
    final List<Relayed> relayed = new CopyOnWriteArrayList<>();

    @Override public Optional<Route> route(ProtocolEndpoint endpoint) {
      return endpoint.id().equals("front") ? Optional.ofNullable(route) : Optional.empty();
    }

    @Override public ProtocolExchangeResult received(ProtocolEndpoint endpoint, ProtocolMessageView request,
        List<JsonNode> messages) {
      List<ProtocolResponse> responses = new ArrayList<>();
      for (JsonNode message : messages) {
        relayed.add(new Relayed(request, message));
        responses.add(new ProtocolResponse(message, 0, acknowledged::add));
      }
      return ProtocolExchangeResult.answered("relayed", responses);
    }
  }

  private record Relayed(ProtocolMessageView request, JsonNode message) {
  }

  private final Host host = new Host();
  private final List<ServerSocket> listeners = new ArrayList<>();
  private QuickFixAcceptor front;
  // The front endpoint's own templates, and what a second endpoint of this plugin answers as the real server.
  private volatile Predicate<ProtocolMessageView> mocked = request -> false;
  // What the rules of the upstream change in a message on its way, as the host gives it.
  private volatile Map<String, String> changes = Map.of();
  private volatile java.util.function.UnaryOperator<String> bodyRule;
  private volatile ProtocolExchangeGateway real = request -> ProtocolExchangeResult.noMatch();

  @AfterEach void closeListeners() throws IOException {
    for (ServerSocket listener : listeners) listener.close();
  }

  /**
   * Starts the endpoint clients connect to. Its exchanges do what the host does: a request its
   * templates do not answer is passed on to the route's target, and waited for no longer than
   * the route allows.
   */
  private void startFront(Map<String, Object> frontSettings) throws Exception {
    Map<String, Object> settings = new HashMap<>(Map.of("host", "127.0.0.1", "port", 0));
    settings.putAll(frontSettings);
    kit = ProtocolTestKit.of(plugin).hostService(ProtocolUpstreamHost.class, host).exchanges(request -> {
      requests.add(request);
      if (!request.endpoint().equals("front")) return real.exchange(request);
      ProtocolUpstreamHost.Route route = host.route;
      boolean answeredHere = mocked.test(request);
      if (answeredHere && (route == null || !route.everyRequest())) {
        return result(response("{\"msgType\":\"8\",\"tags\":{\"37\":\"MOCK-1\",\"39\":\"0\"}}", 0));
      }
      if (route == null) return ProtocolExchangeResult.noMatch();
      // As the host does: a rule on the body is given the text of the request and leaves another.
      Map<String, String> onTheWay = changes;
      if (bodyRule != null) {
        onTheWay = new LinkedHashMap<>(changes);
        onTheWay.put("body", bodyRule.apply(request.text("request.body").orElseThrow()));
      }
      var passed = front.forward(request, new ProtocolUpstreamClient.Upstream(route.target(), onTheWay)).toCompletableFuture();
      try {
        passed.get(route.replyTimeoutMs(), TimeUnit.MILLISECONDS);
        return ProtocolExchangeResult.answered("passed-on", List.of());
      } catch (Exception failure) {
        passed.cancel(false);
        Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
        throw new IllegalStateException("No reply from " + route.target() + ": " + cause.getMessage(), cause);
      }
    });
    front = (QuickFixAcceptor) kit.start("front", settings);
    acceptor = front;
  }

  /** A real server written by hand: it listens, and speaks on the one connection it accepts. */
  private int listen() throws IOException {
    ServerSocket listener = new ServerSocket(0);
    listener.setSoTimeout(12000);
    listeners.add(listener);
    return listener.getLocalPort();
  }

  private Client accepted(String serverCompId, String clientCompId) throws IOException {
    Client server = new Client(FIX42, serverCompId, clientCompId, listeners.getLast().accept());
    clients.add(server);
    return server;
  }

  private void route(int port, boolean everyRequest) {
    host.route = new ProtocolUpstreamHost.Route("fix://127.0.0.1:" + port, everyRequest, 3000);
  }

  // A message as it is on the wire, without what belongs to the session it travelled on.
  private static String withoutSequence(String text) {
    return text.replaceAll("\\|34=\\d+\\|", "|34=?|").replaceAll("^8=([^|]+)\\|9=\\d+\\|", "8=$1|9=?|")
        .replaceAll("\\|10=\\d{3}\\|$", "|10=?|");
  }

  @Test void declaresThatItPassesMessagesOnAndChecksTheTarget() {
    kit = ProtocolTestKit.of(plugin);
    ProtocolUpstreamClient client = (ProtocolUpstreamClient) assertDoesNotThrow(
        () -> kit.create("front", Map.of("host", "127.0.0.1", "port", 0)));
    assertTrue(client.repliesLater());
    assertEquals("fix://host:port", client.targetHint());
    client.checkTarget("fix://exchange.test:9876");
    client.checkTarget(" fix://10.0.0.5:9876/ ");
    for (String invalid : java.util.Arrays.asList("exchange.test:9876", "tcp://exchange.test:9876", "fix://exchange.test",
        "fix://exchange.test:9876/session", "not a target", "", null)) {
      assertTrue(assertThrows(IllegalArgumentException.class, () -> client.checkTarget(invalid))
          .getMessage().contains("fix://host:port"), String.valueOf(invalid));
    }
  }

  @Test void messagesCrossToTheRealServerAndBackAsTheyWereSent() throws Exception {
    int port = listen();
    route(port, true);
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    // The client's Logon carries its credentials; the real server is logged on to with them.
    client.sendAs(1, "A", "98", "0", "108", "30", "553", "alice", "554", "secret");
    assertEquals("A", client.receive().msgType());
    Client server = accepted("FMOCK", "CLIENT1");
    Received logon = server.receive();
    assertEquals("A", logon.msgType());
    assertEquals("CLIENT1", logon.get(49));
    assertEquals("FMOCK", logon.get(56));
    assertEquals("1", logon.get(34));
    assertEquals("30", logon.get(108));
    assertEquals("alice", logon.get(553));
    assertEquals("secret", logon.get(554));
    server.sendAs(1, "A", "98", "0", "108", "30");

    // Header fields of its own, body tags out of order, a tag no dictionary knows and one that
    // repeats outside any group: the real server gets the very same text.
    String order = client.send("D", "115", "DESK", "50", "TRADER7", "55", "IBM", "11", "ORD-1", "5001", "custom",
        "5101", "first", "5101", "second", "54", "1", "38", "100", "40", "1");
    Received passed = server.receive();
    assertEquals(order, passed.text());
    assertEquals(1, kit.statistics("front").value("forwarded"));
    assertEquals(0, kit.statistics("front").value(ProtocolStatistics.UNMATCHED));

    // And the client the very same text the real server sent, whenever it comes.
    String accepted = server.send("8", "128", "DESK", "39", "0", "11", "ORD-1", "37", "X-1", "6001", "venue",
        "5101", "first", "5101", "second", "150", "0", "17", "E-1");
    Received report = client.receive();
    assertEquals(accepted, report.text());
    String filled = server.send("8", "39", "2", "11", "ORD-1", "37", "X-1", "150", "2", "17", "E-2");
    assertEquals(filled, client.receive().text());
    // A message nobody asked for crosses too.
    String news = server.send("B", "148", "Trading halts at noon", "33", "1", "58", "Halt");
    assertEquals(news, client.receive().text());

    // The host is given each of them: with the order it answers, as the message itself says,
    // or as a message that answers nothing.
    awaitEquals(3, host.relayed::size);
    assertEquals("ORD-1", host.relayed.get(0).request().text("request.tag.11").orElseThrow());
    assertSame(host.relayed.get(0).request(), host.relayed.get(1).request());
    assertEquals("D", host.relayed.get(0).request().messageType());
    assertEquals(mapper.createObjectNode().put("msgType", "8").put("body", accepted), host.relayed.get(0).message());
    assertEquals(mapper.createObjectNode().put("msgType", "8").put("body", filled), host.relayed.get(1).message());
    assertEquals("B", host.relayed.get(2).request().messageType());
    assertTrue(host.relayed.get(2).request().logFields().isEmpty());
    assertEquals(mapper.createObjectNode().put("msgType", "B").put("body", news), host.relayed.get(2).message());
    awaitEquals(3, acknowledged::size);
    assertEquals(List.of((long) accepted.length(), (long) filled.length(), (long) news.length()), acknowledged);
    assertEquals(3, kit.statistics("front").value("relayed"));
    assertEquals(List.of("CLIENT1 -> FMOCK at fix://127.0.0.1:" + port), front.healthDetails().get("upstreamSessions"));

    // The client leaves: so does its session with the real server.
    client.send("5");
    assertEquals("5", client.receive().msgType());
    assertEquals("5", server.receive().msgType());
  }

  @Test void theRulesOfAnUpstreamChangeTagsAndLeaveTheRestAsItCame() throws Exception {
    int port = listen();
    route(port, true);
    startFront(Map.of());
    assertEquals(Set.of("tag", "body"), front.rewritableFields());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    Client server = accepted("FMOCK", "CLIENT1");
    assertEquals("A", server.receive().msgType());
    server.sendAs(1, "A", "98", "0", "108", "30");

    Map<String, String> rules = new LinkedHashMap<>();
    rules.put("tag.58", "***");          // a body tag gets another value where it stands
    rules.put("tag.5001", null);         // one is left out
    rules.put("tag.9002", "added");      // one the message did not have follows the others
    rules.put("tag.50", "DESK9");        // a header tag is changed in the header
    rules.put("tag.115", "FUND");        // and one it did not have is added to it
    rules.put("tag.5999", null);         // leaving out what is not there changes nothing
    rules.put("tag.49", "INTRUDER");     // what the session writes itself stays the session's
    rules.put("tag.34", "77");
    rules.put("header.x", "not a tag");
    changes = rules;
    String order = client.send("D", "50", "TRADER7", "55", "IBM", "58", "secret", "11", "ORD-1", "5001", "internal",
        "5101", "first", "5101", "second", "54", "1");
    Received passed = server.receive();
    String expected = order.replace("|50=TRADER7|", "|50=DESK9|115=FUND|").replace("|58=secret|", "|58=***|")
        .replace("|5001=internal|", "|").replaceAll("\\|10=\\d{3}\\|$", "|9002=added|10=?|");
    assertEquals(withoutSequence(expected), withoutSequence(passed.text()));
    assertEquals("CLIENT1", passed.get(49));
    assertEquals("2", passed.get(34));

    // Without rules the next message is the text the client sent again.
    changes = Map.of();
    String plain = client.send("D", "50", "TRADER7", "55", "IBM", "58", "secret", "11", "ORD-2", "5001", "internal");
    assertEquals(plain, server.receive().text());
    assertEquals(Map.of(58, "x"), QuickFixUpstream.tags(Map.of("tag.58", "x", "tag.8", "FIX.9", "tag.zero", "1", "body", "2")));

    // A rule on the body works on the whole text, and tag rules on what it leaves. What kind of
    // message it is and whose it is are not a rule's to change.
    bodyRule = text -> text.replace("|55=IBM|", "|55=MSFT|48=US5949181045|").replace("|35=D|", "|35=8|").replace("|49=CLIENT1|", "|49=OTHER|");
    changes = Map.of("tag.58", "***");
    String third = client.send("D", "50", "TRADER7", "55", "IBM", "58", "secret", "11", "ORD-3");
    Received rewritten = server.receive();
    assertEquals(withoutSequence(third.replace("|55=IBM|", "|55=MSFT|48=US5949181045|").replace("|58=secret|", "|58=***|")),
        withoutSequence(rewritten.text()));
    assertEquals("D", rewritten.msgType());
    assertEquals("CLIENT1", rewritten.get(49));
    // The server answers with the ID it was sent, and the answer is recorded with the request.
    bodyRule = text -> text.replace("|11=ORD-4|", "|11=RENAMED-4|");
    changes = Map.of();
    client.send("D", "11", "ORD-4", "55", "IBM");
    assertEquals("RENAMED-4", server.receive().get(11));
    client.send("D", "11", "LATER", "55", "IBM");
    assertEquals("LATER", server.receive().get(11));
    int recorded = host.relayed.size();
    server.send("8", "11", "RENAMED-4", "37", "X-4", "39", "0", "150", "0", "17", "E-4");
    assertEquals("X-4", client.receive().get(37));
    awaitEquals(recorded + 1, host.relayed::size);
    assertEquals("ORD-4", host.relayed.getLast().request().text("request.tag.11").orElseThrow());

    // What the rules leave has to be a FIX message; the client is told when it is not.
    bodyRule = text -> "not a message";
    client.send("D", "11", "ORD-5", "55", "IBM");
    Received refused = client.receive();
    assertEquals("j", refused.msgType());
    assertTrue(refused.get(58).contains("do not leave a FIX message"), refused.get(58));
  }

  @Test void whatTemplatesAnswerStaysHereAndARejectNamesTheClientsOwnMessage() throws Exception {
    int port = listen();
    route(port, false);
    mocked = request -> request.text("request.tag.11").orElse("").startsWith("MOCK");
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    Client server = accepted("FMOCK", "CLIENT1");
    assertEquals("A", server.receive().msgType());
    server.sendAs(1, "A", "98", "0", "108", "30");

    // A template answers this one: the real server never sees it.
    client.send("D", "11", "MOCK-1", "55", "IBM");
    Received mockedReport = client.receive();
    assertEquals("MOCK-1", mockedReport.get(37));
    // This one no template answers. It is the client's third message and the second sent on.
    String order = client.send("D", "11", "REAL-1", "55", "IBM");
    Received passed = server.receive();
    assertEquals("2", passed.get(34));
    assertEquals(withoutSequence(order), withoutSequence(passed.text()));

    // The real server refuses it by the number it has there; the client is told by its own.
    server.send("j", "45", "2", "372", "D", "380", "3", "58", "Unknown symbol");
    Received businessReject = client.receive();
    assertEquals("j", businessReject.msgType());
    assertEquals("3", businessReject.get(45));
    assertEquals("Unknown symbol", businessReject.get(58));
    server.send("3", "45", "2", "371", "55", "373", "5", "58", "Value is incorrect");
    Received reject = client.receive();
    assertEquals("3", reject.msgType());
    assertEquals("3", reject.get(45));
    assertEquals("55", reject.get(371));
    // Both are recorded with the order they refuse, though neither repeats its ID.
    awaitEquals(2, host.relayed::size);
    assertEquals("REAL-1", host.relayed.get(0).request().text("request.tag.11").orElseThrow());
    assertEquals("REAL-1", host.relayed.get(1).request().text("request.tag.11").orElseThrow());
    // And as the client received them, with the numbers of its own session.
    assertEquals(mapper.createObjectNode().put("msgType", "j").put("body", businessReject.text()),
        host.relayed.get(0).message());
    assertEquals(mapper.createObjectNode().put("msgType", "3").put("body", reject.text()),
        host.relayed.get(1).message());
    assertEquals(1, kit.statistics("front").value("forwarded"));
  }

  @Test void anAnswerWithoutAnIdBelongsToTheRequestStillWaitingForOne() throws Exception {
    int port = listen();
    route(port, true);
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    Client server = accepted("FMOCK", "CLIENT1");
    server.receive();
    server.sendAs(1, "A", "98", "0", "108", "30");

    // A message of the server's own kind, with nothing that names the request.
    client.send("U1", "9001", "ping");
    assertEquals("U1", server.receive().msgType());
    server.send("U2", "9002", "pong");
    assertEquals("U2", client.receive().msgType());
    server.send("U2", "9002", "and again");
    assertEquals("and again", client.receive().get(9002));
    awaitEquals(2, host.relayed::size);
    assertEquals("U1", host.relayed.get(0).request().messageType());
    assertEquals("ping", host.relayed.get(0).request().text("request.tag.9001").orElseThrow());
    // The request has had its answer: what follows is on its own.
    assertEquals("U2", host.relayed.get(1).request().messageType());
    assertTrue(host.relayed.get(1).request().logFields().isEmpty());
  }

  @Test void aRelayedMessageIsSentAgainAsItWas() throws Exception {
    int port = listen();
    route(port, true);
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    Client server = accepted("FMOCK", "CLIENT1");
    server.receive();
    server.sendAs(1, "A", "98", "0", "108", "30");
    String report = server.send("8", "39", "0", "11", "ORD-1", "5101", "first", "5101", "second", "37", "X-1");
    Received original = client.receive();
    assertEquals(report, original.text());

    // The client asks for it again: the same fields in the same order, marked as a duplicate.
    client.send("2", "7", "2", "16", "2");
    Received again = client.receive();
    assertEquals("8", again.msgType());
    assertEquals("Y", again.get(43));
    assertNotNull(again.get(122));
    assertEquals("2", again.get(34));
    String body = report.substring(report.indexOf("|39=0|"), report.indexOf("|10="));
    assertTrue(again.text().contains(body), again.text());
  }

  @Test void theRealServerIsJoinedWhereTheClientIsAndStartsAnewWhenTheClientDoes() throws Exception {
    int port = listen();
    startFront(Map.of());
    mocked = request -> true;
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    client.send("D", "11", "ORD-1");
    client.receive();
    client.send("D", "11", "ORD-2");
    client.receive();
    client.send("5");
    client.receive();

    // Now the endpoint's messages go to a real server. The client comes back with its fourth
    // message: to a server that has known this client, that is what comes next.
    route(port, true);
    Client back = connect("CLIENT1", "FMOCK");
    back.sendAs(5, "A", "98", "0", "108", "20");
    assertEquals("A", back.receive().msgType());
    Client server = accepted("FMOCK", "CLIENT1");
    Received logon = server.receive();
    assertEquals("5", logon.get(34));
    assertEquals("20", logon.get(108));
    assertNull(logon.get(141));
    // The server answers with numbers of its own past. They are taken as they are: nothing is
    // asked for again, and what it sends next is passed on.
    server.sendAs(2001, "A", "98", "0", "108", "20");
    String report = server.send("8", "39", "0", "11", "ORD-9", "37", "X-9");
    Received relayed = back.receive();
    assertEquals("8", relayed.msgType());
    assertEquals(withoutSequence(report), withoutSequence(relayed.text()));
    back.send("5");
    back.receive();
    assertEquals("5", server.receive().msgType());

    // A client that starts its numbers anew has the real server do the same.
    int second = listen();
    route(second, true);
    Client fresh = connect("CLIENT2", "FMOCK");
    fresh.sendAs(1, "A", "98", "0", "108", "30", "141", "Y");
    assertEquals("A", fresh.receive().msgType());
    Client other = accepted("FMOCK", "CLIENT2");
    Received reset = other.receive();
    assertEquals("1", reset.get(34));
    assertEquals("Y", reset.get(141));
  }

  @Test void aServerThatIsNotThereRefusesOnlyWhatWouldHaveGoneToIt() throws Exception {
    int closedPort;
    try (ServerSocket probe = new ServerSocket(0)) {
      closedPort = probe.getLocalPort();
    }
    route(closedPort, false);
    mocked = request -> request.text("request.tag.11").orElse("").startsWith("MOCK");
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    assertEquals("A", client.logon(30).msgType());
    client.send("D", "11", "MOCK-1");
    assertEquals("8", client.receive().msgType());
    client.send("D", "11", "REAL-1");
    Received refused = client.receive();
    assertEquals("j", refused.msgType());
    assertEquals("3", refused.get(45));
    assertTrue(refused.get(58).contains("fix://127.0.0.1:" + closedPort), refused.get(58));
    awaitEquals(1, () -> String.valueOf(front.healthDetails().get("lastFailure"))
        .contains("fix://127.0.0.1:" + closedPort) ? 1 : 0);
    // Templates still answer.
    client.send("D", "11", "MOCK-2");
    assertEquals("8", client.receive().msgType());
  }

  @Test void whereEveryMessageGoesToTheServerItsRefusalEndsTheClientsSessionToo() throws Exception {
    int port = listen();
    route(port, true);
    startFront(Map.of());
    Client client = connect("CLIENT1", "FMOCK");
    client.sendAs(1, "A", "98", "0", "108", "30", "554", "wrong");
    assertEquals("A", client.receive().msgType());
    Client server = accepted("FMOCK", "CLIENT1");
    assertEquals("wrong", server.receive().get(554));
    server.sendAs(1, "5", "58", "Invalid password");
    server.socket.close();
    Received logout = client.receive();
    assertEquals("5", logout.msgType());
    assertEquals("Invalid password", logout.get(58));
  }

  @Test void aSecondEndpointOfThisPluginCanBeTheRealServer() throws Exception {
    startFront(Map.of());
    QuickFixAcceptor realServer = (QuickFixAcceptor) kit.start("real", Map.of("host", "127.0.0.1", "port", 0));
    real = request -> result(
        response("{\"msgType\":\"8\",\"tags\":{\"37\":\"REAL-1\",\"11\":\"" + request.text("request.tag.11").orElse("")
            + "\",\"39\":\"0\"}}", 0),
        response("{\"msgType\":\"8\",\"tags\":{\"37\":\"REAL-1\",\"11\":\"" + request.text("request.tag.11").orElse("")
            + "\",\"39\":\"2\"}}", 120));
    route(realServer.port(), true);
    Client client = connect("CLIENT1", "FMOCK");
    client.logon(30);
    awaitEquals(1, () -> ((List<?>) realServer.healthDetails().get("sessions")).size());
    assertEquals(List.of("CLIENT1 -> FMOCK"), realServer.healthDetails().get("sessions"));

    client.send("D", "11", "ORD-7", "55", "IBM");
    Received acceptedReport = client.receive();
    Received filledReport = client.receive();
    assertEquals("0", acceptedReport.get(39));
    assertEquals("2", filledReport.get(39));
    assertEquals("REAL-1", filledReport.get(37));
    assertEquals(List.of("front", "real"), requests.stream().map(ProtocolMessageView::endpoint).toList());
    awaitEquals(2, host.relayed::size);
    assertEquals("ORD-7", host.relayed.get(1).request().text("request.tag.11").orElseThrow());

    // The route is taken away: the next thing the real server says ends the session with it,
    // and is not passed on.
    host.route = null;
    realServer.send(mapper.readTree("{\"msgType\":\"B\",\"tags\":{\"148\":\"late news\"}}"))
        .toCompletableFuture().get(5, TimeUnit.SECONDS);
    awaitEquals(0, () -> ((List<?>) realServer.healthDetails().get("sessions")).size());
    assertEquals(2, host.relayed.size());
    // The client is still logged on, and its endpoint answers from templates again.
    client.send("1", "112", "STILL-HERE");
    assertEquals("STILL-HERE", client.receive().get(112));

    // Routed again, the next message that is passed on logs the real server on again.
    route(realServer.port(), true);
    client.send("D", "11", "ORD-8", "55", "IBM");
    assertEquals("0", client.receive().get(39));
    assertEquals("ORD-8", client.receive().get(11));
  }

  private void start(ProtocolExchangeGateway gateway, Map<String, Object> overrides) throws Exception {
    Map<String, Object> settings = new HashMap<>(Map.of("host", "127.0.0.1", "port", 0));
    settings.putAll(overrides);
    kit = ProtocolTestKit.of(plugin).exchanges(request -> {
      requests.add(request);
      return gateway.exchange(request);
    });
    acceptor = (QuickFixAcceptor) kit.start("fix-test", settings);
  }

  private ProtocolResponse response(String document, long delayMs) {
    try {
      return new ProtocolResponse(mapper.readTree(document), delayMs, acknowledged::add);
    } catch (IOException invalid) {
      throw new UncheckedIOException(invalid);
    }
  }

  private static ProtocolExchangeResult result(ProtocolResponse... responses) {
    return ProtocolExchangeResult.matched("fix-test-template", List.of(responses));
  }

  private Client connect(String sender, String target) throws IOException {
    return connect(acceptor, String.valueOf(acceptor.healthDetails().get("beginString")), sender, target);
  }

  private Client connect(QuickFixAcceptor to, String beginString, String sender, String target) throws IOException {
    Client client = new Client(beginString, sender, target, to.port());
    clients.add(client);
    return client;
  }

  private static void awaitEquals(int expected, Supplier<Integer> actual) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (actual.get() != expected && System.nanoTime() < deadline) Thread.sleep(10);
    assertEquals(expected, actual.get());
  }

  /** One received message: its text with | for SOH, and its fields in wire order. */
  private record Received(String text, List<String[]> fields) {
    String get(int tag) {
      for (String[] field : fields) {
        if (field[0].equals(Integer.toString(tag))) return field[1];
      }
      return null;
    }

    String msgType() {
      return get(35);
    }

    List<Integer> tags() {
      return fields.stream().map(field -> Integer.parseInt(field[0])).toList();
    }
  }

  /** A minimal initiator: it frames and numbers what it sends and reads framed messages. */
  private static final class Client {
    private static final DateTimeFormatter TIME =
        DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final Socket socket;
    private final InputStream input;
    private final String beginString;
    private final String sender;
    private final String target;
    private int next = 1;

    private Client(String beginString, String sender, String target, int port) throws IOException {
      this(beginString, sender, target, new Socket("127.0.0.1", port));
    }

    /** The same over a connection that is already there: the accepted side of a hand-written server. */
    private Client(String beginString, String sender, String target, Socket socket) throws IOException {
      this.beginString = beginString;
      this.sender = sender;
      this.target = target;
      this.socket = socket;
      socket.setSoTimeout(12000);
      this.input = new BufferedInputStream(socket.getInputStream());
    }

    private String now() {
      return TIME.format(Instant.now());
    }

    private Received logon(int heartbeat) throws IOException {
      return logonAs(next, heartbeat);
    }

    private Received logonAs(int seqNum, int heartbeat) throws IOException {
      sendAs(seqNum, "A", "98", "0", "108", String.valueOf(heartbeat));
      return receive();
    }

    /** Sends with the next sequence number; returns the message with | for SOH. */
    private String send(String msgType, String... pairs) throws IOException {
      return sendAs(next, msgType, pairs);
    }

    /** Sends with an explicit sequence number; later messages continue after it. */
    private String sendAs(int seqNum, String msgType, String... pairs) throws IOException {
      String sendingTime = now();
      StringBuilder fields = new StringBuilder();
      for (int index = 0; index < pairs.length; index += 2) {
        if (pairs[index].equals("52")) sendingTime = pairs[index + 1];
        else fields.append(pairs[index]).append('=').append(pairs[index + 1]).append('|');
      }
      String body = "35=" + msgType + "|49=" + sender + "|56=" + target + "|34=" + seqNum + "|52=" + sendingTime
          + "|" + fields;
      String message = "8=" + beginString + "|9=" + body.length() + "|" + body;
      int sum = 0;
      for (byte value : message.replace('|', '\u0001').getBytes(StandardCharsets.ISO_8859_1)) sum += value & 0xff;
      message += String.format("10=%03d|", sum % 256);
      next = seqNum + 1;
      socket.getOutputStream().write(message.replace('|', '\u0001').getBytes(StandardCharsets.ISO_8859_1));
      return message;
    }

    /** The next message; fails when the acceptor closed the connection instead. */
    private Received receive() throws IOException {
      Received received = receiveOrNull();
      if (received == null) throw new IOException("The connection was closed");
      return received;
    }

    /** The next message, or null once the acceptor closed the connection. */
    private Received receiveOrNull() throws IOException {
      try {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        // 8=FIX.4.x<SOH>9=<length><SOH>, then the body and 10=nnn<SOH>.
        for (int separators = 0; separators < 2; ) {
          int value = input.read();
          if (value < 0) return null;
          frame.write(value);
          if (value == 1) separators++;
        }
        String head = frame.toString(StandardCharsets.ISO_8859_1);
        int length = Integer.parseInt(head.substring(head.indexOf("\u00019=") + 3, head.length() - 1));
        frame.writeBytes(input.readNBytes(length + 7));
        String text = frame.toString(StandardCharsets.ISO_8859_1).replace('\u0001', '|');
        List<String[]> fields = new ArrayList<>();
        for (String pair : text.split("\\|")) fields.add(pair.split("=", 2));
        return new Received(text, fields);
      } catch (SocketException reset) {
        return null;
      }
    }
  }
}
