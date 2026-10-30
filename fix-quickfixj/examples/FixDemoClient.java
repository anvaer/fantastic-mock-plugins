import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * A minimal FIX initiator for trying the example templates, written without QuickFIX/J. It
 * logs on, sends one limit order, prints the two execution reports and logs out. The version
 * defaults to FIX.4.2 and must be the one the instance on that port speaks.
 *
 * <pre>java examples/FixDemoClient.java [host] [port] [version]</pre>
 */
public class FixDemoClient {
  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private final InputStream input;
  private final OutputStream output;
  private final String beginString;
  private int seqNum = 1;

  private FixDemoClient(Socket socket, String beginString) throws IOException {
    this.beginString = beginString;
    input = socket.getInputStream();
    output = socket.getOutputStream();
  }

  public static void main(String[] args) throws IOException {
    String host = args.length > 0 ? args[0] : "127.0.0.1";
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 9886;
    String beginString = args.length > 2 ? args[2] : "FIX.4.2";
    try (Socket socket = new Socket(host, port)) {
      socket.setSoTimeout(5000);
      FixDemoClient client = new FixDemoClient(socket, beginString);
      // ResetSeqNumFlag (141): both sides start at 1, whatever an earlier run ended on.
      client.send("A", "98=0|108=30|141=Y|");
      client.receive();
      client.send("D", "11=ORD-42|21=1|55=IBM|54=1|60=" + TIME.format(Instant.now()) + "|38=100|40=2|44=125.50|");
      client.receive();
      client.receive();
      client.send("5", "");
      client.receive();
    }
  }

  private void send(String msgType, String fields) throws IOException {
    String body = ("35=" + msgType + "|49=CLIENT1|56=FMOCK|34=" + seqNum++ + "|52=" + TIME.format(Instant.now())
        + "|" + fields).replace('|', '\u0001');
    String message = "8=" + beginString + "\u00019=" + body.length() + "\u0001" + body;
    int sum = 0;
    for (byte value : message.getBytes(StandardCharsets.ISO_8859_1)) sum += value & 0xff;
    message += String.format("10=%03d\u0001", sum % 256);
    System.out.println("sent     " + message.replace('\u0001', '|'));
    output.write(message.getBytes(StandardCharsets.ISO_8859_1));
    output.flush();
  }

  // A message ends with the CheckSum field: <SOH>10=nnn<SOH>.
  private void receive() throws IOException {
    ByteArrayOutputStream message = new ByteArrayOutputStream();
    while (!message.toString(StandardCharsets.ISO_8859_1).matches("(?s).*\u000110=\\d{3}\u0001")) {
      int value = input.read();
      if (value < 0) throw new IOException("The connection was closed");
      message.write(value);
    }
    System.out.println("received " + message.toString(StandardCharsets.ISO_8859_1).replace('\u0001', '|'));
  }
}
