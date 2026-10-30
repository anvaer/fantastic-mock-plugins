# FIX 4.2 and 4.4 plugin on QuickFIX/J

A FIX acceptor for FIX 4.2 and FIX 4.4, built on [QuickFIX/J](https://www.quickfixj.org/).
Your trading client logs on, sends application messages such as NewOrderSingle, and gets
back the messages your templates describe, for example a sequence of ExecutionReports.
QuickFIX/J runs the session layer (logon, sequence numbers, heartbeats, resends) the way a
real counterparty does; Fantastic Mock does the matching, variables, delays, callbacks,
traffic logs and metrics.

## Install

Follow the [installation steps](../README.md#install) in the repository README. The plugin
folder is `protocol-plugins/fix-quickfixj/`: the plugin JAR and its libraries (QuickFIX/J
and Apache MINA) must sit side by side in it. It needs Fantastic Mock 1.0.5 downloaded on
October 26, 2026 or later; with an older copy, startup fails with `NoClassDefFoundError:
com/fantasticmock/protocol/api/plugin/ProtocolRawMessages`.

Then add one instance per FIX version to `application.yml`. This is
[`examples/application-fragment.yml`](examples/application-fragment.yml):

```yaml
fantasticmock:
  protocols:
    instances:
      fixqfj42-demo:
        protocol: fix-quickfixj
        enabled: true
        settings:
          host: 127.0.0.1
          port: 9886
          begin-string: FIX.4.2
          sender-comp-id: FMOCK
      fixqfj44-demo:
        protocol: fix-quickfixj
        enabled: true
        settings:
          host: 127.0.0.1
          port: 9887
          begin-string: FIX.4.4
          sender-comp-id: FMOCK
```

Copy [`examples/FIXQFJ42__NewOrderSingle.json`](examples/FIXQFJ42__NewOrderSingle.json) and
[`examples/FIXQFJ44__NewOrderSingle.json`](examples/FIXQFJ44__NewOrderSingle.json) into your
templates folder (`mock-templates/` by default) and restart Fantastic Mock.

## Try it

From the plugin folder (`protocol-plugins/fix-quickfixj/`), with Fantastic Mock running:

```powershell
java examples/FixDemoClient.java                          # FIX 4.2 on port 9886
java examples/FixDemoClient.java 127.0.0.1 9887 FIX.4.4   # FIX 4.4 on port 9887
```

The client logs on as `CLIENT1`, sends a limit order and prints what it receives: an
ExecutionReport `39=0` (New) at once and `39=2` (Filled) 500 ms later.

```text
received 8=FIX.4.4|9=154|35=8|34=2|49=FMOCK|52=...|56=CLIENT1|6=0|11=ORD-42|14=0|17=MOCK-ORD-42-1|37=MOCK-ORD-42|38=100|39=0|...
received 8=FIX.4.4|9=176|35=8|34=3|49=FMOCK|52=...|56=CLIENT1|6=125.50|11=ORD-42|14=100|17=MOCK-ORD-42-2|31=125.50|32=100|...
```

Any FIX 4.2 or 4.4 initiator works. Point it at the instance of its version and set its
TargetCompID to that instance's `sender-comp-id`. Let it reset sequence numbers on logon
(`ResetOnLogon=Y` in QuickFIX), or the first logon after the application restarts is
answered with a ResendRequest for everything the client sent before.

## Versions

An instance speaks one version, named by its `begin-string` setting: `FIX.4.2` (the default)
or `FIX.4.4`. To serve both, configure one instance per version, each on its own port, as
the example fragment does. A template belongs to one instance through `settings.endpoint`,
and so to one version.

A client of the other version is answered in its own version with a Logout,
`This endpoint speaks FIX.4.4, not FIX.4.2`, and disconnected.

Instances are independent, also when they speak the same version and see the same CompIDs:
each keeps its own sessions and sequence numbers.

## Templates

Set `settings.transport` to `fix-quickfixj` and `settings.endpoint` to your instance ID. The
instance decides the version, so the fields below are the same for FIX 4.2 and 4.4.

### Request

`msgType` is the routing field: set it directly in the template request for an exact,
indexed match. Everything else is matched with `request.matchers`.

```json
"request": {
  "msgType": "D",
  "matchers": [
    { "location": "request.tag.40", "regex": "^2$" },
    { "location": "request.senderCompId", "regex": "^CLIENT1$" }
  ]
}
```

| Request source | Value |
| --- | --- |
| `request.msgType` | MsgType (35), the routing field, for example `D` or, in FIX 4.4, `AB` |
| `request.senderCompId`, `request.targetCompId` | The client's and the mock's CompID |
| `request.tag.<number>` | Any field by tag number, such as `request.tag.11` for ClOrdID; the first value of a tag that repeats |
| `request.body` | Raw message: the whole message as it arrived, with `\|` in place of SOH |

The same sources feed variables: `{ "name": "clOrdId", "source": "request.tag.11" }`. To
reach a later entry of a repeating group, extract it from `request.body` with a regex.

The other request fields are views of the raw message. In a template's **Example request**
they stay in step with it: a changed tag, MsgType or CompID is written into the raw message
(BodyLength and CheckSum, where it has them, are counted again), and an edited raw message
gives the other fields their values. A test sends the raw message, so whatever a field says,
the template is tested with what a client would send. Repeated tags and groups are written in
the raw message; Tags shows the first value of each tag.

| Response and callback field | Value |
| --- | --- |
| `msgType` | MsgType (35) of the message to send |
| `tags` | Tag number to value; a tag with an empty value is left out |
| `body` | `tag=value` pairs separated by `\|` or line breaks |
| `targetCompId` | Callbacks only: the client that receives the message; empty for every session |

```json
"response": {
  "msgType": "8",
  "tags": { "37": "MOCK-{{clOrdId}}", "17": "E-{{clOrdId}}", "20": "0", "150": "2", "39": "2" }
}
```

A template may define a `response` array; each item may add `delayMs` or
`minDelayMs`/`maxDelayMs`. Responses of one request are sent in order, each delay starting
after the previous message, as in the example templates: a New ExecutionReport at once and a
Filled one 500 ms later. A tag with an empty value is left out, so an optional field can take
a variable that may resolve to nothing.

A callback sends an unsolicited message to logged-on sessions. Templates of any transport can
use it, for example an HTTP template that pushes a fill:

```json
{
  "transport": "fix-quickfixj",
  "endpoint": "fixqfj42-demo",
  "targetCompId": "CLIENT1",
  "msgType": "8",
  "tags": { "37": "{{orderId}}", "17": "PUSH-1", "20": "0", "150": "2", "39": "2" },
  "delayMs": 100,
  "async": true
}
```

`endpoint` selects the instance and `targetCompId` the client by its CompID; with an empty
`targetCompId` the message goes to every logged-on session. The callback fails when no
session received it.

Good to know:

- **Groups come from the dictionary.** QuickFIX/J builds a repeating group from the pairs in
  `body` when its dictionary defines that group for the message type. A tag that repeats
  without such a definition is refused: the request is answered with BusinessMessageReject
  (`380=0`) naming the tags, because QuickFIX/J would otherwise send one entry of the group.
  For your own groups, set `data-dictionary`.
- **QuickFIX/J orders the fields.** A sent message has its body fields in tag order and its
  groups in dictionary order, not in the order of the template.
- **The session header cannot be overridden.** BeginString, BodyLength, MsgSeqNum, CheckSum,
  SenderCompID, TargetCompID and SendingTime are always the session's own.

The template editor shows each response and callback as the **Raw message** it makes,
built from MsgType, Tags and Raw fields with the values as the template writes them; the
session adds SenderCompID, TargetCompID, MsgSeqNum and SendingTime when it sends. A traffic
log keeps the message as it was sent, with the session's header, as the response's Raw
message; a test shows the preview there, since it sends nothing.

An application message that matches no template is answered with BusinessMessageReject
(`35=j`, `380=3`). Set `reject-unmatched: false` to ignore it instead.

## Session layer

QuickFIX/J handles Logon, Logout, Heartbeat, TestRequest, ResendRequest, SequenceReset and
Reject; only application messages reach templates.

- **Logon.** Any SenderCompID is accepted, and a session is created for it on its first
  logon. With `sender-comp-id` set, a Logon to another TargetCompID is refused with a Logout
  that says why. A second connection for a session that is logged on is closed.
- **Sequence numbers.** Both directions are kept in memory until the instance stops, across
  connections. A message numbered too high is held back and a ResendRequest sent; one
  numbered too low ends the session with a Logout. ResetSeqNumFlag (141=Y) on a Logon
  restarts both at 1.
- **ResendRequest.** Application messages the mock sent are resent with PossDupFlag (43=Y);
  administrative ones are skipped with a gap fill. Templates are not asked again. QuickFIX/J
  keeps every sent message for this until the instance stops, so a session that runs long at a
  high rate grows the heap; `persist-messages: false` keeps nothing and answers every
  ResendRequest with a gap fill instead.
- **Heartbeats.** A silent client gets a TestRequest and is disconnected when it stays
  silent. `disconnect-idle: false` turns the disconnect off, which helps while a client is
  paused in a debugger; heartbeats and TestRequests are still sent.
- **Validation.** By default messages are parsed with the dictionary but not validated, and
  SendingTime is not checked against the clock. With `validate: true` a message that breaks
  the dictionary, for example one without a required field, is answered with a Reject
  (35=3), as a strict counterparty would.

A response is acknowledged for the traffic log when QuickFIX/J has queued it for the
connection, which is not proof that the client received it.

## Settings

| Instance setting | Default | |
| --- | --- | --- |
| `host` | `127.0.0.1` | |
| `port` | `9886` | `0` picks a free port |
| `begin-string` | `FIX.4.2` | The version this instance speaks: `FIX.4.2` or `FIX.4.4` |
| `sender-comp-id` | empty | The mock's own CompID; empty accepts any TargetCompID |
| `max-in-flight` | `1024` | Requests being answered, including delayed responses; beyond it `35=j`, `380=4` |
| `reject-unmatched` | `true` | BusinessMessageReject for a message without a template |
| `disconnect-idle` | `true` | Disconnect a client that stays silent |
| `persist-messages` | `true` | Keep sent messages for ResendRequest; `false` answers resends with a gap fill and keeps memory flat |
| `validate` | `false` | Validate incoming messages against the dictionary |
| `data-dictionary` | empty | Path of a QuickFIX/J dictionary file, relative to the application's working directory; empty uses the standard one |

`GET /__admin/protocols` reports the bound address, the version as `beginString`, logged-on
sessions as `client -> mock`, the last error QuickFIX/J reported as `lastFailure`, and the
counters `messages`, `rejected`, `unmatched`, `failures`, `logons`, `refusedLogons` and
`businessRejects`.

## Limits

- FIX 4.2 and 4.4 only, one version per instance. FIX 5.0 (FIXT.1.1) is not supported.
- An acceptor to its clients: no template logs on to another server.
- Plain TCP: no TLS.
- Sequence numbers and sent messages are kept in memory and are lost when the instance
  stops or reloads.
- Text is ISO-8859-1; there is no charset setting.
- A connection that never logs on is not timed out.

Bind beyond loopback only in an isolated test environment.

## Implementation

- `QuickFixProtocolPlugin`: entry point; creates the acceptor for an endpoint.
- `QuickFixAcceptor`: the QuickFIX/J settings and engine lifecycle, session creation, health
  details and the callback sender.
- `QuickFixApplication`: the engine's callbacks: logon checks, application messages to
  `ProtocolEndpoint.handle`, and a `ProtocolResponseDelivery` per logged-on session.
- `QuickFixMessages`: rendered documents to QuickFIX/J messages, and received messages to
  fields.
- `QuickFixMessageView`: the lazy request fields declared in `protocol.yml`.
- `QuickFixUpstream`: the optional `ProtocolUpstreamClient` of an instance. A client session
  gets a session of its own with another FIX server (`fix://host:port`), under the client's
  CompIDs and logged on with what the client's Logon carried. Application messages are sent
  on with the text they came with, and the server's are handed back the same way whenever
  they come; each session keeps its own sequence numbers, heartbeats and resends. The
  rules of an upstream may change `tag` and `body` (`rewritableFields`): a tag keeps its
  place with its new value and a new one is added; a rule on the body works on the whole
  text. The tags a session writes itself and the MsgType stay as they are.
- `WireMessage`: a message that is written as it was received, with the header of the
  session it is sent on. QuickFIX/J alone would write the fields it parsed, in tag order.

QuickFIX/J runs one thread per session (`ThreadedSocketAcceptor`), so a session's messages
are matched in order and its immediate responses sent in order. Sessions are created with
the instance id as their QuickFIX/J session qualifier: QuickFIX/J keeps one registry of
sessions for all its acceptors, and the qualifier keeps the instances apart.

Tests in `src/test/java` use `ProtocolTestKit` and real sockets, with a hand-written
initiator so that the acceptor is not only tested against itself, and a hand-written
server for the messages that are sent on.

The libraries are downloaded by the build and are not part of this repository.
QuickFIX/J is distributed under the QuickFIX Software License and Apache MINA under the
Apache License 2.0. SLF4J is provided by Fantastic Mock.
