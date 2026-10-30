# Fantastic Mock protocol plugins

Plugins that teach [Fantastic Mock](https://fantasticmock.com/), the free, self-hosted API
mock server, more protocols. Each one is a folder you drop next to Fantastic Mock: your
templates then match, answer and log that protocol like any HTTP template, with variables,
delays, callbacks, traffic logs and metrics.

| Plugin | Mocks | Listens on | Bundled libraries |
| --- | --- | --- | --- |
| [http-netty](http-netty/README.md) 1.2.0 | HTTP/1.1 on an extra, faster port for load tests; serves your existing HTTP templates | its own port (example: 9083) | Netty |
| [websocket-jakarta](websocket-jakarta/README.md) 1.1.0 | WebSocket services: answers, sequences, loops until an event, callbacks | Fantastic Mock's own port (`ws://localhost:8080/events`) | none |
| [grpc](grpc/README.md) 1.5.0 | gRPC unary and server-streaming methods, from your `.proto` files, no generated code | its own port (example: 9091) | gRPC, protobuf |
| [fix-quickfixj](fix-quickfixj/README.md) 1.4.0 | A FIX 4.2 and 4.4 counterparty: logon, sequence numbers, resends, execution reports | its own port per FIX version (example: 9886, 9887) | QuickFIX/J, Apache MINA |

**Requirements:** Fantastic Mock 1.0.5 or later and Java 21. A JRE is enough for the
ready-made plugins; building them needs a JDK. fix-quickfixj needs a copy of 1.0.5
downloaded on October 26, 2026 or later; download it again if yours is older.

## Install

1. **Get the plugin.** Download its zip, such as `grpc-1.5.0.zip`, from this repository's
   **Releases** page, or [build it](#build-from-source).

2. **Unzip it into `protocol-plugins/`.** Fantastic Mock looks for plugins in a folder called
   `protocol-plugins` in the folder you start it from. Each plugin gets its own folder, with
   its JAR and its libraries side by side:

   ```text
   my-mock/
     fantastic-mock-1.0.5.jar
     application.yml
     mock-templates/
     protocol-plugins/
       grpc/
         fantasticmock-grpc-1.5.0.jar
         grpc-api-1.83.1.jar
         ...                          (the plugin's libraries)
         examples/
         README.md
   ```

   Keep the libraries next to the plugin JAR. A plugin JAR copied on its own fails to start
   with a `NoClassDefFoundError`.

3. **Add an instance to `application.yml`.** An instance is one running endpoint of the
   plugin: a port, or a path for WebSocket. Fantastic Mock reads `application.yml` from the
   folder you start it from; create the file if it is not there. For example:

   ```yaml
   fantasticmock:
     protocols:
       instances:
         grpc-demo:
           protocol: grpc
           enabled: true
           settings:
             host: 127.0.0.1
             port: 9091
             descriptor-file: protocol-plugins/grpc/examples/hello.protoset
   ```

   Each plugin's README lists its settings, and its `examples/application-fragment.yml` holds
   a ready-made configuration. Several plugins go under the same `instances:` key.

4. **Add templates.** Copy the example templates from the plugin's `examples/` folder into
   your templates folder (`mock-templates/` by default), or create your own in the workspace
   with **New** and the plugin's protocol. A template picks its instance with
   `settings.endpoint`.

5. **Restart Fantastic Mock** (`java -jar fantastic-mock-1.0.5.jar`). The console lists each
   plugin and instance as it starts:

   ```text
   Initializing protocol plugin grpc... ok
   Starting protocol endpoint grpc/grpc-demo... ok
   ```

   The plugin's README has a **Try it** section with a client call for the example template,
   and every call shows up in **Traffic logs**.

**To update** a plugin, replace its folder with the new version and restart. **To remove**
one, delete its folder and its instances from `application.yml`.

## Build from source

Requires a JDK 21. The build downloads the libraries and the Fantastic Mock protocol API
(checked against its SHA-256) by itself:

```shell
./gradlew packagePlugins          # Windows: gradlew.bat packagePlugins
```

This writes a ready-to-install folder per plugin to `build/plugins/<plugin>/` and a zip per
plugin to `build/distributions/`. `./gradlew build` compiles and runs the tests. To build
against a protocol API jar you already have, add `-PhostApi=<path to the jar>`.

### Run a plugin from source

Where Fantastic Mock runs on a JDK, it can also compile a plugin from its sources when it
starts, which is handy while you change one. Run `./gradlew prepareLibs` to download each
plugin's libraries into its `lib/` folder, then copy the plugin's `protocol.yml`, `src/main/`,
`lib/` and `examples/` into `protocol-plugins/<plugin>/`. A folder with a `src/` is compiled;
one without is loaded as it is. To check a source folder before starting:

```shell
java -jar fantastic-mock-1.0.5.jar --protocol-plugin check protocol-plugins/<plugin>
```

## Write your own

These plugins are complete examples of the protocol plugin API: a `protocol.yml` that
declares the protocol, its settings and the fields templates use, and a few Java classes that
handle the transport. Fantastic Mock does the rest. To start a new one, open the **Plugins**
tab in the workspace and use **New**: it asks a few questions and generates a plugin you can
build. A running Fantastic Mock also explains the plugin model, with diagrams, at
`http://localhost:8080/__admin/protocol-guide/`.

## License

The plugins in this repository are licensed under the [Apache License 2.0](LICENSE).

The libraries the build downloads, and the zips include, keep their own licenses: for
example Netty, gRPC, Guava and Apache MINA under the Apache License 2.0, protobuf under the
BSD 3-Clause license, and QuickFIX/J under the QuickFIX Software License. Fantastic Mock itself is free to use but not open source; it
is not covered by this license.
