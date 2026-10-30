package com.fantasticmock.example.grpc;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import java.nio.file.*;
import java.util.*;

/** Resolve a protoc --include_imports descriptor set once; no generated Java stubs. */
final class GrpcSchema {
  record InboundMessage(DynamicMessage message, long payloadSize) {}
  record Method(String service, MethodDescriptor<DynamicMessage, DynamicMessage> wire, Descriptor input, Descriptor output) {
    /** Whether the server answers with a stream of messages (one request, many replies). */
    boolean streaming() { return wire.getType() == MethodDescriptor.MethodType.SERVER_STREAMING; }

    MethodDescriptor<InboundMessage, DynamicMessage> serverWire() {
      var requestMarshaller = wire.getRequestMarshaller();
      return MethodDescriptor.<InboundMessage, DynamicMessage>newBuilder()
          .setType(wire.getType()).setFullMethodName(wire.getFullMethodName())
          .setRequestMarshaller(new MethodDescriptor.Marshaller<InboundMessage>() {
            @Override public java.io.InputStream stream(InboundMessage value) {
              return requestMarshaller.stream(value.message());
            }
            @Override public InboundMessage parse(java.io.InputStream input) {
              try {
                // Count the received protobuf bytes before parsing can merge duplicate fields.
                byte[] bytes = input.readAllBytes();
                return new InboundMessage(requestMarshaller.parse(new java.io.ByteArrayInputStream(bytes)), bytes.length);
              } catch (java.io.IOException error) {
                throw io.grpc.Status.INTERNAL.withCause(error).asRuntimeException();
              }
            }
          })
          .setResponseMarshaller(wire.getResponseMarshaller()).build();
    }
  }
  final List<Method> methods = new ArrayList<>();
  /** Client-streaming and bidirectional methods, which are not served; a call to one gets UNIMPLEMENTED. */
  final List<String> skipped = new ArrayList<>();
  private final Map<String, Method> byName = new HashMap<>();
  final JsonFormat.Printer printer;
  final JsonFormat.Parser parser;

  GrpcSchema(Path file) throws Exception {
    var protos = new LinkedHashMap<String, DescriptorProtos.FileDescriptorProto>();
    try (var input = Files.newInputStream(file)) {
      for (var proto : DescriptorProtos.FileDescriptorSet.parseFrom(input).getFileList())
        if (protos.put(proto.getName(), proto) != null)
          throw new IllegalArgumentException("Duplicate descriptor file: " + proto.getName());
    }
    var built = new LinkedHashMap<String, FileDescriptor>();
    for (String name : protos.keySet()) resolve(name, protos, built, new HashSet<>());
    var types = JsonFormat.TypeRegistry.newBuilder();
    for (FileDescriptor descriptor : built.values()) {
      types.add(descriptor.getMessageTypes());
      for (var service : descriptor.getServices()) for (var method : service.getMethods()) {
        if (method.isClientStreaming()) {
          skipped.add(service.getFullName() + "/" + method.getName());
          continue;
        }
        var wire = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
            .setType(method.isServerStreaming() ? MethodDescriptor.MethodType.SERVER_STREAMING : MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(service.getFullName() + "/" + method.getName())
            .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getInputType())))
            .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getOutputType())))
            .build();
        Method compiled = new Method(service.getFullName(), wire, method.getInputType(), method.getOutputType());
        methods.add(compiled);
        byName.put(wire.getFullMethodName(), compiled);
      }
    }
    if (methods.isEmpty()) throw new IllegalArgumentException(
        "Descriptor set contains no unary or server-streaming methods" + (skipped.isEmpty() ? "" : "; not served: " + skipped));
    var registry = types.build();
    printer = JsonFormat.printer().usingTypeRegistry(registry).omittingInsignificantWhitespace();
    parser = JsonFormat.parser().usingTypeRegistry(registry);
  }

  /** A method by its full name, such as {@code demo.Greeter/SayHello}; null when unknown. */
  Method method(String fullName) {
    return byName.get(fullName);
  }

  private static FileDescriptor resolve(String name, Map<String, DescriptorProtos.FileDescriptorProto> protos,
      Map<String, FileDescriptor> built, Set<String> visiting) throws Exception {
    if (built.containsKey(name)) return built.get(name);
    var proto = protos.get(name);
    if (proto == null) throw new IllegalArgumentException("Missing imported descriptor " + name + "; use protoc --include_imports");
    if (!visiting.add(name)) throw new IllegalArgumentException("Cyclic descriptor import: " + name);
    var dependencies = new ArrayList<FileDescriptor>();
    for (String dependency : proto.getDependencyList()) dependencies.add(resolve(dependency, protos, built, visiting));
    var descriptor = FileDescriptor.buildFrom(proto, dependencies.toArray(FileDescriptor[]::new));
    visiting.remove(name);
    built.put(name, descriptor);
    return descriptor;
  }
}
