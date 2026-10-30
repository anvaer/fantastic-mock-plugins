package com.fantasticmock.example.websocket;

import jakarta.servlet.ServletContext;
import jakarta.websocket.Session;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Keeps the container-owned endpoint stable while a plugin reload swaps its
 * runtime behavior. Only JDK/Jakarta types cross plugin class-loader boundaries.
 */
final class JakartaWebSocketEndpointRegistry {
  static final String ENDPOINT_BINDING_KEY =
      "fantasticmock.websocket-jakarta.endpoint-binding";
  private static final String REGISTRY_ATTRIBUTE =
      "fantasticmock.websocket-jakarta.endpoint-bindings";
  private static final int OWNER_ID_INDEX = 3;
  private static final int OWNER_LOADER_INDEX = 4;

  private JakartaWebSocketEndpointRegistry() {
  }

  static Registration bind(
      ServletContext servletContext,
      String path,
      String instanceId,
      Consumer<Session> opened,
      Consumer<Session> closed,
      Consumer<Throwable> failed) {
    ConcurrentMap<String, AtomicReference<Object[]>> registry = registry(servletContext);
    AtomicReference<Object[]> created = new AtomicReference<>();
    AtomicReference<Object[]> binding = registry.putIfAbsent(path, created);
    boolean newMapping = binding == null;
    if (newMapping) binding = created;

    Object[] delegates = {
        opened,
        closed,
        failed,
        instanceId,
        JakartaWebSocketEndpointRegistry.class.getClassLoader()
    };
    Object[] previous;
    synchronized (binding) {
      previous = binding.get();
      rejectDuplicateInCurrentPlugin(previous, instanceId, path);
      binding.set(delegates);
    }
    return new Registration(registry, path, binding, delegates, previous, newMapping);
  }

  private static void rejectDuplicateInCurrentPlugin(
      Object[] previous,
      String instanceId,
      String path) {
    if (previous == null || previous.length <= OWNER_LOADER_INDEX) return;
    Object previousId = previous[OWNER_ID_INDEX];
    Object previousLoader = previous[OWNER_LOADER_INDEX];
    ClassLoader currentLoader = JakartaWebSocketEndpointRegistry.class.getClassLoader();
    if (previousLoader == currentLoader && !Objects.equals(previousId, instanceId)) {
      throw new IllegalStateException("Duplicate WebSocket mapping for path: " + path);
    }
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentMap<String, AtomicReference<Object[]>> registry(
      ServletContext servletContext) {
    synchronized (servletContext) {
      Object existing = servletContext.getAttribute(REGISTRY_ATTRIBUTE);
      if (existing instanceof ConcurrentMap<?, ?>) {
        return (ConcurrentMap<String, AtomicReference<Object[]>>) existing;
      }
      ConcurrentMap<String, AtomicReference<Object[]>> created = new ConcurrentHashMap<>();
      servletContext.setAttribute(REGISTRY_ATTRIBUTE, created);
      return created;
    }
  }

  record Registration(
      ConcurrentMap<String, AtomicReference<Object[]>> registry,
      String path,
      AtomicReference<Object[]> binding,
      Object[] delegates,
      Object[] previous,
      boolean newMapping) {

    void rollback() {
      if (!binding.compareAndSet(delegates, previous)) return;
      if (newMapping && previous == null) registry.remove(path, binding);
    }

    void release() {
      binding.compareAndSet(delegates, null);
    }
  }
}
