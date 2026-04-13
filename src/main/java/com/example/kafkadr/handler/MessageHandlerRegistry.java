package com.example.kafkadr.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps handler names (from YAML config) to MessageHandler implementations.
 * Resolves and caches the generic type parameter T on registration.
 */
public class MessageHandlerRegistry {

    private final Map<String, MessageHandler<?>> handlers = new ConcurrentHashMap<>();
    private final Map<String, Class<?>> handlerTypes = new ConcurrentHashMap<>();

    public <T> void register(String name, MessageHandler<T> handler) {
        handlers.put(name, handler);
        handlerTypes.put(name, HandlerTypeResolver.resolve(handler));
    }

    public MessageHandler<?> getHandler(String name) {
        MessageHandler<?> handler = handlers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("No handler registered with name: " + name);
        }
        return handler;
    }

    /**
     * Returns the resolved generic type T for a registered handler.
     */
    public Class<?> getHandlerType(String name) {
        Class<?> type = handlerTypes.get(name);
        if (type == null) {
            throw new IllegalArgumentException("No handler registered with name: " + name);
        }
        return type;
    }

    public boolean hasHandler(String name) {
        return handlers.containsKey(name);
    }
}
