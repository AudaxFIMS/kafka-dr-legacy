package com.example.kafkadr.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps handler names (from YAML config) to MessageHandler implementations.
 * Resolves and caches both K and V type parameters on registration.
 */
public class MessageHandlerRegistry {

    private final Map<String, MessageHandler<?, ?>> handlers = new ConcurrentHashMap<>();
    private final Map<String, HandlerTypeResolver.ResolvedTypes> handlerTypes = new ConcurrentHashMap<>();

    public <K, V> void register(String name, MessageHandler<K, V> handler) {
        handlers.put(name, handler);
        handlerTypes.put(name, HandlerTypeResolver.resolve(handler));
    }

    public MessageHandler<?, ?> getHandler(String name) {
        MessageHandler<?, ?> handler = handlers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("No handler registered with name: " + name);
        }
        return handler;
    }

    public HandlerTypeResolver.ResolvedTypes getHandlerTypes(String name) {
        HandlerTypeResolver.ResolvedTypes types = handlerTypes.get(name);
        if (types == null) {
            throw new IllegalArgumentException("No handler registered with name: " + name);
        }
        return types;
    }

    public boolean hasHandler(String name) {
        return handlers.containsKey(name);
    }
}
