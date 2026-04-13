package com.example.kafkadr.handler;

import com.example.kafkadr.avro.PaymentEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HandlerTypeResolverTest {

    // --- direct implementations ---

    static class StringHandler implements MessageHandler<String> {
        @Override
        public void handle(MessageEnvelope<String> message) {}
    }

    static class JsonNodeHandler implements MessageHandler<JsonNode> {
        @Override
        public void handle(MessageEnvelope<JsonNode> message) {}
    }

    static class ByteArrayHandler implements MessageHandler<byte[]> {
        @Override
        public void handle(MessageEnvelope<byte[]> message) {}
    }

    static class ObjectHandler implements MessageHandler<Object> {
        @Override
        public void handle(MessageEnvelope<Object> message) {}
    }

    static class MapHandler implements MessageHandler<Map<String, Object>> {
        @Override
        public void handle(MessageEnvelope<Map<String, Object>> message) {}
    }

    // --- via abstract base class ---

    static abstract class BaseHandler<T> implements MessageHandler<T> {}

    static class ExtendedStringHandler extends BaseHandler<String> {
        @Override
        public void handle(MessageEnvelope<String> message) {}
    }

    @Test
    void shouldResolveStringType() {
        assertEquals(String.class, HandlerTypeResolver.resolve(new StringHandler()));
    }

    @Test
    void shouldResolveJsonNodeType() {
        assertEquals(JsonNode.class, HandlerTypeResolver.resolve(new JsonNodeHandler()));
    }

    @Test
    void shouldResolveByteArrayType() {
        assertEquals(byte[].class, HandlerTypeResolver.resolve(new ByteArrayHandler()));
    }

    @Test
    void shouldResolveObjectType() {
        assertEquals(Object.class, HandlerTypeResolver.resolve(new ObjectHandler()));
    }

    @Test
    void shouldResolveParameterizedMapType() {
        assertEquals(Map.class, HandlerTypeResolver.resolve(new MapHandler()));
    }

    @Test
    void shouldResolveFromAbstractBaseClass() {
        assertEquals(String.class, HandlerTypeResolver.resolve(new ExtendedStringHandler()));
    }

    @Test
    void shouldResolveFromAnonymousClass() {
        MessageHandler<JsonNode> anonymous = new MessageHandler<JsonNode>() {
            @Override
            public void handle(MessageEnvelope<JsonNode> message) {}
        };
        assertEquals(JsonNode.class, HandlerTypeResolver.resolve(anonymous));
    }

    @Test
    void shouldResolveFromDemoHandlers() {
        assertEquals(String.class, HandlerTypeResolver.resolve(new DemoHandlers.ProcessDemoEvent()));
        assertEquals(JsonNode.class, HandlerTypeResolver.resolve(new DemoHandlers.ProcessOrder()));
        assertEquals(PaymentEvent.class, HandlerTypeResolver.resolve(new DemoHandlers.ProcessPayment()));
        assertEquals(byte[].class, HandlerTypeResolver.resolve(new DemoHandlers.ProcessRawData()));
    }
}
