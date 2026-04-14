package com.example.kafkadr.handler;

import com.example.kafkadr.avro.PaymentEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HandlerTypeResolverTest {

    static class StringStringHandler implements MessageHandler<String, String> {
        @Override public void handle(ConsumerRecord<String, String> record) {}
    }

    static class StringJsonHandler implements MessageHandler<String, JsonNode> {
        @Override public void handle(ConsumerRecord<String, JsonNode> record) {}
    }

    static class LongBytesHandler implements MessageHandler<Long, byte[]> {
        @Override public void handle(ConsumerRecord<Long, byte[]> record) {}
    }

    static class StringObjectHandler implements MessageHandler<String, Object> {
        @Override public void handle(ConsumerRecord<String, Object> record) {}
    }

    static abstract class BaseHandler<K, V> implements MessageHandler<K, V> {}

    static class ExtendedHandler extends BaseHandler<String, JsonNode> {
        @Override public void handle(ConsumerRecord<String, JsonNode> record) {}
    }

    @Test
    void shouldResolveStringStringTypes() {
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(new StringStringHandler());
        assertEquals(String.class, types.getKeyType());
        assertEquals(String.class, types.getValueType());
    }

    @Test
    void shouldResolveStringJsonNodeTypes() {
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(new StringJsonHandler());
        assertEquals(String.class, types.getKeyType());
        assertEquals(JsonNode.class, types.getValueType());
    }

    @Test
    void shouldResolveLongByteArrayTypes() {
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(new LongBytesHandler());
        assertEquals(Long.class, types.getKeyType());
        assertEquals(byte[].class, types.getValueType());
    }

    @Test
    void shouldResolveObjectValueType() {
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(new StringObjectHandler());
        assertEquals(String.class, types.getKeyType());
        assertEquals(Object.class, types.getValueType());
    }

    @Test
    void shouldResolveFromAbstractBaseClass() {
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(new ExtendedHandler());
        assertEquals(String.class, types.getKeyType());
        assertEquals(JsonNode.class, types.getValueType());
    }

    @Test
    void shouldResolveFromAnonymousClass() {
        MessageHandler<Long, String> anonymous = new MessageHandler<>() {
	        @Override
	        public void handle(ConsumerRecord<Long, String> record) {
	        }
        };
        HandlerTypeResolver.ResolvedTypes types = HandlerTypeResolver.resolve(anonymous);
        assertEquals(Long.class, types.getKeyType());
        assertEquals(String.class, types.getValueType());
    }

    @Test
    void shouldResolveFromDemoHandlers() {
        var demo = HandlerTypeResolver.resolve(new MessageHandlers.ProcessDemoEvent());
        assertEquals(String.class, demo.getKeyType());
        assertEquals(String.class, demo.getValueType());

        var order = HandlerTypeResolver.resolve(new MessageHandlers.ProcessOrder());
        assertEquals(String.class, order.getKeyType());
        assertEquals(JsonNode.class, order.getValueType());

        var payment = HandlerTypeResolver.resolve(new MessageHandlers.ProcessPayment());
        assertEquals(String.class, payment.getKeyType());
        assertEquals(PaymentEvent.class, payment.getValueType());

        var raw = HandlerTypeResolver.resolve(new MessageHandlers.ProcessRawData());
        assertEquals(String.class, raw.getKeyType());
        assertEquals(byte[].class, raw.getValueType());
    }
}
