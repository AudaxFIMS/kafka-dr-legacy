package com.example.kafkadr.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageSerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void jsonStringShouldBeSentAsIsNotDoubleEncoded() throws Exception {
        String body = "{\"orderId\":\"ORD-1\",\"items\":2}";

        Object serialized = MessageSerializer.serialize(body, ContentType.JSON);

        assertEquals(body, serialized);
        JsonNode node = mapper.readTree((String) serialized);
        assertTrue(node.isObject());
        assertEquals("ORD-1", node.path("orderId").asText());
    }

    @Test
    void jsonArrayStringShouldBeSentAsIs() {
        assertEquals("[1,2,3]", MessageSerializer.serialize("[1,2,3]", ContentType.JSON));
    }

    @Test
    void plainStringShouldBecomeJsonStringLiteral() {
        assertEquals("\"hello world\"", MessageSerializer.serialize("hello world", ContentType.JSON));
    }

    @Test
    void blankStringShouldBecomeJsonStringLiteral() {
        assertEquals("\"\"", MessageSerializer.serialize("", ContentType.JSON));
    }

    @Test
    void objectShouldBeSerializedToJson() throws Exception {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("orderId", "ORD-2");
        order.put("items", 3);

        Object serialized = MessageSerializer.serialize(order, ContentType.JSON);

        assertEquals("{\"orderId\":\"ORD-2\",\"items\":3}", serialized);
    }

    @Test
    void jsonNodeShouldBeSerializedToJson() throws Exception {
        JsonNode node = mapper.readTree("{\"a\":1}");

        assertEquals("{\"a\":1}", MessageSerializer.serialize(node, ContentType.JSON));
    }

    @Test
    void stringContentTypeShouldBeUnchanged() {
        assertEquals("{\"a\":1}", MessageSerializer.serialize("{\"a\":1}", ContentType.STRING));
    }
}
