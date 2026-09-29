package com.example.kafkadr.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes values before sending to Kafka based on the configured content type.
 */
public class MessageSerializer {

    private static final Logger log = LoggerFactory.getLogger(MessageSerializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Object serialize(Object value, ContentType contentType) {
        if (value == null) return null;

        switch (contentType) {
            case STRING:
                return value.toString();

            case JSON:
                // A String that already holds JSON (e.g. a REST request body) is sent as is —
                // writeValueAsString would encode it a second time as a JSON string literal.
                if (value instanceof String && isValidJson((String) value)) {
                    return value;
                }
                try {
                    return objectMapper.writeValueAsString(value);
                } catch (Exception e) {
                    log.warn("Failed to serialize to JSON: {}", e.getMessage());
                    return value.toString();
                }

            case BYTES:
                if (value instanceof byte[]) return value;
                return value.toString().getBytes();

            case NATIVE:
                return value;

            default:
                return value;
        }
    }

    private static boolean isValidJson(String value) {
        try {
            objectMapper.readTree(value);
            return !value.isBlank();
        } catch (Exception e) {
            return false;
        }
    }
}
