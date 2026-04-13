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
}
