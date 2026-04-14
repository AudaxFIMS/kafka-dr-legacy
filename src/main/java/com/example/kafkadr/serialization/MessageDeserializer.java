package com.example.kafkadr.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes raw Kafka record key and value based on content-type and target type.
 */
public class MessageDeserializer {

    private static final Logger log = LoggerFactory.getLogger(MessageDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Deserialize a raw value (key or value) using content-type and target type.
     */
    public static Object deserialize(Object raw, ContentType contentType, Class<?> targetType,
                                     String topic, int partition, long offset) {
        if (raw == null) return null;

	    return switch (contentType) {
		    case STRING -> raw instanceof byte[] ? new String((byte[]) raw) : raw.toString();
		    case JSON -> deserializeJson(raw, targetType, topic, partition, offset);
		    case BYTES -> raw instanceof byte[] ? raw : raw.toString().getBytes();
		    case NATIVE -> raw;
		    default -> raw;
	    };
    }

    private static Object deserializeJson(Object raw, Class<?> targetType,
                                          String topic, int partition, long offset) {
        try {
            byte[] jsonBytes = raw instanceof byte[] ? (byte[]) raw : raw.toString().getBytes();

            if (JsonNode.class.isAssignableFrom(targetType)) {
                return objectMapper.readTree(jsonBytes);
            }

            return objectMapper.readValue(jsonBytes, targetType);
        } catch (Exception e) {
            log.warn("Failed to deserialize JSON into {} from topic={}, partition={}, offset={}: {}",
                    targetType.getSimpleName(), topic, partition, offset, e.getMessage());
            throw new RuntimeException("JSON deserialization failed for type " + targetType.getName(), e);
        }
    }
}
