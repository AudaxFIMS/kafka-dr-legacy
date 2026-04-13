package com.example.kafkadr.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes Kafka records based on content-type and the handler's target type.
 *
 * <p>Deserialization strategy:
 * <ul>
 *   <li>{@code STRING} → {@code String}</li>
 *   <li>{@code JSON}   → deserialize into {@code targetType} via Jackson
 *       (JsonNode, POJO, Map, etc.)</li>
 *   <li>{@code BYTES}  → {@code byte[]}</li>
 *   <li>{@code NATIVE} → value as-is from Kafka deserializer (Avro, Protobuf)</li>
 * </ul>
 */
public class MessageDeserializer {

    private static final Logger log = LoggerFactory.getLogger(MessageDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Deserialize a record using content-type and the handler's resolved generic type.
     *
     * @param record      the raw Kafka record
     * @param contentType the content-type from consumer config
     * @param targetType  the handler's generic parameter T, resolved via reflection
     * @return deserialized value matching targetType
     */
    public static Object deserialize(ConsumerRecord<?, ?> record, ContentType contentType, Class<?> targetType) {
        Object value = record.value();
        if (value == null) return null;

        switch (contentType) {
            case STRING:
                return value instanceof byte[] ? new String((byte[]) value) : value.toString();

            case JSON:
                return deserializeJson(record, value, targetType);

            case BYTES:
                return value instanceof byte[] ? value : value.toString().getBytes();

            case NATIVE:
                return value;

            default:
                return value;
        }
    }

    private static Object deserializeJson(ConsumerRecord<?, ?> record, Object value, Class<?> targetType) {
        try {
            byte[] jsonBytes = value instanceof byte[] ? (byte[]) value : value.toString().getBytes();

            // If handler expects JsonNode, use readTree (no need for class mapping)
            if (JsonNode.class.isAssignableFrom(targetType)) {
                return objectMapper.readTree(jsonBytes);
            }

            // Otherwise deserialize into the handler's declared POJO type
            return objectMapper.readValue(jsonBytes, targetType);
        } catch (Exception e) {
            log.warn("Failed to deserialize JSON into {} from topic={}, partition={}, offset={}: {}",
                    targetType.getSimpleName(),
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            throw new RuntimeException("JSON deserialization failed for type " + targetType.getName(), e);
        }
    }
}
