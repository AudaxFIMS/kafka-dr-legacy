package com.example.kafkadr.handler;

/**
 * Type-safe message handler. The generic parameter {@code T} determines the
 * deserialized value type that the handler receives.
 *
 * <p>The framework resolves {@code T} automatically via reflection from the
 * class hierarchy — no need to override {@code getMessageType()} or similar.
 *
 * <p>Content-type mapping (YAML → Java type):
 * <pre>
 *   content-type: string  → MessageHandler&lt;String&gt;
 *   content-type: json    → MessageHandler&lt;JsonNode&gt;   (or any Jackson-deserializable POJO)
 *   content-type: bytes   → MessageHandler&lt;byte[]&gt;
 *   content-type: native  → MessageHandler&lt;PaymentAvro&gt; (Avro/Protobuf generated class)
 * </pre>
 *
 * @param <T> the deserialized message value type
 */
public interface MessageHandler<T> {

    void handle(MessageEnvelope<T> message);
}
