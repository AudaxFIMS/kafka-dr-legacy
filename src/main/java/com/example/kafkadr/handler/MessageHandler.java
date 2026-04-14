package com.example.kafkadr.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Type-safe message handler working directly with Kafka's {@link ConsumerRecord}.
 *
 * <p>Both key type {@code K} and value type {@code V} are resolved automatically
 * via reflection — just declare the generic parameters:
 *
 * <pre>
 * // String key, JsonNode value
 * public class ProcessOrder implements MessageHandler&lt;String, JsonNode&gt; {
 *     public void handle(ConsumerRecord&lt;String, JsonNode&gt; record) {
 *         String orderId = record.key();
 *         JsonNode order  = record.value();
 *         Headers headers = record.headers();
 *         long timestamp  = record.timestamp();
 *     }
 * }
 * </pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MessageHandler<K, V> {

    void handle(ConsumerRecord<K, V> record);
}
