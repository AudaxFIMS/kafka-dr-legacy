package com.example.kafkadr.idempotency;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Builds idempotency keys that stay identical across DR clusters.
 *
 * <p>Partition/offset differ between replicated clusters, so the key is based on the
 * message itself: the {@value #MESSAGE_ID_HEADER} header (set by DrProducerManager),
 * falling back to the record key.
 *
 * <p>Format: {@code {prefix}:{topic}:{id}}
 */
public final class IdempotencyKeys {

    public static final String MESSAGE_ID_HEADER = "message-id";

    private IdempotencyKeys() {
    }

    /**
     * @return the idempotency key, or {@code null} if the record has neither a
     *         {@value #MESSAGE_ID_HEADER} header nor a key (cannot be deduplicated)
     */
    public static String of(String prefix, ConsumerRecord<?, ?> record) {
        return of(prefix, record.topic(), record.headers(), record.key());
    }

    /**
     * Same as {@link #of(String, ConsumerRecord)}, for callers that hold the key
     * separately (e.g. already deserialized from raw bytes).
     */
    public static String of(String prefix, String topic, Headers headers, Object key) {
        Header header = headers != null ? headers.lastHeader(MESSAGE_ID_HEADER) : null;
        String id;
        if (header != null && header.value() != null) {
            id = new String(header.value(), StandardCharsets.UTF_8);
        } else if (key instanceof byte[]) {
            id = Base64.getEncoder().encodeToString((byte[]) key);
        } else if (key != null) {
            id = key.toString();
        } else {
            return null;
        }
        return prefix + ":" + topic + ":" + id;
    }
}
