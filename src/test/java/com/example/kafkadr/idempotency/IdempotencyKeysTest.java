package com.example.kafkadr.idempotency;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class IdempotencyKeysTest {

    @Test
    void shouldPreferMessageIdHeader() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5, "ORD-1", "v");
        record.headers().add(IdempotencyKeys.MESSAGE_ID_HEADER, "abc-123".getBytes(StandardCharsets.UTF_8));

        assertEquals("idem:orders:abc-123", IdempotencyKeys.of("idem", record));
    }

    @Test
    void shouldFallBackToRecordKey() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5, "ORD-1", "v");

        assertEquals("idem:orders:ORD-1", IdempotencyKeys.of("idem", record));
    }

    @Test
    void shouldEncodeByteArrayKey() {
        ConsumerRecord<byte[], String> record = new ConsumerRecord<>("raw", 0, 5, new byte[]{1, 2, 3}, "v");

        assertEquals("idem:raw:AQID", IdempotencyKeys.of("idem", record));
    }

    @Test
    void shouldReturnNullWithoutHeaderAndKey() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5, null, "v");

        assertNull(IdempotencyKeys.of("idem", record));
    }

    @Test
    void shouldBeIndependentOfPartitionAndOffset() {
        ConsumerRecord<String, String> onPrimary = new ConsumerRecord<>("orders", 0, 100, "k", "v");
        ConsumerRecord<String, String> onSecondary = new ConsumerRecord<>("orders", 2, 7, "k", "v");
        onPrimary.headers().add(IdempotencyKeys.MESSAGE_ID_HEADER, "id-1".getBytes(StandardCharsets.UTF_8));
        onSecondary.headers().add(IdempotencyKeys.MESSAGE_ID_HEADER, "id-1".getBytes(StandardCharsets.UTF_8));

        assertEquals(IdempotencyKeys.of("p", onPrimary), IdempotencyKeys.of("p", onSecondary));
    }
}
