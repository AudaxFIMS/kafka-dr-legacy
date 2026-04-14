package com.example.kafkadr.idempotency;

import com.example.kafkadr.config.IdempotencyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryIdempotencyStoreTest {

    private IdempotencyStore store;

    @BeforeEach
    void setUp() {
        IdempotencyConfig config = new IdempotencyConfig();
        config.setTtlSeconds(3600);
        config.setKeyPrefix("test");
        store = new InMemoryIdempotencyStore(config);
    }

    @AfterEach
    void tearDown() {
        store.stop();
    }

    @Test
    void shouldNotDetectDuplicateForNewKey() {
        assertFalse(store.isDuplicate("test:order-events:ORD-001"));
    }

    @Test
    void shouldDetectDuplicateForExistingKey() {
        String key = "test:order-events:ORD-001";
        assertFalse(store.isDuplicate(key));
        assertTrue(store.isDuplicate(key));
    }

    @Test
    void shouldDistinguishDifferentKeys() {
        assertFalse(store.isDuplicate("test:order-events:ORD-001"));
        assertFalse(store.isDuplicate("test:order-events:ORD-002"));
        assertTrue(store.isDuplicate("test:order-events:ORD-001"));
    }

    @Test
    void shouldDistinguishDifferentTopics() {
        assertFalse(store.isDuplicate("test:order-events:KEY-1"));
        assertFalse(store.isDuplicate("test:payment-events:KEY-1"));
    }

    @Test
    void shouldClearAllEntries() {
        store.isDuplicate("test:t1:k1");
        store.isDuplicate("test:t1:k2");
        assertEquals(2, store.size());

        store.clear();
        assertEquals(0, store.size());
    }

    @Test
    void shouldMarkProcessedExplicitly() {
        String key = "test:order-events:ORD-099";
        store.markProcessed(key);
        assertTrue(store.isDuplicate(key));
    }

    @Test
    void shouldHandleTtlExpiration() {
        IdempotencyConfig config = new IdempotencyConfig();
        config.setTtlSeconds(0);
        config.setKeyPrefix("expired");
        IdempotencyStore shortTtlStore = new InMemoryIdempotencyStore(config);

        try {
            String key = "expired:topic:k1";
            assertFalse(shortTtlStore.isDuplicate(key));
            assertFalse(shortTtlStore.isDuplicate(key)); // TTL=0, already expired
        } finally {
            shortTtlStore.stop();
        }
    }
}
