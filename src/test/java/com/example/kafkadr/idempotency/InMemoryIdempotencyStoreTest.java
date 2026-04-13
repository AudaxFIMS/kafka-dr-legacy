package com.example.kafkadr.idempotency;

import com.example.kafkadr.config.IdempotencyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryIdempotencyStoreTest {

    private InMemoryIdempotencyStore store;

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
        String key = store.buildKey("topic", 0, 1);
        assertFalse(store.isDuplicate(key));
    }

    @Test
    void shouldDetectDuplicateForExistingKey() {
        String key = store.buildKey("topic", 0, 1);
        assertFalse(store.isDuplicate(key));
        assertTrue(store.isDuplicate(key));
    }

    @Test
    void shouldBuildKeyWithPrefix() {
        String key = store.buildKey("topic", 0, 42);
        assertEquals("test:topic:0:42", key);
    }

    @Test
    void shouldBuildCustomKey() {
        String key = store.buildKey("my-custom-key");
        assertEquals("test:my-custom-key", key);
    }

    @Test
    void shouldClearAllEntries() {
        store.isDuplicate(store.buildKey("topic", 0, 1));
        store.isDuplicate(store.buildKey("topic", 0, 2));
        assertEquals(2, store.size());

        store.clear();
        assertEquals(0, store.size());
    }

    @Test
    void shouldMarkProcessedExplicitly() {
        String key = store.buildKey("manual-key");
        store.markProcessed(key);
        assertTrue(store.isDuplicate(key));
    }

    @Test
    void shouldHandleTtlExpiration() {
        IdempotencyConfig config = new IdempotencyConfig();
        config.setTtlSeconds(0); // immediate expiration
        config.setKeyPrefix("expired");
        InMemoryIdempotencyStore shortTtlStore = new InMemoryIdempotencyStore(config);

        try {
            String key = shortTtlStore.buildKey("topic", 0, 1);
            assertFalse(shortTtlStore.isDuplicate(key)); // marks it
            // With 0 TTL, it should be considered expired immediately
            assertFalse(shortTtlStore.isDuplicate(key));
        } finally {
            shortTtlStore.stop();
        }
    }
}
