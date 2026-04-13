package com.example.kafkadr.idempotency;

import com.example.kafkadr.config.IdempotencyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * In-memory idempotency store that prevents duplicate message processing.
 * Uses a ConcurrentHashMap with TTL-based eviction.
 *
 * The key is constructed as: {prefix}:{topic}:{partition}:{offset}
 * or a custom idempotency key from the message header.
 */
public class InMemoryIdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);

    private final ConcurrentHashMap<String, Long> processedKeys = new ConcurrentHashMap<>();
    private final long ttlMillis;
    private final String keyPrefix;
    private final ScheduledExecutorService cleanupScheduler;

    public InMemoryIdempotencyStore(IdempotencyConfig config) {
        this.ttlMillis = config.getTtlSeconds() * 1000L;
        this.keyPrefix = config.getKeyPrefix();

        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "idempotency-cleanup");
            t.setDaemon(true);
            return t;
        });

        // Run cleanup every 1/10 of TTL or at least every 60 seconds
        long cleanupInterval = Math.max(ttlMillis / 10, 60_000L);
        cleanupScheduler.scheduleWithFixedDelay(this::evictExpired, cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);

        log.info("IdempotencyStore initialized with TTL={}s, prefix='{}'", config.getTtlSeconds(), keyPrefix);
    }

    /**
     * Build a key from topic/partition/offset.
     */
    public String buildKey(String topic, int partition, long offset) {
        return keyPrefix + ":" + topic + ":" + partition + ":" + offset;
    }

    /**
     * Build a key from a custom idempotency header value.
     */
    public String buildKey(String customKey) {
        return keyPrefix + ":" + customKey;
    }

    /**
     * Check if a message has already been processed. If not, mark it as processed.
     *
     * @return true if the message is a duplicate (already processed), false if it's new
     */
    public boolean isDuplicate(String key) {
        long now = System.currentTimeMillis();
        Long existingTimestamp = processedKeys.putIfAbsent(key, now);

        if (existingTimestamp != null) {
            // Key exists — check if it's still within TTL
            if (now - existingTimestamp < ttlMillis) {
                log.debug("Duplicate detected for key: {}", key);
                return true;
            }
            // Expired entry — update timestamp and treat as new
            processedKeys.put(key, now);
            return false;
        }

        return false;
    }

    /**
     * Explicitly mark a key as processed (e.g., after successful handling).
     */
    public void markProcessed(String key) {
        processedKeys.put(key, System.currentTimeMillis());
    }

    /**
     * Remove all entries (e.g., when switching clusters and you want a clean state).
     */
    public void clear() {
        int size = processedKeys.size();
        processedKeys.clear();
        log.info("IdempotencyStore cleared ({} entries removed)", size);
    }

    public int size() {
        return processedKeys.size();
    }

    public void stop() {
        cleanupScheduler.shutdown();
        try {
            cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void evictExpired() {
        long now = System.currentTimeMillis();
        int before = processedKeys.size();
        processedKeys.entrySet().removeIf(entry -> now - entry.getValue() >= ttlMillis);
        int evicted = before - processedKeys.size();
        if (evicted > 0) {
            log.debug("Evicted {} expired idempotency entries, {} remaining", evicted, processedKeys.size());
        }
    }
}
