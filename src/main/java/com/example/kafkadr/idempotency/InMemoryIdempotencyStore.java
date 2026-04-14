package com.example.kafkadr.idempotency;

import com.example.kafkadr.config.IdempotencyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * In-memory {@link IdempotencyStore} implementation using ConcurrentHashMap with TTL-based eviction.
 * Suitable for single-instance deployments. For multi-instance, use a Redis or DB implementation.
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

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

        long cleanupInterval = Math.max(ttlMillis / 10, 60_000L);
        cleanupScheduler.scheduleWithFixedDelay(this::evictExpired, cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);

        log.info("InMemoryIdempotencyStore initialized: TTL={}s, prefix='{}'", config.getTtlSeconds(), keyPrefix);
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    @Override
    public boolean isDuplicate(String key) {
        long now = System.currentTimeMillis();
        Long existingTimestamp = processedKeys.putIfAbsent(key, now);

        if (existingTimestamp != null) {
            if (now - existingTimestamp < ttlMillis) {
                log.debug("Duplicate detected for key: {}", key);
                return true;
            }
            processedKeys.put(key, now);
            return false;
        }

        return false;
    }

    @Override
    public void markProcessed(String key) {
        processedKeys.put(key, System.currentTimeMillis());
    }

    @Override
    public void clear() {
        int size = processedKeys.size();
        processedKeys.clear();
        log.info("IdempotencyStore cleared ({} entries removed)", size);
    }

    @Override
    public int size() {
        return processedKeys.size();
    }

    @Override
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
