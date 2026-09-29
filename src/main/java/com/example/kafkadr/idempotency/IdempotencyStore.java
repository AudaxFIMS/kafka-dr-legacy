package com.example.kafkadr.idempotency;

/**
 * Idempotency store for preventing duplicate message processing.
 *
 * <p>Key format: {@code {prefix}:{topic}:{message_key}}
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@link InMemoryIdempotencyStore} — ConcurrentHashMap with TTL (single instance)</li>
 *   <li>Redis — {@code SET key NX EX ttl} (multi-instance)</li>
 *   <li>DB — unique constraint on key column (multi-instance)</li>
 * </ul>
 */
public interface IdempotencyStore {

    /**
     * Check if a message has already been processed.
     *
     * @param key the idempotency key ({prefix}:{topic}:{message_key})
     * @return true if duplicate, false if new
     */
    boolean isDuplicate(String key);

    /**
     * Read-only check: was the message already marked as processed (within TTL)?
     * Unlike {@link #isDuplicate(String)}, does not record the key — call
     * {@link #markProcessed(String)} after successful processing.
     */
    boolean isProcessed(String key);

    /**
     * Mark a message as successfully processed.
     */
    void markProcessed(String key);

    /**
     * Remove all entries.
     */
    void clear();

    /**
     * Number of tracked entries.
     */
    int size();

    /**
     * Release resources.
     */
    void stop();
}
