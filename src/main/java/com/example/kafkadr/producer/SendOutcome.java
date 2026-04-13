package com.example.kafkadr.producer;

/**
 * Result of a producer send attempt — drives retry and failover logic.
 */
public enum SendOutcome {
    SUCCESS,
    SERIALIZATION_ERROR,
    CLUSTER_UNAVAILABLE,
    RETRIES_EXHAUSTED
}
