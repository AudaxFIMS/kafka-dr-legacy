package com.example.kafkadr.cluster;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runtime state for a single Kafka cluster, including health tracking counters.
 */
public class ClusterInfo {

    private final String name;
    private final String bootstrapServers;
    private final int priority;
    private final AtomicReference<ClusterState> state = new AtomicReference<>(ClusterState.UNKNOWN);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicInteger consecutiveSuccesses = new AtomicInteger(0);

    public ClusterInfo(String name, String bootstrapServers, int priority) {
        this.name = name;
        this.bootstrapServers = bootstrapServers;
        this.priority = priority;
    }

    public String getName() {
        return name;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public int getPriority() {
        return priority;
    }

    public ClusterState getState() {
        return state.get();
    }

    public void setState(ClusterState newState) {
        state.set(newState);
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public int incrementFailures() {
        consecutiveSuccesses.set(0);
        return consecutiveFailures.incrementAndGet();
    }

    public int getConsecutiveSuccesses() {
        return consecutiveSuccesses.get();
    }

    public int incrementSuccesses() {
        consecutiveFailures.set(0);
        return consecutiveSuccesses.incrementAndGet();
    }

    public void resetCounters() {
        consecutiveFailures.set(0);
        consecutiveSuccesses.set(0);
    }

    @Override
    public String toString() {
        return "ClusterInfo{name='" + name + "', bootstrapServers='" + bootstrapServers +
                "', priority=" + priority + ", state=" + state.get() + "}";
    }
}
