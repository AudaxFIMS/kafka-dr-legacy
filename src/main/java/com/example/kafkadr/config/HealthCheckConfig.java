package com.example.kafkadr.config;

public class HealthCheckConfig {

    private long intervalMs = 5000;
    private long timeoutMs = 3000;
    private int failureThreshold = 3;
    private int recoveryThreshold = 3;

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public int getRecoveryThreshold() {
        return recoveryThreshold;
    }

    public void setRecoveryThreshold(int recoveryThreshold) {
        this.recoveryThreshold = recoveryThreshold;
    }
}
