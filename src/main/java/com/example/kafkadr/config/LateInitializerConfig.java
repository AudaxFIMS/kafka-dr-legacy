package com.example.kafkadr.config;

public class LateInitializerConfig {

    private long timeoutMs = 3000;

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}
