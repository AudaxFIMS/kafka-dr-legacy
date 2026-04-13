package com.example.kafkadr.config;

public class ClusterConfig {

    private String bootstrapServers;
    private int priority;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return "ClusterConfig{bootstrapServers='" + bootstrapServers + "', priority=" + priority + "}";
    }
}
