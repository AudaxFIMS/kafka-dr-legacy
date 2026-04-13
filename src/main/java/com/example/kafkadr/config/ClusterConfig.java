package com.example.kafkadr.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterConfig {

    private String bootstrapServers;
    private int priority;
    private Map<String, Object> properties = new LinkedHashMap<>();

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

    /**
     * Per-cluster Kafka client property overrides.
     * Keys under {@code configuration:} are applied directly to Kafka client Properties.
     * Overrides values from {@code default-environment}.
     *
     * <pre>
     * clusters:
     *   eu-west:
     *     bootstrap-servers: kafka-eu:9093
     *     priority: 2
     *     properties:
     *       configuration:
     *         ssl.truststore.location: /certs/eu-truststore.p12
     *         ssl.truststore.password: changeit
     * </pre>
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "ClusterConfig{bootstrapServers='" + bootstrapServers + "', priority=" + priority + "}";
    }
}
