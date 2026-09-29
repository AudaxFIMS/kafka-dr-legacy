package com.example.kafkadr.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Root configuration model mapped from kafka-dr.yml.
 */
public class KafkaDrConfig {

    private Map<String, Object> defaultProperties = new LinkedHashMap<>();
    private boolean autoCreateTopics;
    private String instanceId;
    private boolean staticMembership = true;
    private Map<String, Object> defaultConsumerProperties = new LinkedHashMap<>();
    private Map<String, Object> defaultProducerProperties = new LinkedHashMap<>();
    private Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
    private List<DrConsumerConfig> consumers;
    private List<DrProducerConfig> producers;
    private HealthCheckConfig healthCheck = new HealthCheckConfig();
    private LateInitializerConfig lateInitializer = new LateInitializerConfig();
    private IdempotencyConfig idempotency = new IdempotencyConfig();

    public Map<String, Object> getDefaultProperties() {
        return defaultProperties;
    }

    public void setDefaultProperties(Map<String, Object> defaultProperties) {
        this.defaultProperties = defaultProperties;
    }

    public boolean isAutoCreateTopics() {
        return autoCreateTopics;
    }

    public void setAutoCreateTopics(boolean autoCreateTopics) {
        this.autoCreateTopics = autoCreateTopics;
    }

    /**
     * Stable id of this application instance (prefix of consumer {@code group.instance.id}).
     * Blank → hostname.
     */
    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /** Use Kafka static membership ({@code group.instance.id}) for DR consumers. Default: true. */
    public boolean isStaticMembership() {
        return staticMembership;
    }

    public void setStaticMembership(boolean staticMembership) {
        this.staticMembership = staticMembership;
    }

    public Map<String, Object> getDefaultConsumerProperties() {
        return defaultConsumerProperties;
    }

    public void setDefaultConsumerProperties(Map<String, Object> defaultConsumerProperties) {
        this.defaultConsumerProperties = defaultConsumerProperties;
    }

    public Map<String, Object> getDefaultProducerProperties() {
        return defaultProducerProperties;
    }

    public void setDefaultProducerProperties(Map<String, Object> defaultProducerProperties) {
        this.defaultProducerProperties = defaultProducerProperties;
    }

    public Map<String, ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(Map<String, ClusterConfig> clusters) {
        this.clusters = clusters;
    }

    public List<DrConsumerConfig> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<DrConsumerConfig> consumers) {
        this.consumers = consumers;
    }

    public List<DrProducerConfig> getProducers() {
        return producers;
    }

    public void setProducers(List<DrProducerConfig> producers) {
        this.producers = producers;
    }

    public HealthCheckConfig getHealthCheck() {
        return healthCheck;
    }

    public void setHealthCheck(HealthCheckConfig healthCheck) {
        this.healthCheck = healthCheck;
    }

    public LateInitializerConfig getLateInitializer() {
        return lateInitializer;
    }

    public void setLateInitializer(LateInitializerConfig lateInitializer) {
        this.lateInitializer = lateInitializer;
    }

    public IdempotencyConfig getIdempotency() {
        return idempotency;
    }

    public void setIdempotency(IdempotencyConfig idempotency) {
        this.idempotency = idempotency;
    }
}
