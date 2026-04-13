package com.example.kafkadr.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Root configuration model mapped from kafka-dr.yml.
 */
public class KafkaDrConfig {

    private Map<String, Object> defaultEnvironment = new LinkedHashMap<>();
    private boolean autoCreateTopics;
    private Map<String, Object> defaultConsumerProperties = new LinkedHashMap<>();
    private Map<String, Object> defaultProducerProperties = new LinkedHashMap<>();
    private Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
    private List<ConsumerConfig> consumers;
    private List<ProducerConfig> producers;
    private HealthCheckConfig healthCheck = new HealthCheckConfig();
    private LateInitializerConfig lateInitializer = new LateInitializerConfig();
    private IdempotencyConfig idempotency = new IdempotencyConfig();

    public Map<String, Object> getDefaultEnvironment() {
        return defaultEnvironment;
    }

    public void setDefaultEnvironment(Map<String, Object> defaultEnvironment) {
        this.defaultEnvironment = defaultEnvironment;
    }

    public boolean isAutoCreateTopics() {
        return autoCreateTopics;
    }

    public void setAutoCreateTopics(boolean autoCreateTopics) {
        this.autoCreateTopics = autoCreateTopics;
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

    public List<ConsumerConfig> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<ConsumerConfig> consumers) {
        this.consumers = consumers;
    }

    public List<ProducerConfig> getProducers() {
        return producers;
    }

    public void setProducers(List<ProducerConfig> producers) {
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
