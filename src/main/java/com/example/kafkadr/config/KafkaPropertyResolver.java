package com.example.kafkadr.config;

import java.util.Map;
import java.util.Properties;

/**
 * Centralized Kafka client property resolution.
 *
 * <p>Builds a {@link Properties} object by merging configuration layers in order
 * (each layer overrides the previous):
 * <ol>
 *   <li>{@code default-environment.spring.cloud.stream.kafka.binder.configuration} — base (SSL, timeouts, etc.)</li>
 *   <li>Per-cluster {@code properties.configuration} — cluster-specific overrides</li>
 *   <li>Per-consumer/producer {@code default-*-properties.configuration} — role defaults</li>
 *   <li>Per-topic {@code properties.configuration} — topic-specific overrides</li>
 * </ol>
 *
 * <p>This ensures that SSL, SASL, and any standard Kafka client properties propagate
 * consistently to <b>all</b> Kafka clients: producers, consumers, AdminClients (health checks,
 * probes, topic provisioning).
 */
public final class KafkaPropertyResolver {

    private KafkaPropertyResolver() {
    }

    /**
     * Build base properties from default-environment + per-cluster overrides.
     * Suitable for AdminClient (health checks, probes, topic provisioning).
     */
    public static Properties resolveBaseProperties(KafkaDrConfig config, String clusterName) {
        Properties props = new Properties();

        // Layer 1: default-environment
        applyDefaultEnvironment(props, config);

        // Layer 2: per-cluster overrides
        if (clusterName != null) {
            ClusterConfig cc = config.getClusters().get(clusterName);
            if (cc != null) {
                applyConfiguration(props, cc.getProperties());
            }
        }

        return props;
    }

    /**
     * Build consumer properties: base + default-consumer-properties + per-topic overrides.
     */
    public static Properties resolveConsumerProperties(KafkaDrConfig config, String clusterName,
                                                       ConsumerConfig consumerConfig) {
        Properties props = resolveBaseProperties(config, clusterName);

        // Layer 3: default-consumer-properties
        applyConfiguration(props, config.getDefaultConsumerProperties());

        // Layer 4: per-topic consumer overrides
        if (consumerConfig != null) {
            applyConfiguration(props, consumerConfig.getProperties());
        }

        return props;
    }

    /**
     * Build producer properties: base + default-producer-properties + per-topic overrides.
     */
    public static Properties resolveProducerProperties(KafkaDrConfig config, String clusterName,
                                                       ProducerConfig producerConfig) {
        Properties props = resolveBaseProperties(config, clusterName);

        // Layer 3: default-producer-properties
        applyConfiguration(props, config.getDefaultProducerProperties());

        // Layer 4: per-topic producer overrides
        if (producerConfig != null) {
            applyConfiguration(props, producerConfig.getProperties());
        }

        return props;
    }

    // ─── internal ───────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void applyDefaultEnvironment(Properties props, KafkaDrConfig config) {
        Map<String, Object> defaultEnv = config.getDefaultEnvironment();
        if (defaultEnv == null) return;

        Object binder = getNestedValue(defaultEnv, "spring.cloud.stream.kafka.binder");
        if (!(binder instanceof Map)) return;

        Map<String, Object> binderMap = (Map<String, Object>) binder;
        Object configuration = binderMap.get("configuration");
        if (configuration instanceof Map) {
            ((Map<String, Object>) configuration).forEach((k, v) -> props.put(k, String.valueOf(v)));
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyConfiguration(Properties props, Map<String, Object> propertiesMap) {
        if (propertiesMap == null) return;

        Object configuration = propertiesMap.get("configuration");
        if (configuration instanceof Map) {
            ((Map<String, Object>) configuration).forEach((k, v) -> props.put(k, String.valueOf(v)));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object getNestedValue(Map<String, Object> map, String dottedKey) {
        String[] keys = dottedKey.split("\\.");
        Object current = map;
        for (String key : keys) {
            if (current instanceof Map) {
                current = ((Map<String, Object>) current).get(key);
            } else {
                return null;
            }
        }
        return current;
    }
}
