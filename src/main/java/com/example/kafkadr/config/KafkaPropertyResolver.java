package com.example.kafkadr.config;

import java.util.Map;
import java.util.Properties;

/**
 * Centralized Kafka client property resolution.
 *
 * <p>Builds a {@link Properties} object by merging configuration layers in order
 * (each layer overrides the previous):
 * <ol>
 *   <li>{@code default-properties.configuration} — base (SSL, SASL, timeouts, Schema Registry)</li>
 *   <li>Per-cluster {@code properties.configuration} — cluster-specific overrides</li>
 *   <li>{@code default-consumer/producer-properties.configuration} — role-specific defaults</li>
 *   <li>Per-topic {@code properties.configuration} — topic-specific overrides</li>
 * </ol>
 *
 * <p>All keys under {@code configuration:} are standard Kafka client property names
 * (e.g. {@code security.protocol}, {@code ssl.truststore.location}, {@code acks}).
 */
public final class KafkaPropertyResolver {

    private KafkaPropertyResolver() {
    }

    /**
     * Build base properties from default-properties + per-cluster overrides.
     * Used for AdminClient (health checks, probes, topic provisioning).
     */
    public static Properties resolveBaseProperties(KafkaDrConfig config, String clusterName) {
        Properties props = new Properties();

        // Layer 1: default-properties
        applyConfiguration(props, config.getDefaultProperties());

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
                                                       DrConsumerConfig drConsumerConfig) {
        Properties props = resolveBaseProperties(config, clusterName);

        // Layer 3: default-consumer-properties
        applyConfiguration(props, config.getDefaultConsumerProperties());

        // Layer 4: per-topic consumer overrides
        if (drConsumerConfig != null) {
            applyConfiguration(props, drConsumerConfig.getProperties());
        }

        return props;
    }

    /**
     * Build producer properties: base + default-producer-properties + per-topic overrides.
     */
    public static Properties resolveProducerProperties(KafkaDrConfig config, String clusterName,
                                                       DrProducerConfig drProducerConfig) {
        Properties props = resolveBaseProperties(config, clusterName);

        // Layer 3: default-producer-properties
        applyConfiguration(props, config.getDefaultProducerProperties());

        // Layer 4: per-topic producer overrides
        if (drProducerConfig != null) {
            applyConfiguration(props, drProducerConfig.getProperties());
        }

        return props;
    }

    // ─── internal ───────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void applyConfiguration(Properties props, Map<String, Object> propertiesMap) {
        if (propertiesMap == null) return;

        Object configuration = propertiesMap.get("configuration");
        if (configuration instanceof Map) {
            ((Map<String, Object>) configuration).forEach((k, v) -> props.put(k, String.valueOf(v)));
        }
    }
}
