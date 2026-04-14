package com.example.kafkadr.producer;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.cluster.ClusterSwitchListener;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.config.KafkaPropertyResolver;
import com.example.kafkadr.config.DrProducerConfig;
import com.example.kafkadr.serialization.ContentType;
import com.example.kafkadr.serialization.MessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Resilient Kafka producer with error classification and instant failover.
 *
 * <p>Error handling strategy:
 * <ul>
 *   <li><b>Serialization errors</b> — fatal, no retry, no failover (application bug)</li>
 *   <li><b>Cluster unavailable</b> — instant {@code forceUnhealthy} + failover</li>
 *   <li><b>Transient errors</b> — retry up to {@code failureThreshold} times, then failover</li>
 * </ul>
 */
public class DrProducerManager implements ClusterSwitchListener {

    private static final Logger log = LoggerFactory.getLogger(DrProducerManager.class);

    private final KafkaDrConfig config;
    private final ClusterManager clusterManager;
    private final Map<String, KafkaProducer<String, Object>> producers = new ConcurrentHashMap<>();
    private final Map<String, DrProducerConfig> producerConfigs = new ConcurrentHashMap<>();
    private final int maxRetries;
    private volatile ClusterInfo currentCluster;

    public DrProducerManager(KafkaDrConfig config, ClusterManager clusterManager) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.maxRetries = config.getHealthCheck().getFailureThreshold();
        if (config.getProducers() != null) {
            for (DrProducerConfig pc : config.getProducers()) {
                producerConfigs.put(pc.getTopic(), pc);
            }
        }
    }

    @Override
    public void onClusterSwitch(ClusterInfo previousCluster, ClusterInfo newCluster) {
        log.info("Producer cluster switch: {} -> {}",
                previousCluster != null ? previousCluster.getName() : "none",
                newCluster.getName());
        closeAllProducers();
        this.currentCluster = newCluster;
        createAllProducers(newCluster);
    }

    /**
     * Send a message with resilient error handling.
     * On cluster-unavailable errors, triggers instant failover via ClusterManager.
     */
    public RecordMetadata send(String topic, String key, Object value) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            ClusterInfo cluster = currentCluster;
            if (cluster == null) {
                throw new IllegalStateException("No active cluster available");
            }

            KafkaProducer<String, Object> producer = producers.get(topic);
            if (producer == null) {
                throw new IllegalStateException("No producer for topic: " + topic +
                        ". Active cluster: " + cluster.getName());
            }

            DrProducerConfig pc = producerConfigs.get(topic);
            ContentType contentType = pc != null ? ContentType.fromString(pc.getContentType()) : ContentType.STRING;
            Object serializedValue = MessageSerializer.serialize(value, contentType);

            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, serializedValue);

            try {
                Future<RecordMetadata> future = producer.send(record);
                if (isSyncMode()) {
                    RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
                    log.debug("Message sent to {}-{} offset={} cluster={}",
                            metadata.topic(), metadata.partition(), metadata.offset(), cluster.getName());
                    return metadata;
                }
                return null; // async
            } catch (Exception e) {
                Throwable root = unwrap(e);

                // 1. Serialization error — fatal, don't retry or failover
                if (isSerializationError(root)) {
                    log.error("Serialization error on topic={}, skipping (application bug): {}",
                            topic, root.getMessage());
                    throw new RuntimeException("Serialization error for topic: " + topic, root);
                }

                // 2. Cluster unavailable — instant failover
                if (isClusterUnavailable(root)) {
                    log.warn("Cluster '{}' unavailable on send to topic={}: {}. Triggering instant failover.",
                            cluster.getName(), topic, root.getClass().getSimpleName());
                    clusterManager.forceUnhealthy(cluster.getName());
                    // After forceUnhealthy, currentCluster may have changed — retry with new cluster
                    if (currentCluster != null && !currentCluster.getName().equals(cluster.getName())) {
                        producer = producers.get(topic);
                        continue; // retry on new cluster
                    }
                    throw new RuntimeException("Cluster unavailable and no failover target for topic: " + topic, root);
                }

                // 3. Transient error — retry within same cluster
                log.warn("Transient send error on topic={}, cluster={}, attempt {}/{}: {}",
                        topic, cluster.getName(), attempt, maxRetries, root.getMessage());
                if (attempt >= maxRetries) {
                    log.error("Retries exhausted for topic={} on cluster '{}'. Triggering failover.",
                            topic, cluster.getName());
                    clusterManager.forceUnhealthy(cluster.getName());
                    throw new RuntimeException("Retries exhausted for topic: " + topic, root);
                }
            }
        }
        throw new IllegalStateException("Send loop exited unexpectedly for topic: " + topic);
    }

    public ClusterInfo getCurrentCluster() {
        return currentCluster;
    }

    public void stop() {
        closeAllProducers();
    }

    // ─── Error classification ───────────────────────────────────

    private boolean isSerializationError(Throwable e) {
        while (e != null) {
            if (e instanceof SerializationException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private boolean isClusterUnavailable(Throwable e) {
        while (e != null) {
            if (e instanceof TimeoutException
                    || e instanceof NetworkException
                    || e instanceof DisconnectException
                    || e instanceof BrokerNotAvailableException
                    || e instanceof NotLeaderOrFollowerException
                    || e instanceof java.net.ConnectException) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private Throwable unwrap(Throwable e) {
        while (e.getCause() != null && e != e.getCause()) {
            e = e.getCause();
        }
        return e;
    }

    // ─── Producer lifecycle ─────────────────────────────────────

    private void createAllProducers(ClusterInfo cluster) {
        for (DrProducerConfig pc : producerConfigs.values()) {
            Properties props = buildProducerProperties(cluster, pc);
            KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
            producers.put(pc.getTopic(), producer);
            log.info("Created producer for topic='{}' on cluster '{}'", pc.getTopic(), cluster.getName());
        }
    }

    private void closeAllProducers() {
        producers.forEach((topic, producer) -> {
            try {
                producer.flush();
                producer.close(Duration.ofSeconds(5));
                log.info("Closed producer for topic='{}'", topic);
            } catch (Exception e) {
                log.warn("Error closing producer for topic='{}'", topic, e);
            }
        });
        producers.clear();
    }

    private Properties buildProducerProperties(ClusterInfo cluster, DrProducerConfig drProducerConfig) {
        // Resolve full property chain: default-properties -> per-cluster -> default-producer -> per-topic
        Properties props = KafkaPropertyResolver.resolveProducerProperties(
                config, cluster.getName(), drProducerConfig);

        // Producer-specific fixed settings
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                cluster.getBootstrapServers());

        ContentType contentType = ContentType.fromString(drProducerConfig.getContentType());

        if (contentType != ContentType.NATIVE) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            if (contentType == ContentType.BYTES) {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");
            }
        } else {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
        }

        return props;
    }

    private boolean isSyncMode() {
        Map<String, Object> defaultProducerProps = config.getDefaultProducerProperties();
        if (defaultProducerProps != null && defaultProducerProps.containsKey("sync")) {
            return Boolean.TRUE.equals(defaultProducerProps.get("sync"));
        }
        return false;
    }
}
