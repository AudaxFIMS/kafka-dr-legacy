package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterSwitchListener;
import com.example.kafkadr.config.ConsumerConfig;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.config.KafkaPropertyResolver;
import com.example.kafkadr.handler.MessageEnvelope;
import com.example.kafkadr.handler.MessageHandler;
import com.example.kafkadr.handler.MessageHandlerRegistry;
import com.example.kafkadr.idempotency.InMemoryIdempotencyStore;
import com.example.kafkadr.serialization.ContentType;
import com.example.kafkadr.serialization.MessageDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages Kafka consumers across DR cluster switches.
 * On cluster switch, all existing consumers are closed and recreated
 * for the new active cluster.
 */
public class DrConsumerManager implements ClusterSwitchListener {

    private static final Logger log = LoggerFactory.getLogger(DrConsumerManager.class);
    private static final String IDEMPOTENCY_KEY_HEADER = "x-idempotency-key";

    private final KafkaDrConfig config;
    private final MessageHandlerRegistry handlerRegistry;
    private final InMemoryIdempotencyStore idempotencyStore;

    private final List<ConsumerWorker> activeWorkers = new CopyOnWriteArrayList<>();
    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DrConsumerManager(KafkaDrConfig config, MessageHandlerRegistry handlerRegistry,
                             InMemoryIdempotencyStore idempotencyStore) {
        this.config = config;
        this.handlerRegistry = handlerRegistry;
        this.idempotencyStore = idempotencyStore;
    }

    @Override
    public void onClusterSwitch(ClusterInfo previousCluster, ClusterInfo newCluster) {
        log.info("Consumer cluster switch: {} -> {}",
                previousCluster != null ? previousCluster.getName() : "none",
                newCluster.getName());
        stopConsumers();
        startConsumers(newCluster);
    }

    public void startConsumers(ClusterInfo cluster) {
        if (config.getConsumers() == null || config.getConsumers().isEmpty()) {
            log.info("No consumers configured");
            return;
        }

        running.set(true);
        executorService = Executors.newFixedThreadPool(config.getConsumers().size(), r -> {
            Thread t = new Thread(r, "kafka-consumer-" + Thread.activeCount());
            t.setDaemon(true);
            return t;
        });

        for (ConsumerConfig consumerConfig : config.getConsumers()) {
            ConsumerWorker worker = new ConsumerWorker(cluster, consumerConfig);
            activeWorkers.add(worker);
            executorService.submit(worker);
            log.info("Started consumer for topic='{}', group='{}', handler='{}', content-type='{}' on cluster '{}'",
                    consumerConfig.getTopic(), consumerConfig.getGroup(),
                    consumerConfig.getHandler(), consumerConfig.getContentType(),
                    cluster.getName());
        }
    }

    public void stopConsumers() {
        running.set(false);

        for (ConsumerWorker worker : activeWorkers) {
            worker.shutdown();
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        activeWorkers.clear();
        log.info("All consumers stopped");
    }

    private Properties buildConsumerProperties(ClusterInfo cluster, ConsumerConfig consumerConfig) {
        // Resolve full property chain: default-env -> per-cluster -> default-consumer -> per-topic
        Properties props = KafkaPropertyResolver.resolveConsumerProperties(
                config, cluster.getName(), consumerConfig);

        // Consumer-specific fixed settings
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                cluster.getBootstrapServers());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,
                consumerConfig.getGroup());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ContentType contentType = ContentType.fromString(consumerConfig.getContentType());

        if (contentType != ContentType.NATIVE) {
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        } else {
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        return props;
    }

    /**
     * Internal worker that runs a single KafkaConsumer in a poll loop.
     * Resolves the handler's generic type T via reflection at startup,
     * then deserializes each record into T before dispatching.
     */
    private class ConsumerWorker implements Runnable {

        private final ClusterInfo cluster;
        private final ConsumerConfig consumerConfig;
        private final ContentType contentType;
        private volatile KafkaConsumer<?, ?> consumer;
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        ConsumerWorker(ClusterInfo cluster, ConsumerConfig consumerConfig) {
            this.cluster = cluster;
            this.consumerConfig = consumerConfig;
            this.contentType = ContentType.fromString(consumerConfig.getContentType());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Properties props = buildConsumerProperties(cluster, consumerConfig);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(consumerConfig.getTopic()));

            MessageHandler<?> handler = handlerRegistry.getHandler(consumerConfig.getHandler());
            Class<?> targetType = handlerRegistry.getHandlerType(consumerConfig.getHandler());

            log.info("Consumer worker started: topic={}, contentType={}, targetType={}, cluster={}",
                    consumerConfig.getTopic(), contentType, targetType.getSimpleName(), cluster.getName());

            try {
                while (!stopped.get() && running.get()) {
                    try {
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));
                        if (records.isEmpty()) continue;

                        records.forEach(record -> {
                            String idempotencyKey = extractIdempotencyKey(record);
                            if (idempotencyKey == null) {
                                idempotencyKey = idempotencyStore.buildKey(
                                        record.topic(), record.partition(), record.offset());
                            } else {
                                idempotencyKey = idempotencyStore.buildKey(idempotencyKey);
                            }

                            if (idempotencyStore.isDuplicate(idempotencyKey)) {
                                log.debug("Skipping duplicate: topic={}, partition={}, offset={}",
                                        record.topic(), record.partition(), record.offset());
                                return;
                            }

                            try {
                                // Deserialize using content-type + handler's resolved generic type T
                                Object deserialized = MessageDeserializer.deserialize(record, contentType, targetType);
                                dispatchTyped(handler, record, deserialized, cluster.getName());
                                idempotencyStore.markProcessed(idempotencyKey);
                            } catch (Exception e) {
                                log.error("Error processing message: topic={}, partition={}, offset={}",
                                        record.topic(), record.partition(), record.offset(), e);
                            }
                        });

                        consumer.commitSync();
                    } catch (WakeupException e) {
                        if (!stopped.get()) {
                            log.warn("Consumer wakeup without shutdown signal, topic={}", consumerConfig.getTopic());
                        }
                    }
                }
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    log.warn("Error closing consumer for topic={}", consumerConfig.getTopic(), e);
                }
                log.info("Consumer worker stopped: topic={}, cluster={}", consumerConfig.getTopic(), cluster.getName());
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private void dispatchTyped(MessageHandler handler,
                                   org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                   Object deserialized, String clusterName) {
            MessageEnvelope envelope = new MessageEnvelope(
                    record.topic(), record.key(), deserialized,
                    record.partition(), record.offset(),
                    clusterName, contentType);
            handler.handle(envelope);
        }

        void shutdown() {
            stopped.set(true);
            if (consumer != null) {
                consumer.wakeup();
            }
        }

        private String extractIdempotencyKey(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record) {
            Header header = record.headers().lastHeader(IDEMPOTENCY_KEY_HEADER);
            if (header != null && header.value() != null) {
                return new String(header.value());
            }
            return null;
        }
    }
}
