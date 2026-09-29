package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterSwitchListener;
import com.example.kafkadr.config.DrConsumerConfig;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.config.KafkaPropertyResolver;
import com.example.kafkadr.handler.HandlerTypeResolver;
import com.example.kafkadr.handler.MessageHandler;
import com.example.kafkadr.handler.MessageHandlerRegistry;
import com.example.kafkadr.idempotency.IdempotencyKeys;
import com.example.kafkadr.idempotency.IdempotencyStore;
import com.example.kafkadr.serialization.ContentType;
import com.example.kafkadr.serialization.MessageDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
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
    private final KafkaDrConfig config;
    private final MessageHandlerRegistry handlerRegistry;
    private final IdempotencyStore idempotencyStore;
    /** group.instance.id per consumer config; claimed once, kept across cluster switches. */
    private final Map<DrConsumerConfig, String> groupInstanceIds = new IdentityHashMap<>();

    private final List<ConsumerWorker> activeWorkers = new CopyOnWriteArrayList<>();
    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DrConsumerManager(KafkaDrConfig config, MessageHandlerRegistry handlerRegistry,
                             IdempotencyStore idempotencyStore) {
        this(config, handlerRegistry, idempotencyStore, new GroupInstanceIds(config));
    }

    public DrConsumerManager(KafkaDrConfig config, MessageHandlerRegistry handlerRegistry,
                             IdempotencyStore idempotencyStore, GroupInstanceIds instanceIds) {
        this.config = config;
        this.handlerRegistry = handlerRegistry;
        this.idempotencyStore = idempotencyStore;
        if (config.getConsumers() != null) {
            for (DrConsumerConfig cc : config.getConsumers()) {
                String id = instanceIds.claim(cc.getGroup(), List.of(cc.getTopic()));
                if (id != null) {
                    groupInstanceIds.put(cc, id);
                }
            }
        }
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

        for (DrConsumerConfig drConsumerConfig : config.getConsumers()) {
            ConsumerWorker worker = new ConsumerWorker(cluster, drConsumerConfig);
            activeWorkers.add(worker);
            executorService.submit(worker);
            log.info("Started consumer for topic='{}', group='{}', handler='{}', content-type='{}' on cluster '{}'",
                    drConsumerConfig.getTopic(), drConsumerConfig.getGroup(),
                    drConsumerConfig.getHandler(), drConsumerConfig.getContentType(),
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

    private Properties buildConsumerProperties(ClusterInfo cluster, DrConsumerConfig drConsumerConfig) {
        Properties props = KafkaPropertyResolver.resolveConsumerProperties(
                config, cluster.getName(), drConsumerConfig);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                cluster.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                drConsumerConfig.getGroup());
        // Static membership: an explicit group.instance.id from YAML wins
        String instanceId = groupInstanceIds.get(drConsumerConfig);
        if (instanceId != null && !props.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ContentType keyType = ContentType.fromString(drConsumerConfig.getKeyContentType());
        ContentType valueType = ContentType.fromString(drConsumerConfig.getContentType());

        // Key deserializer: use ByteArray for non-native, let Kafka handle native
        if (keyType != ContentType.NATIVE) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }
        // Value deserializer: use ByteArray for non-native, let Kafka handle native
        if (valueType != ContentType.NATIVE) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

        return props;
    }

    /**
     * Internal worker that runs a single KafkaConsumer in a poll loop.
     * Deserializes both key (K) and value (V) into the types declared
     * by the handler's generic parameters, then dispatches a typed ConsumerRecord.
     */
    private class ConsumerWorker implements Runnable {

        private final ClusterInfo cluster;
        private final DrConsumerConfig drConsumerConfig;
        private final ContentType keyContentType;
        private final ContentType valueContentType;
        private volatile KafkaConsumer<?, ?> consumer;
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        ConsumerWorker(ClusterInfo cluster, DrConsumerConfig drConsumerConfig) {
            this.cluster = cluster;
            this.drConsumerConfig = drConsumerConfig;
            this.keyContentType = ContentType.fromString(drConsumerConfig.getKeyContentType());
            this.valueContentType = ContentType.fromString(drConsumerConfig.getContentType());
        }

        @Override
        public void run() {
            log.info("Consumer worker creating KafkaConsumer: topic={}, cluster={}",
                    drConsumerConfig.getTopic(), cluster.getName());

            Properties props = buildConsumerProperties(cluster, drConsumerConfig);
            try {
                consumer = new KafkaConsumer<>(props);
            } catch (Exception e) {
                log.error("Failed to create KafkaConsumer: topic={}, cluster={}: {}",
                        drConsumerConfig.getTopic(), cluster.getName(), e.getMessage(), e);
                return;
            }
            consumer.subscribe(Collections.singletonList(drConsumerConfig.getTopic()));

            MessageHandler<?, ?> handler = handlerRegistry.getHandler(drConsumerConfig.getHandler());
            HandlerTypeResolver.ResolvedTypes types = handlerRegistry.getHandlerTypes(drConsumerConfig.getHandler());

            log.info("Consumer worker started: topic={}, key={}<{}>, value={}<{}>, group={}, cluster={}",
                    drConsumerConfig.getTopic(),
                    keyContentType, types.getKeyType().getSimpleName(),
                    valueContentType, types.getValueType().getSimpleName(),
                    drConsumerConfig.getGroup(), cluster.getName());

            try {
                boolean assignmentLogged = false;
                int pollCount = 0;
                while (!stopped.get() && running.get()) {
                    try {
                        pollCount++;
                        if (pollCount <= 5 || pollCount % 60 == 0) {
                            log.debug("Consumer poll #{}: topic={}, cluster={}",
                                    pollCount, drConsumerConfig.getTopic(), cluster.getName());
                        }

                        long pollStart = System.currentTimeMillis();
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));
                        long pollMs = System.currentTimeMillis() - pollStart;

                        // Warn if poll took much longer than expected (GC stall, network hang)
                        if (pollMs > 5000) {
                            log.warn("Consumer poll took {}ms (expected ~1000ms): topic={}, cluster={}. " +
                                            "Possible GC pressure or network issue.",
                                    pollMs, drConsumerConfig.getTopic(), cluster.getName());
                        }

                        // Log partition assignment once after first successful poll
                        if (!assignmentLogged) {
                            var assignment = consumer.assignment();
                            if (!assignment.isEmpty()) {
                                log.info("Consumer partition assignment: topic={}, partitions={}, cluster={}",
                                        drConsumerConfig.getTopic(), assignment, cluster.getName());
                                assignmentLogged = true;
                            } else {
                                log.warn("Consumer has NO partition assignment after poll #{}: topic={}, group={}, cluster={}",
                                        pollCount, drConsumerConfig.getTopic(),
                                        drConsumerConfig.getGroup(), cluster.getName());
                            }
                        }

                        if (records.isEmpty()) continue;

                        String keyPrefix = config.getIdempotency().getKeyPrefix();
                        records.forEach(record -> {
                            try {
                                // Deserialize key first — used for the idempotency key fallback
                                Object key = MessageDeserializer.deserialize(
                                        record.key(), keyContentType, types.getKeyType(),
                                        record.topic(), record.partition(), record.offset());

                                // message-id header, else record key; null → cannot deduplicate
                                String idempotencyKey = IdempotencyKeys.of(
                                        keyPrefix, record.topic(), record.headers(), key);

                                if (idempotencyKey != null && idempotencyStore.isProcessed(idempotencyKey)) {
                                    log.debug("Skipping duplicate: topic={}, partition={}, offset={}, idempotencyKey={}",
                                            record.topic(), record.partition(), record.offset(), idempotencyKey);
                                    return;
                                }

                                Object value = MessageDeserializer.deserialize(
                                        record.value(), valueContentType, types.getValueType(),
                                        record.topic(), record.partition(), record.offset());

                                dispatchTyped(handler, record, key, value);

                                // Mark only after successful handling — a failed record is not treated as processed
                                if (idempotencyKey != null) {
                                    idempotencyStore.markProcessed(idempotencyKey);
                                }
                            } catch (Exception e) {
                                log.error("Error processing message: topic={}, partition={}, offset={}",
                                        record.topic(), record.partition(), record.offset(), e);
                            }
                        });

                        consumer.commitSync();
                    } catch (WakeupException e) {
                        if (!stopped.get()) {
                            log.warn("Consumer wakeup without shutdown signal, topic={}", drConsumerConfig.getTopic());
                        }
                    }
                }
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    log.warn("Error closing consumer for topic={}", drConsumerConfig.getTopic(), e);
                }
                log.info("Consumer worker stopped: topic={}, cluster={}", drConsumerConfig.getTopic(), cluster.getName());
            }
        }

        /**
         * Build a typed ConsumerRecord&lt;K, V&gt; with deserialized key and value,
         * preserving all original metadata (headers, timestamp, partition, offset).
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        private void dispatchTyped(MessageHandler handler,
                                   ConsumerRecord<?, ?> raw,
                                   Object key, Object value) {
            ConsumerRecord typedRecord = new ConsumerRecord(
                    raw.topic(), raw.partition(), raw.offset(),
                    raw.timestamp(), raw.timestampType(),
                    raw.serializedKeySize(), raw.serializedValueSize(),
                    key, value,
                    raw.headers(), raw.leaderEpoch());
            handler.handle(typedRecord);
        }

        void shutdown() {
            stopped.set(true);
            if (consumer != null) {
                consumer.wakeup();
            }
        }

    }
}
