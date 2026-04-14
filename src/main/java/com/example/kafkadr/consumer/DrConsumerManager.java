package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterSwitchListener;
import com.example.kafkadr.config.DrConsumerConfig;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.config.KafkaPropertyResolver;
import com.example.kafkadr.handler.HandlerTypeResolver;
import com.example.kafkadr.handler.MessageHandler;
import com.example.kafkadr.handler.MessageHandlerRegistry;
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

    private final List<ConsumerWorker> activeWorkers = new CopyOnWriteArrayList<>();
    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DrConsumerManager(KafkaDrConfig config, MessageHandlerRegistry handlerRegistry,
                             IdempotencyStore idempotencyStore) {
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
            Properties props = buildConsumerProperties(cluster, drConsumerConfig);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(drConsumerConfig.getTopic()));

            MessageHandler<?, ?> handler = handlerRegistry.getHandler(drConsumerConfig.getHandler());
            HandlerTypeResolver.ResolvedTypes types = handlerRegistry.getHandlerTypes(drConsumerConfig.getHandler());

            log.info("Consumer worker started: topic={}, key={}<{}>, value={}<{}>, cluster={}",
                    drConsumerConfig.getTopic(),
                    keyContentType, types.getKeyType().getSimpleName(),
                    valueContentType, types.getValueType().getSimpleName(),
                    cluster.getName());

            try {
                while (!stopped.get() && running.get()) {
                    try {
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));
                        if (records.isEmpty()) continue;

                        String keyPrefix = config.getIdempotency().getKeyPrefix();
                        records.forEach(record -> {
                            String msgKey = record.key() != null ? record.key().toString() : "";
                            String idempotencyKey = keyPrefix + ":" + record.topic() + ":" + msgKey;

                            if (idempotencyStore.isDuplicate(idempotencyKey)) {
                                log.debug("Skipping duplicate: topic={}, partition={}, offset={}",
                                        record.topic(), record.partition(), record.offset());
                                return;
                            }

                            try {
                                Object key = MessageDeserializer.deserialize(
                                        record.key(), keyContentType, types.getKeyType(),
                                        record.topic(), record.partition(), record.offset());
                                Object value = MessageDeserializer.deserialize(
                                        record.value(), valueContentType, types.getValueType(),
                                        record.topic(), record.partition(), record.offset());

                                dispatchTyped(handler, record, key, value);
                                idempotencyStore.markProcessed(idempotencyKey);
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
