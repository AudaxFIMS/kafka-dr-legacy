package com.example.kafkadr.example;

import com.example.kafkadr.consumer.DrConsumerFactory;
import com.example.kafkadr.consumer.DrKafkaConsumer;
import com.example.kafkadr.idempotency.IdempotencyKeys;
import com.example.kafkadr.idempotency.IdempotencyStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Example of an application-owned consumer loop running on top of DR.
 *
 * <p>Same shape as a plain KafkaConsumer loop ({@code subscribe → poll → process → commit}),
 * but the consumer comes from {@link DrConsumerFactory}, so it follows the active cluster:
 * on failover the poll that performs the switch returns empty records and reading
 * continues on the new cluster.
 *
 * <p>Semantics:
 * <ul>
 *   <li>At-least-once with deduplication: records already processed (e.g. re-read on the
 *       failover cluster due to offset-sync lag) are skipped via {@link IdempotencyStore}.</li>
 *   <li>Success → {@code markProcessed} → commit {@code offset + 1} of that record only.</li>
 *   <li>Failure → record is skipped (logged); the offset moves past it with the next
 *       successful commit in the same partition.</li>
 * </ul>
 *
 * <pre>
 * ExampleDrConsumer consumer = new ExampleDrConsumer(
 *         app.getConsumerFactory(), app.getIdempotencyStore(), "idempotency",
 *         "order-events", "legacy-order-group", new Properties());
 * consumer.start();
 * ...
 * consumer.close();
 * </pre>
 */
public class ExampleDrConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExampleDrConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final DrConsumerFactory consumerFactory;
    private final IdempotencyStore idempotencyStore;
    private final String keyPrefix;
    private final String topic;
    private final Properties props;

    private volatile boolean running;
    private volatile DrKafkaConsumer<String, String> consumer;
    private Thread thread;

    /**
     * @param extraProps additional consumer properties (max.poll.records, isolation.level, ...).
     *                   bootstrap.servers and SSL/SASL are taken from kafka-dr.yml.
     */
    public ExampleDrConsumer(DrConsumerFactory consumerFactory, IdempotencyStore idempotencyStore,
                             String keyPrefix, String topic, String groupId, Properties extraProps) {
        this.consumerFactory = consumerFactory;
        this.idempotencyStore = idempotencyStore;
        this.keyPrefix = keyPrefix;
        this.topic = topic;

        this.props = new Properties();
        props.putAll(extraProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Class names, not instances — the consumer is recreated on every cluster switch
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    public void start() {
        running = true;
        thread = new Thread(this, "dr-consumer-" + topic);
        thread.start();
    }

    @Override
    public void run() {
        try (DrKafkaConsumer<String, String> c = consumerFactory.create(props)) {
            consumer = c;
            c.subscribe(List.of(topic));
            log.info("ExampleDrConsumer started: topic={}", topic);

            while (running) {
                ConsumerRecords<String, String> records = c.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(c, record);
                }
            }
        } catch (WakeupException e) {
            if (running) throw e; // unexpected wakeup, not a shutdown
        } catch (Exception e) {
            log.error("ExampleDrConsumer crashed: topic={}", topic, e);
        } finally {
            consumer = null;
            log.info("ExampleDrConsumer stopped: topic={}", topic);
        }
    }

    private void handleRecord(DrKafkaConsumer<String, String> c, ConsumerRecord<String, String> record) {
        String dedupKey = IdempotencyKeys.of(keyPrefix, record);
        if (dedupKey != null && idempotencyStore.isProcessed(dedupKey)) {
            log.debug("Skipping already processed record: {}", dedupKey);
            return;
        }

        boolean success;
        try {
            success = process(record);
        } catch (Exception e) {
            log.error("Processing failed, skipping: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
            success = false;
        }

        if (success) {
            if (dedupKey != null) {
                idempotencyStore.markProcessed(dedupKey);
            }
            c.commitAsync(Map.of(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)), null);
        }
        // failure: skipped — the next successful commit in this partition moves past it
    }

    /**
     * Business logic. Return {@code true} on success; {@code false} or an exception skips the record.
     */
    protected boolean process(ConsumerRecord<String, String> record) {
        log.info("[ExampleDrConsumer] topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        return true;
    }

    @Override
    public void close() {
        running = false;
        DrKafkaConsumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
