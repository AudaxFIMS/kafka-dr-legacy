package com.example.kafkadr.example;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.IdempotencyConfig;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.consumer.DrConsumerFactory;
import com.example.kafkadr.consumer.GroupInstanceIds;
import com.example.kafkadr.idempotency.IdempotencyKeys;
import com.example.kafkadr.idempotency.IdempotencyStore;
import com.example.kafkadr.idempotency.InMemoryIdempotencyStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class ExampleDrConsumerTest {

    private static final String TOPIC = "orders";
    private static final String GROUP = "example-group";
    private static final String PREFIX = "ex";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private KafkaDrConfig config;
    private ClusterManager manager;
    private IdempotencyStore store;
    private final List<TrackingMockConsumer> created = new CopyOnWriteArrayList<>();
    private final List<Properties> createdProps = new CopyOnWriteArrayList<>();
    private RecordingConsumer consumer;

    @BeforeEach
    void setUp() {
        config = ConfigLoader.load("kafka-dr.yml");
        config.setInstanceId("test-host");
        manager = new ClusterManager(config, Runnable::run); // synchronous switch delivery
        store = new InMemoryIdempotencyStore(new IdempotencyConfig());
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close();
        store.stop();
    }

    @Test
    void shouldProcessMarkAndCommitOffsetPlusOne() {
        manager.reportHealthy("primary");
        startConsumer();
        TrackingMockConsumer primary = awaitConsumer(0);

        assignFrom(primary, 0L);
        primary.addRecord(record(0, "m1", "hello"));

        await(() -> primary.lastAsyncCommit(TP) != null, "commit of record 0");
        assertEquals(List.of("hello"), consumer.processed);
        assertEquals(List.of(Map.of(TP, new OffsetAndMetadata(1L))), primary.asyncCommits);
        assertTrue(store.isProcessed(PREFIX + ":" + TOPIC + ":m1"));
    }

    @Test
    void shouldPassGroupAndDeserializersToConsumer() {
        manager.reportHealthy("primary");
        startConsumer();
        awaitConsumer(0);

        Properties props = createdProps.get(0);
        assertEquals(GROUP, props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("false", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("test-host-example-group-orders", props.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
        assertEquals("localhost:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer",
                props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void shouldSkipFailedRecordsWithoutMarkOrCommit() {
        manager.reportHealthy("primary");
        startConsumer();
        TrackingMockConsumer primary = awaitConsumer(0);

        assignFrom(primary, 0L);
        primary.addRecord(record(0, "m-bad", "fail"));    // process() returns false
        primary.addRecord(record(1, "m-boom", "throw"));  // process() throws
        primary.addRecord(record(2, "m-ok", "ok"));

        await(() -> consumer.attempts.size() == 3 && !primary.asyncCommits.isEmpty(), "all three records");
        assertEquals(List.of("fail", "throw", "ok"), consumer.attempts);
        assertEquals(List.of("ok"), consumer.processed);
        // Only the successful record is committed; the offset moves past the failed ones
        assertEquals(List.of(Map.of(TP, new OffsetAndMetadata(3L))), primary.asyncCommits);
        assertFalse(store.isProcessed(PREFIX + ":" + TOPIC + ":m-bad"));
        assertFalse(store.isProcessed(PREFIX + ":" + TOPIC + ":m-boom"));
        assertTrue(store.isProcessed(PREFIX + ":" + TOPIC + ":m-ok"));
    }

    @Test
    void shouldSkipAlreadyProcessedRecord() {
        store.markProcessed(PREFIX + ":" + TOPIC + ":m1");
        manager.reportHealthy("primary");
        startConsumer();
        TrackingMockConsumer primary = awaitConsumer(0);

        assignFrom(primary, 0L);
        primary.addRecord(record(0, "m1", "duplicate"));
        primary.addRecord(record(1, "m2", "new"));

        await(() -> !primary.asyncCommits.isEmpty(), "commit of record 1");
        assertEquals(List.of("new"), consumer.attempts);
        assertEquals(List.of(Map.of(TP, new OffsetAndMetadata(2L))), primary.asyncCommits);
    }

    @Test
    void shouldContinueOnSecondaryAfterFailoverAndSkipReplayedRecord() {
        manager.reportHealthy("primary");
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }
        startConsumer();
        TrackingMockConsumer primary = awaitConsumer(0);

        assignFrom(primary, 10L);
        primary.addRecord(record(10, "m1", "first"));
        await(() -> consumer.processed.size() == 1, "m1 on primary");

        manager.forceUnhealthy("primary");

        TrackingMockConsumer secondary = awaitConsumer(1);
        await(primary::closed, "primary consumer closed");
        assertEquals("localhost:9094", createdProps.get(1).get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

        // Replicated copy of m1 (different offset on secondary) + a new message
        assignFrom(secondary, 0L);
        secondary.addRecord(record(0, "m1", "first-replayed"));
        secondary.addRecord(record(1, "m3", "third"));

        await(() -> !secondary.asyncCommits.isEmpty(), "commit on secondary");
        assertEquals(List.of("first", "third"), consumer.processed);
        assertEquals(List.of(Map.of(TP, new OffsetAndMetadata(2L))), secondary.asyncCommits);
    }

    @Test
    void shouldIdleWithoutActiveClusterAndStartWhenElected() {
        startConsumer();
        sleep(100);
        assertTrue(created.isEmpty(), "no consumer before election");

        manager.reportHealthy("primary");

        awaitConsumer(0);
    }

    @Test
    void closeShouldStopLoopAndCloseConsumer() {
        manager.reportHealthy("primary");
        startConsumer();
        TrackingMockConsumer primary = awaitConsumer(0);

        consumer.close();

        assertTrue(primary.closed());
        consumer = null;
    }

    // ─── helpers ───────────────────────────────────────────────────

    private void startConsumer() {
        DrConsumerFactory factory = new DrConsumerFactory(manager, config, new GroupInstanceIds(config), props -> {
            TrackingMockConsumer mock = new TrackingMockConsumer();
            createdProps.add(props);
            created.add(mock);
            return mock;
        });
        consumer = new RecordingConsumer(factory, store);
        consumer.start();
    }

    private TrackingMockConsumer awaitConsumer(int index) {
        await(() -> created.size() > index, "consumer #" + index + " created");
        return created.get(index);
    }

    private static void assignFrom(TrackingMockConsumer mock, long beginningOffset) {
        // Offsets first: the consumer thread may poll right after the assignment
        mock.updateBeginningOffsets(Map.of(TP, beginningOffset));
        mock.rebalance(List.of(TP));
    }

    private static ConsumerRecord<String, String> record(long offset, String messageId, String value) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>(TOPIC, 0, offset, null, value);
        r.headers().add(IdempotencyKeys.MESSAGE_ID_HEADER, messageId.getBytes(StandardCharsets.UTF_8));
        return r;
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("Timed out waiting for: " + what);
            }
            sleep(5);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** ExampleDrConsumer with observable, controllable business logic. */
    private static class RecordingConsumer extends ExampleDrConsumer {
        final List<String> attempts = new CopyOnWriteArrayList<>();
        final List<String> processed = new CopyOnWriteArrayList<>();

        RecordingConsumer(DrConsumerFactory factory, IdempotencyStore store) {
            super(factory, store, PREFIX, TOPIC, GROUP, new Properties());
        }

        @Override
        protected boolean process(ConsumerRecord<String, String> record) {
            attempts.add(record.value());
            if ("fail".equals(record.value())) return false;
            if ("throw".equals(record.value())) throw new IllegalStateException("boom");
            processed.add(record.value());
            return true;
        }
    }

    private static class TrackingMockConsumer extends MockConsumer<String, String> {
        final List<Map<TopicPartition, OffsetAndMetadata>> asyncCommits = new CopyOnWriteArrayList<>();

        TrackingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            ConsumerRecords<String, String> records = super.poll(timeout);
            if (records.isEmpty()) {
                sleep(5); // MockConsumer never blocks — avoid a hot loop
            }
            return records;
        }

        @Override
        public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             OffsetCommitCallback callback) {
            asyncCommits.add(offsets);
            super.commitAsync(offsets, callback);
        }

        OffsetAndMetadata lastAsyncCommit(TopicPartition tp) {
            return asyncCommits.isEmpty() ? null : asyncCommits.get(asyncCommits.size() - 1).get(tp);
        }
    }
}
