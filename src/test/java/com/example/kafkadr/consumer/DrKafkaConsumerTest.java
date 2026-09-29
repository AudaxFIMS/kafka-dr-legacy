package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DrKafkaConsumerTest {

    private static final String TOPIC = "orders";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private KafkaDrConfig config;
    private ClusterManager manager;
    private GroupInstanceIds instanceIds;
    private final List<TrackingMockConsumer> created = new ArrayList<>();
    private final List<Properties> createdProps = new ArrayList<>();

    @BeforeEach
    void setUp() {
        config = ConfigLoader.load("kafka-dr.yml");
        config.setInstanceId("test-host");
        instanceIds = new GroupInstanceIds(config);
        manager = new ClusterManager(config, Runnable::run); // synchronous delivery
    }

    @Test
    void shouldNotCreateConsumerWithoutActiveCluster() {
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));

        assertTrue(consumer.poll(Duration.ZERO).isEmpty());
        assertTrue(created.isEmpty());
        assertNull(consumer.getCurrentClusterName());
    }

    @Test
    void shouldCreateConsumerOnActiveClusterWithDrBootstrap() {
        manager.reportHealthy("primary");
        Properties userProps = new Properties();
        userProps.put(ConsumerConfig.GROUP_ID_CONFIG, "legacy-group");
        userProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "should-be-ignored:9999");

        DrKafkaConsumer<String, String> consumer = newConsumer(userProps);
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        assertEquals(1, created.size());
        assertEquals("primary", consumer.getCurrentClusterName());
        Properties props = createdProps.get(0);
        assertEquals("localhost:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("legacy-group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("500", props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)); // default-consumer-properties
        assertEquals(Set.of(TOPIC), created.get(0).subscription());
    }

    @Test
    void shouldRecreateConsumerOnFailover() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);
        TrackingMockConsumer primary = created.get(0);

        manager.forceUnhealthy("primary");

        // Poll that performs the switch returns empty records
        assertTrue(consumer.poll(Duration.ZERO).isEmpty());
        assertEquals(2, created.size());
        assertTrue(primary.closed());
        assertEquals("secondary", consumer.getCurrentClusterName());
        assertEquals("localhost:9094", createdProps.get(1).get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Set.of(TOPIC), created.get(1).subscription());
    }

    @Test
    void shouldReadRecordsFromNewClusterAfterFailover() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        manager.forceUnhealthy("primary");
        consumer.poll(Duration.ZERO);

        TrackingMockConsumer secondary = created.get(1);
        assignAndAddRecord(secondary, 7L);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);

        assertEquals(1, records.count());
        assertEquals(7L, records.iterator().next().offset());
    }

    @Test
    void shouldCommitRecordOffsetOnCurrentCluster() {
        manager.reportHealthy("primary");
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);
        TrackingMockConsumer primary = created.get(0);
        assignAndAddRecord(primary, 0L);

        ConsumerRecord<String, String> record = consumer.poll(Duration.ZERO).iterator().next();
        consumer.commitAsync(Map.of(TP, new OffsetAndMetadata(record.offset() + 1)), null);

        assertEquals(List.of(Map.of(TP, new OffsetAndMetadata(1L))), primary.asyncCommits);
    }

    @Test
    void shouldDropRecordOffsetCommitFromPreviousCluster() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);
        assignAndAddRecord(created.get(0), 42L);
        ConsumerRecord<String, String> fromPrimary = consumer.poll(Duration.ZERO).iterator().next();

        manager.forceUnhealthy("primary");
        consumer.poll(Duration.ZERO); // switch

        consumer.commitAsync(Map.of(TP, new OffsetAndMetadata(fromPrimary.offset() + 1)), null);

        assertTrue(created.get(1).asyncCommits.isEmpty(),
                "Offset from primary must not be committed to secondary");
    }

    @Test
    void shouldIgnoreCommitSyncInterruptedBySwitch() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);
        created.get(0).failCommitSyncWithWakeup = true;

        manager.forceUnhealthy("primary");

        assertDoesNotThrow(consumer::commitSync);
        consumer.poll(Duration.ZERO);
        assertEquals("secondary", consumer.getCurrentClusterName());
    }

    @Test
    void shouldRethrowUserWakeup() {
        manager.reportHealthy("primary");
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
        assertEquals(1, created.size());
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    void shouldStopFollowingSwitchesAfterClose() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        consumer.close();
        manager.forceUnhealthy("primary");

        assertTrue(created.get(0).closed());
        assertEquals(1, created.size());
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    void shouldSwitchWithAsyncNotification() throws Exception {
        ClusterManager async = new ClusterManager(config);
        try {
            async.reportHealthy("primary");
            for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
                async.reportHealthy("secondary");
            }
            DrKafkaConsumer<String, String> consumer = new DrKafkaConsumer<>(async, config, new Properties(), instanceIds, props -> {
                TrackingMockConsumer mock = new TrackingMockConsumer();
                created.add(mock);
                createdProps.add(props);
                return mock;
            });
            consumer.subscribe(List.of(TOPIC));
            consumer.poll(Duration.ZERO);
            assertEquals("primary", consumer.getCurrentClusterName());

            async.forceUnhealthy("primary");

            long deadline = System.currentTimeMillis() + 5000;
            while (!"secondary".equals(consumer.getCurrentClusterName()) && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(10));
            }
            assertEquals("secondary", consumer.getCurrentClusterName());
            assertTrue(created.get(0).closed());
        } finally {
            async.stop();
        }
    }

    // ─── static membership ─────────────────────────────────────────

    @Test
    void shouldUseSameGroupInstanceIdOnEveryCluster() {
        activatePrimaryWithHealthySecondary();
        DrKafkaConsumer<String, String> consumer = newConsumer(groupProps("legacy-group"));
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        manager.forceUnhealthy("primary");
        consumer.poll(Duration.ZERO);

        assertEquals(2, createdProps.size());
        assertEquals("test-host-legacy-group-orders", createdProps.get(0).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
        assertEquals("test-host-legacy-group-orders", createdProps.get(1).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    void shouldKeepExplicitGroupInstanceId() {
        manager.reportHealthy("primary");
        Properties props = groupProps("legacy-group");
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-explicit-id");
        DrKafkaConsumer<String, String> consumer = newConsumer(props);
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        assertEquals("my-explicit-id", createdProps.get(0).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    void shouldFallBackToDynamicMembershipForDuplicateIdAndReleaseOnClose() {
        manager.reportHealthy("primary");
        DrKafkaConsumer<String, String> first = newConsumer(groupProps("legacy-group"));
        first.subscribe(List.of(TOPIC));
        first.poll(Duration.ZERO);
        DrKafkaConsumer<String, String> second = newConsumer(groupProps("legacy-group"));
        second.subscribe(List.of(TOPIC));
        second.poll(Duration.ZERO);

        assertEquals("test-host-legacy-group-orders", createdProps.get(0).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
        assertNull(createdProps.get(1).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG),
                "same id would fence the first consumer");

        first.close();
        DrKafkaConsumer<String, String> third = newConsumer(groupProps("legacy-group"));
        third.subscribe(List.of(TOPIC));
        third.poll(Duration.ZERO);
        assertEquals("test-host-legacy-group-orders", createdProps.get(2).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    void shouldNotSetGroupInstanceIdWhenDisabled() {
        config.setStaticMembership(false);
        instanceIds = new GroupInstanceIds(config);
        manager.reportHealthy("primary");
        DrKafkaConsumer<String, String> consumer = newConsumer(groupProps("legacy-group"));
        consumer.subscribe(List.of(TOPIC));
        consumer.poll(Duration.ZERO);

        assertNull(createdProps.get(0).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    // ─── helpers ───────────────────────────────────────────────────

    private static Properties groupProps(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return props;
    }

    private void activatePrimaryWithHealthySecondary() {
        manager.reportHealthy("primary"); // initial election
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }
        assertEquals("primary", manager.getActiveCluster().getName());
    }

    private DrKafkaConsumer<String, String> newConsumer() {
        return newConsumer(new Properties());
    }

    private DrKafkaConsumer<String, String> newConsumer(Properties userProps) {
        return new DrKafkaConsumer<>(manager, config, userProps, instanceIds, props -> {
            TrackingMockConsumer mock = new TrackingMockConsumer();
            created.add(mock);
            createdProps.add(props);
            return mock;
        });
    }

    private static void assignAndAddRecord(TrackingMockConsumer mock, long offset) {
        mock.rebalance(List.of(TP));
        mock.updateBeginningOffsets(Map.of(TP, offset));
        mock.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "key", "value"));
    }

    private static class TrackingMockConsumer extends MockConsumer<String, String> {
        final List<Map<TopicPartition, OffsetAndMetadata>> asyncCommits = new ArrayList<>();
        boolean failCommitSyncWithWakeup;

        TrackingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             OffsetCommitCallback callback) {
            asyncCommits.add(offsets);
            super.commitAsync(offsets, callback);
        }

        @Override
        public synchronized void commitSync() {
            if (failCommitSyncWithWakeup) throw new WakeupException();
            super.commitSync();
        }
    }
}
