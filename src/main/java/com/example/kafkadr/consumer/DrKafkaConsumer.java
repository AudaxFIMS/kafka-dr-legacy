package com.example.kafkadr.consumer;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.cluster.ClusterSwitchListener;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.config.KafkaPropertyResolver;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * DR-aware replacement for {@link KafkaConsumer} with the familiar
 * {@code subscribe → poll → process → commit} API.
 *
 * <p>Always reads from the cluster that {@link ClusterManager} elected as active.
 * On a cluster switch the underlying KafkaConsumer is closed and recreated on the
 * new cluster with the same subscription.
 *
 * <p>Threading: like KafkaConsumer, this class must be used from a single thread.
 * The switch notification arrives on another thread, so it only records the target
 * cluster and calls {@code wakeup()}; the actual recreation happens inside
 * {@link #poll(Duration)} on the owner thread. The poll that performs the switch
 * returns empty records.
 *
 * <p>Commits after a switch:
 * <ul>
 *   <li>{@link #commitAsync()} / {@link #commitSync()} commit the current consumer's
 *       positions — a freshly created consumer has none, so nothing from the old
 *       cluster can leak to the new one.</li>
 *   <li>{@link #commitAsync(Map, OffsetCommitCallback)} is dropped if the offsets
 *       were obtained before the last switch (offsets differ between clusters).</li>
 *   <li>Commit failures against an abandoned cluster are logged at DEBUG and not
 *       forwarded to the user callback.</li>
 * </ul>
 *
 * <p>Properties: bootstrap servers and SSL/SASL come from {@code kafka-dr.yml}
 * (default-properties → per-cluster → default-consumer-properties), then the
 * caller's properties are applied on top. {@code bootstrap.servers} is always
 * taken from the active cluster. Deserializers must be given as class names,
 * not instances: KafkaConsumer closes deserializer instances on close, so they
 * could not be reused after a switch.
 *
 * <p>Static membership: unless {@code group.instance.id} is set explicitly, the consumer gets
 * {@code {instance-id}-{group}-{topics}} from {@link GroupInstanceIds}, the same on every
 * cluster, so failback does not wait {@code session.timeout.ms} for dead members to expire.
 */
public class DrKafkaConsumer<K, V> implements ClusterSwitchListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DrKafkaConsumer.class);
    private static final Duration SWITCH_CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final ConsumerRebalanceListener NO_OP_REBALANCE_LISTENER = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }
    };

    private final ClusterManager clusterManager;
    private final KafkaDrConfig config;
    private final Properties userProps;
    private final GroupInstanceIds instanceIds;
    private final Function<Properties, Consumer<K, V>> consumerFactory;

    private final AtomicReference<ClusterInfo> pendingSwitch = new AtomicReference<>();
    private volatile Consumer<K, V> delegate;
    private volatile ClusterInfo currentCluster;
    private volatile long generation;
    private volatile boolean userWakeup;
    private volatile boolean switching;
    private volatile boolean closed;

    // Owner-thread state
    private long lastRecordsGeneration = -1;
    private List<String> topics;
    private ConsumerRebalanceListener userRebalanceListener = NO_OP_REBALANCE_LISTENER;
    private volatile String claimedInstanceId;
    private boolean instanceIdResolved;

    /**
     * @param instanceIds source of {@code group.instance.id} for static membership;
     *                    {@code null} disables it (unless set explicitly in {@code userProps})
     */
    DrKafkaConsumer(ClusterManager clusterManager, KafkaDrConfig config, Properties userProps,
                    GroupInstanceIds instanceIds, Function<Properties, Consumer<K, V>> consumerFactory) {
        this.clusterManager = clusterManager;
        this.config = config;
        this.userProps = new Properties();
        this.userProps.putAll(userProps);
        this.instanceIds = instanceIds;
        this.consumerFactory = consumerFactory;

        clusterManager.addListener(this);
        ClusterInfo active = clusterManager.getActiveCluster();
        if (active != null) {
            pendingSwitch.compareAndSet(null, active); // created lazily on first poll
        }
    }

    // ─── Cluster switch (called from a foreign thread) ─────────────

    @Override
    public void onClusterSwitch(ClusterInfo previousCluster, ClusterInfo newCluster) {
        if (closed) return;
        pendingSwitch.set(newCluster);
        Consumer<K, V> d = delegate;
        if (d != null) {
            d.wakeup();
        }
    }

    // ─── Consumer API ──────────────────────────────────────────────

    public void subscribe(Collection<String> topics) {
        subscribe(topics, NO_OP_REBALANCE_LISTENER);
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        this.topics = List.copyOf(topics);
        this.userRebalanceListener = listener;
        Consumer<K, V> d = delegate;
        if (d != null) {
            d.subscribe(this.topics, new SwitchAwareRebalanceListener());
        }
    }

    public ConsumerRecords<K, V> poll(Duration timeout) {
        ensureOpen();
        applyPendingSwitch();

        Consumer<K, V> d = delegate;
        if (d == null) {
            // No active cluster yet (or consumer creation failed) — behave like an empty poll
            sleepQuietly(timeout);
            return ConsumerRecords.empty();
        }

        try {
            ConsumerRecords<K, V> records = d.poll(timeout);
            if (!records.isEmpty()) {
                lastRecordsGeneration = generation;
            }
            return records;
        } catch (WakeupException e) {
            if (userWakeup) {
                userWakeup = false;
                throw e;
            }
            applyPendingSwitch();
            return ConsumerRecords.empty();
        }
    }

    /**
     * Commit offsets of specific records (e.g. {@code record.offset() + 1}).
     * Dropped if the offsets belong to records polled before the last cluster switch.
     */
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        Consumer<K, V> d = delegate;
        if (d == null) return;
        if (lastRecordsGeneration != generation) {
            log.warn("Dropping commit of {} — offsets are from a cluster that is no longer active", offsets.keySet());
            return;
        }
        d.commitAsync(offsets, wrapCallback(callback));
    }

    public void commitAsync(OffsetCommitCallback callback) {
        Consumer<K, V> d = delegate;
        if (d == null) return;
        d.commitAsync(wrapCallback(callback));
    }

    public void commitAsync() {
        commitAsync((OffsetCommitCallback) null);
    }

    public void commitSync() {
        Consumer<K, V> d = delegate;
        if (d == null) return;
        try {
            d.commitSync();
        } catch (WakeupException e) {
            if (userWakeup) {
                userWakeup = false;
                throw e;
            }
            // Switch in progress: offsets of the old cluster are meaningless on the new one
            log.debug("Skipping commitSync — cluster switch pending");
        } catch (RuntimeException e) {
            if (pendingSwitch.get() == null) throw e;
            log.debug("commitSync on abandoned cluster failed (expected on failover): {}", e.toString());
        }
    }

    public Set<TopicPartition> assignment() {
        Consumer<K, V> d = delegate;
        return d != null ? d.assignment() : Collections.emptySet();
    }

    public void wakeup() {
        userWakeup = true;
        Consumer<K, V> d = delegate;
        if (d != null) {
            d.wakeup();
        }
    }

    /** Name of the cluster the consumer currently reads from, or null before the first switch. */
    public String getCurrentClusterName() {
        ClusterInfo c = currentCluster;
        return c != null ? c.getName() : null;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        clusterManager.removeListener(this);
        closeDelegate(CLOSE_TIMEOUT);
        if (instanceIds != null) {
            instanceIds.release(claimedInstanceId);
        }
    }

    // ─── Internals (owner thread) ──────────────────────────────────

    private void applyPendingSwitch() {
        ClusterInfo target = pendingSwitch.getAndSet(null);
        if (target == null) return;

        ClusterInfo current = currentCluster;
        if (delegate != null && current != null && current.getName().equals(target.getName())) {
            return;
        }

        log.info("DR consumer switching {} -> {}, topics={}",
                current != null ? current.getName() : "none", target.getName(), topics);

        switching = true;
        try {
            closeDelegate(SWITCH_CLOSE_TIMEOUT);
        } finally {
            switching = false;
        }

        generation++;
        Consumer<K, V> created;
        try {
            created = consumerFactory.apply(buildProperties(target));
        } catch (Exception e) {
            log.error("Failed to create consumer on cluster '{}': {}", target.getName(), e.getMessage(), e);
            pendingSwitch.compareAndSet(null, target); // retry on next poll
            return;
        }

        if (topics != null) {
            created.subscribe(topics, new SwitchAwareRebalanceListener());
        }
        currentCluster = target;
        delegate = created;

        // A switch may have arrived while the new consumer was being created
        if (pendingSwitch.get() != null) {
            created.wakeup();
        }
    }

    private Properties buildProperties(ClusterInfo cluster) {
        Properties props = KafkaPropertyResolver.resolveConsumerProperties(config, cluster.getName(), null);
        props.putAll(userProps);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        applyStaticMembership(props);
        return props;
    }

    /**
     * Same group.instance.id on every cluster: on failback the new consumer replaces the member
     * that died with the cluster instead of waiting session.timeout.ms for partitions.
     * An explicitly configured group.instance.id is left untouched.
     */
    private void applyStaticMembership(Properties props) {
        if (props.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)) return;
        if (!instanceIdResolved) {
            instanceIdResolved = true;
            Object group = props.get(ConsumerConfig.GROUP_ID_CONFIG);
            if (instanceIds != null && group != null) {
                claimedInstanceId = instanceIds.claim(String.valueOf(group), topics);
            }
        }
        if (claimedInstanceId != null) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, claimedInstanceId);
        }
    }

    private void closeDelegate(Duration timeout) {
        Consumer<K, V> d = delegate;
        delegate = null;
        if (d == null) return;
        try {
            d.close(timeout);
        } catch (Exception e) {
            log.warn("Error closing consumer for topics={}: {}", topics, e.toString());
        }
    }

    private OffsetCommitCallback wrapCallback(OffsetCommitCallback userCallback) {
        long commitGeneration = generation;
        return (offsets, exception) -> {
            if (exception != null && (commitGeneration != generation || pendingSwitch.get() != null)) {
                log.debug("Async commit to abandoned cluster failed (expected on failover): {}",
                        exception.toString());
                return;
            }
            if (userCallback != null) {
                userCallback.onComplete(offsets, exception);
            } else if (exception != null) {
                log.warn("Async commit failed for {}: {}", offsets, exception.toString());
            }
        };
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("DrKafkaConsumer is closed");
        }
    }

    private static void sleepQuietly(Duration timeout) {
        try {
            Thread.sleep(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * While the old consumer is being closed because of a switch, its partitions are
     * reported as lost (not revoked) — committing to a dead cluster would block.
     */
    private final class SwitchAwareRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (switching) {
                userRebalanceListener.onPartitionsLost(partitions);
            } else {
                userRebalanceListener.onPartitionsRevoked(partitions);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            userRebalanceListener.onPartitionsAssigned(partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            userRebalanceListener.onPartitionsLost(partitions);
        }
    }
}
