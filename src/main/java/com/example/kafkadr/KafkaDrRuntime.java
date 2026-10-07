package com.example.kafkadr;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.cluster.LateBindingInitializer;
import com.example.kafkadr.config.ClusterConfig;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.consumer.DrConsumerFactory;
import com.example.kafkadr.consumer.GroupInstanceIds;
import com.example.kafkadr.health.ClusterHealthChecker;
import com.example.kafkadr.health.KafkaAdminHelper;
import com.example.kafkadr.idempotency.IdempotencyStore;
import com.example.kafkadr.idempotency.InMemoryIdempotencyStore;
import com.example.kafkadr.producer.DrProducerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The DR core shared by all Kafka clients of one process: cluster election, health checks,
 * idempotency store, consumer factory and producer.
 *
 * <p>Create exactly one per process — {@link GroupInstanceIds} inside it guarantees that no two
 * consumers get the same {@code group.instance.id}.
 *
 * <p>Lifecycle is two-phase so that {@link com.example.kafkadr.cluster.ClusterSwitchListener}s can
 * be registered before the first election (a listener added later does not receive past switches):
 * <ol>
 *   <li>constructor — builds components, nothing is started;</li>
 *   <li>{@link #start()} — provisions topics if {@code auto-create-topics} is enabled, then starts
 *       health checks; the first healthy cluster becomes active;</li>
 *   <li>{@link #close()} — stops switching and releases resources. Close application-owned
 *       consumers first.</li>
 * </ol>
 *
 * <pre>
 * KafkaDrRuntime dr = KafkaDrRuntime.start("kafka-dr.yml");
 * ExampleDrConsumer consumer = new ExampleDrConsumer(dr.getConsumerFactory(), dr.getIdempotencyStore(),
 *         "legacy-order-group", "order-events", "legacy-order-group", new Properties());
 * consumer.start();
 * ...
 * consumer.close();
 * dr.close();
 * </pre>
 */
public class KafkaDrRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDrRuntime.class);

    private final KafkaDrConfig config;
    private final ClusterManager clusterManager;
    private final IdempotencyStore idempotencyStore;
    private final GroupInstanceIds groupInstanceIds;
    private final DrConsumerFactory consumerFactory;
    private final DrProducerManager producerManager;
    private final ClusterHealthChecker healthChecker;
    private LateBindingInitializer lateInitializer;

    public KafkaDrRuntime(KafkaDrConfig config) {
        this.config = config;
        this.clusterManager = new ClusterManager(config);
        this.idempotencyStore = new InMemoryIdempotencyStore(config.getIdempotency());
        this.groupInstanceIds = new GroupInstanceIds(config);
        this.consumerFactory = new DrConsumerFactory(clusterManager, config, groupInstanceIds);
        this.producerManager = new DrProducerManager(config, clusterManager);
        clusterManager.addListener(producerManager);
        this.healthChecker = new ClusterHealthChecker(clusterManager, config);
    }

    /**
     * Loads the config from the classpath, builds and starts the runtime.
     */
    public static KafkaDrRuntime start(String configPath) {
        KafkaDrRuntime runtime = new KafkaDrRuntime(ConfigLoader.load(configPath));
        runtime.start();
        return runtime;
    }

    /**
     * Starts cluster election. Never blocks on unreachable clusters beyond the probe timeout.
     */
    public void start() {
        if (config.isAutoCreateTopics()) {
            provisionTopics();
        }

        // First healthy cluster gets instant election
        healthChecker.start();
    }

    /**
     * Creates topics on clusters reachable now; clusters that are down get them later
     * from {@link LateBindingInitializer} when they come back.
     */
    private void provisionTopics() {
        long probeTimeoutMs = config.getLateInitializer().getTimeoutMs();
        Set<String> reachableClusters = probeAllClusters(probeTimeoutMs);
        log.info("Startup probe: {}/{} clusters reachable: {}",
                reachableClusters.size(), config.getClusters().size(), reachableClusters);

        for (String name : reachableClusters) {
            ClusterConfig cc = config.getClusters().get(name);
            KafkaAdminHelper.diagnoseClusterNodes(name, cc.getBootstrapServers(), config, probeTimeoutMs);
            KafkaAdminHelper.provisionTopics(name, cc.getBootstrapServers(), config, probeTimeoutMs);
        }

        lateInitializer = new LateBindingInitializer(config, clusterManager, reachableClusters);
        lateInitializer.start();
    }

    private Set<String> probeAllClusters(long timeoutMs) {
        Set<String> reachable = new LinkedHashSet<>();
        for (Map.Entry<String, ClusterConfig> entry : config.getClusters().entrySet()) {
            String name = entry.getKey();
            String brokers = entry.getValue().getBootstrapServers();

            log.info("Probing cluster '{}' ({})...", name, brokers);
            if (KafkaAdminHelper.probeCluster(brokers, timeoutMs, config, name)) {
                reachable.add(name);
                log.info("Cluster '{}' is REACHABLE", name);
            } else {
                log.warn("Cluster '{}' ({}) UNREACHABLE at startup — will be initialized later", name, brokers);
            }
        }
        return reachable;
    }

    /**
     * Stops cluster switching first (no listener is notified afterwards), then releases resources.
     */
    @Override
    public void close() {
        if (lateInitializer != null) lateInitializer.stop();
        healthChecker.stop();
        clusterManager.stop();
        producerManager.stop();
        idempotencyStore.stop();
    }

    // ─── Accessors ──────────────────────────────────────────────

    public KafkaDrConfig getConfig() {
        return config;
    }

    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    public IdempotencyStore getIdempotencyStore() {
        return idempotencyStore;
    }

    /**
     * For consumers built outside {@link DrConsumerFactory} (e.g. {@code DrConsumerManager}):
     * they must claim instance ids from this same registry.
     */
    public GroupInstanceIds getGroupInstanceIds() {
        return groupInstanceIds;
    }

    public DrConsumerFactory getConsumerFactory() {
        return consumerFactory;
    }

    public DrProducerManager getProducerManager() {
        return producerManager;
    }
}
