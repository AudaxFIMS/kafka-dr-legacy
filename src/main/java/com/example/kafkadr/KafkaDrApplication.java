package com.example.kafkadr;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.cluster.LateBindingInitializer;
import com.example.kafkadr.config.ClusterConfig;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.consumer.DrConsumerFactory;
import com.example.kafkadr.consumer.DrConsumerManager;
import com.example.kafkadr.consumer.GroupInstanceIds;
import com.example.kafkadr.example.ExampleDrConsumer;
import com.example.kafkadr.handler.MessageHandlers;
import com.example.kafkadr.handler.MessageHandlerRegistry;
import com.example.kafkadr.health.ClusterHealthChecker;
import com.example.kafkadr.health.KafkaAdminHelper;
import com.example.kafkadr.idempotency.IdempotencyStore;
import com.example.kafkadr.idempotency.InMemoryIdempotencyStore;
import com.example.kafkadr.producer.DrProducerManager;
import com.example.kafkadr.rest.RestServer;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Main entry point for the Kafka DR Legacy application.
 *
 * <p>Startup sequence (resilient — never blocks on unreachable clusters):
 * <ol>
 *   <li>Load YAML configuration</li>
 *   <li>Probe all clusters (fast, with timeout) — separate reachable from unreachable</li>
 *   <li>Provision topics on reachable clusters</li>
 *   <li>Initialize components (ClusterManager starts all clusters as UNHEALTHY)</li>
 *   <li>Start health checker — first healthy cluster gets instant election</li>
 *   <li>Start LateBindingInitializer — monitors unreachable clusters in background</li>
 *   <li>Start REST server</li>
 * </ol>
 */
public class KafkaDrApplication {

    private static final Logger log = LoggerFactory.getLogger(KafkaDrApplication.class);
    private static final int DEFAULT_REST_PORT = 8088;

    private KafkaDrConfig config;
    private ClusterManager clusterManager;
    private ClusterHealthChecker healthChecker;
    private DrConsumerManager consumerManager;
    private DrConsumerFactory consumerFactory;
    private DrProducerManager producerManager;
    private IdempotencyStore idempotencyStore;
    private LateBindingInitializer lateInitializer;
    private RestServer restServer;
    private ExampleDrConsumer exampleConsumer;

    public void start(String configPath) {
        log.info("=== Kafka DR Application starting ===");

        // 1. Load config
        config = ConfigLoader.load(configPath);
        log.info("Configuration loaded: {} clusters, {} consumers, {} producers",
                config.getClusters().size(),
                config.getConsumers() != null ? config.getConsumers().size() : 0,
                config.getProducers() != null ? config.getProducers().size() : 0);

        // 2. Probe all clusters — resilient startup
        long probeTimeoutMs = config.getLateInitializer().getTimeoutMs();
        Set<String> reachableClusters = probeAllClusters(probeTimeoutMs);
        log.info("Startup probe: {}/{} clusters reachable: {}",
                reachableClusters.size(), config.getClusters().size(), reachableClusters);

        // 3. Diagnose broker nodes + provision topics on reachable clusters
        for (String name : reachableClusters) {
            ClusterConfig cc = config.getClusters().get(name);
            KafkaAdminHelper.diagnoseClusterNodes(name, cc.getBootstrapServers(), config, probeTimeoutMs);
            KafkaAdminHelper.provisionTopics(name, cc.getBootstrapServers(), config, probeTimeoutMs);
        }

        // 4. Initialize components (all clusters start UNHEALTHY)
        clusterManager = new ClusterManager(config);
        idempotencyStore = new InMemoryIdempotencyStore(config.getIdempotency());

        MessageHandlerRegistry handlerRegistry = new MessageHandlerRegistry();
        MessageHandlers.registerAll(handlerRegistry);

        // One id registry for all consumers: no two may share a group.instance.id
        GroupInstanceIds groupInstanceIds = new GroupInstanceIds(config);
        consumerManager = new DrConsumerManager(config, handlerRegistry, idempotencyStore, groupInstanceIds);
        producerManager = new DrProducerManager(config, clusterManager);

        clusterManager.addListener(consumerManager);
        clusterManager.addListener(producerManager);

        // Factory for DR-aware consumers created by application code (own poll loop)
        consumerFactory = new DrConsumerFactory(clusterManager, config, groupInstanceIds);

        // 5. Start health checker — first healthy cluster gets instant election
        healthChecker = new ClusterHealthChecker(clusterManager, config);
        healthChecker.start();

        // 6. Start late binding initializer for unreachable clusters
        lateInitializer = new LateBindingInitializer(config, clusterManager, reachableClusters);
        lateInitializer.start();

        // 7. Start REST server
        restServer = new RestServer(this);
        try {
            int port = Integer.parseInt(System.getProperty("rest.port",
                    System.getenv().getOrDefault("REST_PORT", String.valueOf(DEFAULT_REST_PORT))));
            restServer.start(port);
        } catch (Exception e) {
            log.error("Failed to start REST server", e);
        }

        // 8. Optional demo of an application-owned DR consumer loop:
        //    -Dexample.consumer.topic=demo-events [-Dexample.consumer.group=example-dr-group]
        startExampleConsumerIfRequested();

        log.info("=== Kafka DR Application started. Waiting for cluster health checks... ===");
    }

    private void startExampleConsumerIfRequested() {
        String topic = System.getProperty("example.consumer.topic");
        if (topic == null || topic.isBlank()) return;

        String group = System.getProperty("example.consumer.group", "example-dr-group");
        // Own idempotency prefix (= group): DrConsumerManager may consume the same topic with the
        // same store, and a shared prefix would make the two consumers skip each other's messages.
        exampleConsumer = new ExampleDrConsumer(consumerFactory, idempotencyStore,
                group, topic, group, new Properties());
        exampleConsumer.start();
        log.info("ExampleDrConsumer enabled: topic={}, group={}", topic, group);
    }

    public void stop() {
        log.info("=== Kafka DR Application shutting down ===");

        if (restServer != null) restServer.stop();
        if (exampleConsumer != null) exampleConsumer.close();
        if (lateInitializer != null) lateInitializer.stop();
        if (healthChecker != null) healthChecker.stop();
        if (clusterManager != null) clusterManager.stop();
        if (consumerManager != null) consumerManager.stopConsumers();
        if (producerManager != null) producerManager.stop();
        if (idempotencyStore != null) idempotencyStore.stop();

        log.info("=== Kafka DR Application stopped ===");
    }

    /**
     * Probe all configured clusters with a short timeout.
     * Returns the set of cluster names that responded.
     */
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

    // ─── Accessors ──────────────────────────────────────────────

    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    public DrProducerManager getProducerManager() {
        return producerManager;
    }

    public DrConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public DrConsumerFactory getConsumerFactory() {
        return consumerFactory;
    }

    public IdempotencyStore getIdempotencyStore() {
        return idempotencyStore;
    }

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : "kafka-dr.yml";

        KafkaDrApplication app = new KafkaDrApplication();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            try {
                app.stop();
            } finally {
                // Log4j's own shutdown hook is disabled (log4j2.xml) so that shutdown logs are not lost;
                // flush and stop logging only after every component has closed.
                LogManager.shutdown();
            }
        }, "shutdown-hook"));

        app.start(configPath);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            app.stop();
        }
    }
}
