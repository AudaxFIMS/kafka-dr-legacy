package com.example.kafkadr;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.cluster.LateBindingInitializer;
import com.example.kafkadr.config.ClusterConfig;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.consumer.DrConsumerManager;
import com.example.kafkadr.handler.DemoHandlers;
import com.example.kafkadr.handler.MessageHandlerRegistry;
import com.example.kafkadr.health.ClusterHealthChecker;
import com.example.kafkadr.health.KafkaAdminHelper;
import com.example.kafkadr.idempotency.InMemoryIdempotencyStore;
import com.example.kafkadr.producer.DrProducerManager;
import com.example.kafkadr.rest.RestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
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
    private DrProducerManager producerManager;
    private InMemoryIdempotencyStore idempotencyStore;
    private LateBindingInitializer lateInitializer;
    private RestServer restServer;

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

        // 3. Provision topics on reachable clusters
        for (String name : reachableClusters) {
            ClusterConfig cc = config.getClusters().get(name);
            KafkaAdminHelper.provisionTopics(name, cc.getBootstrapServers(), config, probeTimeoutMs);
        }

        // 4. Initialize components (all clusters start UNHEALTHY)
        clusterManager = new ClusterManager(config);
        idempotencyStore = new InMemoryIdempotencyStore(config.getIdempotency());

        MessageHandlerRegistry handlerRegistry = new MessageHandlerRegistry();
        DemoHandlers.registerAll(handlerRegistry);

        consumerManager = new DrConsumerManager(config, handlerRegistry, idempotencyStore);
        producerManager = new DrProducerManager(config, clusterManager);

        clusterManager.addListener(consumerManager);
        clusterManager.addListener(producerManager);

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

        log.info("=== Kafka DR Application started. Waiting for cluster health checks... ===");
    }

    public void stop() {
        log.info("=== Kafka DR Application shutting down ===");

        if (restServer != null) restServer.stop();
        if (lateInitializer != null) lateInitializer.stop();
        if (healthChecker != null) healthChecker.stop();
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

    public InMemoryIdempotencyStore getIdempotencyStore() {
        return idempotencyStore;
    }

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : "kafka-dr.yml";

        KafkaDrApplication app = new KafkaDrApplication();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            app.stop();
        }));

        app.start(configPath);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            app.stop();
        }
    }
}
