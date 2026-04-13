package com.example.kafkadr.cluster;

import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.health.KafkaAdminHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Monitors clusters that were unreachable at startup and initializes them
 * (topic provisioning) when they become available.
 *
 * <p>This enables resilient startup: the application starts instantly even if
 * some clusters are down. When a previously-dead cluster comes online,
 * LateBindingInitializer provisions topics on it so it's ready for failover.
 */
public class LateBindingInitializer {

    private static final Logger log = LoggerFactory.getLogger(LateBindingInitializer.class);

    private final KafkaDrConfig config;
    private final ClusterManager clusterManager;
    private final Set<String> initializedClusters;
    private final long probeTimeoutMs;
    private final ScheduledExecutorService scheduler;

    public LateBindingInitializer(KafkaDrConfig config, ClusterManager clusterManager,
                                  Set<String> initiallyReachableClusters) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.initializedClusters = ConcurrentHashMap.newKeySet();
        this.initializedClusters.addAll(initiallyReachableClusters);
        this.probeTimeoutMs = config.getLateInitializer().getTimeoutMs();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "late-binding-initializer");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        int totalClusters = config.getClusters().size();
        int initialized = initializedClusters.size();

        if (initialized >= totalClusters) {
            log.info("All {} clusters were reachable at startup. Late initializer not needed.", totalClusters);
            return;
        }

        log.info("Starting LateBindingInitializer: {}/{} clusters initialized. " +
                        "Monitoring {} unreachable clusters for recovery.",
                initialized, totalClusters, totalClusters - initialized);

        // Check every health-check interval
        long intervalMs = config.getHealthCheck().getIntervalMs();
        scheduler.scheduleWithFixedDelay(this::checkAndInitialize, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isInitialized(String clusterName) {
        return initializedClusters.contains(clusterName);
    }

    private void checkAndInitialize() {
        for (var entry : config.getClusters().entrySet()) {
            String clusterName = entry.getKey();

            if (initializedClusters.contains(clusterName)) {
                continue;
            }

            String brokers = entry.getValue().getBootstrapServers();

            if (!KafkaAdminHelper.probeCluster(brokers, probeTimeoutMs, config, clusterName)) {
                continue;
            }

            log.info("Previously unreachable cluster '{}' ({}) is now available. Initializing...",
                    clusterName, brokers);

            try {
                // Provision topics
                KafkaAdminHelper.provisionTopics(clusterName, brokers, config, probeTimeoutMs);

                initializedClusters.add(clusterName);
                log.info("Cluster '{}' late-initialized successfully. Topics provisioned.", clusterName);

                // Check if all clusters are now initialized
                if (initializedClusters.size() >= config.getClusters().size()) {
                    log.info("All clusters initialized. Stopping LateBindingInitializer.");
                    scheduler.shutdown();
                    return;
                }
            } catch (Exception e) {
                log.error("Failed to late-initialize cluster '{}': {}", clusterName, e.getMessage(), e);
            }
        }
    }
}
