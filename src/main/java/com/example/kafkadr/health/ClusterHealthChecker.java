package com.example.kafkadr.health;

import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.HealthCheckConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically checks health of all configured Kafka clusters using AdminClient.
 * Reports results to ClusterManager which handles failover/failback decisions.
 */
public class ClusterHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealthChecker.class);

    private final ClusterManager clusterManager;
    private final HealthCheckConfig config;
    private final ScheduledExecutorService scheduler;
    private final Map<String, AdminClient> adminClients = new HashMap<>();

    public ClusterHealthChecker(ClusterManager clusterManager, HealthCheckConfig config) {
        this.clusterManager = clusterManager;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kafka-health-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        log.info("Starting health checker: interval={}ms, timeout={}ms, failureThreshold={}, recoveryThreshold={}",
                config.getIntervalMs(), config.getTimeoutMs(),
                config.getFailureThreshold(), config.getRecoveryThreshold());

        for (ClusterInfo cluster : clusterManager.getAllClusters()) {
            adminClients.put(cluster.getName(),
                    KafkaAdminHelper.createAdminClient(cluster.getBootstrapServers(), config.getTimeoutMs()));
        }

        scheduler.scheduleWithFixedDelay(this::checkAllClusters, 0, config.getIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        log.info("Stopping health checker");
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        adminClients.values().forEach(ac -> {
            try {
                ac.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("Error closing AdminClient", e);
            }
        });
        adminClients.clear();
    }

    private void checkAllClusters() {
        for (ClusterInfo cluster : clusterManager.getAllClusters()) {
            checkCluster(cluster);
        }
    }

    private void checkCluster(ClusterInfo cluster) {
        AdminClient adminClient = adminClients.get(cluster.getName());
        if (adminClient == null) {
            clusterManager.reportUnhealthy(cluster.getName());
            return;
        }

        try {
            DescribeClusterResult result = adminClient.describeCluster();
            result.clusterId().get(config.getTimeoutMs(), TimeUnit.MILLISECONDS);
            log.trace("Health check OK for cluster '{}'", cluster.getName());
            clusterManager.reportHealthy(cluster.getName());
        } catch (Exception e) {
            log.debug("Health check FAILED for cluster '{}': {}", cluster.getName(), e.getMessage());
            clusterManager.reportUnhealthy(cluster.getName());

            // Recreate admin client on failure to avoid stale connections
            try {
                adminClient.close(Duration.ofSeconds(1));
            } catch (Exception ignored) {
            }
            adminClients.put(cluster.getName(),
                    KafkaAdminHelper.createAdminClient(cluster.getBootstrapServers(), config.getTimeoutMs()));
        }
    }
}
