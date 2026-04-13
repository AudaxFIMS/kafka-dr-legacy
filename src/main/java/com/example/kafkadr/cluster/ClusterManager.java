package com.example.kafkadr.cluster;

import com.example.kafkadr.config.KafkaDrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Manages multiple Kafka clusters and orchestrates failover/failback.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li>All clusters start as {@code UNHEALTHY}</li>
 *   <li>First health check success triggers <b>instant election</b> (no recovery threshold)</li>
 *   <li>Subsequent recovery requires full {@code recoveryThreshold} consecutive successes</li>
 *   <li>{@link #forceUnhealthy(String)} enables instant failover from the producer on send errors</li>
 *   <li>Higher-priority cluster automatically triggers failback when it recovers</li>
 * </ul>
 */
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final List<ClusterInfo> clusters;
    private final int failureThreshold;
    private final int recoveryThreshold;
    private final List<ClusterSwitchListener> listeners = new CopyOnWriteArrayList<>();

    private volatile ClusterInfo activeCluster;
    private volatile boolean initialElectionDone = false;

    public ClusterManager(KafkaDrConfig config) {
        this.failureThreshold = config.getHealthCheck().getFailureThreshold();
        this.recoveryThreshold = config.getHealthCheck().getRecoveryThreshold();

        this.clusters = config.getClusters().entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getValue().getPriority()))
                .map(e -> new ClusterInfo(e.getKey(), e.getValue().getBootstrapServers(), e.getValue().getPriority()))
                .collect(Collectors.toList());

        if (clusters.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster must be configured");
        }

        // All clusters start as UNHEALTHY
        clusters.forEach(c -> c.setState(ClusterState.UNHEALTHY));

        log.info("Initialized ClusterManager with {} clusters (all UNHEALTHY): {}", clusters.size(),
                clusters.stream().map(c -> c.getName() + "(priority=" + c.getPriority() + ")").collect(Collectors.joining(", ")));
    }

    public void addListener(ClusterSwitchListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ClusterSwitchListener listener) {
        listeners.remove(listener);
    }

    public ClusterInfo getActiveCluster() {
        return activeCluster;
    }

    public List<ClusterInfo> getAllClusters() {
        return new ArrayList<>(clusters);
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    /**
     * Called by HealthChecker when a cluster health check succeeds.
     */
    public synchronized void reportHealthy(String clusterName) {
        ClusterInfo cluster = findCluster(clusterName);
        if (cluster == null) return;

        // Already healthy — just reset counters
        if (cluster.getState() == ClusterState.HEALTHY) {
            cluster.incrementSuccesses();
            return;
        }

        // First health check ever — instant election, no threshold
        if (!initialElectionDone) {
            cluster.setState(ClusterState.HEALTHY);
            cluster.resetCounters();
            log.info(">>> Cluster '{}' marked HEALTHY (initial election) <<<", clusterName);
            initialElectionDone = true;
            reelectActive();
            return;
        }

        // Normal recovery — requires threshold
        int successes = cluster.incrementSuccesses();
        log.debug("Cluster '{}' recovery check passed ({}/{})", clusterName, successes, recoveryThreshold);

        if (successes >= recoveryThreshold) {
            cluster.setState(ClusterState.HEALTHY);
            log.info(">>> Cluster '{}' marked HEALTHY after {} consecutive successes <<<",
                    clusterName, successes);
            reelectActive();
        }
    }

    /**
     * Called by HealthChecker when a cluster health check fails.
     */
    public synchronized void reportUnhealthy(String clusterName) {
        ClusterInfo cluster = findCluster(clusterName);
        if (cluster == null) return;

        // Already unhealthy — just count
        if (cluster.getState() == ClusterState.UNHEALTHY) {
            cluster.incrementFailures();
            return;
        }

        int failures = cluster.incrementFailures();

        if (failures >= failureThreshold) {
            cluster.setState(ClusterState.UNHEALTHY);
            log.warn(">>> Cluster '{}' marked UNHEALTHY after {} consecutive failures <<<", clusterName, failures);

            if (activeCluster != null && activeCluster.getName().equals(clusterName)) {
                reelectActive();
            }
        }
    }

    /**
     * Force a cluster to UNHEALTHY state immediately — called by the producer
     * when a send error indicates the cluster is unreachable.
     * Bypasses the failure threshold for instant failover.
     */
    public synchronized void forceUnhealthy(String clusterName) {
        ClusterInfo cluster = findCluster(clusterName);
        if (cluster == null) return;

        if (cluster.getState() == ClusterState.HEALTHY) {
            cluster.setState(ClusterState.UNHEALTHY);
            cluster.resetCounters();
            // Set failure count to threshold so health checker doesn't immediately re-elect
            for (int i = 0; i < failureThreshold; i++) {
                cluster.incrementFailures();
            }
            log.warn(">>> Cluster '{}' FORCE-MARKED UNHEALTHY by producer <<<", clusterName);
            reelectActive();
        }
    }

    /**
     * Re-elect the active cluster based on priority and health.
     * Picks the highest-priority (lowest number) healthy cluster.
     */
    private void reelectActive() {
        ClusterInfo best = clusters.stream()
                .filter(c -> c.getState() == ClusterState.HEALTHY)
                .min(Comparator.comparingInt(ClusterInfo::getPriority))
                .orElse(null);

        if (best == null) {
            log.error("No healthy cluster available for election!");
            return;
        }

        if (activeCluster != null && activeCluster.getName().equals(best.getName())) {
            return; // already active
        }

        switchTo(best);
    }

    /**
     * Switch to a specific cluster and notify all listeners.
     */
    private void switchTo(ClusterInfo newCluster) {
        ClusterInfo previous = this.activeCluster;
        this.activeCluster = newCluster;

        String prevName = previous != null ? previous.getName() : "none";
        log.info(">>> CLUSTER SWITCH: {} -> {} (bootstrap: {}) <<<",
                prevName, newCluster.getName(), newCluster.getBootstrapServers());

        for (ClusterSwitchListener listener : listeners) {
            try {
                listener.onClusterSwitch(previous, newCluster);
            } catch (Exception e) {
                log.error("Error in ClusterSwitchListener: {}", listener.getClass().getSimpleName(), e);
            }
        }
    }

    public ClusterInfo findCluster(String name) {
        return clusters.stream()
                .filter(c -> c.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
