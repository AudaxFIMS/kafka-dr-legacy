package com.example.kafkadr.cluster;

import com.example.kafkadr.config.KafkaDrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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
 *
 * <p>Listener notification is <b>asynchronous</b>: {@link #getActiveCluster()} changes
 * immediately, while {@link ClusterSwitchListener}s are notified on a notifier pool.
 * Each listener has its own serial queue, so:
 * <ul>
 *   <li>callers of {@code reportHealthy/reportUnhealthy/forceUnhealthy} never wait for listeners</li>
 *   <li>a slow listener (e.g. stopping consumers) does not delay the others</li>
 *   <li>events for one listener are delivered in order; switches that happen while the
 *       listener is still busy are coalesced — it receives only the latest target</li>
 * </ul>
 */
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final List<ClusterInfo> clusters;
    private final int failureThreshold;
    private final int recoveryThreshold;
    private final List<ListenerDispatcher> dispatchers = new CopyOnWriteArrayList<>();
    private final Executor notificationExecutor;
    private final ExecutorService ownedNotificationPool;

    private volatile ClusterInfo activeCluster;
    private volatile boolean initialElectionDone = false;

    public ClusterManager(KafkaDrConfig config) {
        this(config, null);
    }

    /**
     * @param notificationExecutor executor used to notify listeners; {@code null} creates an
     *                             internal daemon pool. Pass {@code Runnable::run} for
     *                             synchronous delivery (tests).
     */
    public ClusterManager(KafkaDrConfig config, Executor notificationExecutor) {
        if (notificationExecutor == null) {
            AtomicInteger threadCounter = new AtomicInteger();
            this.ownedNotificationPool = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "cluster-switch-notifier-" + threadCounter.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
            this.notificationExecutor = ownedNotificationPool;
        } else {
            this.ownedNotificationPool = null;
            this.notificationExecutor = notificationExecutor;
        }

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
        dispatchers.add(new ListenerDispatcher(listener));
    }

    public void removeListener(ClusterSwitchListener listener) {
        dispatchers.removeIf(d -> d.listener == listener);
    }

    /**
     * Stop the internal notifier pool (if owned). Pending notifications are dropped.
     */
    public void stop() {
        if (ownedNotificationPool != null) {
            ownedNotificationPool.shutdownNow();
        }
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
     * Switch to a specific cluster and schedule listener notifications.
     */
    private void switchTo(ClusterInfo newCluster) {
        ClusterInfo previous = this.activeCluster;
        this.activeCluster = newCluster;

        String prevName = previous != null ? previous.getName() : "none";
        log.info(">>> CLUSTER SWITCH: {} -> {} (bootstrap: {}) <<<",
                prevName, newCluster.getName(), newCluster.getBootstrapServers());

        for (ListenerDispatcher dispatcher : dispatchers) {
            dispatcher.submit(newCluster);
        }
    }

    public ClusterInfo findCluster(String name) {
        return clusters.stream()
                .filter(c -> c.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Serial, coalescing delivery of switch events to one listener.
     * At most one drain task per listener runs at a time; while it runs, newer
     * targets overwrite {@code pendingTarget}, so a busy listener only sees the latest one.
     */
    private final class ListenerDispatcher {

        private final ClusterSwitchListener listener;
        private final Object lock = new Object();
        private ClusterInfo pendingTarget;
        private ClusterInfo delivered;
        private boolean draining;

        ListenerDispatcher(ClusterSwitchListener listener) {
            this.listener = listener;
        }

        void submit(ClusterInfo target) {
            synchronized (lock) {
                pendingTarget = target;
                if (draining) return;
                draining = true;
            }
            try {
                notificationExecutor.execute(this::drain);
            } catch (RejectedExecutionException e) {
                synchronized (lock) {
                    draining = false;
                }
                log.warn("Switch notification to {} rejected (notifier stopped)", listenerName());
            }
        }

        private void drain() {
            while (true) {
                ClusterInfo target;
                ClusterInfo previous;
                synchronized (lock) {
                    target = pendingTarget;
                    pendingTarget = null;
                    if (target == null) {
                        draining = false;
                        return;
                    }
                    previous = delivered;
                }

                if (previous != null && previous.getName().equals(target.getName())) {
                    log.debug("Coalesced switch for {}: already on '{}'", listenerName(), target.getName());
                    continue;
                }

                try {
                    listener.onClusterSwitch(previous, target);
                } catch (Exception e) {
                    log.error("Error in ClusterSwitchListener: {}", listenerName(), e);
                }
                synchronized (lock) {
                    delivered = target;
                }
            }
        }

        private String listenerName() {
            return listener.getClass().getSimpleName();
        }
    }
}
