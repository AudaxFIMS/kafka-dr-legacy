package com.example.kafkadr.cluster;

import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ClusterManagerTest {

    private KafkaDrConfig config;
    private ClusterManager manager;

    @BeforeEach
    void setUp() {
        config = ConfigLoader.load("kafka-dr.yml");
        // Synchronous delivery keeps state-machine tests deterministic;
        // async delivery is covered by the *Async* tests below.
        manager = new ClusterManager(config, Runnable::run);
    }

    @Test
    void shouldInitializeAllClustersAsUnhealthy() {
        assertEquals(3, manager.getAllClusters().size());
        assertNull(manager.getActiveCluster());

        for (ClusterInfo c : manager.getAllClusters()) {
            assertEquals(ClusterState.UNHEALTHY, c.getState(),
                    "Cluster " + c.getName() + " should start as UNHEALTHY");
        }
    }

    @Test
    void shouldElectInstantlyOnFirstHealthCheck() {
        // First health check — instant election, no threshold needed
        manager.reportHealthy("primary");

        assertNotNull(manager.getActiveCluster());
        assertEquals("primary", manager.getActiveCluster().getName());
        assertEquals(ClusterState.HEALTHY, manager.findCluster("primary").getState());
    }

    @Test
    void shouldElectSecondaryIfPrimaryNeverReports() {
        // If secondary reports healthy first, it gets elected instantly
        manager.reportHealthy("secondary");

        assertNotNull(manager.getActiveCluster());
        assertEquals("secondary", manager.getActiveCluster().getName());
    }

    @Test
    void shouldRequireThresholdForRecoveryAfterInitialElection() {
        // Initial election on secondary
        manager.reportHealthy("secondary");
        assertEquals("secondary", manager.getActiveCluster().getName());

        // Primary recovery should require full threshold
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold() - 1; i++) {
            manager.reportHealthy("primary");
        }
        // Still on secondary — threshold not met
        assertEquals("secondary", manager.getActiveCluster().getName());

        // One more success — threshold met, failback to primary (higher priority)
        manager.reportHealthy("primary");
        assertEquals("primary", manager.getActiveCluster().getName());
    }

    @Test
    void shouldFailoverWhenActiveClusterFails() {
        // Setup: primary active, secondary healthy
        manager.reportHealthy("primary"); // instant election
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }
        assertEquals("primary", manager.getActiveCluster().getName());

        // Fail primary
        for (int i = 0; i < config.getHealthCheck().getFailureThreshold(); i++) {
            manager.reportUnhealthy("primary");
        }

        assertEquals("secondary", manager.getActiveCluster().getName());
    }

    @Test
    void shouldFailbackWhenHigherPriorityRecovers() {
        // Setup: primary active
        manager.reportHealthy("primary");
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }

        // Fail primary -> failover to secondary
        for (int i = 0; i < config.getHealthCheck().getFailureThreshold(); i++) {
            manager.reportUnhealthy("primary");
        }
        assertEquals("secondary", manager.getActiveCluster().getName());

        // Recover primary -> failback (requires threshold)
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("primary");
        }
        assertEquals("primary", manager.getActiveCluster().getName());
    }

    @Test
    void shouldForceUnhealthyWithInstantFailover() {
        // Setup: primary active, secondary healthy
        manager.reportHealthy("primary");
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }
        assertEquals("primary", manager.getActiveCluster().getName());

        // Force unhealthy — instant failover, no threshold
        manager.forceUnhealthy("primary");

        assertEquals("secondary", manager.getActiveCluster().getName());
        assertEquals(ClusterState.UNHEALTHY, manager.findCluster("primary").getState());
    }

    @Test
    void shouldNotifyListenersOnSwitch() {
        List<String> switches = new ArrayList<>();
        manager.addListener((prev, next) ->
                switches.add((prev != null ? prev.getName() : "null") + "->" + next.getName()));

        manager.reportHealthy("primary");

        assertEquals(1, switches.size());
        assertEquals("null->primary", switches.get(0));
    }

    @Test
    void shouldNotFailoverBeforeThreshold() {
        manager.reportHealthy("primary");
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
        }

        // Report failures less than threshold
        for (int i = 0; i < config.getHealthCheck().getFailureThreshold() - 1; i++) {
            manager.reportUnhealthy("primary");
        }

        assertEquals("primary", manager.getActiveCluster().getName());
    }

    @Test
    void shouldCascadeFailoverToTertiary() {
        // All healthy
        manager.reportHealthy("primary"); // instant election
        for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
            manager.reportHealthy("secondary");
            manager.reportHealthy("tertiary");
        }

        // Fail primary -> secondary
        for (int i = 0; i < config.getHealthCheck().getFailureThreshold(); i++) {
            manager.reportUnhealthy("primary");
        }
        assertEquals("secondary", manager.getActiveCluster().getName());

        // Fail secondary -> tertiary
        for (int i = 0; i < config.getHealthCheck().getFailureThreshold(); i++) {
            manager.reportUnhealthy("secondary");
        }
        assertEquals("tertiary", manager.getActiveCluster().getName());
    }

    @Test
    void forceUnhealthyShouldDoNothingIfAlreadyUnhealthy() {
        // primary is UNHEALTHY by default
        List<String> switches = new ArrayList<>();
        manager.addListener((prev, next) ->
                switches.add(next.getName()));

        manager.forceUnhealthy("primary"); // already unhealthy — no-op

        assertTrue(switches.isEmpty());
    }

    // ─── Async notification ────────────────────────────────────────

    @Test
    void asyncNotificationShouldNotBlockCaller() throws Exception {
        ClusterManager async = new ClusterManager(config);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        async.addListener((prev, next) -> {
            entered.countDown();
            awaitQuietly(release);
        });

        try {
            async.reportHealthy("primary"); // returns although the listener is blocked

            assertEquals("primary", async.getActiveCluster().getName());
            assertTrue(entered.await(5, TimeUnit.SECONDS), "listener should be invoked on notifier thread");
        } finally {
            release.countDown();
            async.stop();
        }
    }

    @Test
    void asyncSlowListenerShouldNotDelayOthers() throws Exception {
        ClusterManager async = new ClusterManager(config);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch fastNotified = new CountDownLatch(1);
        async.addListener((prev, next) -> awaitQuietly(release)); // slow, registered first
        async.addListener((prev, next) -> fastNotified.countDown());

        try {
            async.reportHealthy("primary");

            assertTrue(fastNotified.await(5, TimeUnit.SECONDS), "fast listener must not wait for slow one");
        } finally {
            release.countDown();
            async.stop();
        }
    }

    @Test
    void asyncShouldCoalesceSwitchesWhileListenerBusy() throws Exception {
        ClusterManager async = new ClusterManager(config);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        List<String> switches = Collections.synchronizedList(new ArrayList<>());
        async.addListener((prev, next) -> {
            switches.add((prev != null ? prev.getName() : "null") + "->" + next.getName());
            firstEntered.countDown();
            awaitQuietly(release);
            done.countDown();
        });

        try {
            async.reportHealthy("primary");
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS));

            // While the listener is busy: primary -> secondary -> tertiary
            for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
                async.reportHealthy("secondary");
                async.reportHealthy("tertiary");
            }
            async.forceUnhealthy("primary");
            async.forceUnhealthy("secondary");
            assertEquals("tertiary", async.getActiveCluster().getName());

            release.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));

            // Intermediate switch to secondary is skipped
            assertEquals(List.of("null->primary", "primary->tertiary"), switches);
        } finally {
            release.countDown();
            async.stop();
        }
    }

    @Test
    void asyncShouldSkipSwitchBackToAlreadyDeliveredCluster() throws Exception {
        ClusterManager async = new ClusterManager(config);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstEntered = new CountDownLatch(1);
        List<String> switches = Collections.synchronizedList(new ArrayList<>());
        async.addListener((prev, next) -> {
            switches.add(next.getName());
            firstEntered.countDown();
            awaitQuietly(release);
        });

        try {
            async.reportHealthy("primary");
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS));

            // primary -> secondary -> primary while listener is busy
            for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
                async.reportHealthy("secondary");
            }
            async.forceUnhealthy("primary");
            for (int i = 0; i < config.getHealthCheck().getRecoveryThreshold(); i++) {
                async.reportHealthy("primary");
            }
            assertEquals("primary", async.getActiveCluster().getName());

            release.countDown();
            Thread.sleep(200); // let the drain loop finish

            assertEquals(List.of("primary"), switches, "listener is already on primary — no restart");
        } finally {
            release.countDown();
            async.stop();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
