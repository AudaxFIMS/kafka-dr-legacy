package com.example.kafkadr.cluster;

import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterManagerTest {

    private KafkaDrConfig config;
    private ClusterManager manager;

    @BeforeEach
    void setUp() {
        config = ConfigLoader.load("kafka-dr.yml");
        manager = new ClusterManager(config);
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
}
