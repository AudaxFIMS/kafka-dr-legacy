package com.example.kafkadr.health;

import com.example.kafkadr.config.KafkaDrConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Shared AdminClient utilities: cluster probing and topic provisioning.
 * Used by ClusterHealthChecker, LateBindingInitializer, and startup logic.
 */
public final class KafkaAdminHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminHelper.class);

    private KafkaAdminHelper() {
    }

    /**
     * Probe a cluster to check if it's reachable.
     *
     * @param bootstrapServers the broker addresses
     * @param timeoutMs        probe timeout in milliseconds
     * @return true if the cluster responded within timeout
     */
    public static boolean probeCluster(String bootstrapServers, long timeoutMs) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        props.put(AdminClientConfig.RETRIES_CONFIG, 0);

        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Provision topics on a cluster if auto-create-topics is enabled.
     * Creates topics from both consumer and producer configs.
     */
    public static void provisionTopics(String clusterName, String bootstrapServers,
                                       KafkaDrConfig config, long timeoutMs) {
        if (!config.isAutoCreateTopics()) return;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get(timeoutMs, TimeUnit.MILLISECONDS);
            List<NewTopic> toCreate = new ArrayList<>();

            // Collect topics from consumers
            if (config.getConsumers() != null) {
                for (var cc : config.getConsumers()) {
                    if (!existingTopics.contains(cc.getTopic())) {
                        toCreate.add(new NewTopic(cc.getTopic(), 1, (short) 1));
                    }
                }
            }
            // Collect topics from producers
            if (config.getProducers() != null) {
                for (var pc : config.getProducers()) {
                    if (!existingTopics.contains(pc.getTopic()) &&
                            toCreate.stream().noneMatch(t -> t.name().equals(pc.getTopic()))) {
                        toCreate.add(new NewTopic(pc.getTopic(), 1, (short) 1));
                    }
                }
            }

            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(timeoutMs, TimeUnit.MILLISECONDS);
                log.info("Provisioned {} topics on cluster '{}': {}", toCreate.size(), clusterName,
                        toCreate.stream().map(NewTopic::name).reduce((a, b) -> a + ", " + b).orElse(""));
            }
        } catch (Exception e) {
            log.warn("Failed to provision topics on cluster '{}': {}", clusterName, e.getMessage());
        }
    }

    /**
     * Create a configured AdminClient for health checking.
     */
    public static AdminClient createAdminClient(String bootstrapServers, long timeoutMs) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        props.put(AdminClientConfig.RETRIES_CONFIG, 0);
        return AdminClient.create(props);
    }
}
