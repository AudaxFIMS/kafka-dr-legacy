package com.example.kafkadr.consumer;

import com.example.kafkadr.config.KafkaDrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Builds and hands out {@code group.instance.id} values for Kafka static membership.
 *
 * <p>Why: when a cluster dies, consumers on it are closed without being able to send
 * LeaveGroup. After the cluster comes back (failback), the broker keeps those dead members
 * until {@code session.timeout.ms} (45s by default) expires, and new consumers get no
 * partitions meanwhile. A static member that rejoins with the same {@code group.instance.id}
 * replaces its old registration immediately.
 *
 * <p>Id format: {@code {instance-id}-{group}-{topics}} — unique per consumer inside a group
 * and stable across restarts, as long as {@code instance-id} is unique per running application
 * instance and stable (hostname by default, override with {@code KAFKA_DR_INSTANCE_ID}).
 *
 * <p>Two live consumers with the same id would fence each other
 * ({@code FencedInstanceIdException}), so ids are claimed: a second claim of the same id in
 * this process is refused and that consumer falls back to dynamic membership.
 */
public class GroupInstanceIds {

    private static final Logger log = LoggerFactory.getLogger(GroupInstanceIds.class);
    private static final Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9._-]");
    private static final int MAX_LENGTH = 249;

    private final String instanceId;
    private final Set<String> claimed = ConcurrentHashMap.newKeySet();

    public GroupInstanceIds(KafkaDrConfig config) {
        this.instanceId = config.isStaticMembership() ? resolveInstanceId(config.getInstanceId()) : null;
        if (instanceId != null) {
            log.info("Consumer static membership enabled: instance-id='{}'", instanceId);
        } else {
            log.info("Consumer static membership disabled");
        }
    }

    /**
     * Claim a {@code group.instance.id} for a consumer of {@code group} subscribed to {@code topics}.
     *
     * @return the id, or {@code null} if static membership is disabled, the group is unknown,
     *         or the same id is already used by another consumer in this process
     */
    public String claim(String group, Collection<String> topics) {
        if (instanceId == null || group == null || group.isBlank() || topics == null || topics.isEmpty()) {
            return null;
        }
        String id = build(group, topics);
        if (!claimed.add(id)) {
            log.warn("group.instance.id '{}' is already used by another consumer in this process — " +
                    "falling back to dynamic membership. Set group.instance.id explicitly to avoid this.", id);
            return null;
        }
        return id;
    }

    public void release(String id) {
        if (id != null) {
            claimed.remove(id);
        }
    }

    /** Application instance id used as the prefix, or {@code null} if static membership is disabled. */
    public String getInstanceId() {
        return instanceId;
    }

    private String build(String group, Collection<String> topics) {
        String raw = instanceId + "-" + group + "-" + String.join(".", new TreeSet<>(topics));
        String id = INVALID_CHARS.matcher(raw).replaceAll("_");
        if (id.length() > MAX_LENGTH) {
            String hash = Integer.toHexString(id.hashCode());
            id = id.substring(0, MAX_LENGTH - hash.length() - 1) + "-" + hash;
        }
        return id;
    }

    private static String resolveInstanceId(String configured) {
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.warn("Cannot resolve hostname for instance-id, using 'kafka-dr': {}", e.getMessage());
            return "kafka-dr";
        }
    }
}
