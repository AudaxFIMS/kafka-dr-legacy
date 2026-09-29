package com.example.kafkadr.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads and parses kafka-dr.yml configuration with environment variable substitution.
 */
public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([^:}]+)(?::([^}]*))?}");

    public static KafkaDrConfig load(String resourcePath) {
        Yaml yaml = new Yaml();
        Map<String, Object> root;

        try (InputStream is = resolveInputStream(resourcePath)) {
            root = yaml.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config from: " + resourcePath, e);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> kafkaDr = (Map<String, Object>) root.get("kafka-dr");
        if (kafkaDr == null) {
            throw new RuntimeException("Missing 'kafka-dr' root key in configuration");
        }

        resolveEnvVars(kafkaDr);

        return mapToConfig(kafkaDr);
    }

    private static InputStream resolveInputStream(String resourcePath) throws IOException {
        // Try classpath first, then filesystem
        InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath);
        if (is != null) {
            log.info("Loading config from classpath: {}", resourcePath);
            return is;
        }
        log.info("Loading config from filesystem: {}", resourcePath);
        return new FileInputStream(resourcePath);
    }

    @SuppressWarnings("unchecked")
    private static void resolveEnvVars(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                entry.setValue(resolveEnvString((String) value));
            } else if (value instanceof Map) {
                resolveEnvVars((Map<String, Object>) value);
            } else if (value instanceof List) {
                resolveEnvVarsList((List<Object>) value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void resolveEnvVarsList(List<Object> list) {
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            if (value instanceof String) {
                list.set(i, resolveEnvString((String) value));
            } else if (value instanceof Map) {
                resolveEnvVars((Map<String, Object>) value);
            } else if (value instanceof List) {
                resolveEnvVarsList((List<Object>) value);
            }
        }
    }

    private static String resolveEnvString(String value) {
        Matcher matcher = ENV_PATTERN.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String envName = matcher.group(1);
            String defaultValue = matcher.group(2);
            String envValue = System.getenv(envName);
            String replacement = envValue != null ? envValue : (defaultValue != null ? defaultValue : "");
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static KafkaDrConfig mapToConfig(Map<String, Object> map) {
        KafkaDrConfig config = new KafkaDrConfig();

        if (map.containsKey("default-properties")) {
            config.setDefaultProperties((Map<String, Object>) map.get("default-properties"));
        }

        if (map.containsKey("auto-create-topics")) {
            config.setAutoCreateTopics(Boolean.TRUE.equals(map.get("auto-create-topics")));
        }

        if (map.containsKey("instance-id") && map.get("instance-id") != null) {
            config.setInstanceId(String.valueOf(map.get("instance-id")));
        }

        if (map.containsKey("static-membership")) {
            config.setStaticMembership(!Boolean.FALSE.equals(map.get("static-membership"))
                    && !"false".equalsIgnoreCase(String.valueOf(map.get("static-membership"))));
        }

        if (map.containsKey("default-consumer-properties")) {
            config.setDefaultConsumerProperties((Map<String, Object>) map.get("default-consumer-properties"));
        }

        if (map.containsKey("default-producer-properties")) {
            config.setDefaultProducerProperties((Map<String, Object>) map.get("default-producer-properties"));
        }

        // Parse clusters
        if (map.containsKey("clusters")) {
            Map<String, Object> clustersMap = (Map<String, Object>) map.get("clusters");
            Map<String, ClusterConfig> clusters = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : clustersMap.entrySet()) {
                Map<String, Object> clusterMap = (Map<String, Object>) entry.getValue();
                ClusterConfig cc = new ClusterConfig();
                cc.setBootstrapServers(String.valueOf(clusterMap.get("bootstrap-servers")));
                cc.setPriority(((Number) clusterMap.get("priority")).intValue());
                if (clusterMap.containsKey("properties")) {
                    cc.setProperties((Map<String, Object>) clusterMap.get("properties"));
                }
                clusters.put(entry.getKey(), cc);
            }
            config.setClusters(clusters);
        }

        // Parse consumers
        if (map.containsKey("consumers")) {
            List<Map<String, Object>> consumersList = (List<Map<String, Object>>) map.get("consumers");
            List<DrConsumerConfig> consumers = new ArrayList<>();
            for (Map<String, Object> cm : consumersList) {
                DrConsumerConfig cc = new DrConsumerConfig();
                cc.setTopic((String) cm.get("topic"));
                cc.setGroup((String) cm.get("group"));
                cc.setHandler((String) cm.get("handler"));
                if (cm.containsKey("key-content-type")) {
                    cc.setKeyContentType((String) cm.get("key-content-type"));
                }
                cc.setContentType((String) cm.get("content-type"));
                if (cm.containsKey("properties")) {
                    cc.setProperties((Map<String, Object>) cm.get("properties"));
                }
                consumers.add(cc);
            }
            config.setConsumers(consumers);
        }

        // Parse producers
        if (map.containsKey("producers")) {
            List<Map<String, Object>> producersList = (List<Map<String, Object>>) map.get("producers");
            List<DrProducerConfig> producers = new ArrayList<>();
            for (Map<String, Object> pm : producersList) {
                DrProducerConfig pc = new DrProducerConfig();
                pc.setTopic((String) pm.get("topic"));
                pc.setContentType((String) pm.get("content-type"));
                if (pm.containsKey("properties")) {
                    pc.setProperties((Map<String, Object>) pm.get("properties"));
                }
                producers.add(pc);
            }
            config.setProducers(producers);
        }

        // Parse health-check
        if (map.containsKey("health-check")) {
            Map<String, Object> hc = (Map<String, Object>) map.get("health-check");
            HealthCheckConfig hcc = new HealthCheckConfig();
            if (hc.containsKey("interval-ms")) hcc.setIntervalMs(((Number) hc.get("interval-ms")).longValue());
            if (hc.containsKey("timeout-ms")) hcc.setTimeoutMs(((Number) hc.get("timeout-ms")).longValue());
            if (hc.containsKey("failure-threshold")) hcc.setFailureThreshold(((Number) hc.get("failure-threshold")).intValue());
            if (hc.containsKey("recovery-threshold")) hcc.setRecoveryThreshold(((Number) hc.get("recovery-threshold")).intValue());
            config.setHealthCheck(hcc);
        }

        // Parse late-initializer
        if (map.containsKey("late-initializer")) {
            Map<String, Object> li = (Map<String, Object>) map.get("late-initializer");
            LateInitializerConfig lic = new LateInitializerConfig();
            if (li.containsKey("timeout-ms")) lic.setTimeoutMs(((Number) li.get("timeout-ms")).longValue());
            config.setLateInitializer(lic);
        }

        // Parse idempotency
        if (map.containsKey("idempotency")) {
            Map<String, Object> idem = (Map<String, Object>) map.get("idempotency");
            IdempotencyConfig ic = new IdempotencyConfig();
            if (idem.containsKey("ttl-seconds")) ic.setTtlSeconds(((Number) idem.get("ttl-seconds")).longValue());
            if (idem.containsKey("key-prefix")) ic.setKeyPrefix((String) idem.get("key-prefix"));
            config.setIdempotency(ic);
        }

        return config;
    }
}
