package com.example.kafkadr.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    @Test
    void shouldLoadConfigFromClasspath() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertNotNull(config);
        assertEquals(3, config.getClusters().size());
        assertTrue(config.getClusters().containsKey("primary"));
        assertTrue(config.getClusters().containsKey("secondary"));
        assertTrue(config.getClusters().containsKey("tertiary"));
    }

    @Test
    void shouldParseClusterPriorities() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertEquals(1, config.getClusters().get("primary").getPriority());
        assertEquals(2, config.getClusters().get("secondary").getPriority());
        assertEquals(3, config.getClusters().get("tertiary").getPriority());
    }

    @Test
    void shouldParseBootstrapServersWithDefaults() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertEquals("localhost:9092", config.getClusters().get("primary").getBootstrapServers());
        assertEquals("localhost:9094", config.getClusters().get("secondary").getBootstrapServers());
    }

    @Test
    void shouldParseConsumers() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertNotNull(config.getConsumers());
        assertEquals(4, config.getConsumers().size());

        ConsumerConfig first = config.getConsumers().get(0);
        assertEquals("demo-events", first.getTopic());
        assertEquals("dr-demo-group", first.getGroup());
        assertEquals("processDemoEvent", first.getHandler());
        assertEquals("string", first.getContentType());
    }

    @Test
    void shouldParseConsumerWithNativeProperties() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        ConsumerConfig payment = config.getConsumers().get(2);
        assertEquals("payment-events", payment.getTopic());
        assertEquals("native", payment.getContentType());
        assertNotNull(payment.getProperties());
        assertNotNull(payment.getProperties().get("configuration"));
    }

    @Test
    void shouldParseProducers() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertNotNull(config.getProducers());
        assertEquals(4, config.getProducers().size());
    }

    @Test
    void shouldParseHealthCheckConfig() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertEquals(5000, config.getHealthCheck().getIntervalMs());
        assertEquals(3000, config.getHealthCheck().getTimeoutMs());
        assertEquals(3, config.getHealthCheck().getFailureThreshold());
        assertEquals(3, config.getHealthCheck().getRecoveryThreshold());
    }

    @Test
    void shouldParseIdempotencyConfig() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");

        assertEquals(3600, config.getIdempotency().getTtlSeconds());
        assertEquals("idempotency", config.getIdempotency().getKeyPrefix());
    }

    @Test
    void shouldParseAutoCreateTopics() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml");
        assertTrue(config.isAutoCreateTopics());
    }
}
