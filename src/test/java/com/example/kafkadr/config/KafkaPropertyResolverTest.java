package com.example.kafkadr.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaPropertyResolverTest {

    private KafkaDrConfig config;

    @BeforeEach
    void setUp() {
        config = ConfigLoader.load("kafka-dr.yml");
    }

    @Test
    void basePropertiesShouldContainSchemaRegistryUrl() {
        Properties props = KafkaPropertyResolver.resolveBaseProperties(config, "primary");
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }

    @Test
    void basePropertiesShouldContainTimeouts() {
        Properties props = KafkaPropertyResolver.resolveBaseProperties(config, "primary");
        assertEquals("5000", props.getProperty("request.timeout.ms"));
        assertEquals("3000", props.getProperty("socket.connection.setup.timeout.ms"));
    }

    @Test
    void producerPropertiesShouldMergeAllLayers() {
        Properties props = KafkaPropertyResolver.resolveProducerProperties(config, "primary", null);

        // From default-properties
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
        // From default-producer-properties
        assertEquals("all", props.getProperty("acks"));
        assertEquals("5000", props.getProperty("max.block.ms"));
    }

    @Test
    void consumerPropertiesShouldMergeAllLayers() {
        Properties props = KafkaPropertyResolver.resolveConsumerProperties(config, "primary", null);

        // From default-properties
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
        // From default-consumer-properties
        assertEquals("500", props.getProperty("max.poll.records"));
    }

    @Test
    void perTopicConsumerPropertiesShouldOverrideDefaults() {
        ConsumerConfig paymentConsumer = config.getConsumers().stream()
                .filter(c -> c.getTopic().equals("payment-events"))
                .findFirst().orElseThrow();

        Properties props = KafkaPropertyResolver.resolveConsumerProperties(
                config, "primary", paymentConsumer);

        // Per-topic override
        assertEquals("io.confluent.kafka.serializers.KafkaAvroDeserializer",
                props.getProperty("value.deserializer"));
        assertEquals("true", props.getProperty("specific.avro.reader"));
        // Base layer still present
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }

    @Test
    void perTopicProducerPropertiesShouldOverrideDefaults() {
        ProducerConfig paymentProducer = config.getProducers().stream()
                .filter(p -> p.getTopic().equals("payment-events"))
                .findFirst().orElseThrow();

        Properties props = KafkaPropertyResolver.resolveProducerProperties(
                config, "primary", paymentProducer);

        assertEquals("io.confluent.kafka.serializers.KafkaAvroSerializer",
                props.getProperty("value.serializer"));
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }

    @Test
    void nullClusterNameShouldStillWork() {
        Properties props = KafkaPropertyResolver.resolveBaseProperties(config, null);
        assertEquals("http://localhost:8081", props.getProperty("schema.registry.url"));
    }
}
