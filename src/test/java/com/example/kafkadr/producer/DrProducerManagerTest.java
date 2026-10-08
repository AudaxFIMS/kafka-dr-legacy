package com.example.kafkadr.producer;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DrProducerManagerTest {

    private static final Serializer<Object> VALUE_SERIALIZER =
            (topic, data) -> data == null ? null : data.toString().getBytes(StandardCharsets.UTF_8);

    private ClusterManager clusterManager;
    private DrProducerManager producerManager;
    private final List<MockProducer<String, Object>> created = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        KafkaDrConfig config = ConfigLoader.load("kafka-dr.yml"); // sync: true
        clusterManager = new ClusterManager(config, Runnable::run);
        producerManager = new DrProducerManager(config, clusterManager, props -> {
            MockProducer<String, Object> producer =
                    new MockProducer<>(true, new StringSerializer(), VALUE_SERIALIZER);
            created.add(producer);
            return producer;
        });
        clusterManager.addListener(producerManager);
    }

    @AfterEach
    void tearDown() {
        producerManager.stop();
        clusterManager.stop();
    }

    @Test
    void sendBeforeElectionWaitsForFirstCluster() throws Exception {
        CompletableFuture<RecordMetadata> send =
                CompletableFuture.supplyAsync(() -> producerManager.send("demo-events", "k", "v"));

        Thread.sleep(200);
        assertFalse(send.isDone(), "send must wait for the election instead of failing");

        clusterManager.reportHealthy("primary"); // instant initial election → producers created

        RecordMetadata metadata = send.get(5, TimeUnit.SECONDS);
        assertNotNull(metadata);
        assertEquals("primary", producerManager.getCurrentCluster().getName());
        long sent = created.stream().mapToLong(p -> p.history().size()).sum();
        assertEquals(1, sent);
    }

    @Test
    void awaitActiveReturnsImmediatelyOnceElected() {
        clusterManager.reportHealthy("secondary");

        assertTrue(producerManager.awaitActive(0));
    }

    @Test
    void awaitActiveTimesOutWithoutCluster() {
        long start = System.nanoTime();

        assertFalse(producerManager.awaitActive(100));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 100);
    }

    @Test
    void stopWakesUpWaitingSenders() throws Exception {
        CompletableFuture<Boolean> waiting =
                CompletableFuture.supplyAsync(() -> producerManager.awaitActive(10_000));

        Thread.sleep(100);
        producerManager.stop();

        assertFalse(waiting.get(2, TimeUnit.SECONDS));
    }
}
