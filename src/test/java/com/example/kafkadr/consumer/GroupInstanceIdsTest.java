package com.example.kafkadr.consumer;

import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GroupInstanceIdsTest {

    private KafkaDrConfig config;

    @BeforeEach
    void setUp() {
        config = new KafkaDrConfig();
        config.setInstanceId("node-1");
    }

    @Test
    void shouldBuildIdFromInstanceGroupAndTopic() {
        GroupInstanceIds ids = new GroupInstanceIds(config);

        assertEquals("node-1-orders-group-order-events", ids.claim("orders-group", List.of("order-events")));
    }

    @Test
    void shouldSortTopicsSoIdDoesNotDependOnSubscriptionOrder() {
        GroupInstanceIds ids = new GroupInstanceIds(config);
        String id = ids.claim("g", List.of("b-topic", "a-topic"));
        ids.release(id);

        assertEquals("node-1-g-a-topic.b-topic", id);
        assertEquals(id, ids.claim("g", List.of("a-topic", "b-topic")));
    }

    @Test
    void shouldReplaceCharactersNotAllowedByKafka() {
        config.setInstanceId("pod/1 east");
        GroupInstanceIds ids = new GroupInstanceIds(config);

        assertEquals("pod_1_east-my_group-t", ids.claim("my:group", List.of("t")));
    }

    @Test
    void shouldLimitLengthTo249() {
        GroupInstanceIds ids = new GroupInstanceIds(config);
        String id = ids.claim("g".repeat(300), List.of("t"));

        assertEquals(249, id.length());
    }

    @Test
    void shouldRefuseDuplicateUntilReleased() {
        GroupInstanceIds ids = new GroupInstanceIds(config);
        String id = ids.claim("g", List.of("t"));

        assertNull(ids.claim("g", List.of("t")));
        ids.release(id);
        assertEquals(id, ids.claim("g", List.of("t")));
    }

    @Test
    void shouldReturnNullWhenDisabled() {
        config.setStaticMembership(false);
        GroupInstanceIds ids = new GroupInstanceIds(config);

        assertNull(ids.getInstanceId());
        assertNull(ids.claim("g", List.of("t")));
    }

    @Test
    void shouldFallBackToHostnameWhenInstanceIdBlank() {
        config.setInstanceId("  ");
        GroupInstanceIds ids = new GroupInstanceIds(config);

        assertNotNull(ids.getInstanceId());
        assertFalse(ids.getInstanceId().isBlank());
    }

    @Test
    void shouldReadSettingsFromYaml() {
        KafkaDrConfig loaded = ConfigLoader.load("kafka-dr.yml");

        assertTrue(loaded.isStaticMembership());
        assertTrue(loaded.getInstanceId() == null || loaded.getInstanceId().isBlank()
                || System.getenv("KAFKA_DR_INSTANCE_ID") != null);
    }
}
