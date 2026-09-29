# Kafka Multi-Cluster Disaster Recovery (Legacy / No Spring)

A standalone Java application implementing **active-passive disaster recovery** across N Kafka clusters. The application automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

No Spring Boot, no Spring Cloud Stream -- plain Java 17 with the Kafka client library, SnakeYAML, Jackson, and a built-in JDK HTTP server.

> **Important: Cross-cluster replication is required.**
> This application handles failover at the *application level* -- switching producers and consumers between clusters. It does **not** replicate data between Kafka clusters. To ensure no messages are lost during failover, configure cross-cluster replication independently using [MirrorMaker 2](https://kafka.apache.org/documentation/#georeplication), Confluent Cluster Linking, or Confluent Replicator.

## Architecture

```
                        +---------------------+
                        |    Application       |
                        |                      |
                        | DrProducerManager ---+-----> Active Cluster
                        | DrConsumerManager <--+------ Active Cluster
                        |                      |
                        | ClusterManager       |
                        |   (failover engine)  |
                        |        |             |
                        | ClusterHealthChecker |
                        | LateBindingInit.     |
                        +--------|-------------+
                                 |
                +----------------+----------------+
                |                |                |
          +-----v------+  +-----v------+  +------v-----+
          |   Kafka    |  |   Kafka    |  |   Kafka    |
          |  Primary   |  | Secondary  |  |  Tertiary  |
          | priority=1 |  | priority=2 |  | priority=3 |
          +------------+  +------------+  +------------+
```

## Features

- **N-cluster support** -- configure any number of Kafka clusters with priority-based failover
- **Resilient startup** -- application starts instantly even if some clusters are down; unreachable clusters are initialized dynamically when they come online
- **Late binding initialization** -- background monitor provisions topics on recovered clusters without restart
- **Automatic failover** -- health checker detects failures; producer triggers instant failover on send failure with error classification (serialization / cluster unavailable / transient) and configurable retries
- **Automatic failback** -- returns to the highest-priority healthy cluster when it recovers
- **Instant first election** -- all clusters start as UNHEALTHY; first successful health check triggers immediate election without waiting for recovery threshold
- **Synchronous send with ACK** -- `sync: true` + `acks: all` ensures broker acknowledgement before returning
- **Asynchronous switch notifications** -- listeners are notified on a background pool with a per-listener serial queue; a slow listener never blocks health checks, producers, or other listeners; switches that happen while a listener is busy are coalesced
- **Consumer binding management** -- only the active cluster's consumers are running; they are stopped and recreated on switch
- **DR-aware consumer for your own poll loop** -- `DrKafkaConsumer` keeps the familiar `subscribe → poll → commit` API and transparently moves to the new active cluster
- **Idempotent message processing** -- pluggable deduplication (`IdempotencyStore` interface) with in-memory TTL implementation; key format: `{prefix}:{topic}:{id}`; every produced message carries a `message-id` header that survives cross-cluster replication
- **Multi-format support** -- String, JSON, Avro, and raw bytes payloads with per-topic configuration
- **Type-safe handlers** -- `MessageHandler<K, V>` works directly with `ConsumerRecord<K, V>`; both key and value types resolved automatically via reflection
- **Configurable key types** -- `key-content-type` per consumer for typed key deserialization (string, json, bytes, native)
- **REST API** -- built-in endpoints for status monitoring and test message production
- **Full SSL/SASL support** -- `default-properties.configuration` propagates to all Kafka clients including AdminClient (health checks, probes, topic provisioning)
- **Per-cluster property overrides** -- different SSL certs or SASL credentials per cluster/region
- **Fully dynamic configuration** -- clusters, consumers, and producers defined in YAML; no code changes needed

## Project Structure

```
src/main/
  avro/
    PaymentEvent.avsc                      # Avro schema (generates Java class)
  java/com/example/kafkadr/
    KafkaDrApplication.java                # Entry point, resilient startup orchestration
    config/
      ConfigLoader.java                    # YAML parsing with ${ENV:default} substitution
      KafkaDrConfig.java                   # Root configuration model
      KafkaPropertyResolver.java           # 4-layer property merge chain
      ClusterConfig.java                   # Cluster: bootstrap-servers, priority, properties
      DrConsumerConfig.java                # Consumer: topic, group, handler, content-type, key-content-type
      DrProducerConfig.java                # Producer: topic, content-type, properties
      HealthCheckConfig.java               # Health check tuning
      IdempotencyConfig.java               # Idempotency tuning
      LateInitializerConfig.java           # Late initializer tuning
    cluster/
      ClusterManager.java                  # Failover engine (election, forceUnhealthy, failback, async notifications)
      ClusterInfo.java                     # Runtime cluster state + health counters
      ClusterState.java                    # HEALTHY | UNHEALTHY | UNKNOWN
      ClusterSwitchListener.java           # Observer interface for failover events
      LateBindingInitializer.java          # Background recovery of unreachable clusters
    health/
      ClusterHealthChecker.java            # Periodic AdminClient health probe
      KafkaAdminHelper.java                # Shared utilities (probe, topic provisioning)
    consumer/
      DrConsumerManager.java               # Consumer lifecycle across cluster switches (YAML-configured handlers)
      DrKafkaConsumer.java                 # DR-aware KafkaConsumer replacement for your own poll loop
      DrConsumerFactory.java               # Creates DrKafkaConsumer instances
    producer/
      DrProducerManager.java               # Resilient send with error classification, retry, message-id header
      SendOutcome.java                     # Error classification enum
    handler/
      MessageHandler.java                  # Generic handler: MessageHandler<K, V>
      MessageHandlerRegistry.java          # Handler lookup + type caching
      HandlerTypeResolver.java             # Reflection-based K, V type extraction
      MessageHandlers.java                 # Example handlers for all content types
    idempotency/
      IdempotencyStore.java                # Interface (swap in Redis/DB implementation)
      InMemoryIdempotencyStore.java        # ConcurrentHashMap with TTL eviction
      IdempotencyKeys.java                 # Cluster-independent dedup keys (message-id header / record key)
    serialization/
      ContentType.java                     # STRING | JSON | BYTES | NATIVE
      MessageDeserializer.java             # Content-type + target-type deserialization
      MessageSerializer.java               # Content-type serialization
    rest/
      RestServer.java                      # JDK HttpServer REST API
    example/
      ExampleDrConsumer.java               # Template: own poll loop on DrKafkaConsumer + dedup + per-record commit
  resources/
    kafka-dr.yml                           # All configuration in one place
    log4j2.xml                             # Logging configuration
docker-compose.yml                         # 3 Kafka clusters + Schema Registry + Kafka UI
scripts/e2e-test.sh                        # End-to-end test script
```

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose

### Run

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Build and run (self-contained jar with all dependencies, built by maven-shade-plugin)
mvn clean package -DskipTests
java -jar target/kafka-dr-legacy-1.0.0-SNAPSHOT.jar

# Optional: also start the example application-owned DR consumer
java -Dexample.consumer.topic=demo-events -jar target/kafka-dr-legacy-1.0.0-SNAPSHOT.jar
```

On shutdown (Ctrl+C / SIGTERM) all components are closed first, then logging is flushed -- Log4j's own shutdown hook is disabled in `log4j2.xml` so the final shutdown logs are not lost.

### Test All Payload Types

```bash
# String
curl -X POST localhost:8088/produce/demo-events -d "hello world"

# JSON
curl -X POST localhost:8088/produce/order-events \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-001","items":2,"total":999.99}'

# Avro (send as JSON, server builds PaymentEvent)
curl -X POST localhost:8088/produce/payment-events \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"PAY-001","orderId":"ORD-001","amount":999.99,"currency":"USD","status":"COMPLETED"}'

# Raw bytes
curl -X POST localhost:8088/produce/raw-telemetry -d "sensor-42:temp=36.6"

# Cluster status
curl -s localhost:8088/status | python3 -m json.tool
```

### Test Failover

```bash
# Kill primary cluster
docker compose stop kafka-primary

# Send a message -- triggers instant failover to secondary
curl -X POST localhost:8088/produce/demo-events -d "after failover"

# Verify switch
curl -s localhost:8088/status | python3 -m json.tool

# Restore primary -- auto failback after recovery threshold
docker compose start kafka-primary

# Cascade failover -- kill two clusters
docker compose stop kafka-primary kafka-secondary
curl -X POST localhost:8088/produce/demo-events -d "on tertiary"
docker compose start kafka-primary kafka-secondary
```

### Test Startup with Dead Cluster

```bash
# Stop primary before starting the app
docker compose stop kafka-primary

# Start the app -- starts instantly on secondary, no blocking
java -jar target/kafka-dr-legacy-1.0.0-SNAPSHOT.jar

# Verify: app is on secondary
curl -s localhost:8088/status | python3 -m json.tool

# Restore primary -- app auto-initializes and fails back
docker compose start kafka-primary
```

### Run E2E Test Script

```bash
./scripts/e2e-test.sh
```

## Configuration

Everything is configured under the `kafka-dr` prefix in `kafka-dr.yml`. Environment variables are supported with `${VAR:default}` syntax.

### Clusters

```yaml
kafka-dr:
  clusters:
    us-east:
      bootstrap-servers: kafka-us-east:9092
      priority: 1                    # Lowest value = highest priority
      properties:                    # Per-cluster overrides
        configuration:
          ssl.truststore.location: /certs/us-east-truststore.p12
    eu-west:
      bootstrap-servers: kafka-eu-west:9092
      priority: 2
```

### Default Properties

Base Kafka client properties applied to **all** clients: producers, consumers, and AdminClients (health checks, probes, topic provisioning). SSL, SASL, timeouts, Schema Registry -- configure once, used everywhere:

```yaml
kafka-dr:
  default-properties:
    configuration:
      reconnect.backoff.ms: 1000
      request.timeout.ms: 5000
      schema.registry.url: http://localhost:8081
      # SSL
      security.protocol: SSL
      ssl.truststore.location: /certs/truststore.p12
      ssl.truststore.password: ${SSL_TRUSTSTORE_PASSWORD:changeit}
      # SASL
      # security.protocol: SASL_SSL
      # sasl.mechanism: PLAIN
      # sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="secret";
```

### Property Resolution Chain

Properties are merged in 4 layers (each overrides the previous):

```
1. default-properties.configuration          -- base (SSL, SASL, timeouts)
2. clusters.{name}.properties.configuration  -- per-cluster overrides
3. default-consumer/producer-properties.configuration  -- role defaults
4. consumers/producers[].properties.configuration      -- per-topic overrides
```

### Consumers

Each consumer defines a topic, consumer group, handler name, and content types for key and value:

```yaml
kafka-dr:
  default-consumer-properties:
    configuration:
      max.poll.records: 500

  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder          # Name in MessageHandlerRegistry
      key-content-type: string       # Key type: string (default) | json | bytes | native
      content-type: json             # Value type: string | json | bytes | native

    - topic: payment-events
      group: my-group
      handler: processPayment
      content-type: native
      properties:
        configuration:
          value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
          specific.avro.reader: "true"
```

**Content types:**

| Type | Java type | Use case |
|---|---|---|
| `string` | `String` | Plain text messages |
| `json` | `JsonNode` or any Jackson POJO | JSON payloads |
| `native` | Kafka deserializer output (Avro, Protobuf) | Schema Registry payloads |
| `bytes` | `byte[]` | Binary data |

### Producers

```yaml
kafka-dr:
  default-producer-properties:
    sync: true                        # Block until broker ACK
    configuration:
      acks: all
      max.block.ms: 5000
      delivery.timeout.ms: 10000
      request.timeout.ms: 5000

  producers:
    - topic: order-events
      content-type: json
    - topic: payment-events
      content-type: native
      properties:
        configuration:
          value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

### Health Check & Failover Tuning

```yaml
kafka-dr:
  health-check:
    interval-ms: 5000        # How often to probe each cluster
    timeout-ms: 3000          # AdminClient timeout per probe
    failure-threshold: 3      # Consecutive failures before marking unhealthy
                              # Also used as retry count for transient send errors
    recovery-threshold: 3     # Consecutive successes before marking healthy
                              # (bypassed for initial election)
```

### Late Initializer

```yaml
kafka-dr:
  late-initializer:
    timeout-ms: 3000          # Probe timeout for unreachable clusters
```

### Consumer Static Membership

```yaml
kafka-dr:
  instance-id: ${KAFKA_DR_INSTANCE_ID:}                   # empty → hostname
  static-membership: ${KAFKA_DR_STATIC_MEMBERSHIP:true}
```

Every consumer (`DrConsumerManager` workers and `DrKafkaConsumer`) gets `group.instance.id = {instance-id}-{group}-{topics}` -- the same id on every cluster.

Why: when a cluster dies, its consumers are closed without being able to leave the group. When the cluster comes back (failback), the broker keeps those dead members until `session.timeout.ms` (45s by default) and new consumers get **no partitions** meanwhile. A static member rejoining with the same id replaces its old registration immediately. Measured on the docker-compose setup: failback from switch to partition assignment **~1s** (vs ~29s with dynamic membership); app restart ~1s.

Rules and trade-offs:

- `instance-id` must be **unique per running application instance** and **stable across restarts** (hostname, StatefulSet pod name, `KAFKA_DR_INSTANCE_ID`). A Deployment pod name changes on restart -- still correct, just no fast rejoin after restarts.
- Two live consumers with the same id fence each other (`FencedInstanceIdException`). Inside one process ids are claimed; a duplicate (same group + topics twice) falls back to dynamic membership with a WARN. Across instances this is guaranteed only by a unique `instance-id`.
- An explicit `group.instance.id` in per-consumer properties (YAML or `DrKafkaConsumer` props) is kept as is. Do not put it into `default-consumer-properties` -- all consumers would share it.
- Static members do not leave the group on shutdown. For a single instance that is what makes restart fast; when an instance is removed for good (scale-down), its partitions are reassigned only after `session.timeout.ms`.
- Disable with `KAFKA_DR_STATIC_MEMBERSHIP=false`.

### Idempotency

```yaml
kafka-dr:
  idempotency:
    ttl-seconds: 3600         # How long to remember processed message keys
    key-prefix: idempotency   # Prefix for deduplication keys
```

Idempotency key format: `{key-prefix}:{topic}:{id}`, where `id` is (via `IdempotencyKeys.of`):

1. the `message-id` header -- `DrProducerManager` adds a UUID to every message (generated once per `send`, so retries and failover resends carry the same id);
2. otherwise the record key (`byte[]` keys are Base64-encoded);
3. otherwise `null` -- the record cannot be deduplicated.

Example: `idempotency:order-events:3f1c2a9e-...`

Partition and offset are never part of the key: they differ between replicated clusters, while headers, key and value are preserved by MirrorMaker 2 / Cluster Linking.

`IdempotencyStore` API:

| Method | Semantics |
|---|---|
| `isProcessed(key)` | Read-only check; use in your own loop **before** processing |
| `markProcessed(key)` | Call **after** successful processing |
| `isDuplicate(key)` | Legacy check-and-mark in one call -- marks the key even if processing later fails; prefer the two calls above |

Both `DrConsumerManager` (YAML-configured handlers) and `DrKafkaConsumer` loops use `IdempotencyKeys` + `isProcessed`/`markProcessed`. In `DrConsumerManager` the fallback key is the *deserialized* record key (per `key-content-type`); records with neither a `message-id` header nor a key are processed without deduplication.

`ttl-seconds` must cover the whole window in which re-reads are possible: failure detection time + consumer offset sync lag between clusters.

The in-memory store works per instance. For multi-instance deployments swap in a Redis or DB implementation -- after failover, partitions may be assigned to a different instance.

## Adding Business Logic

### 1. Create a handler

Handlers work directly with Kafka's `ConsumerRecord<K, V>` -- full access to key, value, headers, timestamp, and all metadata:

```java
public class ProcessOrder implements MessageHandler<String, JsonNode> {
    @Override
    public void handle(ConsumerRecord<String, JsonNode> record) {
        String orderId = record.key();           // typed String
        JsonNode order = record.value();         // typed JsonNode

        // Full Kafka record access
        Headers headers = record.headers();
        long timestamp = record.timestamp();
        int partition = record.partition();
        long offset = record.offset();
    }
}
```

Both `K` and `V` are resolved automatically via reflection from the generic parameters. No extra methods to override.

### 2. Register the handler

```java
handlerRegistry.register("processOrder", new ProcessOrder());
```

### 3. Add consumer and producer in `kafka-dr.yml`

```yaml
kafka-dr:
  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder
      content-type: json

  producers:
    - topic: order-events
      content-type: json
```

### 4. Send messages

```java
producerManager.send("order-events", "ORD-001", orderData);
```

### Own Consumer Loop (`DrKafkaConsumer`)

If your code already has a plain `KafkaConsumer` loop with its own topic/group parameters, replace `new KafkaConsumer<>(props)` with the DR factory -- the rest of the loop stays the same:

```java
DrConsumerFactory factory = app.getConsumerFactory();

Properties props = new Properties();
props.put(ConsumerConfig.GROUP_ID_CONFIG, "legacy-order-group");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
// bootstrap.servers and SSL/SASL come from kafka-dr.yml

try (DrKafkaConsumer<String, String> consumer = factory.create(props)) {
    consumer.subscribe(List.of("order-events"));
    while (running) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
            String dedupKey = IdempotencyKeys.of(keyPrefix, r);
            if (dedupKey != null && idempotencyStore.isProcessed(dedupKey)) {
                continue;                                   // re-read after failover
            }
            if (process(r)) {
                if (dedupKey != null) idempotencyStore.markProcessed(dedupKey);
                consumer.commitAsync(Map.of(
                        new TopicPartition(r.topic(), r.partition()),
                        new OffsetAndMetadata(r.offset() + 1)), null);
            }
            // failure: record is skipped
        }
    }
}
```

A complete template is in `example/ExampleDrConsumer.java` (own thread, graceful `close()`, overridable `process()`).

Run it inside the app with a system property (disabled by default):

```bash
java -Dexample.consumer.topic=demo-events [-Dexample.consumer.group=example-dr-group] ...
```

The example uses its group name as the idempotency prefix. `DrConsumerManager` also consumes `demo-events` with the same `IdempotencyStore`; with a shared prefix the two consumers would skip each other's messages -- give every independent consumer its own prefix.

Unit tests without Kafka: pass a `MockConsumer` to the factory -- `new DrConsumerFactory(clusterManager, config, props -> new MockConsumer<>(OffsetResetStrategy.EARLIEST))` with `new ClusterManager(config, Runnable::run)` for synchronous switches (see `ExampleDrConsumerTest`).

Rules:

- **Properties** -- merged as `default-properties → cluster → default-consumer-properties → your props`; `bootstrap.servers` is always taken from the active cluster.
- **Deserializers as class names, not instances** -- `KafkaConsumer.close()` closes deserializer instances, and the consumer is recreated on every switch.
- **Single thread** -- like `KafkaConsumer`, use one `DrKafkaConsumer` from one thread.
- **Commit per record, not `commitAsync()` without arguments** -- the no-arg form commits the position after the *whole* polled batch; if the cluster fails mid-batch, the unprocessed tail is committed and (after offset translation) skipped on the failover cluster.

What happens on failover:

```
1. ClusterManager elects the new cluster; DrKafkaConsumer only records the target
   and calls wakeup() (KafkaConsumer is not thread-safe)
2. Your thread: the current poll()/commitSync() is interrupted by the wakeup;
   in-flight commitAsync() calls to the dead cluster fail quietly (DEBUG log)
3. Inside poll(): old consumer closed (1s), new one created on the new cluster,
   same subscription -- this poll returns empty records
4. Next polls: join the group on the new cluster, read from its committed offsets;
   records re-read because of offset-sync lag are skipped by isProcessed()
```

Safety guarantees of the wrapper:

| Situation | Behavior |
|---|---|
| `commitAsync(Map)` with offsets polled before the switch | Dropped with WARN -- offsets of one cluster are meaningless on another |
| `commitAsync()` / `commitSync()` right after the switch | Safe -- the new consumer has no positions yet |
| `commitSync()` interrupted by a switch | Does not throw; commit is skipped |
| Rebalance listener during a switch | `onPartitionsLost` instead of `onPartitionsRevoked` (no commits to a dead cluster) |
| Your own `wakeup()` | Propagated as `WakeupException`, as with `KafkaConsumer` |
| No active cluster yet | `poll()` returns empty records |

### Handler Examples

```java
// String key, String value
public class ProcessDemoEvent implements MessageHandler<String, String> {
    public void handle(ConsumerRecord<String, String> record) {
        String event = record.value();
    }
}

// String key, Avro value
public class ProcessPayment implements MessageHandler<String, PaymentEvent> {
    public void handle(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent payment = record.value();
        payment.getAmount();
    }
}

// String key, raw bytes
public class ProcessRawData implements MessageHandler<String, byte[]> {
    public void handle(ConsumerRecord<String, byte[]> record) {
        byte[] data = record.value();
    }
}

// Long key, JsonNode value (with key-content-type: json in YAML)
public class ProcessSensor implements MessageHandler<Long, JsonNode> {
    public void handle(ConsumerRecord<Long, JsonNode> record) {
        Long sensorId = record.key();
        JsonNode reading = record.value();
    }
}
```

## How Failover Works

### Startup

```
1. ConfigLoader parses kafka-dr.yml with environment variable substitution
2. KafkaDrApplication probes all clusters (3s timeout each, non-blocking)
3. Reachable clusters: topics provisioned immediately
4. Unreachable clusters: logged as WARNING, skipped
5. ClusterManager initializes all clusters as UNHEALTHY
6. ClusterHealthChecker starts -- probes every 5s
7. First successful health check: instant election (no recovery threshold)
8. ClusterSwitchListener -> consumers started, producers created on elected cluster
9. LateBindingInitializer monitors unreachable clusters in background
```

### Normal Failover

```
Normal operation:
  Producer  ---> primary (priority=1)
  Consumers: primary STARTED, secondary STOPPED

Primary goes down:
  1. Health checker detects failure (3 consecutive failures)
     OR producer send fails (instant failover via forceUnhealthy)
  2. ClusterManager.forceUnhealthy("primary")
     - Bypasses failure threshold
     - Sets state to UNHEALTHY immediately
  3. reelectActive() -> picks "secondary" (priority=2); getActiveCluster() changes immediately
  4. ClusterSwitchListener notifications (asynchronous, each listener in its own serial queue):
     - DrConsumerManager: stop primary consumers, start secondary consumers
     - DrProducerManager: create secondary producers, swap, close primary producers
     - DrKafkaConsumer:   wakeup -> recreated on secondary inside the owner's poll()
  5. Next producer.send() goes to secondary

Primary recovers:
  1. Health checker detects 3 consecutive successes
  2. reelectActive() -> picks "primary" (priority=1, healthy again)
  3. Consumers and producers switch back automatically
```

### Switch Notifications

`ClusterManager` never calls listeners on the caller's thread. Health checks and `forceUnhealthy()` return immediately; listeners run on the `cluster-switch-notifier-N` daemon pool:

- **Per-listener serial queue** -- events for one listener are delivered in order, never concurrently.
- **Isolation** -- a slow listener (e.g. `DrConsumerManager` stopping consumers, up to 10s) does not delay others.
- **Coalescing** -- if more switches happen while a listener is busy, it receives only the latest target: `primary → secondary → tertiary` is delivered as `primary → tertiary`; `primary → secondary → primary` is not delivered at all (the listener is already on primary).
- A listener added after the initial election is notified only about subsequent switches -- read `getActiveCluster()` for the current state (`DrKafkaConsumer` does this).
- For tests, pass a synchronous executor: `new ClusterManager(config, Runnable::run)`.

### Late Cluster Initialization

```
1. LateBindingInitializer probes unreachable clusters periodically
2. Cluster responds -> provision topics via KafkaAdminHelper
3. Cluster marked as initialized (available for failover/failback)
4. When all clusters initialized, LateBindingInitializer stops itself
```

### Producer Error Handling

`DrProducerManager` classifies send errors into three categories:

| Error type | Examples | Behavior |
|---|---|---|
| **Serialization** | `SerializationException` | Fatal -- throw immediately, no retry or failover |
| **Cluster unavailable** | `TimeoutException`, `NetworkException`, `DisconnectException`, `BrokerNotAvailableException`, `NotLeaderOrFollowerException`, `ConnectException` | Immediate `forceUnhealthy()`, wait (up to 10s) until producers are recreated on the next cluster, resend there; fails immediately if no healthy cluster is left |
| **Transient** | Any other exception | Retry up to `failure-threshold` times, then `forceUnhealthy()` + failover |

On switch, new producers are created *before* the old ones are closed and swapped in atomically, so concurrent `send()` calls never see an empty producer map. Resends after failover carry the same `message-id` header.

## REST API

Built-in HTTP server on port 8088 (configurable via `REST_PORT` env var or `-Drest.port` system property):

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/status` | Active cluster, all cluster states with health counters, idempotency stats |
| `POST` | `/produce/demo-events` | Send string message (empty body generates default) |
| `POST` | `/produce/order-events` | Send JSON order (empty body generates default) |
| `POST` | `/produce/payment-events` | Send Avro PaymentEvent (JSON body or empty for default) |
| `POST` | `/produce/raw-telemetry` | Send raw bytes |

### Example `/status` Response

```json
{
  "activeCluster": {
    "name": "primary",
    "bootstrapServers": "localhost:9092",
    "priority": 1,
    "state": "HEALTHY"
  },
  "clusters": [
    {
      "name": "primary",
      "state": "HEALTHY",
      "consecutiveFailures": 0,
      "consecutiveSuccesses": 12
    },
    {
      "name": "secondary",
      "state": "HEALTHY",
      "consecutiveFailures": 0,
      "consecutiveSuccesses": 8
    }
  ],
  "idempotency": {
    "entries": 42
  }
}
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| No Spring framework | Minimal dependencies, full lifecycle control, suitable for legacy environments |
| All clusters start UNHEALTHY | First health check immediately elects the first reachable cluster; subsequent recovery requires full threshold |
| Instant failover from producer | Producer detects failures before health checker and forces immediate re-election via `forceUnhealthy()` |
| `MessageHandler<K, V>` with `ConsumerRecord` | Handlers work directly with Kafka's native API -- full access to headers, timestamps, metadata without wrappers |
| Reflection-based type resolution | `HandlerTypeResolver` extracts both `K` and `V` from handler's generic parameters -- no boilerplate |
| Non-blocking startup | Cluster probing with timeout ensures app starts even if all brokers are down |
| `KafkaPropertyResolver` 4-layer merge | SSL, SASL, and all Kafka properties propagate consistently to producers, consumers, and AdminClients |
| Per-cluster property overrides | Different SSL certs or SASL credentials per region via `clusters.{name}.properties.configuration` |
| SR on single cluster | Independent clusters have separate `CLUSTER_ID`s; SR's `_schemas` topic lives on one cluster. All clients share the same SR endpoint via `schema.registry.url` |
| `IdempotencyStore` interface | In-memory implementation for single-instance; swap in Redis (`SET NX EX`) or DB for multi-instance |
| Idempotency key = prefix + topic + `message-id` header | Stable across replicated clusters (unlike partition/offset); falls back to the record key |
| Asynchronous, per-listener notifications | Failover detection is never blocked by slow listeners; coalescing avoids needless consumer restarts |
| `DrKafkaConsumer` switches inside `poll()` | `KafkaConsumer` is single-threaded; the switch thread only signals via `wakeup()` |
| Static membership (`group.instance.id`) | Consumers closed on a dead cluster cannot leave their group; on failback a static member replaces its dead registration instantly instead of waiting `session.timeout.ms` |
| JDK HttpServer for REST | Zero-dependency HTTP server built into Java -- fits "legacy" philosophy |

## Tech Stack

- Java 17
- Apache Kafka Client 3.6.1
- Apache Avro 1.12.1
- Confluent Kafka Avro Serializer 8.2.0
- Jackson 2.16.1
- SnakeYAML 2.2
- SLF4J 2.0.11 + Log4j2 2.25.3
- JUnit 5.10.1 + Mockito 5.8.0

## Infrastructure (Docker Compose)

| Service | Port | Description |
|---|---|---|
| `kafka-primary` | 9092 | KRaft broker, priority 1 |
| `kafka-secondary` | 9094 | KRaft broker, priority 2 |
| `kafka-tertiary` | 9096 | KRaft broker, priority 3 |
| `schema-registry` | 8081 | Confluent Schema Registry |
| `kafka-ui` | 8080 | Web UI for monitoring all clusters |

All Kafka brokers run in KRaft mode (no ZooKeeper).

## License

MIT
