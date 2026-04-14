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
- **Consumer binding management** -- only the active cluster's consumers are running; they are stopped and recreated on switch
- **Idempotent message processing** -- pluggable deduplication (`IdempotencyStore` interface) with in-memory TTL implementation; key format: `{prefix}:{topic}:{message_key}`
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
      ClusterManager.java                  # Failover engine (election, forceUnhealthy, failback)
      ClusterInfo.java                     # Runtime cluster state + health counters
      ClusterState.java                    # HEALTHY | UNHEALTHY | UNKNOWN
      ClusterSwitchListener.java           # Observer interface for failover events
      LateBindingInitializer.java          # Background recovery of unreachable clusters
    health/
      ClusterHealthChecker.java            # Periodic AdminClient health probe
      KafkaAdminHelper.java                # Shared utilities (probe, topic provisioning)
    consumer/
      DrConsumerManager.java               # Consumer lifecycle across cluster switches
    producer/
      DrProducerManager.java               # Resilient send with error classification + retry
      SendOutcome.java                     # Error classification enum
    handler/
      MessageHandler.java                  # Generic handler: MessageHandler<K, V>
      MessageHandlerRegistry.java          # Handler lookup + type caching
      HandlerTypeResolver.java             # Reflection-based K, V type extraction
      MessageHandlers.java                 # Example handlers for all content types
    idempotency/
      IdempotencyStore.java                # Interface (swap in Redis/DB implementation)
      InMemoryIdempotencyStore.java        # ConcurrentHashMap with TTL eviction
    serialization/
      ContentType.java                     # STRING | JSON | BYTES | NATIVE
      MessageDeserializer.java             # Content-type + target-type deserialization
      MessageSerializer.java               # Content-type serialization
    rest/
      RestServer.java                      # JDK HttpServer REST API
  resources/
    kafka-dr.yml                           # All configuration in one place
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

# 2. Build and run
mvn clean package -DskipTests
java -jar target/kafka-dr-legacy-1.0.0-SNAPSHOT.jar
```

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

### Idempotency

```yaml
kafka-dr:
  idempotency:
    ttl-seconds: 3600         # How long to remember processed message keys
    key-prefix: idempotency   # Prefix for deduplication keys
```

Idempotency key format: `{key-prefix}:{topic}:{message_key}`

Example: `idempotency:order-events:ORD-001`

The `IdempotencyStore` interface can be swapped for a Redis or DB implementation for multi-instance deployments.

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
  3. reelectActive() -> picks "secondary" (priority=2)
  4. ClusterSwitchListener notifications:
     - DrConsumerManager: stop primary consumers, start secondary consumers
     - DrProducerManager: close primary producers, create secondary producers
  5. Next producer.send() goes to secondary

Primary recovers:
  1. Health checker detects 3 consecutive successes
  2. reelectActive() -> picks "primary" (priority=1, healthy again)
  3. Consumers and producers switch back automatically
```

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
| **Cluster unavailable** | `TimeoutException`, `NetworkException`, `DisconnectException`, `BrokerNotAvailableException`, `ConnectException` | Immediate `forceUnhealthy()` + failover to next cluster |
| **Transient** | Any other exception | Retry up to `failure-threshold` times, then `forceUnhealthy()` + failover |

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
| Idempotency key = prefix + topic + message key | Natural deduplication based on business key; no extra headers needed |
| JDK HttpServer for REST | Zero-dependency HTTP server built into Java -- fits "legacy" philosophy |

## Tech Stack

- Java 17
- Apache Kafka Client 3.6.1
- Apache Avro 1.12.1
- Confluent Kafka Avro Serializer 8.2.0
- Jackson 2.16.1
- SnakeYAML 2.2
- SLF4J 2.0.11 + Logback 1.4.14
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
