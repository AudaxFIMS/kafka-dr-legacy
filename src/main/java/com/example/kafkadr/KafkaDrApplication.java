package com.example.kafkadr;

import com.example.kafkadr.cluster.ClusterManager;
import com.example.kafkadr.config.ConfigLoader;
import com.example.kafkadr.config.KafkaDrConfig;
import com.example.kafkadr.consumer.DrConsumerFactory;
import com.example.kafkadr.consumer.DrConsumerManager;
import com.example.kafkadr.example.ExampleDrConsumer;
import com.example.kafkadr.handler.MessageHandlers;
import com.example.kafkadr.handler.MessageHandlerRegistry;
import com.example.kafkadr.idempotency.IdempotencyStore;
import com.example.kafkadr.producer.DrProducerManager;
import com.example.kafkadr.rest.RestServer;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Main entry point for the Kafka DR Legacy application.
 *
 * <p>Startup sequence (resilient — never blocks on unreachable clusters):
 * <ol>
 *   <li>Load YAML configuration</li>
 *   <li>Build the DR core ({@link KafkaDrRuntime}) and the YAML-driven {@link DrConsumerManager}</li>
 *   <li>Start the runtime: provision topics on reachable clusters, start health checks
 *       (first healthy cluster gets instant election) and late initialization of unreachable ones</li>
 *   <li>Start REST server</li>
 * </ol>
 */
public class KafkaDrApplication {

    private static final Logger log = LoggerFactory.getLogger(KafkaDrApplication.class);
    private static final int DEFAULT_REST_PORT = 8088;

    private KafkaDrRuntime runtime;
    private DrConsumerManager consumerManager;
    private RestServer restServer;
    private ExampleDrConsumer exampleConsumer;

    public void start(String configPath) {
        log.info("=== Kafka DR Application starting ===");

        // 1. Load config
        KafkaDrConfig config = ConfigLoader.load(configPath);
        log.info("Configuration loaded: {} clusters, {} consumers, {} producers",
                config.getClusters().size(),
                config.getConsumers() != null ? config.getConsumers().size() : 0,
                config.getProducers() != null ? config.getProducers().size() : 0);

        // 2. Build components (all clusters start UNHEALTHY)
        runtime = new KafkaDrRuntime(config);

        MessageHandlerRegistry handlerRegistry = new MessageHandlerRegistry();
        MessageHandlers.registerAll(handlerRegistry);

        consumerManager = new DrConsumerManager(config, handlerRegistry,
                runtime.getIdempotencyStore(), runtime.getGroupInstanceIds());
        // Before start(): a listener added after the first election would miss it
        runtime.getClusterManager().addListener(consumerManager);

        // 3. Provision topics, start health checks and late initialization
        runtime.start();

        // 4. Start REST server
        restServer = new RestServer(this);
        try {
            int port = Integer.parseInt(System.getProperty("rest.port",
                    System.getenv().getOrDefault("REST_PORT", String.valueOf(DEFAULT_REST_PORT))));
            restServer.start(port);
        } catch (Exception e) {
            log.error("Failed to start REST server", e);
        }

        // 5. Optional demo of an application-owned DR consumer loop:
        //    -Dexample.consumer.topic=demo-events [-Dexample.consumer.group=example-dr-group]
        startExampleConsumerIfRequested();

        log.info("=== Kafka DR Application started. Waiting for cluster health checks... ===");
    }

    private void startExampleConsumerIfRequested() {
        String topic = System.getProperty("example.consumer.topic");
        if (topic == null || topic.isBlank()) return;

        String group = System.getProperty("example.consumer.group", "example-dr-group");
        // Own idempotency prefix (= group): DrConsumerManager may consume the same topic with the
        // same store, and a shared prefix would make the two consumers skip each other's messages.
        exampleConsumer = new ExampleDrConsumer(runtime.getConsumerFactory(), runtime.getIdempotencyStore(),
                group, topic, group, new Properties());
        exampleConsumer.start();
        log.info("ExampleDrConsumer enabled: topic={}, group={}", topic, group);
    }

    public void stop() {
        log.info("=== Kafka DR Application shutting down ===");

        if (restServer != null) restServer.stop();
        if (exampleConsumer != null) exampleConsumer.close();
        // Runtime first: once switching has stopped, no switch can restart the YAML consumers
        if (runtime != null) runtime.close();
        if (consumerManager != null) consumerManager.stopConsumers();

        log.info("=== Kafka DR Application stopped ===");
    }

    // ─── Accessors ──────────────────────────────────────────────

    public KafkaDrRuntime getRuntime() {
        return runtime;
    }

    public ClusterManager getClusterManager() {
        return runtime.getClusterManager();
    }

    public DrProducerManager getProducerManager() {
        return runtime.getProducerManager();
    }

    public DrConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public DrConsumerFactory getConsumerFactory() {
        return runtime.getConsumerFactory();
    }

    public IdempotencyStore getIdempotencyStore() {
        return runtime.getIdempotencyStore();
    }

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : "kafka-dr.yml";

        KafkaDrApplication app = new KafkaDrApplication();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            try {
                app.stop();
            } finally {
                // Log4j's own shutdown hook is disabled (log4j2.xml) so that shutdown logs are not lost;
                // flush and stop logging only after every component has closed.
                LogManager.shutdown();
            }
        }, "shutdown-hook"));

        app.start(configPath);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            app.stop();
        }
    }
}
