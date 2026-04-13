package com.example.kafkadr.rest;

import com.example.kafkadr.KafkaDrApplication;
import com.example.kafkadr.avro.PaymentEvent;
import com.example.kafkadr.avro.PaymentStatus;
import com.example.kafkadr.cluster.ClusterInfo;
import com.example.kafkadr.cluster.ClusterState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Lightweight REST server built on JDK's HttpServer (zero external dependencies).
 *
 * Endpoints:
 *   GET  /status                  — cluster states, active cluster, idempotency stats
 *   POST /produce/demo-events     — send a string message
 *   POST /produce/order-events    — send a JSON order
 *   POST /produce/payment-events  — send an Avro PaymentEvent
 *   POST /produce/raw-telemetry   — send raw bytes
 *   POST /produce/{topic}         — generic send (string body)
 */
public class RestServer {

    private static final Logger log = LoggerFactory.getLogger(RestServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaDrApplication app;
    private HttpServer server;

    public RestServer(KafkaDrApplication app) {
        this.app = app;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));

        server.createContext("/status", this::handleStatus);
        server.createContext("/produce/demo-events", ex -> handleProduce(ex, "demo-events"));
        server.createContext("/produce/order-events", ex -> handleProduce(ex, "order-events"));
        server.createContext("/produce/payment-events", ex -> handleProduce(ex, "payment-events"));
        server.createContext("/produce/raw-telemetry", ex -> handleProduce(ex, "raw-telemetry"));

        server.start();
        log.info("REST server started on port {}", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(2);
            log.info("REST server stopped");
        }
    }

    // ─── GET /status ────────────────────────────────────────────

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, errorJson("Method not allowed"));
            return;
        }

        ObjectNode root = mapper.createObjectNode();

        // Active cluster
        ClusterInfo active = app.getClusterManager().getActiveCluster();
        if (active != null) {
            ObjectNode activeNode = mapper.createObjectNode();
            activeNode.put("name", active.getName());
            activeNode.put("bootstrapServers", active.getBootstrapServers());
            activeNode.put("priority", active.getPriority());
            activeNode.put("state", active.getState().name());
            root.set("activeCluster", activeNode);
        } else {
            root.putNull("activeCluster");
        }

        // All clusters
        ArrayNode clustersArray = mapper.createArrayNode();
        for (ClusterInfo c : app.getClusterManager().getAllClusters()) {
            ObjectNode cn = mapper.createObjectNode();
            cn.put("name", c.getName());
            cn.put("bootstrapServers", c.getBootstrapServers());
            cn.put("priority", c.getPriority());
            cn.put("state", c.getState().name());
            cn.put("consecutiveFailures", c.getConsecutiveFailures());
            cn.put("consecutiveSuccesses", c.getConsecutiveSuccesses());
            clustersArray.add(cn);
        }
        root.set("clusters", clustersArray);

        // Idempotency stats
        ObjectNode idem = mapper.createObjectNode();
        idem.put("entries", app.getIdempotencyStore().size());
        root.set("idempotency", idem);

        sendResponse(exchange, 200, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
    }

    // ─── POST /produce/{topic} ──────────────────────────────────

    private void handleProduce(HttpExchange exchange, String topic) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, errorJson("Method not allowed. Use POST."));
            return;
        }

        if (app.getClusterManager().getActiveCluster() == null) {
            sendResponse(exchange, 503, errorJson("No active cluster available"));
            return;
        }

        String body = readBody(exchange);
        String idempotencyKey = UUID.randomUUID().toString();

        try {
            RecordMetadata metadata;

            switch (topic) {
                case "demo-events":
                    metadata = produceDemoEvent(body, idempotencyKey);
                    break;
                case "order-events":
                    metadata = produceOrderEvent(body, idempotencyKey);
                    break;
                case "payment-events":
                    metadata = producePaymentEvent(body, idempotencyKey);
                    break;
                case "raw-telemetry":
                    metadata = produceRawTelemetry(body, idempotencyKey);
                    break;
                default:
                    metadata = app.getProducerManager().send(topic, null, body, idempotencyKey);
                    break;
            }

            ObjectNode result = mapper.createObjectNode();
            result.put("status", "sent");
            result.put("topic", topic);
            result.put("idempotencyKey", idempotencyKey);
            result.put("cluster", app.getClusterManager().getActiveCluster().getName());
            if (metadata != null) {
                result.put("partition", metadata.partition());
                result.put("offset", metadata.offset());
            }

            sendResponse(exchange, 200, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        } catch (Exception e) {
            log.error("Failed to produce to topic={}", topic, e);
            sendResponse(exchange, 500, errorJson("Produce failed: " + e.getMessage()));
        }
    }

    // ─── topic-specific producers ───────────────────────────────

    private RecordMetadata produceDemoEvent(String body, String idempotencyKey) {
        // content-type: string — send body as-is
        String message = body.isEmpty() ? "demo-event-" + System.currentTimeMillis() : body;
        return app.getProducerManager().send("demo-events", null, message, idempotencyKey);
    }

    private RecordMetadata produceOrderEvent(String body, String idempotencyKey) {
        // content-type: json — parse or build a default order
        if (body.isEmpty()) {
            Map<String, Object> order = new HashMap<>();
            order.put("orderId", "ORD-" + UUID.randomUUID().toString().substring(0, 8));
            order.put("items", 3);
            order.put("total", 99.95);
            order.put("timestamp", System.currentTimeMillis());
            return app.getProducerManager().send("order-events", null, order, idempotencyKey);
        }
        return app.getProducerManager().send("order-events", null, body, idempotencyKey);
    }

    private RecordMetadata producePaymentEvent(String body, String idempotencyKey) {
        // content-type: native — build Avro PaymentEvent
        PaymentEvent payment;
        if (body.isEmpty()) {
            payment = PaymentEvent.newBuilder()
                    .setPaymentId("PAY-" + UUID.randomUUID().toString().substring(0, 8))
                    .setOrderId("ORD-" + UUID.randomUUID().toString().substring(0, 8))
                    .setAmount(49.99)
                    .setCurrency("USD")
                    .setStatus(PaymentStatus.PENDING)
                    .setTimestamp(System.currentTimeMillis())
                    .build();
        } else {
            try {
                JsonNode json = mapper.readTree(body);
                payment = PaymentEvent.newBuilder()
                        .setPaymentId(json.path("paymentId").asText("PAY-" + UUID.randomUUID().toString().substring(0, 8)))
                        .setOrderId(json.path("orderId").asText("ORD-unknown"))
                        .setAmount(json.path("amount").asDouble(0.0))
                        .setCurrency(json.path("currency").asText("USD"))
                        .setStatus(PaymentStatus.valueOf(json.path("status").asText("PENDING")))
                        .setTimestamp(json.path("timestamp").asLong(System.currentTimeMillis()))
                        .build();
            } catch (Exception e) {
                throw new RuntimeException("Invalid PaymentEvent JSON: " + e.getMessage(), e);
            }
        }
        return app.getProducerManager().send("payment-events", payment.getPaymentId().toString(), payment, idempotencyKey);
    }

    private RecordMetadata produceRawTelemetry(String body, String idempotencyKey) {
        // content-type: bytes
        byte[] data = body.isEmpty()
                ? ("telemetry-" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)
                : body.getBytes(StandardCharsets.UTF_8);
        return app.getProducerManager().send("raw-telemetry", null, data, idempotencyKey);
    }

    // ─── helpers ────────────────────────────────────────────────

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String errorJson(String message) {
        return "{\"error\":\"" + message.replace("\"", "'") + "\"}";
    }
}
