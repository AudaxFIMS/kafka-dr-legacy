package com.example.kafkadr.handler;

import com.example.kafkadr.avro.PaymentEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo message handlers. Each handler declares its type via {@code MessageHandler<T>}.
 * The framework resolves T automatically — no extra methods to override.
 */
public class DemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoHandlers.class);

    /** content-type: string → MessageHandler&lt;String&gt; */
    public static class ProcessDemoEvent implements MessageHandler<String> {
        @Override
        public void handle(MessageEnvelope<String> message) {
            String event = message.getValue();
            log.info("[processDemoEvent] cluster={}, topic={}, partition={}, offset={}, value='{}'",
                    message.getClusterName(), message.getTopic(),
                    message.getPartition(), message.getOffset(), event);
        }
    }

    /** content-type: json → MessageHandler&lt;JsonNode&gt; */
    public static class ProcessOrder implements MessageHandler<JsonNode> {
        @Override
        public void handle(MessageEnvelope<JsonNode> message) {
            JsonNode order = message.getValue();
            log.info("[processOrder] cluster={}, topic={}, partition={}, offset={}, orderId={}, items={}",
                    message.getClusterName(), message.getTopic(),
                    message.getPartition(), message.getOffset(),
                    order.path("orderId").asText(), order.path("items"));
        }
    }

    /** content-type: native → MessageHandler&lt;PaymentEvent&gt; (Avro generated class) */
    public static class ProcessPayment implements MessageHandler<PaymentEvent> {
        @Override
        public void handle(MessageEnvelope<PaymentEvent> message) {
            PaymentEvent payment = message.getValue();
            log.info("[processPayment] cluster={}, topic={}, partition={}, offset={}, " +
                            "paymentId={}, orderId={}, amount={} {}, status={}",
                    message.getClusterName(), message.getTopic(),
                    message.getPartition(), message.getOffset(),
                    payment.getPaymentId(), payment.getOrderId(),
                    payment.getAmount(), payment.getCurrency(), payment.getStatus());
        }
    }

    /** content-type: bytes → MessageHandler&lt;byte[]&gt; */
    public static class ProcessRawData implements MessageHandler<byte[]> {
        @Override
        public void handle(MessageEnvelope<byte[]> message) {
            byte[] data = message.getValue();
            log.info("[processRawData] cluster={}, topic={}, partition={}, offset={}, size={}",
                    message.getClusterName(), message.getTopic(),
                    message.getPartition(), message.getOffset(),
                    data != null ? data.length : 0);
        }
    }

    public static void registerAll(MessageHandlerRegistry registry) {
        registry.register("processDemoEvent", new ProcessDemoEvent());
        registry.register("processOrder", new ProcessOrder());
        registry.register("processPayment", new ProcessPayment());
        registry.register("processRawData", new ProcessRawData());
    }
}
