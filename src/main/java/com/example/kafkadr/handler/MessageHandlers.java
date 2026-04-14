package com.example.kafkadr.handler;

import com.example.kafkadr.avro.PaymentEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo message handlers. Each handler works directly with {@link ConsumerRecord ConsumerRecord&lt;K, V&gt;}
 * — full access to key, value, headers, timestamp, and all Kafka metadata.
 */
public class MessageHandlers {

    private static final Logger log = LoggerFactory.getLogger(MessageHandlers.class);

    /** key: string, value: string */
    public static class ProcessDemoEvent implements MessageHandler<String, String> {
        @Override
        public void handle(ConsumerRecord<String, String> record) {
            log.info("[processDemoEvent] topic={}, partition={}, offset={}, ts={}, key={}, value='{}'",
                    record.topic(), record.partition(), record.offset(),
                    record.timestamp(), record.key(), record.value());
        }
    }

    /** key: string, value: json */
    public static class ProcessOrder implements MessageHandler<String, JsonNode> {
        @Override
        public void handle(ConsumerRecord<String, JsonNode> record) {
            JsonNode order = record.value();
            log.info("[processOrder] topic={}, partition={}, offset={}, key={}, orderId={}, items={}",
                    record.topic(), record.partition(), record.offset(),
                    record.key(), order.path("orderId").asText(), order.path("items"));
        }
    }

    /** key: string, value: native (Avro PaymentEvent) */
    public static class ProcessPayment implements MessageHandler<String, PaymentEvent> {
        @Override
        public void handle(ConsumerRecord<String, PaymentEvent> record) {
            PaymentEvent payment = record.value();
            log.info("[processPayment] topic={}, partition={}, offset={}, " +
                            "paymentId={}, orderId={}, amount={} {}, status={}",
                    record.topic(), record.partition(), record.offset(),
                    payment.getPaymentId(), payment.getOrderId(),
                    payment.getAmount(), payment.getCurrency(), payment.getStatus());
        }
    }

    /** key: string, value: bytes */
    public static class ProcessRawData implements MessageHandler<String, byte[]> {
        @Override
        public void handle(ConsumerRecord<String, byte[]> record) {
            byte[] data = record.value();
            log.info("[processRawData] topic={}, partition={}, offset={}, key={}, size={}",
                    record.topic(), record.partition(), record.offset(),
                    record.key(), data != null ? data.length : 0);
        }
    }

    public static void registerAll(MessageHandlerRegistry registry) {
        registry.register("processDemoEvent", new ProcessDemoEvent());
        registry.register("processOrder", new ProcessOrder());
        registry.register("processPayment", new ProcessPayment());
        registry.register("processRawData", new ProcessRawData());
    }
}
