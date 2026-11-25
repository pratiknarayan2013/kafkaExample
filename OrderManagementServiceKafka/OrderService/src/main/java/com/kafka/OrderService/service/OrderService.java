package com.kafka.OrderService.service;


import com.kafka.OrderService.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    public static final String ORDER_CREATED_TOPIC = "order.created";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public OrderCreatedEvent createOrder(String customerId, String itemId, int quantity, BigDecimal total) {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                customerId,
                itemId,
                quantity,
                total,
                Instant.now()
        );

        // Using orderId as the key keeps events for the same order on the same partition
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(ORDER_CREATED_TOPIC, orderId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish OrderCreatedEvent for orderId={}", orderId, ex);
            } else if (result != null) {
                var meta = result.getRecordMetadata();
                log.info("Published OrderCreatedEvent orderId={} to topic={} partition={} offset={}",
                        orderId, meta.topic(), meta.partition(), meta.offset());
            }
        });

        return event;
    }
}
