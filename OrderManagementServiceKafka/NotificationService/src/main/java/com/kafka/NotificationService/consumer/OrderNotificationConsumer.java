package com.kafka.NotificationService.consumer;

import com.kafka.NotificationService.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationConsumer.class);

    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info(
                "[notification-service] Sending order confirmation to customerId={} for orderId={} (itemId={}, quantity={}, total={})",
                event.customerId(),
                event.orderId(),
                event.itemId(),
                event.quantity(),
                event.total()
        );
    }
}