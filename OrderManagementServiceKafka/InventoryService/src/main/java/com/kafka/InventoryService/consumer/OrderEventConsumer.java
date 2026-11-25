package com.kafka.InventoryService.consumer;

import com.kafka.InventoryService.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info(
                "[inventory-service] Reserving quantity={} for itemId={} on orderId={} (customerId={}, total={})",
                event.quantity(),
                event.itemId(),
                event.orderId(),
                event.customerId(),
                event.total()
        );
    }
}
