package com.kafka.OrderService.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String itemId,
        int quantity,
        BigDecimal total,
        Instant createdAt
) {}