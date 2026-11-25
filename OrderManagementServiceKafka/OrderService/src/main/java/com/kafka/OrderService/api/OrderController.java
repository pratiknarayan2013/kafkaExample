package com.kafka.OrderService.api;


import com.kafka.OrderService.event.OrderCreatedEvent;
import com.kafka.OrderService.service.OrderService;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderCreatedEvent createOrder(@RequestBody CreateOrderRequest request) {
        return orderService.createOrder(
                request.customerId(),
                request.itemId(),
                request.quantity(),
                request.total()
        );
    }

    public record CreateOrderRequest(
            @NotBlank String customerId,
            @NotBlank String itemId,
            @Min(1) int quantity,
            @DecimalMin("0.01") BigDecimal total
    ) {}

    @GetMapping("/ping")
    public String ping() {
        return "Server is running..";
    }
}