package dev.jay.payment_processor.controller;

import dev.jay.payment_processor.event.PaymentRequested;
import dev.jay.payment_processor.kafka.PaymentProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentProducer producer;

    @PostMapping
    public String createPayment(@RequestBody CreatePaymentRequest request) {
        String paymentId = request.paymentId() != null ? request.paymentId() : UUID.randomUUID().toString();

        PaymentRequested event = new PaymentRequested(
                paymentId,
                request.customerId(),
                request.amountCents(),
                request.currency(),
                Instant.now()
        );
        producer.send(event);
        return paymentId;
    }

    public record CreatePaymentRequest(String paymentId, String customerId, long amountCents, String currency) {
    }


}
