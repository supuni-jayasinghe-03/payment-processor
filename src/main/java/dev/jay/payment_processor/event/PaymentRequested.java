package dev.jay.payment_processor.event;

import java.time.Instant;

public record PaymentRequested(
        String paymentId,
        String customerId,
        long amountCents,
        String currency,
        Instant requestedAt
) {
}
