package dev.jay.payment_processor.event;

import java.time.Instant;

public record PaymentCompleted(
        String paymentId,
        String providerReference,
        long amountCents,
        String currency,
        Instant completedAt
) {
}
