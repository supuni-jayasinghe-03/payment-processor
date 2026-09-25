package dev.jay.payment_processor.event;

import java.time.Instant;

public record PaymentFailed(
        String paymentId,
        String reason,
        Instant failedAt
) {
}
