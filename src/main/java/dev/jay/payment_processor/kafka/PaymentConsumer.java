package dev.jay.payment_processor.kafka;

import dev.jay.payment_processor.client.PaymentProviderClient;
import dev.jay.payment_processor.domain.Payment;
import dev.jay.payment_processor.enums.Status;
import dev.jay.payment_processor.event.PaymentCompleted;
import dev.jay.payment_processor.event.PaymentFailed;
import dev.jay.payment_processor.event.PaymentRequested;
import dev.jay.payment_processor.redis.PaymentRateLimiter;
import dev.jay.payment_processor.redis.ProviderResponseCache;
import dev.jay.payment_processor.repository.PaymentRepository;
import dev.jay.payment_processor.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentRepository paymentRepository;
    private final PaymentProducer producer;
    private final PaymentService paymentService;
    private final PaymentRateLimiter rateLimiter;
    private final ProviderResponseCache responseCache;

    @KafkaListener(topics = "payment-requests", groupId = "payment-processor")
    public void onPaymentRequest(PaymentRequested event) {
        log.info("Processing payment request: {}", event.paymentId());

        // 0. Rate limit check
        if (!rateLimiter.isAllowed(event.customerId())) {
            log.warn("Payment {} rejected — customer {} rate-limited", event.paymentId(), event.customerId());
            producer.sendFailed(new PaymentFailed(event.paymentId(), "Rate limit exceeded", Instant.now()));
            return;
        }

        // 1. Idempotency check - try to insert PROCESSING; if already exists, skip
        try {
            Payment payment = Payment.builder()
                    .paymentId(event.paymentId())
                    .customerId(event.customerId())
                    .amountCents(event.amountCents())
                    .currency(event.currency())
                    .status(Status.PROCESSING)
                    .providerReference(null)
                    .createdAt(Instant.now())
                    .completedAt(null)
                    .build();

            paymentRepository.insert(payment);
        } catch (DuplicateKeyException e) {
            log.warn("Payment {} already exists — skipping duplicate", event.paymentId());
            return;
        }


        // 2. Check cache before calling provider
        var response = responseCache.get(event.paymentId());
        if (response == null) {
            // Cache miss — call provider (with retry + circuit breaker)
            response = paymentService.charge(new PaymentProviderClient.ChargeRequest(
                    event.paymentId(), event.amountCents(), event.currency()));
            if (response != null) {
                responseCache.put(event.paymentId(), response);
            }
        }

        Payment payment = paymentRepository.findById(event.paymentId()).orElseThrow();

        // 3. Handle success or failure
        if (response != null) {
            // Success — update to COMPLETED
            payment.setStatus(Status.COMPLETED);
            payment.setProviderReference(response.providerReference());
            payment.setCompletedAt(Instant.now());
            paymentRepository.save(payment);

            producer.sendCompleted(new PaymentCompleted(
                    event.paymentId(),
                    response.providerReference(),
                    event.amountCents(),
                    event.currency(),
                    Instant.now()
            ));
            log.info("Payment {} completed, provider ref {}", event.paymentId(), response.providerReference());
        } else {
            // All retries exhausted — mark FAILED, dead-letter the original event
            payment.setStatus(Status.FAILED);
            payment.setCompletedAt(Instant.now());
            paymentRepository.save(payment);

            producer.sendFailed(new PaymentFailed(
                    event.paymentId(),
                    "Provider unavailable after retries",
                    Instant.now()
            ));
            producer.sendDeadLetter(event);
            log.error("Payment {} FAILED — sent to dead letter", event.paymentId());
        }
    }
}
