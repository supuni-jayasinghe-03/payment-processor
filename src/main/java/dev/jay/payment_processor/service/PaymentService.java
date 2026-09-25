package dev.jay.payment_processor.service;

import dev.jay.payment_processor.client.PaymentProviderClient;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentProviderClient providerClient;

    @CircuitBreaker(name = "paymentProvider")
    @Retry(name = "paymentProvider", fallbackMethod = "chargeFallback")
    public PaymentProviderClient.ChargeResponse charge(PaymentProviderClient.ChargeRequest request) {
        log.info("Attempting to charge payment {}", request.paymentId());
        return providerClient.charge(request);
    }

    private PaymentProviderClient.ChargeResponse chargeFallback(
            PaymentProviderClient.ChargeRequest request, Throwable t) {
        log.error("All retries/circuit-breaker exhausted for payment {}. Reason: {}",
                request.paymentId(), t.getMessage());
        return null; // null signals the consumer that charging failed
    }
}
