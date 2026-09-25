package dev.jay.payment_processor.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "payment-provider", url = "${provider.url}")
public interface PaymentProviderClient {

    @PostMapping("/mock-provider/charge")
    ChargeResponse charge(@RequestBody ChargeRequest request);

    record ChargeRequest(String paymentId, long amountCents, String currency) {}
    record ChargeResponse(String providerReference, String status) {}
}
