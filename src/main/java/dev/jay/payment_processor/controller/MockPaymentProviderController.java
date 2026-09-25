package dev.jay.payment_processor.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.Random;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/mock-provider")
public class MockPaymentProviderController {

    private final Random random = new Random();

    // Controllable mode for tests
    private int failuresRemaining = 0;
    private boolean alwaysFail = false;
    private boolean randomMode = true;

    // Test control endpoints
    @PostMapping("/config/always-succeed")
    public void setAlwaysSucceed() {
        this.randomMode = false;
        this.alwaysFail = false;
        this.failuresRemaining = 0;
        log.info("Provider mode: ALWAYS SUCCEED");
    }

    @PostMapping("/config/always-fail")
    public void setAlwaysFail() {
        this.randomMode = false;
        this.alwaysFail = true;
        log.info("Provider mode: ALWAYS FAIL");
    }

    @PostMapping("/config/fail-times/{count}")
    public void setFailTimes(@PathVariable int count) {
        this.randomMode = false;
        this.alwaysFail = false;
        this.failuresRemaining = count;
        log.info("Provider mode: FAIL {} times then succeed", count);
    }


    @PostMapping("/charge")
    public ChargeResponse charge(@RequestBody ChargeRequest request) throws InterruptedException {
        boolean shouldFail;

        if (randomMode) {
            // Original random behavior — 40% failure
            int outcome = random.nextInt(10);
            if (outcome < 2) {
                log.warn("Provider SLOW for payment {}", request.paymentId());
                Thread.sleep(5000);
            }
            shouldFail = outcome < 4;
        } else if (alwaysFail) {
            shouldFail = true;
        } else if (failuresRemaining > 0) {
            failuresRemaining--;
            shouldFail = true;
        } else {
            shouldFail = false;
        }

        if (shouldFail) {
            log.error("Provider ERROR for payment {}", request.paymentId());
            throw new RuntimeException("Provider unavailable");
        }

        log.info("Provider SUCCESS for payment {}", request.paymentId());
        String providerReference = "prov-" + UUID.randomUUID();
        return new ChargeResponse(providerReference, "SUCCESS");
    }



    public record ChargeRequest(String paymentId, long amountCents, String currency) {}
    public record ChargeResponse(String providerReference, String status) {}

}
