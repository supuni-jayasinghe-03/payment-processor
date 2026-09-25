package dev.jay.payment_processor.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentRateLimiter {

    private static final int MAX_REQUESTS_PER_MINUTE = 5;

    private final StringRedisTemplate redisTemplate;

    public boolean isAllowed(String customerId) {
        String key = "rate:" + customerId;
        Long count = redisTemplate.opsForValue().increment(key);

        if (count == 1) {
            // First request in this window — set expiry
            redisTemplate.expire(key, Duration.ofMinutes(1));
        }

        if (count > MAX_REQUESTS_PER_MINUTE) {
            log.warn("Rate limit exceeded for customer {}", customerId);
            return false;
        }
        return true;
    }
}
