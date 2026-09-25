package dev.jay.payment_processor.redis;

import dev.jay.payment_processor.client.PaymentProviderClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProviderResponseCache {

    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public PaymentProviderClient.ChargeResponse get(String paymentId) {
        String key = "provider-response:" + paymentId;
        String cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            try {
                log.info("Cache Hit for payment {}", paymentId);
                return objectMapper.readValue(cached, PaymentProviderClient.ChargeResponse.class);
            } catch (JacksonException e) {
                log.warn("Failed to deserialize cached response for {} | error: {}", paymentId, e.getMessage());
            }
        }
        return null;
    }

    public void put(String paymentId, PaymentProviderClient.ChargeResponse response) {
        String key = "provider-response:" + paymentId;
        try {
            redisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(response), CACHE_TTL);
            log.info("Cached provider response for payment {}", paymentId);
        } catch (JacksonException e) {
            log.warn("Failed to cache response for {} | error: {}", paymentId, e.getMessage());
        }

    }

}
