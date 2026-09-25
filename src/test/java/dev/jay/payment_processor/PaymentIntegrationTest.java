package dev.jay.payment_processor;

import dev.jay.payment_processor.domain.Payment;
import dev.jay.payment_processor.enums.Status;
import dev.jay.payment_processor.event.PaymentRequested;
import dev.jay.payment_processor.kafka.PaymentProducer;
import dev.jay.payment_processor.repository.PaymentRepository;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
class PaymentIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:4.3.0")
    );

    @Container
    static MongoDBContainer mongo = new MongoDBContainer(
            DockerImageName.parse("mongo:7")
    );

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:8"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private PaymentProducer producer;

    @Autowired
    private PaymentRepository repository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    private final RestTemplate restTemplate = new RestTemplate();


    @BeforeEach
    void cleanUp() {
        repository.deleteAll();
        // Clear Redis
        var keys = redisTemplate.keys("*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }

        // Reset circuit breaker so each test starts fresh
        circuitBreakerRegistry.getAllCircuitBreakers()
                .forEach(CircuitBreaker::reset);
    }

    private PaymentRequested createPaymentRequest(String paymentId, String customerId) {
        return new PaymentRequested(paymentId, customerId, 5000L, "USD", Instant.now());
    }

    // ====== TEST 1: Happy path ======
    @Test
    @Order(1)
    void processPaymentSuccessfully() {
        setProviderMode("always-succeed");

        String paymentId = "pay-id" + System.currentTimeMillis();
        producer.send(createPaymentRequest(paymentId, "cust-1"));

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> payment = repository.findById(paymentId);
            assertTrue(payment.isPresent(), "Payment should exist in DB");
            assertEquals(Status.COMPLETED, payment.get().getStatus());
            assertNotNull(payment.get().getProviderReference());
        });
    }

    // ====== TEST 2: Idempotent replay ======
    @Test
    @Order(2)
    void chargeOnlyOnceWhenSamePaymentSentTwice() {
        setProviderMode("always-succeed");

        String paymentId = "idempotent-" + System.currentTimeMillis();
        PaymentRequested request = createPaymentRequest(paymentId, "cust-2");

        // Send the same payment twice
        producer.send(request);
        producer.send(request);

        // Wait for processing
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> payment = repository.findById(paymentId);
            assertTrue(payment.isPresent(), "Payment should exit in DB");
            assertEquals(Status.COMPLETED, payment.get().getStatus());
        });

        // Verify only one document exists with this paymentId
        long count = repository.count();
        Optional<Payment> payment = repository.findById(paymentId);
        assertTrue(payment.isPresent());
        assertEquals(1, count, "Should have exactly 1 payment");
        assertEquals(Status.COMPLETED, payment.get().getStatus());
    }

    // ====== TEST 3: Retry then succeed ======
    @Test
    @Order(3)
    void retryAndSucceedAfterTransientFailures() {
        setProviderMode("fail-times/1");  // fail twice, then succeed

        String paymentId = "retry-" + System.currentTimeMillis();
        producer.send(createPaymentRequest(paymentId, "cust-3"));

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> payment = repository.findById(paymentId);
            assertTrue(payment.isPresent(), "Payment should exist in DB");
            assertEquals(Status.COMPLETED, payment.get().getStatus());
            assertNotNull(payment.get().getProviderReference());
        });
    }

    // ====== TEST 4: Rate limiting ======
    @Test
    @Order(4)
    void rejectPaymentsWhenRateLimitExceeded() {
        setProviderMode("always-succeed");

        String customerId = "cust-flood-" + System.currentTimeMillis();

        // Send 7 payments for the same customer (limit is 5)
        for (int i = 0; i < 7; i++){
            String paymentId = "rate-" + i + "-" + System.currentTimeMillis();
            producer.send(createPaymentRequest(paymentId, customerId));
        }

        // Wait for processing
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            long paymentsCount = repository.count();
            assertTrue(paymentsCount >= 5, "At least 5 payments should be attempted");
        });

        // Wait a bit more for all events to be consumed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            long totalPayments = repository.count();
            assertTrue(totalPayments <= 5,
                    "Should have at most 5 payments — rest were rate-limited. Got: " + totalPayments);
        });
    }

    // ====== TEST 5: Circuit open — all retries exhausted ======
    @Test
    @Order(5)
    void deadLetterWhenProviderKeepsFailing() {
        setProviderMode("always-fail");

        String paymentId = "circuit-" + System.currentTimeMillis();
        producer.send(createPaymentRequest(paymentId, "cust-4"));

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Payment> payment = repository.findById(paymentId);
            assertTrue(payment.isPresent(), "Payment should exist in DB");
            assertEquals(Status.FAILED, payment.get().getStatus());
        });
    }

    private void setProviderMode(String mode) {
        restTemplate.postForEntity("http://localhost:8888/mock-provider/config/" + mode, null, Void.class);
    }
}
