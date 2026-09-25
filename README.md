# Idempotent Payment Processing Service

A payment-processing service that consumes payment-request events from Kafka, charges an external payment provider via Feign, and guarantees each payment is processed **exactly once** — surviving provider outages and Kafka's at-least-once redelivery through idempotency keys, retries with backoff, a circuit breaker, and dead-lettering.

Built as a portfolio project to demonstrate event-driven, resilient backend engineering with real infrastructure (not mocks).

## The Problem

A payment request arrives as a Kafka event. The service must charge the customer through an external provider **exactly once**, even though:

- **Kafka delivers at-least-once** — the same event can arrive more than once
- **The provider can be slow, time out, or error** — and you may not know whether the charge went through
- **The service can crash mid-processing** and reprocess the event on restart

Getting this right requires idempotency and careful retry logic — the same class of problem real payment systems, billing platforms, and integration services solve every day.

## Architecture

```
Kafka: payment-requests (3 partitions)
    │
    ▼
┌──────────────────────────────────────────────┐
│  Consumer                                    │
│  ┌─────────────┐                             │
│  │ Rate Limiter│◄── Redis (per-customer)     │
│  └──────┬──────┘                             │
│         ▼                                    │
│  ┌─────────────┐                             │
│  │ Idempotency │◄── MongoDB (unique index    │
│  │ Check       │    on paymentId as _id)     │
│  └──────┬──────┘                             │
│         ▼                                    │
│  ┌──────────────────┐    ┌─────────────────┐ │
│  │ PaymentService   │───►│ Feign Client    │─┼──► External Provider
│  │ (Retry + Circuit │    └─────────────────┘ │    (mock: random fail/slow/success)
│  │  Breaker)        │                        │
│  └──────┬───────────┘                        │
│         ▼                                    │
│  ┌──────────────┐                            │
│  │ Response     │◄── Redis (cache with TTL)  │
│  │ Cache        │                            │
│  └──────────────┘                            │
└──────────────────────────────────────────────┘
    │                    │                  │
    ▼                    ▼                  ▼
payment-completed   payment-failed   payment-dead-letter
```

## How Idempotency Works

This is the core design decision. Before charging, the service tries to insert a record with `paymentId` as MongoDB's `_id`:

1. **Not seen before** → insert succeeds (status = `PROCESSING`), proceed to charge
2. **Already exists** → `DuplicateKeyException` caught → skip (payment already handled)
3. **After successful charge** → update status to `COMPLETED` with provider reference

The key insight: MongoDB enforces uniqueness on `_id` by default, so two concurrent duplicates cannot both insert — only one charges. 

## Resilience Features

| Feature | What it does | Why it matters |
|---------|-------------|----------------|
| **Idempotency** | Unique paymentId prevents double-charging | Kafka redelivers; the service must be safe to retry |
| **Retries with backoff** | Configurable retry count on transient failures | Providers have intermittent issues; don't give up on the first error |
| **Circuit breaker** | Stops calling a failing provider; fails fast | One bad provider shouldn't back up the entire system |
| **Dead-letter topic** | Failed payments go to `payment-dead-letter` | Nothing is silently lost; failed events can be inspected and replayed |
| **Rate limiting** | Redis-based per-customer limit (fixed window) | Prevents abuse and protects downstream providers |
| **Response caching** | Redis cache with TTL for provider responses | Avoids redundant calls to the external provider |
| **Manual offset commit** | Kafka offset committed only after full processing | A crash mid-processing safely reprocesses (idempotency handles it) |

## Kafka Configuration

Topics are created explicitly via `KafkaTopicConfig` with deliberate partition and replication settings:

- **3 partitions per topic** — enables parallel consumption and distributes load
- **Message key = paymentId** — all events for the same payment route to the same partition, guaranteeing per-payment ordering
- **Producer `acks=all`** — writes are acknowledged only when all in-sync replicas confirm

**Production durability (documented, not run locally):** In a multi-broker cluster, topics would use `replication-factor: 3` and `min.insync.replicas: 2`, ensuring no payment event is lost if a broker fails. The topic config class defines these settings; locally the project runs on a single broker for simplicity.

## Tech Stack

- **Java 21** (records for event schemas)
- **Spring Boot 4.x**
- **Spring Kafka** (producer, consumer, topic management)
- **OpenFeign** (declarative HTTP client for external provider)
- **Resilience4j** (retry, circuit breaker)
- **MongoDB** (payment persistence, idempotency via unique `_id`)
- **Redis** (rate limiting, response caching with TTL)
- **Testcontainers** (integration tests with real Kafka, Mongo, Redis)
- **JUnit 5 + Awaitility** (async test assertions)
- **Docker Compose** (local Kafka + MongoDB + Redis)

## Running Locally

**Prerequisites:** Docker, Java 17+, Gradle

```bash
# Start infrastructure
docker compose up -d

# Run the application
./gradlew bootRun

# Send a payment
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","amountCents":4999,"currency":"USD"}'

# Send the same payment twice (test idempotency)
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"paymentId":"test-123","customerId":"cust-1","amountCents":5000,"currency":"USD"}'

curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"paymentId":"test-123","customerId":"cust-1","amountCents":5000,"currency":"USD"}'
# Second one logs "already exists — skipping duplicate"

# Check MongoDB
docker exec -it mongo mongosh --eval 'db.payments.find().pretty()' payments

# Check Redis keys
docker exec -it redis redis-cli KEYS "*"
```

## Running Tests

```bash
./gradlew test
```

Tests use Testcontainers to spin up real Kafka, MongoDB, and Redis — no mocks for infrastructure. The mock payment provider is controllable (always-succeed, always-fail, fail-N-times-then-succeed) so each test scenario is deterministic.

### Test Coverage

| Test | What it proves |
|------|---------------|
| **Happy path** | One payment → one charge → COMPLETED |
| **Idempotent replay** | Same paymentId sent twice → provider charged **once**, one DB record |
| **Retry then succeed** | Provider fails once, succeeds on retry → one charge |
| **Circuit open** | Provider keeps failing → payment marked FAILED, sent to dead-letter |
| **Rate limiting** | 7 rapid payments for one customer → at most 5 processed, rest rejected |

## Design Decisions

**Why Feign over RestTemplate/WebClient?** Feign keeps the HTTP contract declarative (an interface), separate from the resilience logic (in `PaymentService`). The Feign client defines *what* to call; the service wrapper defines *how to call it safely*.

**Why a separate PaymentService?** Resilience4j annotations work through Spring proxies. Layering them on a Feign interface (which is already a proxy) is unreliable. A wrapper service gives a clean place for retry + circuit breaker logic.

**Why `insert()` not `save()` for idempotency?** `save()` does an upsert — if the `_id` exists, it overwrites silently. `insert()` throws `DuplicateKeyException` on a duplicate `_id`, which is exactly the duplicate-detection signal we need.

**Why Spring Boot instead of Dropwizard?** Spring Boot is my primary framework. Dropwizard is architecturally equivalent — the patterns (Feign, resilience, event-driven) transfer directly.

## What I Learned

- How Kafka's at-least-once delivery creates the need for idempotency, and how to implement it with MongoDB's unique `_id`
- The difference between Kafka-level retries (redelivering a message) and application-level retries (Resilience4j retrying an HTTP call) — and why you need both
- How `@Retry` and `@CircuitBreaker` interact (circuit breaker records; retry exhausts and falls back)
- Kafka listener configuration, consumer groups, and offset management
- Multi-listener Kafka networking (internal vs external listeners for Docker)
- Redis as a rate limiter (INCR + TTL) and a response cache

## What I'd Add Next

- Kafka Streams aggregation (completed vs failed per time window)
- Structured logging with correlation IDs for tracing a payment across services
- Prometheus/Grafana metrics for circuit breaker state and retry counts
- A standalone mock-provider service (separate from the main app)
- Multi-broker Kafka cluster with replication-factor 3 and min.insync.replicas 2

---

Built by [Supuni Jayasinghe](mailto:supunijayasinghe80@gmail.com) as a portfolio project demonstrating event-driven, resilient backend engineering.
