# HookShield: Enterprise-Grade Webhook Ingestion & DLQ Replay Hub
## Comprehensive Platform Architecture, Component Deep Dive, and Operational Guide

---

## 1. Executive Summary: Why HookShield?

In modern distributed microservice ecosystems, webhooks are the lifeblood of real-time event-driven architecture—connecting payment processors (Stripe), source control platforms (GitHub), e-commerce systems (Shopify), and third-party SaaS integrations. 

However, building and maintaining webhooks in production presents **critical distributed systems failure modes**:

```
Traditional Webhook Bottlenecks:
[Inbound Traffic Spike] ──> [Blocking Controller] ──> [DB Lock Contention] ──> [504 Gateway Timeout]
[Downstream Outage]     ──> [Thread Sleep Retries]──> [Kafka Head-of-Line Blocking] ──> [System Collapse]
[Duplicate Retries]     ──> [No Distributed Lock] ──> [Double Billing / Inconsistent State]
[DLQ Failures]          ──> [Unstructured Text Log]──> [Silent Failure Graveyard / No Replay]
```

### The Core Challenges HookShield Solves:

| Problem in Traditional Systems | Impact in Production | HookShield Architectural Solution |
| :--- | :--- | :--- |
| **Ingestion Head-of-Line Blocking** | A traffic spike (e.g. Black Friday) causes thread exhaustion and HTTP 504 gateway timeouts. | **Sub-15ms Ingestion SLA**: Synchronous fast-ack to Kafka `inbound-raw` on Java 21 Virtual Threads. |
| **Duplicate Delivery & Race Conditions** | Third parties retry aggressively, leading to duplicate financial debits or corrupted inventory. | **Atomic Distributed Idempotency**: Redis `SET NX EX 86400` locking on composite SHA-256 hashes. |
| **Downstream Subscriber Outages** | Subscriber downtime blocks worker threads during exponential backoff sleep. | **Non-Blocking Multi-Tier Retries**: 1m $\to$ 5m $\to$ 15m delay topics without consumer thread sleep. |
| **Cascading Downstream Overload** | Hammering a degraded subscriber worsens their outage. | **Resilience4j Circuit Breakers**: Fast-fail circuit breaking per subscriber host, diverting directly to retry topics. |
| **Unactionable Dead-Letter Queues** | Thousands of failed webhooks pile up in unindexed logs; engineers cannot discern root causes. | **Spring AI & PostgreSQL pgvector**: Semantic vector clustering (1536-dim) & zero-shot LLM triage. |
| **Risky, Manual DLQ Replay** | Replaying failed webhooks blindly risks re-dispatching toxic payloads or triggering auth loops. | **Safe Replay Engine**: AI classifies safe vs. unsafe failures; provides 1-click or automated cooling replays. |

---

## 2. End-to-End System Architecture

HookShield is designed as a **6-stage decoupled pipeline** combining Apache Kafka (KRaft), Redis (Redisson), PostgreSQL 16 (`pgvector`), Java 21 Loom Virtual Threads, and Spring AI:

```mermaid
flowchart TD
    subgraph Client["Third-Party Webhook Provider"]
        A["Inbound Webhook POST\n(Stripe, GitHub, Shopify)"]
    end

    subgraph Tier1["Tier 1: High-Speed Ingress Gateway (<15ms SLA)"]
        B["WebhookIngressController"]
        C["Tenant Secret Cache\n(Redis / L1 Cache)"]
        D["HmacSignatureVerifier\n(Constant-Time SHA-256)"]
        E["RateLimitingService\n(Redis Token-Bucket)"]
        F["Kafka Producer\n(Zstandard / Idempotent)"]
        Resp["HTTP 202 Accepted\nX-HookShield-Delivery-ID\nX-Ingestion-Latency-Ms"]
    end

    subgraph KafkaCore["Apache Kafka KRaft Pipeline"]
        K1[("webhook-inbound-raw")]
        K2[("webhook-dispatch-queue")]
        K3[("webhook-retry-1m")]
        K4[("webhook-retry-5m")]
        K5[("webhook-retry-15m")]
        K6[("webhook-dead-letter-queue")]
    end

    subgraph Tier2["Tier 2: Inbound Deduplication & Audit"]
        G["InboundConsumer\n(Loom Virtual Thread)"]
        H["Redis Atomic Lock\nSET NX EX 86400"]
        I[("PostgreSQL\nwebhook_deliveries\nStatus: RECEIVED")]
    end

    subgraph Tier3["Tier 3: Loom Dispatch & Circuit Breaking"]
        J["DispatchConsumer\n(Loom Virtual Thread)"]
        K["WebhookDispatcherService\n(Spring RestClient)"]
        L{"Resilience4j\nCircuit Breaker"}
        Sub[("Downstream Subscriber\nEndpoint")]
    end

    subgraph Tier4["Tier 4: Non-Blocking Retry Pipeline"]
        M["RetryConsumers\n(Virtual Thread Scheduler)"]
    end

    subgraph Tier5["Tier 5: AI DLQ Semantic Triage & pgvector"]
        N["DlqConsumer"]
        O["AiDiagnosticService"]
        P["Spring AI ChatClient\n(Zero-Shot LLM Reasoning)"]
        Q["EmbeddingModel\n(1536-dim Unit Vector)"]
        R[("PostgreSQL pgvector\nerror_clusters (IVFFlat)\ndlq_incident_reports")]
        S["DlqReplayService\n(Automated / Manual Replay)"]
    end

    A --> B
    B <--> C
    B --> D
    B --> E
    B --> F
    F --> Resp
    F --> K1

    K1 --> G
    G <--> H
    G --> I
    G --> K2

    K2 --> J
    J --> K
    K <--> L
    K -->|HTTP POST| Sub

    K -->|Failure / CB Open| K3
    K3 --> M
    M -->|Retry Attempt 1| K
    M -->|Failure 2| K4
    K4 --> M
    M -->|Retry Attempt 2| K
    M -->|Failure 3| K5
    K5 --> M
    M -->|Retry Attempt 3| K
    M -->|Exhausted (Attempt >=3)| K6

    K6 --> N
    N --> O
    O <--> P
    O <--> Q
    O <--> R
    R --> S
    S -.->|Re-enqueue Replayed Delivery| K2
```

---

## 3. Component Deep Dive

### 3.1 Tier 1: Ingestion Gateway & Security Verification

- **Class**: `WebhookIngressController.java`
- **Security Guard**: `HmacSignatureVerifier.java`
- **Rate Limiting**: `RateLimitingService.java`
- **Secret Caching**: `TenantSecretCacheService.java`

#### Mechanics:
1. **Dynamic Secret Retrieval**: Uses Redis cache with a 1-hour TTL (`tenant:secret:{tenantId}:{provider}`) to fetch tenant verification secrets in `<1ms`, falling back to PostgreSQL only on cache misses.
2. **Timing Attack Protection**: Cryptographic HMAC-SHA256 tokens are verified using `MessageDigest.isEqual(...)` constant-time byte comparisons, preventing timing side-channel exploits. Provider-specific header schemas (e.g. `t=...,v1=...` for Stripe, `sha256=...` for GitHub) are automatically normalized.
3. **High-Throughput Token Bucket Rate Limiter**: Implemented using Redis Lua-atomic / Redisson buckets with instant refills. If a tenant exceeds their quota, HookShield immediately rejects with `HTTP 429 Too Many Requests` and a `Retry-After: 1` header before entering the queuing pipeline.
4. **Fast-Ack Asynchronous Handshake**: Once verified and rate-checked, the controller assigns a UUID delivery ID, stamps `Instant.now()`, publishes to Kafka topic `webhook-inbound-raw`, and returns `HTTP 202 Accepted` with header `X-Ingestion-Latency-Ms` within **2–10 milliseconds**.

---

### 3.2 Tier 2: Distributed Idempotency & Transactional Audit

- **Class**: `InboundConsumer.java`
- **Locking Service**: `IdempotencyService.java`
- **Data Entity**: `WebhookDelivery.java`

#### Mechanics:
1. **Composite SHA-256 Idempotency Key**:
   $$\text{Hash} = \text{SHA256}(\text{TenantID} \,\|\, \text{Provider} \,\|\, \text{RawPayload} \,\|\, \text{ExternalEventID})$$
2. **Atomic Redis Locking**:
   Executes `SET idemp:lock:{hash} "LOCKED" NX EX 86400` in Redis. If the key already exists, the event is identified as a duplicate, logged, counted in Micrometer (`webhooks.deduplicated.count`), and committed without further downstream dispatch.
3. **Transactional Audit Insertion**:
   Persists the record to PostgreSQL table `webhook_deliveries` with `status: RECEIVED`, client-assigned UUID, and Hibernate 6 `@JdbcTypeCode(SqlTypes.JSON)` payload formatting.
4. **Handoff to Dispatch Queue**:
   Publishes the validated payload to `webhook-dispatch-queue` and marks the Redis state as `"PROCESSED"`.

---

### 3.3 Tier 3: Outbound Dispatch on Java 21 Loom Virtual Threads

- **Class**: `DispatchConsumer.java`
- **Dispatcher Service**: `WebhookDispatcherService.java`
- **Resilience Config**: `ResilienceConfig.java`

#### Mechanics:
1. **Concurrency via Virtual Threads**:
   Because `spring.threads.virtual.enabled: true` is configured, each outbound HTTP request executes on a lightweight Java 21 Virtual Thread. Blocking I/O during slow downstream network calls parks the virtual thread onto JVM carrier workers without tying up OS-level threads.
2. **Per-Host Resilience4j Circuit Breakers**:
   Each target subscriber host dynamically registers a circuit breaker with a sliding count window of 20 calls and a 50% failure threshold.
   - **Open State Fast-Fail**: If a subscriber's circuit breaker trips open, outbound network calls are skipped entirely, and the webhook is immediately diverted to the non-blocking retry pipeline.
3. **Atomic Delivery Transitions**:
   Outbound results are wrapped in Spring `TransactionTemplate`:
   - On HTTP $2xx$: Status updated to `DISPATCHED` in PostgreSQL.
   - On HTTP $4xx / 5xx$ or connection timeout: Routed to `handleDispatchFailure(...)`.

---

### 3.4 Tier 4: Non-Blocking Multi-Tier Retry Pipeline

- **Consumer**: `RetryConsumers.java`
- **Delayed Topics**: `webhook-retry-1m`, `webhook-retry-5m`, `webhook-retry-15m`

#### Mechanics:
Traditional retry mechanisms use `Thread.sleep(...)` inside consumer loops, which pauses Kafka partition consumers and causes partition rebalances. HookShield implements **non-blocking topic hopping**:

```
Dispatch Failure (Attempt 0)
       │
       ▼
[Publish to webhook-retry-1m + Header X-Retry-Next-Epoch-Ms] ──> Offset Committed Immediately
       │
       ▼
Consumer reads message ──> Computes delayRemainingMs = NextEpoch - Now
       │
       ├─► If delay elapsed ──> Immediate Virtual Thread Dispatch
       └─► If delay pending ──> Scheduled via Virtual Thread ScheduledExecutorService
                                (Consumer Thread Freed & Loop Continues)
```

1. **Attempt 0 Failure**: Updated to `RETRYING (retry_count = 1)`, published to `webhook-retry-1m` with target epoch $+60\text{s}$.
2. **Attempt 1 Failure**: Updated to `RETRYING (retry_count = 2)`, published to `webhook-retry-5m` with target epoch $+300\text{s}$.
3. **Attempt 2 Failure**: Updated to `RETRYING (retry_count = 3)`, published to `webhook-retry-15m` with target epoch $+900\text{s}$.
4. **Attempt $\ge 3$ Failure**: Retry budget exhausted. Delivery status updated to `DEAD_LETTER`, published to `webhook-dead-letter-queue` with headers `X-Error-Message` and `X-Http-Status-Code`.

---

### 3.5 Tier 5: Spring AI & PostgreSQL pgvector Semantic Triage

- **Consumer**: `DlqConsumer.java`
- **Diagnostic Service**: `AiDiagnosticService.java`
- **Vector Repository**: `ErrorClusterRepository.java`
- **Database Tables**: `error_clusters`, `dlq_incident_reports`

```
Dead-Letter Webhook
       │
       ▼
Extract Signature: [Target Endpoint] + [Error Message] + [Payload Excerpt]
       │
       ▼
Generate 1536-Dimensional Unit Vector (EmbeddingModel)
       │
       ▼
Native PostgreSQL pgvector Query:
SELECT * FROM error_clusters WHERE (signature_embedding <=> :embedding) <= 0.12 LIMIT 1
       │
       ├─► [Cluster Exists] ──> Increment Occurrences (total_occurrences++)
       │                        Link to dlq_incident_reports
       │
       └─► [New Failure]    ──> Zero-Shot LLM Reasoning (Spring AI ChatClient)
                                Categorize: EPHEMERAL_NETWORK, SCHEMA_MISMATCH, etc.
                                Compute Confidence Score & Safe-To-Replay Flag
                                Insert new ErrorCluster with IVFFlat Cosine Index
```

#### Cosine Distance Operator (`<=>`):
In PostgreSQL `pgvector`, the cosine distance between two vectors $u$ and $v$ is defined as:
$$\text{dist}(u, v) = 1 - \frac{u \cdot v}{\|u\|_2 \|v\|_2}$$
With a configured threshold of `0.12`, any error whose signature has **$\ge 88\%$ semantic similarity** automatically clusters into the existing failure group, preventing duplicate manual analysis by operations teams.

#### Automated Replay Engine:
If the LLM or cluster classifies the failure as `EPHEMERAL_NETWORK` (e.g. temporary 503 gateway outage or connection timeout) with `isSafeToAutoReplay = true`, HookShield automatically schedules a cooling auto-replay after 30 minutes.

---

## 4. Architectural Trade-Off Analysis

| Decision | Selected Choice | Rejected Alternative | Engineering Rationale |
| :--- | :--- | :--- | :--- |
| **Concurrency Runtime** | Java 21 Project Loom (Virtual Threads) | Reactive WebFlux / Netty | Virtual threads preserve straightforward imperative programming, standard blocking JPA drivers, and intuitive debugging while scaling to millions of concurrent I/O operations without reactive operator overhead. |
| **Messaging Engine** | Apache Kafka (KRaft Mode) | RabbitMQ / ActiveMQ | Kafka provides partition-level ordering, distributed log immutability, zero-data-loss durability under traffic bursts, and non-blocking replay topologies without ZooKeeper overhead. |
| **Vector Storage** | Native PostgreSQL `pgvector` | Dedicated Vector DB (Pinecone, Milvus) | Keeping vector embeddings in PostgreSQL ensures atomic transactional integrity between webhook audit logs and error clusters, eliminating cross-database distributed transaction consistency bugs. |
| **Distributed Locking** | Redis Redisson `SET NX EX` | PostgreSQL Table Lock (`FOR UPDATE`) | Redis locks resolve in $<1\text{ms}$ in-memory without holding open database connection pool slots in HikariCP. |

---

## 5. Summary of API Endpoints

### 5.1 Ingestion Gateway
- `POST /api/v1/webhooks/ingest/{tenantId}/{provider}` — Ultra-low latency ingest with HMAC verification.
- `POST /api/v1/webhooks/mock-subscriber` — Built-in mock receiver supporting optional status simulation (`?status=503`).

### 5.2 Operator Dashboard & Simulation
- `GET /api/v1/dashboard/overview` — Aggregated system telemetry, health, and SLA metrics.
- `GET /api/v1/dashboard/deliveries` — Paginated audit stream with tenant name and full payload details.
- `GET /api/v1/dashboard/tenants` — Active tenant policies and registered provider keys.
- `POST /api/v1/dashboard/simulate` — Interactive testbed that computes HMAC signatures and tests end-to-end ingestion in `<15ms`.

### 5.3 Dead-Letter Queue & Replay
- `GET /api/v1/dlq/clusters` — Grouped semantic error clusters sorted by occurrence count.
- `GET /api/v1/dlq/incidents?clusterId={id}` — Detailed incident records within an error cluster.
- `POST /api/v1/dlq/replay` — Bulk replay trigger by cluster ID or delivery IDs.

### 5.4 Observability
- `GET /actuator/health` — Spring Boot Actuator component status (DB, Redis, Kafka, Virtual Threads).
- `GET /actuator/prometheus` — Metrics for Prometheus monitoring (`webhooks_ingested_total`, `webhooks_dispatch_latency_seconds`).
