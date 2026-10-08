# HookShield: Enterprise Distributed Webhook Hub
## Comprehensive Platform Guide: Motivation, System Design, and Component Deep Dive

---

## 1. Executive Summary & Problem Space: Why HookShield?

In modern distributed microservice architectures, **webhooks are the primary mechanism for asynchronous inter-system communication**. Whether integrating with payment processors (Stripe), version control providers (GitHub), e-commerce platforms (Shopify), or identity management systems (Okta), external platforms deliver business-critical notifications via HTTP POST payloads.

However, receiving, auditing, and processing webhooks in production presents severe distributed systems challenges that cause **data loss, cascading failures, double-billing, and operational exhaustion**.

```
Traditional Webhook Processing Failures:
┌─────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│ Inbound Burst       │ ───► │ Synchronous Controller │ ───► │ DB Connection Pool      │ ───► HTTP 504 Gateway
│ (10,000+ rps spike) │      │ (Blocking OS Threads)  │      │ Exhaustion & Lock Cont. │      Timeout & Data Loss
└─────────────────────┘      └────────────────────────┘      └─────────────────────────┘
┌─────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│ Downstream Outage   │ ───► │ In-Thread Sleep Retry  │ ───► │ Kafka Consumer Partition│ ───► Broker Rebalance &
│ (Subscriber 503)    │      │ (Thread.sleep(60000))  │      │ Head-of-Line Blocking   │      System Freeze
└─────────────────────┘      └────────────────────────┘      └─────────────────────────┘
┌─────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│ Duplicate Delivery  │ ───► │ No Distributed Lock    │ ───► │ Concurrent Processing   │ ───► Double Debiting /
│ (Provider Retry)    │      │ (Local Cache / DB Check│      │ Race Condition          │      Corrupted Inventory
└─────────────────────┘      └────────────────────────┘      └─────────────────────────┘
┌─────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│ Dead-Letter Queue   │ ───► │ Unstructured Text Logs │ ───► │ Engineers Overwhelmed   │ ───► Silent Graveyard /
│ (Failed Messages)   │      │ (No Categorization)    │      │ By 100,000+ Raw JSONs   │      Zero Safe Replay
└─────────────────────┘      └────────────────────────┘      └─────────────────────────┘
```

---

### 1.1 The Critical Flaws of Conventional Architectures

| Problem in Conventional Architectures | Production Impact | HookShield Architectural Solution |
| :--- | :--- | :--- |
| **Ingress Head-of-Line Blocking** | When downstream databases or internal services slow down, incoming webhooks block on synchronous HTTP controller threads until the web server exhausts its thread pool, returning `HTTP 504 Gateway Timeout` and causing providers to back off or disable the webhook endpoint. | **Sub-15ms Ingestion SLA with Asynchronous Handshake**: Fast-acks `HTTP 202 Accepted` immediately upon writing the payload to an append-only Kafka topic (`webhook-inbound-raw`), decoupled from processing. |
| **Duplicate Delivery & Idempotency Races** | Third-party providers retry webhooks if they experience a network blip. If two identical webhooks arrive concurrently, naive database checks (`SELECT WHERE idempotency_key = ?`) suffer race conditions before either record commits. | **Atomic Distributed Idempotency**: Redis Redisson `SET NX EX 86400` locking on composite SHA-256 hashes ($<1\text{ms}$) guarantees exactly-once processing across horizontally scaled nodes. |
| **Consumer Partition Stalls (In-Process Sleep)** | Developers often implement retries using exponential backoff with `Thread.sleep(...)` inside consumer threads. This pauses Kafka partition consumption, triggers broker heartbeat timeouts, and forces disruptive consumer group rebalances. | **Non-Blocking Multi-Tier Delayed Topics**: Failed webhooks hop across dedicated delayed Kafka topics (`webhook-retry-1m` $\to$ `webhook-retry-5m` $\to$ `webhook-retry-15m`) without pausing consumer polling. |
| **Cascading Subscriber Meltdowns** | When a downstream subscriber experiences a partial outage, continuous unthrottled retries hammer the degraded service, preventing its recovery. | **Dynamic Per-Host Resilience4j Circuit Breakers**: Automatically trips open upon reaching failure thresholds, immediately bypassing network I/O and diverting traffic to backoff queues. |
| **The "DLQ Graveyard" Problem** | When retry budgets are exhausted, failed webhooks are dumped into a dead-letter queue or log file. Sifting through 50,000 unindexed raw JSON documents to determine which failure was caused by a transient DNS outage versus an invalid schema is virtually impossible. | **Spring AI & PostgreSQL pgvector Semantic Triage**: Zero-shot LLM root-cause classification + 1536-dimensional vector embeddings clustered with IVFFlat cosine similarity ($\le 0.12$ distance). |
| **Dangerous Manual Replays** | Blindly replaying DLQ messages risks re-dispatching toxic payloads that crash downstream consumers again or re-triggering authentication loops. | **Intelligent Safe Replay Engine**: Automatically identifies transient failures (`EPHEMERAL_NETWORK`) for safe 1-click or automated cooling replays while flagging dangerous payloads (`AUTH_FAILURE`, `SCHEMA_MISMATCH`) for human review. |

---

## 2. End-to-End System Architecture: How It Works

HookShield operates as a **6-stage decoupled, high-concurrency event streaming pipeline** built on Java 21 Loom, Apache Kafka in KRaft mode, Redis 7, PostgreSQL 16 with `pgvector`, and Spring AI.

```mermaid
flowchart TD
    subgraph ThirdParty["1. Inbound Webhook Provider"]
        P1["Provider Event\n(Stripe, GitHub, Shopify)"]
    end

    subgraph IngressGateway["2. Ingestion Gateway (<15ms SLA)"]
        IG1["WebhookIngressController\n(Java 21 Virtual Thread)"]
        IG2["TenantSecretCacheService\n(Redis L1 Cache <1ms)"]
        IG3["HmacSignatureVerifier\n(Constant-Time Byte Comparison)"]
        IG4["RateLimitingService\n(Redis Token Bucket per Tenant)"]
        IG5["Kafka Inbound Producer\n(Zstandard Compression, acks=all)"]
        Ack["HTTP 202 Accepted\nX-HookShield-Delivery-ID\nX-Ingestion-Latency-Ms"]
    end

    subgraph KafkaBroker["3. Apache Kafka KRaft Event Pipeline"]
        K_RAW[("webhook-inbound-raw")]
        K_DISP[("webhook-dispatch-queue")]
        K_R1[("webhook-retry-1m")]
        K_R2[("webhook-retry-5m")]
        K_R3[("webhook-retry-15m")]
        K_DLQ[("webhook-dead-letter-queue")]
    end

    subgraph DedupTier["4. Inbound Deduplication & Audit Tier"]
        IC1["InboundConsumer\n(Loom Worker)"]
        IC2["IdempotencyService\nRedis SET NX EX 86400"]
        IC3[("PostgreSQL 16\nwebhook_deliveries\nStatus: RECEIVED")]
    end

    subgraph DispatchTier["5. Outbound Dispatch & Circuit Breaking"]
        DC1["DispatchConsumer\n(Loom Worker)"]
        DC2["WebhookDispatcherService\n(Spring RestClient)"]
        DC3{"Resilience4j\nCircuit Breaker\n(per host)"}
        Sub[("Downstream Subscriber\nTarget Service")]
    end

    subgraph RetryTier["6. Non-Blocking Retry Pipeline"]
        RC1["RetryConsumers\n(Scheduled Virtual Threads)"]
    end

    subgraph AiDlqTier["7. Spring AI & pgvector Triage Hub"]
        DLQ1["DlqConsumer"]
        DLQ2["AiDiagnosticService"]
        DLQ3["Spring AI ChatClient\n(Zero-Shot LLM Reasoning)"]
        DLQ4["EmbeddingModel\n(1536-dim Unit Vector)"]
        DLQ5[("PostgreSQL pgvector\nerror_clusters (IVFFlat)\ndlq_incident_reports")]
        DLQ6["DlqReplayService\n(1-Click & Auto Replay)"]
    end

    P1 -->|POST Webhook Body + Sig Header| IG1
    IG1 <-->|1. Fetch Cached Secret| IG2
    IG1 -->|2. Verify Signature| IG3
    IG1 -->|3. Consume Rate Token| IG4
    IG1 -->|4. Produce Raw Event| IG5
    IG5 -->|5. Fast Return| Ack
    IG5 --> K_RAW

    K_RAW --> IC1
    IC1 <-->|Check & Lock Hash| IC2
    IC1 -->|Persist Audit Record| IC3
    IC1 -->|Publish Valid Delivery| K_DISP

    K_DISP --> DC1
    DC1 --> DC2
    DC2 <--> DC3
    DC3 -->|HTTP POST Payload| Sub

    DC2 -->|2xx Success| IC3
    DC2 -->|Failure / Circuit Open| K_R1

    K_R1 --> RC1
    RC1 -->|Attempt 1 Delay Over| DC2
    RC1 -->|Attempt 1 Failed| K_R2
    K_R2 --> RC1
    RC1 -->|Attempt 2 Delay Over| DC2
    RC1 -->|Attempt 2 Failed| K_R3
    K_R3 --> RC1
    RC1 -->|Attempt 3 Delay Over| DC2
    RC1 -->|Max Retries Exhausted (Attempt >=3)| K_DLQ

    K_DLQ --> DLQ1
    DLQ1 --> DLQ2
    DLQ2 <--> DLQ3
    DLQ2 <--> DLQ4
    DLQ2 <--> DLQ5
    DLQ5 --> DLQ6
    DLQ6 -.->|Replay Payload| K_DISP
```

---

### 2.1 The Request Lifecycle Step-by-Step

```
[Inbound Request] (T = 0.0ms)
       │
       ▼
 1. Header Extraction: Extracts Signature, Tenant ID, and Provider Name.
       │
       ▼
 2. Secret Lookup: Reads cached tenant HMAC secret from Redis in <0.8ms.
       │
       ▼
 3. Cryptographic Verification: Constant-time HMAC-SHA256 byte comparison.
       │
       ▼
 4. Rate Limiting: Evaluates Redis Token Bucket for tenant quota in <0.5ms.
       │
       ▼
 5. Kafka Publish: Asynchronously writes payload to 'webhook-inbound-raw'.
       │
       ▼
 6. Fast-Ack Response: Returns HTTP 202 Accepted (Latency: 2 - 10ms, SLA < 15ms).
       │
═══════╪══════════════════════════════════════════════════════════════════════════
       │ (Asynchronous Decoupled Processing Pipeline)
       ▼
 7. Inbound Consumer: Pulls message from 'webhook-inbound-raw'.
       │
       ▼
 8. Atomic Dedup Lock: Computes SHA-256(tenantId + provider + payload + eventId).
    Executes Redis `SET idemp:lock:{hash} "LOCKED" NX EX 86400`.
    - If duplicate: Immediately commits offset and stops further processing.
    - If new: Inserts audit record into PostgreSQL `webhook_deliveries` (status: RECEIVED).
       │
       ▼
 9. Dispatch Queue: Forwards delivery ID and payload to 'webhook-dispatch-queue'.
       │
       ▼
10. Virtual Thread Worker: Picks up message from 'webhook-dispatch-queue'.
       │
       ▼
11. Circuit Breaker Evaluation: Checks Resilience4j status for target host.
    - If Closed: Executes HTTP POST via Spring RestClient on a Loom Virtual Thread.
    - If Open: Bypasses network call immediately, jumping straight to Retry Tier.
       │
       ├─────────────────────────────────┬─────────────────────────────────┐
       ▼ (HTTP 200 OK)                   ▼ (Network Error / 5xx Outage)    ▼ (Circuit Open)
12. Success Path:                 13. Transient Failure Path:       14. Fast-Fail Path:
    - Status -> DISPATCHED            - Status -> RETRYING              - Status -> RETRYING
    - Dispatched timestamp set        - Publishes to `webhook-retry-1m` - Publishes to `webhook-retry-1m`
    - Finished.                       - Scheduled delay on Loom worker  - Prevents network timeout.
                                         │
                                         ▼
                                  15. Retry Escalation:
                                      - Attempt 1: webhook-retry-1m (60s delay)
                                      - Attempt 2: webhook-retry-5m (300s delay)
                                      - Attempt 3: webhook-retry-15m (900s delay)
                                         │
                                         ▼ (Max Retries Exhausted >= 3)
                                  16. Dead-Letter Queue:
                                      - Status -> DEAD_LETTER
                                      - Publishes to `webhook-dead-letter-queue`
                                         │
                                         ▼
                                  17. AI & pgvector Triage:
                                      - Extracts error signature & payload excerpt
                                      - Generates 1536-dimensional unit embedding vector
                                      - Searches PostgreSQL `error_clusters` using `<=>` cosine distance
                                      - If distance <= 0.12: Increments existing cluster occurrences
                                      - If distance > 0.12: LLM classifies root cause & creates new cluster
                                      - Stores incident report linked to cluster
                                         │
                                         ▼
                                  18. Safe DLQ Replay:
                                      - 1-Click bulk replay via Dashboard or automated cooldown replay.
```

---

## 3. Deep Dive into Core System Components

---

### Component 1: High-Speed Ingress Gateway Controller
- **Source File**: [`WebhookIngressController.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/controller/WebhookIngressController.java)
- **Role**: Public-facing edge API terminating third-party HTTP POST webhook traffic.
- **Key Characteristics**:
  - **Zero Heavy Computation**: Performs only authentication, rate-limiting, and an asynchronous Kafka produce before returning.
  - **Strict Latency Budget**: Guarantees `<15ms` response time by avoiding synchronous database transactions or external network calls.
  - **Dynamic Provider Routing**: Path variables `{tenantId}` and `{provider}` allow multi-tenant multiplexing across Stripe, GitHub, Shopify, and custom protocols.
  - **Response Headers**:
    - `X-HookShield-Delivery-ID`: UUID assigned to track the delivery across all downstream stages.
    - `X-Ingestion-Latency-Ms`: Millisecond precision timing measurement proving SLA adherence.

---

### Component 2: Cryptographic HMAC Signature Verifier
- **Source File**: [`HmacSignatureVerifier.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/security/HmacSignatureVerifier.java)
- **Role**: Validates payload authenticity and integrity before allowing webhooks into the pipeline.
- **Key Characteristics**:
  - **Constant-Time Comparison**: Uses Java's `MessageDigest.isEqual(byte[] a, byte[] b)` to prevent timing side-channel attacks where an attacker could deduce bytes based on microsecond latency variations.
  - **Multi-Provider Header Normalization**:
    - **Stripe**: Parses `t=1712000000,v1=abc123...` format; computes `HMAC-SHA256(secret, t + "." + payload)`.
    - **GitHub**: Parses `sha256=abc123...` format; computes `HMAC-SHA256(secret, payload)`.
    - **Shopify**: Parses Base64-encoded `X-Shopify-Hmac-Sha256` header.
    - **Generic**: Standard hex-encoded HMAC-SHA256.

---

### Component 3: Redis Token-Bucket Rate Limiting Engine
- **Source File**: [`RateLimitingService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/RateLimitingService.java)
- **Role**: Protects internal infrastructure from traffic spikes and enforces contractual tenant quotas.
- **Key Characteristics**:
  - **Redisson RRateLimiter**: Uses distributed atomic token-bucket algorithms running directly inside Redis memory.
  - **Burst Capacity Handling**: Allows configurable burst capacity above standard steady-state requests per second (e.g. 100 req/s with a 200 token burst).
  - **Fast Rejection**: Rejects unauthorized surges with `HTTP 429 Too Many Requests` and standard `Retry-After: 1` headers without consuming thread or database resources.

---

### Component 4: High-Performance Tenant Secret Cache
- **Source File**: [`TenantSecretCacheService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/TenantSecretCacheService.java)
- **Role**: Eliminates relational database queries on the critical ingestion path.
- **Key Characteristics**:
  - **Multi-Tier Caching**: Cached in Redis with key `tenant:secret:{tenantId}:{provider}` and a 1-hour TTL.
  - **Cache Miss Fallback**: On a cache miss, safely queries PostgreSQL table `tenant_provider_configs` and populates Redis.
  - **Sub-Millisecond Retrieval**: Delivers secrets to the HMAC verifier in `<0.8ms`.

---

### Component 5: Inbound Kafka Consumer & Atomic Deduplication Engine
- **Source Files**: [`InboundConsumer.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/consumer/InboundConsumer.java), [`IdempotencyService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/IdempotencyService.java)
- **Role**: Consumes from `webhook-inbound-raw`, guarantees exactly-once processing, and records the delivery in PostgreSQL.
- **Key Characteristics**:
  - **Composite Idempotency Hash**:
    $$\text{Hash} = \text{SHA256}(\text{TenantID} \,\|\, \text{Provider} \,\|\, \text{RawPayload} \,\|\, \text{ExternalEventID})$$
  - **Atomic Distributed Lock**: Executes `SET idemp:lock:{hash} "LOCKED" NX EX 86400` in Redis.
    - If key exists: Identifies duplicate retry, logs warning, increments Micrometer metric `webhooks.deduplicated.count`, and commits Kafka offset without downstream re-dispatch.
    - If key is new: Inserts delivery row into PostgreSQL with status `RECEIVED`, payload JSON, and passes delivery to `webhook-dispatch-queue`.

---

### Component 6: Virtual Thread Outbound Dispatcher
- **Source Files**: [`DispatchConsumer.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/consumer/DispatchConsumer.java), [`WebhookDispatcherService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/WebhookDispatcherService.java)
- **Role**: Dispatches webhook payloads to subscriber target endpoints.
- **Key Characteristics**:
  - **Java 21 Project Loom Concurrency**: Outbound calls execute on lightweight Virtual Threads (`spring.threads.virtual.enabled=true`). When waiting for downstream network I/O, the virtual thread is unmounted from carrier worker threads, allowing tens of thousands of concurrent dispatches without OS thread starvation.
  - **HTTP Delivery Execution**: Uses Spring `RestClient` with configured connect (5s) and read timeouts (10s).
  - **State Transition**: On HTTP 2xx, updates PostgreSQL status to `DISPATCHED`.

---

### Component 7: Resilience4j Dynamic Host-Level Circuit Breakers
- **Source File**: [`ResilienceConfig.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/config/ResilienceConfig.java)
- **Role**: Prevents cascading failures and protects struggling subscriber servers.
- **Key Characteristics**:
  - **Per-Host Granularity**: Circuit breakers are dynamically instantiated per downstream hostname (e.g. `api.merchant-a.com` vs `api.merchant-b.com`).
  - **Sliding Window Metrics**: Evaluates a sliding window of 20 requests with a 50% failure rate threshold.
  - **Fast-Fail Diversion**: If a host's circuit breaker opens, subsequent dispatches to that host skip network requests entirely and immediately transition to the non-blocking retry pipeline, giving the downstream server time to recover.

---

### Component 8: Non-Blocking Multi-Tier Delayed Retry Pipeline
- **Source File**: [`RetryConsumers.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/consumer/RetryConsumers.java)
- **Role**: Handles transient subscriber failures without blocking Kafka consumer loops.
- **Key Characteristics**:
  - **Dedicated Delayed Topics**:
    - Tier 1: `webhook-retry-1m` (60-second backoff)
    - Tier 2: `webhook-retry-5m` (300-second backoff)
    - Tier 3: `webhook-retry-15m` (900-second backoff)
  - **Target Epoch Scheduling**: Published messages include an `X-Retry-Next-Epoch-Ms` header. The consumer computes `delayRemaining = nextEpoch - now`. If delay is pending, the message is scheduled using a Virtual Thread scheduler while the Kafka consumer continues processing other messages.
  - **Zero Thread Stalls**: Unlike traditional `Thread.sleep` loops, Kafka consumer poll loops are never stalled, preventing broker partition rebalances.
  - **DLQ Escalation**: If attempt $\ge 3$ fails, delivery is transitioned to `DEAD_LETTER` and published to `webhook-dead-letter-queue`.

---

### Component 9: Spring AI & PostgreSQL pgvector Semantic Triage Hub
- **Source Files**: [`DlqConsumer.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/consumer/DlqConsumer.java), [`AiDiagnosticService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/AiDiagnosticService.java)
- **Role**: Automatically diagnoses, categorizes, and clusters dead-letter queue failures.
- **Key Characteristics**:
  - **Failure Signature Formulation**: Combines target endpoint, HTTP status code, exception message, and sanitized payload excerpt.
  - **1536-Dimensional Vector Embedding**: Generates unit vector embeddings using Spring AI `EmbeddingModel`.
  - **Native PostgreSQL pgvector Matching**:
    ```sql
    SELECT id, cluster_name, failure_category, total_occurrences,
           (signature_embedding <=> :embedding) AS distance
    FROM error_clusters
    WHERE (signature_embedding <=> :embedding) <= 0.12
    ORDER BY distance ASC
    LIMIT 1;
    ```
  - **Cosine Distance Math**:
    $$\text{dist}(u, v) = 1 - \frac{u \cdot v}{\|u\|_2 \|v\|_2}$$
    A distance $\le 0.12$ corresponds to **$\ge 88\%$ semantic similarity**. Any error matching an existing pattern increments `total_occurrences` and links an incident report without re-querying the LLM.
  - **Zero-Shot LLM Reasoning**: For entirely new error signatures (distance $> 0.12$), Spring AI `ChatClient` categorizes the failure into:
    - `EPHEMERAL_NETWORK`: Transient 503, DNS blip, connection timeout (Safe to auto-replay).
    - `AUTH_FAILURE`: 401/403 Invalid API keys or expired tokens (Manual remediation needed).
    - `SCHEMA_MISMATCH`: 400/422 Payload schema change or missing field (Engineering fix needed).
    - `INTERNAL_SERVER_ERROR`: 500 downstream application bug.

---

### Component 10: DLQ Safe Replay Engine & Webhook Studio
- **Source Files**: [`DlqReplayService.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/service/DlqReplayService.java), [`DashboardController.java`](file:///Users/msri/Documents/Trash%20from%20me/HookShield/src/main/java/com/hookshield/controller/DashboardController.java)
- **Role**: Provides operators with safe 1-click or automated cooling replays and interactive testing.
- **Key Characteristics**:
  - **Cluster-Level Replay**: Replays hundreds of correlated failures with a single click once the downstream service is back online.
  - **Idempotency Protection on Replay**: Re-enqueues payloads to `webhook-dispatch-queue` with updated idempotency context to prevent duplicates while preserving original delivery lineage.
  - **Operator Webhook Studio**: Allows testing real Stripe, GitHub, and Shopify webhooks with live HMAC computation and real-time execution telemetry.

---

## 4. Delivery State Machine

A delivery progresses through strict states recorded in PostgreSQL table `webhook_deliveries`:

```
                 ┌────────────────────────────────────────────────────────┐
                 │                                                        │
                 ▼                                                        │
         ┌───────────────┐                                                │
         │   RECEIVED    │                                                │
         └───────┬───────┘                                                │
                 │                                                        │
                 ▼                                                        │
        ┌─────────────────┐                                               │
        │   DISPATCHING   │                                               │
        └────────┬────────┘                                               │
                 │                                                        │
        ┌────────┴────────┐                                               │
        │                 │                                               │
        ▼                 ▼                                               │
┌───────────────┐ ┌───────────────┐                                       │
│  DISPATCHED   │ │   RETRYING    │ ──(Attempt 1 -> 2 -> 3)               │
│   (HTTP 2xx)  │ └───────┬───────┘                                       │
└───────────────┘         │                                               │
                          ▼ (Retries Exhausted >= 3)                      │
                  ┌───────────────┐                                       │
                  │  DEAD_LETTER  │                                       │
                  └───────┬───────┘                                       │
                          │                                               │
                          ▼ (Manual or Automated Replay Triggered)        │
                  ┌───────────────┐                                       │
                  │   REPLAYED    │ ──────────────────────────────────────┘
                  └───────────────┘
```

---

## 5. Architectural Trade-Off Analysis

| Architectural Decision | Chosen Solution | Evaluated Alternative | Engineering Rationale |
| :--- | :--- | :--- | :--- |
| **Concurrency Runtime** | **Java 21 Virtual Threads (Loom)** | Reactive WebFlux (Netty) | Virtual threads allow standard imperative Java code, synchronous JPA queries, and straightforward stack traces while achieving millions of concurrent I/O operations without reactive operator complexity. |
| **Streaming Message Broker** | **Apache Kafka (KRaft)** | RabbitMQ / AWS SQS | Kafka provides partition-level ordering, distributed log immutability, high throughput under spikes, and non-blocking multi-topic retry architectures without ZooKeeper. |
| **Vector Storage Engine** | **PostgreSQL `pgvector`** | Standalone Vector DB (Pinecone, Qdrant) | Eliminates dual-write distributed transaction bugs by keeping delivery audit logs and vector embeddings in the same relational database with ACID guarantees. |
| **Distributed Idempotency** | **Redis Redisson `SET NX`** | Database Unique Constraints | Redis atomic locking resolves in $<1\text{ms}$ in-memory without consuming database connection pool slots in HikariCP. |
| **Error Triage** | **Spring AI Hybrid (Vector + LLM)** | Pure Regex Pattern Matching | Regex fails when downstream error messages change slightly. Combining 1536-dim vector similarity with zero-shot LLM reasoning catches novel errors while deduplicating recurring ones. |

---

## 6. Complete REST API Reference

### Ingestion & Testing Endpoints
- `POST /api/v1/webhooks/ingest/{tenantId}/{provider}`
  - Receives live third-party webhook.
  - Headers: Provider-specific HMAC signature header (e.g. `Stripe-Signature`, `X-Hub-Signature-256`, `X-Shopify-Hmac-Sha256`).
  - Response: `HTTP 202 Accepted` with `X-HookShield-Delivery-ID` and `X-Ingestion-Latency-Ms`.
- `POST /api/v1/webhooks/mock-subscriber`
  - Built-in mock downstream receiver supporting status codes (e.g. `?status=503` or `?status=200`).

### Operator Control Center Endpoints
- `GET /api/v1/dashboard/overview`
  - Returns total deliveries, dispatched count, retry count, DLQ count, SLA adherence %, average latency ms, and component health.
- `GET /api/v1/dashboard/deliveries?page=0&size=25&status={STATUS}`
  - Paginated audit stream with tenant name, target endpoint, retry count, and formatted JSON payload.
- `GET /api/v1/dashboard/tenants`
  - Active tenant accounts, configured providers, token-bucket rate limits, and burst capacity.
- `POST /api/v1/dashboard/simulate`
  - Dispatches interactive simulation payload, computes cryptographic HMAC signature, tests ingestion, and returns microsecond timing trace.

### AI Dead-Letter Queue & Replay Endpoints
- `GET /api/v1/dlq/clusters`
  - Returns grouped semantic failure clusters with occurrence counts, failure categories, and AI remediation advice.
- `GET /api/v1/dlq/incidents?clusterId={id}`
  - Returns individual incident deliveries assigned to a specific semantic cluster.
- `POST /api/v1/dlq/replay`
  - Body: `{"clusterId": "..."}` or `{"deliveryIds": ["..."]}`.
  - Replays target deliveries back to the dispatch queue.

### Infrastructure & Health Endpoints
- `GET /actuator/health` — Spring Boot Actuator status (PostgreSQL, Redis, Kafka, Virtual Threads).
- `GET /actuator/prometheus` — Metrics for Prometheus and Grafana.

---

## 7. Verification & Runbook

### Running the Application
```bash
# Start Docker infrastructure (PostgreSQL with pgvector, Redis, Kafka KRaft)
docker compose up -d

# Start HookShield with Virtual Threads enabled
java -Dhookshield.ai.mock-enabled=true -jar target/hookshield-1.0.0-SNAPSHOT.jar
```

### Accessing the Web Command Hub
Open **`http://localhost:8080/`** in your browser to access the HookShield Command Hub.
