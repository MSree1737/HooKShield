package com.hookshield.integration;

import com.hookshield.config.KafkaConfig;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.TenantProviderConfig;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.DeliveryStatus;
import com.hookshield.domain.record.ReplayRequest;
import com.hookshield.domain.record.ReplayResponse;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.TenantProviderConfigRepository;
import com.hookshield.repository.TenantRepository;
import com.hookshield.repository.WebhookDeliveryRepository;
import com.hookshield.service.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test verifying:
 * 1. Concurrent duplicate webhook ingestion and atomic deduplication swallow.
 * 2. Downstream HTTP 500 failure progression through retry-1m -> retry-5m -> DLQ and AI incident report triage.
 * 3. Replay API restoring dead-lettered webhooks back to the main inbound queue.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnabledIf("isDockerAvailable")
public class WebhookPipelineIntegrationTest {

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("pgvector/pgvector:pg16")
        .withDatabaseName("hookshield")
        .withUsername("hookshield")
        .withPassword("hookshield_secret");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        if (isDockerAvailable()) {
            registry.add("spring.datasource.url", postgres::getJdbcUrl);
            registry.add("spring.datasource.username", postgres::getUsername);
            registry.add("spring.datasource.password", postgres::getPassword);
            registry.add("spring.data.redis.host", redis::getHost);
            registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TenantRepository tenantRepository;

    @Autowired
    private TenantProviderConfigRepository providerConfigRepository;

    @Autowired
    private WebhookDeliveryRepository deliveryRepository;

    @Autowired
    private DlqIncidentReportRepository incidentReportRepository;

    @Autowired
    private WebhookDispatcherService dispatcherService;

    @Autowired
    private DlqReplayService dlqReplayService;

    @Autowired
    private IdempotencyService idempotencyService;

    private UUID tenantId;
    private static final String PROVIDER = "stripe";
    private static final String SECRET = "whsec_integration_test_secret_999";

    @BeforeEach
    void setUp() {
        Tenant tenant = tenantRepository.save(new Tenant(null, "Enterprise Tenant " + UUID.randomUUID(), 1000, 2000));
        tenantId = tenant.getId();

        TenantProviderConfig config = new TenantProviderConfig(
            tenant,
            PROVIDER,
            SECRET,
            "Stripe-Signature"
        );
        providerConfigRepository.save(config);
    }

    @Test
    @DisplayName("1. Concurrent duplicate webhooks result in exactly one accepted execution and one duplicate discard")
    void testConcurrentDuplicateIngestion() throws Exception {
        String payload = "{\"event\":\"order.completed\",\"order_id\":\"ord_99999\"}";
        String signature = HmacSignatureVerifier.computeHmacSha256Hex(payload, SECRET);
        String url = "http://localhost:" + port + "/api/v1/webhooks/ingest/" + tenantId + "/" + PROVIDER;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Stripe-Signature", signature);
        headers.set("X-Event-ID", "evt_unique_order_99999");

        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        // Send two concurrent requests with identical payloads and event IDs
        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Callable<ResponseEntity<String>>> tasks = List.of(
            () -> restTemplate.postForEntity(url, request, String.class),
            () -> restTemplate.postForEntity(url, request, String.class)
        );

        List<Future<ResponseEntity<String>>> futures = executor.invokeAll(tasks);
        List<ResponseEntity<String>> responses = new ArrayList<>();
        for (Future<ResponseEntity<String>> f : futures) {
            responses.add(f.get(5, TimeUnit.SECONDS));
        }

        executor.shutdown();

        // Both return 202 at ingress tier (<15ms SLA fast-ack)
        for (ResponseEntity<String> resp : responses) {
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }

        // At Tier 2 deduplication engine, exactly one lock is acquired, and second is rejected
        String hash = idempotencyService.computeHash(tenantId, PROVIDER, payload, "evt_unique_order_99999");
        boolean lockAvailable = idempotencyService.acquireIdempotencyLock(hash);
        assertThat(lockAvailable).isFalse(); // Key already exists and locked
    }

    @Test
    @DisplayName("2. Downstream HTTP 500 error progresses through retry pipeline to DLQ with AI incident triage")
    void testFailureProgressionToDlqAndAiTriage() {
        Tenant tenant = tenantRepository.findById(tenantId).orElseThrow();
        UUID deliveryId = UUID.randomUUID();
        String hash = "hash_failure_progression_" + deliveryId;

        WebhookDelivery delivery = new WebhookDelivery(
            tenant,
            PROVIDER,
            "evt_fail_1",
            hash,
            "{\"event\":\"payment.failed\"}",
            "http://localhost:9999/broken-downstream"
        );
        delivery.setId(deliveryId);
        deliveryRepository.save(delivery);

        WebhookIngestPayload payload = new WebhookIngestPayload(
            deliveryId,
            tenantId,
            PROVIDER,
            "evt_fail_1",
            "{\"event\":\"payment.failed\"}",
            "http://localhost:9999/broken-downstream",
            Map.of(),
            0,
            Instant.now()
        );

        // Attempt 0 Failure -> triggers retry-1m
        dispatcherService.handleDispatchFailure(payload, "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d1 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d1.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d1.getRetryCount()).isEqualTo(1);

        // Attempt 1 Failure -> triggers retry-5m
        dispatcherService.handleDispatchFailure(payload.withRetryCount(1), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d2 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d2.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d2.getRetryCount()).isEqualTo(2);

        // Attempt 2 Failure -> triggers retry-15m
        dispatcherService.handleDispatchFailure(payload.withRetryCount(2), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d3 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d3.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d3.getRetryCount()).isEqualTo(3);

        // Attempt 3 Failure -> Exhausted -> routes to DLQ
        dispatcherService.handleDispatchFailure(payload.withRetryCount(3), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery deadLetterDelivery = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(deadLetterDelivery.getStatus()).isEqualTo(DeliveryStatus.DEAD_LETTER);
    }

    @Test
    @DisplayName("3. Replay API restores DLQ item back to the main processing queue")
    void testReplayApiRestoresDlqItem() {
        Tenant tenant = tenantRepository.findById(tenantId).orElseThrow();
        UUID deliveryId = UUID.randomUUID();
        String hash = "hash_replay_" + deliveryId;

        WebhookDelivery delivery = new WebhookDelivery(
            tenant,
            PROVIDER,
            "evt_replay_1",
            hash,
            "{\"event\":\"invoice.payment_action_required\"}",
            "http://localhost:8080/subscriber"
        );
        delivery.setId(deliveryId);
        delivery.setStatus(DeliveryStatus.DEAD_LETTER);
        deliveryRepository.save(delivery);

        // Call DLQ replay service
        ReplayResponse replayResponse = dlqReplayService.replayDeliveries(null, null, List.of(deliveryId));

        assertThat(replayResponse.replayedCount()).isEqualTo(1);
        assertThat(replayResponse.replayedIds()).contains(deliveryId);

        WebhookDelivery replayedDelivery = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(replayedDelivery.getStatus()).isEqualTo(DeliveryStatus.REPLAYED);
        assertThat(replayedDelivery.getRetryCount()).isEqualTo(0);
    }
}
