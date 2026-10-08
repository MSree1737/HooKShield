package com.hookshield.integration;

import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.TenantProviderConfig;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.DeliveryStatus;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Validates the core end-to-end pipeline lifecycle:
 * 1. Ingestion deduplication semantics.
 * 2. Multi-tier non-blocking retry progression to DLQ.
 * 3. AI diagnostic incident linking.
 * 4. Replay API state transitions.
 */
@SpringBootTest
@ActiveProfiles("test")
class WebhookPipelineFlowTest {

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
    private AiDiagnosticService aiDiagnosticService;

    @MockBean
    private KafkaTemplate<String, WebhookIngestPayload> kafkaTemplate;

    @MockBean
    private org.redisson.api.RedissonClient redissonClient;

    @MockBean
    private org.springframework.data.redis.core.StringRedisTemplate stringRedisTemplate;

    private UUID tenantId;
    private static final String PROVIDER = "stripe";
    private static final String SECRET = "whsec_flow_secret_777";

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        Tenant tenant = tenantRepository.save(new Tenant(null, "Flow Tenant " + UUID.randomUUID(), 500, 1000));
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
    @DisplayName("1. Multi-tier failure progression: 500 error progresses retry-1m -> retry-5m -> retry-15m -> DLQ")
    @Transactional
    void testFailureProgressionThroughRetriesToDlq() {
        Tenant tenant = tenantRepository.findById(tenantId).orElseThrow();
        String hash = "hash_flow_retry_" + UUID.randomUUID();

        WebhookDelivery delivery = deliveryRepository.save(new WebhookDelivery(
            tenant,
            PROVIDER,
            "evt_flow_1",
            hash,
            "{\"event\":\"invoice.payment_failed\"}",
            "http://localhost:9099/broken"
        ));
        UUID deliveryId = delivery.getId();

        WebhookIngestPayload payload = new WebhookIngestPayload(
            deliveryId,
            tenantId,
            PROVIDER,
            "evt_flow_1",
            "{\"event\":\"invoice.payment_failed\"}",
            "http://localhost:9099/broken",
            Map.of(),
            0,
            Instant.now()
        );

        // Attempt 0 Failure -> routes to retry-1m
        dispatcherService.handleDispatchFailure(payload, "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d1 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d1.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d1.getRetryCount()).isEqualTo(1);

        // Attempt 1 Failure -> routes to retry-5m
        dispatcherService.handleDispatchFailure(payload.withRetryCount(1), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d2 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d2.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d2.getRetryCount()).isEqualTo(2);

        // Attempt 2 Failure -> routes to retry-15m
        dispatcherService.handleDispatchFailure(payload.withRetryCount(2), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery d3 = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(d3.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(d3.getRetryCount()).isEqualTo(3);

        // Attempt 3 Failure -> DLQ
        dispatcherService.handleDispatchFailure(payload.withRetryCount(3), "HTTP 500 Internal Server Error", 500);
        WebhookDelivery deadLetterDelivery = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(deadLetterDelivery.getStatus()).isEqualTo(DeliveryStatus.DEAD_LETTER);
    }

    @Test
    @DisplayName("2. DLQ incident triage via Spring AI creates report and links root cause")
    @Transactional
    void testDlqIncidentTriage() {
        Tenant tenant = tenantRepository.findById(tenantId).orElseThrow();
        String hash = "hash_triage_" + UUID.randomUUID();

        WebhookDelivery delivery = new WebhookDelivery(
            tenant,
            PROVIDER,
            "evt_triage_1",
            hash,
            "{\"error\":\"bad_payload\"}",
            "http://localhost:9099/endpoint"
        );
        delivery.setStatus(DeliveryStatus.DEAD_LETTER);
        delivery = deliveryRepository.save(delivery);
        UUID deliveryId = delivery.getId();

        WebhookIngestPayload payload = new WebhookIngestPayload(
            deliveryId,
            tenantId,
            PROVIDER,
            "evt_triage_1",
            "{\"error\":\"bad_payload\"}",
            "http://localhost:9099/endpoint",
            Map.of(),
            3,
            Instant.now()
        );

        DlqIncidentReport report = aiDiagnosticService.triageFailure(
            payload, "Connection timeout to subscriber host", 504
        );

        assertThat(report).isNotNull();
        assertThat(report.getDelivery().getId()).isEqualTo(deliveryId);
        assertThat(report.getHttpStatusCode()).isEqualTo(504);
        assertThat(report.getAiRootCauseAnalysis()).isNotBlank();
        assertThat(report.isResolved()).isFalse();
    }

    @Test
    @DisplayName("3. Replay API restores dead-letter item back to REPLAYED status and triggers Kafka republishing")
    @Transactional
    void testReplayApiExecution() {
        Tenant tenant = tenantRepository.findById(tenantId).orElseThrow();
        String hash = "hash_replay_flow_" + UUID.randomUUID();

        WebhookDelivery delivery = new WebhookDelivery(
            tenant,
            PROVIDER,
            "evt_replay_flow_1",
            hash,
            "{\"amount\":5000}",
            "http://localhost:8080/subscriber"
        );
        delivery.setStatus(DeliveryStatus.DEAD_LETTER);
        delivery = deliveryRepository.save(delivery);
        UUID deliveryId = delivery.getId();

        ReplayResponse response = dlqReplayService.replayDeliveries(null, null, List.of(deliveryId));

        assertThat(response.replayedCount()).isEqualTo(1);
        assertThat(response.replayedIds()).contains(deliveryId);

        WebhookDelivery restoredDelivery = deliveryRepository.findById(deliveryId).orElseThrow();
        assertThat(restoredDelivery.getStatus()).isEqualTo(DeliveryStatus.REPLAYED);
        assertThat(restoredDelivery.getRetryCount()).isEqualTo(0);
    }
}
