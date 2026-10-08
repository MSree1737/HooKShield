package com.hookshield.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hookshield.config.SpringAiConfig;
import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.ErrorCluster;
import com.hookshield.domain.entity.Tenant;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.FailureCategory;
import com.hookshield.domain.record.FailureDiagnosis;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.ErrorClusterRepository;
import com.hookshield.repository.WebhookDeliveryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.embedding.EmbeddingModel;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AiDiagnosticServiceTest {

    @Mock
    private ChatClient chatClient;

    @Mock
    private EmbeddingModel embeddingModel;

    @Mock
    private ErrorClusterRepository clusterRepository;

    @Mock
    private DlqIncidentReportRepository incidentReportRepository;

    @Mock
    private WebhookDeliveryRepository deliveryRepository;

    @Mock
    private DlqReplayService dlqReplayService;

    @Mock
    private MetricsService metricsService;

    private ObjectMapper objectMapper;
    private AiDiagnosticService diagnosticService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        diagnosticService = new AiDiagnosticService(
            chatClient,
            embeddingModel,
            clusterRepository,
            incidentReportRepository,
            deliveryRepository,
            dlqReplayService,
            metricsService,
            objectMapper
        );
    }

    @Test
    @DisplayName("Should associate with existing error cluster when pgvector similarity matches")
    void testExistingClusterAssociation() {
        UUID deliveryId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        WebhookIngestPayload payload = new WebhookIngestPayload(
            deliveryId, tenantId, "STRIPE", "evt_1", "{}", "https://example.com", Map.of(), 3, Instant.now()
        );

        float[] mockVector = new float[]{0.1f, 0.2f, 0.3f};
        when(embeddingModel.embed(anyString())).thenReturn(mockVector);

        ErrorCluster existingCluster = new ErrorCluster(
            "Cluster [EPHEMERAL_NETWORK] - Connection reset",
            FailureCategory.EPHEMERAL_NETWORK,
            "[0.1,0.2,0.3]",
            "Wait for recovery",
            true
        );
        when(clusterRepository.findClosestClusterWithinDistance(anyString(), anyDouble()))
            .thenReturn(Optional.of(existingCluster));

        Tenant tenant = new Tenant(tenantId, "Tenant 1", 500, 1000);
        WebhookDelivery delivery = new WebhookDelivery(
            tenant, "STRIPE", "evt_1", "hash1", "{}", "https://example.com"
        );
        delivery.setId(deliveryId);
        when(deliveryRepository.findById(deliveryId)).thenReturn(Optional.of(delivery));

        when(incidentReportRepository.save(any(DlqIncidentReport.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        DlqIncidentReport report = diagnosticService.triageFailure(payload, "Connection reset by peer", 503);

        assertThat(report).isNotNull();
        assertThat(existingCluster.getTotalOccurrences()).isEqualTo(2);
        verify(clusterRepository).save(existingCluster);
        verify(metricsService).incrementDlqRouted("EPHEMERAL_NETWORK");
    }

    @Test
    @DisplayName("Should generate 1536-dimensional deterministic embedding vectors")
    void testDeterministicVectorGeneration() {
        float[] vector = SpringAiConfig.generateDeterministicVector("test-error-signature", 1536);

        assertThat(vector).hasSize(1536);
        // Verify vector is normalized (unit vector length ~ 1.0)
        double norm = 0.0;
        for (float v : vector) {
            norm += v * v;
        }
        assertThat(Math.sqrt(norm)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("Should accurately categorize HTTP 503 transient timeout in rule fallback")
    void testFallbackTransientNetwork() {
        WebhookIngestPayload payload = new WebhookIngestPayload(
            UUID.randomUUID(), UUID.randomUUID(), "STRIPE", "evt_1", "{}", "https://api.example.com", Map.of(), 3, Instant.now()
        );

        FailureDiagnosis diagnosis = diagnosticService.performLlmDiagnosis(payload, "Gateway Timeout 504", 504);

        assertThat(diagnosis.failureCategory()).isEqualTo(FailureCategory.EPHEMERAL_NETWORK);
        assertThat(diagnosis.isSafeToAutoReplay()).isTrue();
    }

    @Test
    @DisplayName("Should categorize HTTP 401 unauthorized as AUTH_FAILURE and unsafe to auto-replay")
    void testFallbackAuthFailure() {
        WebhookIngestPayload payload = new WebhookIngestPayload(
            UUID.randomUUID(), UUID.randomUUID(), "STRIPE", "evt_1", "{}", "https://api.example.com", Map.of(), 3, Instant.now()
        );

        FailureDiagnosis diagnosis = diagnosticService.performLlmDiagnosis(payload, "Unauthorized: bad token", 401);

        assertThat(diagnosis.failureCategory()).isEqualTo(FailureCategory.AUTH_FAILURE);
        assertThat(diagnosis.isSafeToAutoReplay()).isFalse();
    }
}
