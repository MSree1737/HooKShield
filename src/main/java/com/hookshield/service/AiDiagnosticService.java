package com.hookshield.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hookshield.domain.entity.DlqIncidentReport;
import com.hookshield.domain.entity.ErrorCluster;
import com.hookshield.domain.entity.WebhookDelivery;
import com.hookshield.domain.enums.FailureCategory;
import com.hookshield.domain.record.FailureDiagnosis;
import com.hookshield.domain.record.WebhookIngestPayload;
import com.hookshield.repository.DlqIncidentReportRepository;
import com.hookshield.repository.ErrorClusterRepository;
import com.hookshield.repository.WebhookDeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * AI-driven failure triage service utilizing Spring AI (ChatClient & EmbeddingModel)
 * and PostgreSQL pgvector for semantic error clustering and root-cause analysis.
 */
@Service
public class AiDiagnosticService {

    private static final Logger log = LoggerFactory.getLogger(AiDiagnosticService.class);

    private final ChatClient chatClient;
    private final EmbeddingModel embeddingModel;
    private final ErrorClusterRepository clusterRepository;
    private final DlqIncidentReportRepository incidentReportRepository;
    private final WebhookDeliveryRepository deliveryRepository;
    private final DlqReplayService dlqReplayService;
    private final MetricsService metricsService;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    @Value("${hookshield.ai.similarity-threshold:0.12}")
    private double similarityThreshold;

    @Value("${hookshield.ai.auto-replay-cooling-minutes:30}")
    private int autoReplayCoolingMinutes;

    public AiDiagnosticService(ChatClient chatClient,
                                EmbeddingModel embeddingModel,
                                ErrorClusterRepository clusterRepository,
                                DlqIncidentReportRepository incidentReportRepository,
                                WebhookDeliveryRepository deliveryRepository,
                                DlqReplayService dlqReplayService,
                                MetricsService metricsService,
                                ObjectMapper objectMapper) {
        this.chatClient = chatClient;
        this.embeddingModel = embeddingModel;
        this.clusterRepository = clusterRepository;
        this.incidentReportRepository = incidentReportRepository;
        this.deliveryRepository = deliveryRepository;
        this.dlqReplayService = dlqReplayService;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
    }

    /**
     * Triage dead-lettered webhook by embedding error signature, finding/creating pgvector clusters,
     * diagnosing root-cause via Spring AI LLM, and scheduling automated replay if safe.
     */
    @Transactional
    public DlqIncidentReport triageFailure(WebhookIngestPayload payload, String rawErrorMessage, Integer statusCode) {
        log.info("Starting AI diagnostic triage for failed delivery {}", payload.deliveryId());

        String errorSignature = buildErrorSignature(payload.targetEndpoint(), rawErrorMessage, payload.rawPayload());
        float[] embedding = embeddingModel.embed(errorSignature);
        String vectorString = formatVectorAsString(embedding);

        // 1. Search existing semantic clusters in pgvector via cosine distance (<=> operator)
        Optional<ErrorCluster> matchedCluster = Optional.empty();
        try {
            matchedCluster = clusterRepository.findClosestClusterWithinDistance(vectorString, similarityThreshold);
        } catch (Exception e) {
            log.warn("pgvector query failed (possibly uninitialized index or mock DB): {}", e.getMessage());
        }

        ErrorCluster cluster;
        FailureDiagnosis diagnosis;

        if (matchedCluster.isPresent()) {
            cluster = matchedCluster.get();
            cluster.incrementOccurrences();
            clusterRepository.save(cluster);
            log.info("Matched delivery {} to existing error cluster '{}' (occurrences: {})",
                payload.deliveryId(), cluster.getClusterName(), cluster.getTotalOccurrences());

            diagnosis = new FailureDiagnosis(
                cluster.getFailureCategory(),
                85,
                cluster.isSafeToAutoReplay(),
                "Grouped with cluster: " + cluster.getClusterName(),
                cluster.getRecommendedAction()
            );
        } else {
            // 2. Perform zero-shot LLM Root Cause Diagnosis with Spring AI
            diagnosis = performLlmDiagnosis(payload, rawErrorMessage, statusCode);

            String clusterName = "Cluster [" + diagnosis.failureCategory() + "] - " +
                truncate(rawErrorMessage != null ? rawErrorMessage : "Unknown Error", 80);

            cluster = new ErrorCluster(
                clusterName,
                diagnosis.failureCategory(),
                vectorString,
                diagnosis.recommendedRemediation(),
                diagnosis.isSafeToAutoReplay()
            );
            try {
                cluster = clusterRepository.save(cluster);
                log.info("Created new semantic error cluster '{}' for category {}",
                    cluster.getClusterName(), cluster.getFailureCategory());
            } catch (Exception e) {
                log.error("Failed to save new error cluster: {}", e.getMessage());
            }
        }

        // 3. Persist incident report
        WebhookDelivery delivery = deliveryRepository.findById(payload.deliveryId()).orElse(null);
        if (delivery == null) {
            log.warn("Webhook delivery record {} not found in database; skipping incident report link", payload.deliveryId());
            return null;
        }

        DlqIncidentReport incident = new DlqIncidentReport(
            delivery,
            cluster,
            statusCode,
            rawErrorMessage,
            diagnosis.rootCauseAnalysis(),
            diagnosis.confidenceScore()
        );
        incident = incidentReportRepository.save(incident);

        // Track observability metric
        metricsService.incrementDlqRouted(diagnosis.failureCategory().name());

        // 4. Automated Replay: if diagnosed as safe (e.g. transient network/503), schedule automated retry
        if (diagnosis.isSafeToAutoReplay()) {
            scheduleAutoReplay(payload.deliveryId());
        }

        return incident;
    }

    /**
     * Executes structured ChatClient diagnosis using BeanOutputConverter.
     */
    public FailureDiagnosis performLlmDiagnosis(WebhookIngestPayload payload, String rawErrorMessage, Integer statusCode) {
        BeanOutputConverter<FailureDiagnosis> outputConverter =
            new BeanOutputConverter<>(FailureDiagnosis.class, objectMapper);

        String systemPrompt = """
            You are an expert Principal Reliability Engineer analyzing dead-lettered webhook failures.
            Analyze the HTTP status code, target endpoint, error message, and payload excerpt.
            Classify into: EPHEMERAL_NETWORK, SCHEMA_MISMATCH, AUTH_FAILURE, INTERNAL_SERVER_ERROR, or LOGIC_ERROR.
            Determine if safe to auto-replay (e.g., transient network hiccup or temporary 503 is safe; 401/403 or bad schema is unsafe).
            Provide a root cause analysis and recommended remediation.
            Respond strictly using the specified JSON schema format.
            """ + outputConverter.getFormat();

        String userPrompt = String.format("""
            Failure Context:
            - Delivery ID: %s
            - Target Endpoint: %s
            - HTTP Status Code: %s
            - Error Message: %s
            - Payload Excerpt: %s
            """,
            payload.deliveryId(),
            payload.targetEndpoint(),
            statusCode != null ? statusCode : "N/A",
            rawErrorMessage != null ? rawErrorMessage : "None",
            truncate(payload.rawPayload(), 250)
        );

        try {
            String content = chatClient.prompt()
                .system(systemPrompt)
                .user(userPrompt)
                .call()
                .content();

            return outputConverter.convert(content);
        } catch (Exception e) {
            log.warn("LLM diagnosis call failed or returned unparseable content: {}. Using rule-based fallback.", e.getMessage());
            return ruleBasedDiagnosisFallback(rawErrorMessage, statusCode);
        }
    }

    private FailureDiagnosis ruleBasedDiagnosisFallback(String errorMessage, Integer statusCode) {
        String msg = (errorMessage != null ? errorMessage : "").toLowerCase();
        int code = statusCode != null ? statusCode : 500;

        if (code == 503 || code == 504 || msg.contains("timeout") || msg.contains("connection refused")) {
            return new FailureDiagnosis(
                FailureCategory.EPHEMERAL_NETWORK,
                90,
                true,
                "Transient network timeout or upstream gateway unavailable.",
                "Wait for upstream subscriber recovery and re-dispatch."
            );
        } else if (code == 401 || code == 403 || msg.contains("unauthorized") || msg.contains("forbidden")) {
            return new FailureDiagnosis(
                FailureCategory.AUTH_FAILURE,
                95,
                false,
                "Subscriber endpoint rejected credentials or authorization token.",
                "Verify and update authentication secret in subscriber settings."
            );
        } else if (code == 400 || code == 422 || msg.contains("schema") || msg.contains("json")) {
            return new FailureDiagnosis(
                FailureCategory.SCHEMA_MISMATCH,
                88,
                false,
                "Inbound payload structure violated downstream subscriber expectations.",
                "Review downstream API contract schema and update webhook mapping."
            );
        } else {
            return new FailureDiagnosis(
                FailureCategory.INTERNAL_SERVER_ERROR,
                80,
                true,
                "Downstream subscriber server error (HTTP " + code + ").",
                "Check subscriber server logs and replay once service is restored."
            );
        }
    }

    private void scheduleAutoReplay(UUID deliveryId) {
        log.info("Scheduling automated auto-replay for delivery {} in {} minutes", deliveryId, autoReplayCoolingMinutes);
        scheduledExecutor.schedule(() -> {
            try {
                log.info("Executing scheduled cooling auto-replay for delivery {}", deliveryId);
                dlqReplayService.replayDeliveries(null, null, java.util.List.of(deliveryId));
            } catch (Exception e) {
                log.error("Failed scheduled auto-replay for delivery {}: {}", deliveryId, e.getMessage());
            }
        }, autoReplayCoolingMinutes, TimeUnit.MINUTES);
    }

    public static String buildErrorSignature(String targetEndpoint, String rawErrorMessage, String payload) {
        return "Endpoint: " + (targetEndpoint != null ? targetEndpoint : "") + "\n" +
            "Error: " + (rawErrorMessage != null ? rawErrorMessage : "") + "\n" +
            "PayloadExcerpt: " + truncate(payload, 300);
    }

    public static String formatVectorAsString(float[] vector) {
        if (vector == null) return "[]";
        StringBuilder sb = new StringBuilder(vector.length * 8 + 2);
        sb.append('[');
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(vector[i]);
        }
        sb.append(']');
        return sb.toString();
    }

    private static String truncate(String text, int maxLength) {
        if (text == null) return "";
        return text.length() <= maxLength ? text : text.substring(0, maxLength) + "...";
    }
}
