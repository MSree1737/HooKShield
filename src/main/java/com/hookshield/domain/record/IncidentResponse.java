package com.hookshield.domain.record;

import java.time.Instant;
import java.util.UUID;

/**
 * Detailed view of an individual dead-letter incident report.
 */
public record IncidentResponse(
    UUID id,
    UUID deliveryId,
    UUID clusterId,
    Integer httpStatusCode,
    String rawErrorMessage,
    String aiRootCauseAnalysis,
    Integer aiConfidenceScore,
    boolean resolved,
    Instant createdAt
) {}
