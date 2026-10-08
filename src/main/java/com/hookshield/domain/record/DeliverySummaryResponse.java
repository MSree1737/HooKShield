package com.hookshield.domain.record;

import java.time.Instant;
import java.util.UUID;

/**
 * Summary DTO of a webhook delivery for the audit log explorer.
 */
public record DeliverySummaryResponse(
    UUID id,
    UUID tenantId,
    String tenantName,
    String provider,
    String externalEventId,
    String idempotencyHash,
    String status,
    int retryCount,
    String targetEndpoint,
    String payload,
    Instant createdAt,
    Instant updatedAt
) {}
