package com.hookshield.domain.record;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Internal and Kafka transfer record representing an ingested webhook event.
 */
public record WebhookIngestPayload(
    UUID deliveryId,
    UUID tenantId,
    String provider,
    String externalEventId,
    String rawPayload,
    String targetEndpoint,
    Map<String, String> headers,
    int retryCount,
    Instant receivedAt
) {
    public WebhookIngestPayload withRetryCount(int newRetryCount) {
        return new WebhookIngestPayload(
            deliveryId,
            tenantId,
            provider,
            externalEventId,
            rawPayload,
            targetEndpoint,
            headers,
            newRetryCount,
            receivedAt
        );
    }
}
