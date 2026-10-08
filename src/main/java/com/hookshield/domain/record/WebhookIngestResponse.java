package com.hookshield.domain.record;

import java.time.Instant;
import java.util.UUID;

/**
 * Fast-ack response returned immediately upon successful ingestion and Kafka dispatch.
 */
public record WebhookIngestResponse(
    UUID deliveryId,
    String status,
    String message,
    Instant receivedAt
) {
    public static WebhookIngestResponse accepted(UUID deliveryId) {
        return new WebhookIngestResponse(
            deliveryId,
            "ACCEPTED",
            "Webhook successfully queued for asynchronous ingestion and dispatch",
            Instant.now()
        );
    }
}
