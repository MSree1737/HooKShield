package com.hookshield.domain.record;

import java.util.UUID;

/**
 * Result of interactive webhook ingestion simulation.
 */
public record SimulationResponse(
    boolean success,
    UUID deliveryId,
    long latencyMs,
    String signature,
    String headerName,
    String status,
    String message
) {}
