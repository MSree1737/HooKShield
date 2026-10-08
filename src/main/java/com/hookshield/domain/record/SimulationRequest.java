package com.hookshield.domain.record;

import java.util.UUID;

/**
 * Interactive simulation parameters for webhook testing.
 */
public record SimulationRequest(
    UUID tenantId,
    String provider,
    String targetEndpoint,
    String payload,
    Integer simulateStatusCode
) {}
