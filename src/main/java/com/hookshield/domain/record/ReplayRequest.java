package com.hookshield.domain.record;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Request payload for bulk DLQ replay operations.
 */
public record ReplayRequest(
    UUID clusterId,
    UUID tenantId,
    List<UUID> deliveryIds,
    Instant startTime,
    Instant endTime
) {}
