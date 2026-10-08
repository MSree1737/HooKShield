package com.hookshield.domain.record;

import com.hookshield.domain.enums.FailureCategory;

import java.time.Instant;
import java.util.UUID;

/**
 * Summary view of an error cluster for management and triage dashboards.
 */
public record ClusterSummaryResponse(
    UUID id,
    String clusterName,
    FailureCategory failureCategory,
    int totalOccurrences,
    boolean isSafeToAutoReplay,
    String recommendedAction,
    Instant createdAt,
    Instant updatedAt
) {}
