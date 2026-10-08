package com.hookshield.domain.record;

import java.util.Map;

/**
 * High-level system telemetry and delivery health overview.
 */
public record OverviewStatsResponse(
    long totalDeliveries,
    long dispatchedCount,
    long retryingCount,
    long deadLetterCount,
    long replayedCount,
    long totalClusters,
    double slaAdherencePct,
    double avgIngestionLatencyMs,
    Map<String, String> systemHealth
) {}
