package com.hookshield.domain.record;

import com.hookshield.domain.enums.FailureCategory;

/**
 * Structured diagnostic output produced by Spring AI ChatClient for DLQ triage.
 */
public record FailureDiagnosis(
    FailureCategory failureCategory,
    int confidenceScore,
    boolean isSafeToAutoReplay,
    String rootCauseAnalysis,
    String recommendedRemediation
) {
    public FailureDiagnosis {
        if (confidenceScore < 0 || confidenceScore > 100) {
            throw new IllegalArgumentException("confidenceScore must be between 0 and 100, got: " + confidenceScore);
        }
    }
}
