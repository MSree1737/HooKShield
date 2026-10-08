package com.hookshield.domain.enums;

/**
 * Root-cause classification for dead-lettered webhook incidents.
 * Analyzed and classified semantically by Spring AI and pgvector clustering.
 */
public enum FailureCategory {
    EPHEMERAL_NETWORK,
    SCHEMA_MISMATCH,
    AUTH_FAILURE,
    AUTH_REVOKED,
    INTERNAL_SERVER_ERROR,
    LOGIC_ERROR
}
