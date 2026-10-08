package com.hookshield.domain.enums;

/**
 * Lifecycle status of an ingested webhook delivery event.
 */
public enum DeliveryStatus {
    RECEIVED,
    DISPATCHED,
    RETRYING,
    DEAD_LETTER,
    REPLAYED
}
