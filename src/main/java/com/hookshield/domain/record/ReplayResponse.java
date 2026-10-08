package com.hookshield.domain.record;

import java.util.List;
import java.util.UUID;

/**
 * Summary response returned after triggering DLQ replays.
 */
public record ReplayResponse(
    int replayedCount,
    String message,
    List<UUID> replayedIds
) {}
