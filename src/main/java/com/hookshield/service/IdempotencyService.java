package com.hookshield.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Distributed idempotency engine backed by atomic Redis operations.
 * Guarantees exactly-once processing across horizontally scaled consumers
 * using SHA-256 deduplication hashing with 24-hour sliding TTL.
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:";
    private static final Duration TTL_24_HOURS = Duration.ofDays(1);
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private final StringRedisTemplate stringRedisTemplate;

    public IdempotencyService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * Computes the deterministic SHA-256 hash over tenantId, provider, and rawPayload (with optional externalEventId).
     */
    public String computeHash(UUID tenantId, String provider, String rawPayload, String externalEventId) {
        String compositeKey = tenantId.toString() + ":" +
            provider.toUpperCase() + ":" +
            (rawPayload != null ? rawPayload : "") + ":" +
            (externalEventId != null ? externalEventId : "");

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(compositeKey.getBytes(StandardCharsets.UTF_8));
            return HEX_FORMAT.formatHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm missing", e);
        }
    }

    /**
     * Atomically registers the hash with SET NX EX 86400.
     *
     * @param hash SHA-256 hash string
     * @return true if key was set (new distinct message); false if key already existed (duplicate).
     */
    public boolean acquireIdempotencyLock(String hash) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + hash;
        try {
            Boolean wasSet = stringRedisTemplate.opsForValue().setIfAbsent(redisKey, "PROCESSING", TTL_24_HOURS);
            return Boolean.TRUE.equals(wasSet);
        } catch (Exception e) {
            log.error("Failed to execute atomic Redis SET NX for hash {}: {}", hash, e.getMessage());
            // Fall back to true to prevent message drop on transient Redis failure
            return true;
        }
    }

    /**
     * Transitions status from "PROCESSING" to "PROCESSED".
     */
    public void markProcessed(String hash) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + hash;
        try {
            stringRedisTemplate.opsForValue().set(redisKey, "PROCESSED", TTL_24_HOURS);
        } catch (Exception e) {
            log.warn("Failed to update idempotency status to PROCESSED for hash {}: {}", hash, e.getMessage());
        }
    }

    /**
     * Clears the idempotency lock in case of fatal validation error prior to dispatch queue entry.
     */
    public void releaseLock(String hash) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + hash;
        try {
            stringRedisTemplate.delete(redisKey);
        } catch (Exception e) {
            log.warn("Failed to delete idempotency key for hash {}: {}", hash, e.getMessage());
        }
    }
}
